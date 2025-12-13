import datetime as dt
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import yaml
from sqlalchemy.orm import Session

from app.config.country_universe import COUNTRY_UNIVERSE
from app.db.models import RawBis
from app.ingest.base_client import fetch_sdmx_dataset
from app.ingest.sample_mode import (
    SampleConfig,
    validate_timeseries_data,
    IngestionError,
)
from app.ingest.utils import (
    bulk_upsert_timeseries,
    parse_timeseries_rows,
    resolve_indicator_id,
    store_raw_payload,
)

logger = logging.getLogger(__name__)


def _load_catalog() -> Dict[str, Any]:
    catalog_path = Path(__file__).resolve().parents[2] / "config" / "catalogs" / "providers.yaml"
    if not catalog_path.exists():
        return {}
    try:
        with open(catalog_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


CATALOG = _load_catalog()
BIS_CONFIG = CATALOG.get("BIS", {}) if isinstance(CATALOG, dict) else {}

# BIS Statistics API
# NOTE: As of Jan 2025, stats.bis.org v1 API is operational:
# - API v1 (https://stats.bis.org/api/v1/data) returns 200 OK with SDMX XML
# - API v2 (https://stats.bis.org/api/v2) returns 501 (Not Implemented - future release)
# Sample working endpoint: https://stats.bis.org/api/v1/data/WS_CBPOL?c=US&firstNObservations=5
# Documentation: https://stats.bis.org/
BIS_BASE_URL = BIS_CONFIG.get("base_url", "https://stats.bis.org/api/v1/data")

BIS_SERIES: Dict[str, Dict[str, Any]] = {
    "WS_CBPOL": {
        "canonical_indicator": "BIS_POLICY_RATE",
        "countries": ["USA", "GBR", "DEU", "FRA", "JPN", "CHN", "BRA", "AUS"],
    },
    # NOTE: WS_CREDIT and WS_DSR datasets return 404 as of Dec 2025
    # These may have been deprecated or moved to different dataset codes
    # TODO: Research current BIS dataset codes for credit and debt service data
    # "WS_CREDIT": {
    #     "canonical_indicator": "BIS_CREDIT_PRIVATE_PCT_GDP",
    #     "countries": ["USA", "GBR", "DEU", "FRA", "JPN", "CHN", "BRA"],
    # },
    # "WS_DSR": {
    #     "canonical_indicator": "BIS_DEBT_SERVICE_RATIO",
    #     "countries": ["USA", "GBR", "DEU", "FRA", "JPN"],
    # },
}

# BIS uses ISO-2 country codes, but we use ISO-3
# Mapping from ISO-3 (our standard) to ISO-2 (BIS format)
ISO3_TO_ISO2 = {
    "USA": "US", "GBR": "GB", "DEU": "DE", "FRA": "FR", "JPN": "JP",
    "CHN": "CN", "BRA": "BR", "AUS": "AU", "ITA": "IT", "ESP": "ES",
    "CAN": "CA", "MEX": "MX", "IND": "IN", "RUS": "RU", "KOR": "KR",
    "TUR": "TR", "SAU": "SA", "ZAF": "ZA", "ARG": "AR", "IDN": "ID",
}

# Reverse mapping for parsing XML responses
ISO2_TO_ISO3 = {v: k for k, v in ISO3_TO_ISO2.items()}


def ingest_full(
    session: Session,
    *,
    dataset_subset: Optional[Iterable[str]] = None,
    country_subset: Optional[Iterable[str]] = None,
    sample_config: Optional[SampleConfig] = None,
) -> None:
    sample_config = sample_config or SampleConfig()
    selected_datasets = set(dataset_subset) if dataset_subset else None
    selected_countries = set(country_subset) if country_subset else None

    series_to_fetch = list(BIS_SERIES.items())
    if selected_datasets:
        series_to_fetch = [(k, v) for k, v in series_to_fetch if k in selected_datasets]
    if sample_config.enabled:
        series_to_fetch = series_to_fetch[:2]

    for dataset_code, cfg in series_to_fetch:
        if selected_datasets and dataset_code not in selected_datasets:
            continue
        indicator_id = resolve_indicator_id(session, cfg["canonical_indicator"])
        countries = cfg.get("countries", COUNTRY_UNIVERSE)
        for country_id in countries:
            if selected_countries and country_id not in selected_countries:
                continue
            try:
                # BIS uses structure-specific SDMX XML without DSD, which pandasdmx cannot parse
                # Parse XML directly to extract time series observations
                
                # Convert ISO-3 (our format) to ISO-2 (BIS format) for API request
                bis_country_code = ISO3_TO_ISO2.get(country_id, country_id)
                
                url = f"{BIS_BASE_URL}/{dataset_code}"
                params_dict = {"c": bis_country_code}
                if sample_config.enabled:
                    params_dict["firstNObservations"] = str(sample_config.max_records_per_country)
                
                import httpx
                import pandas as pd
                from lxml import etree
                from io import BytesIO
                
                response = httpx.get(url, params=params_dict, timeout=30.0)
                response.raise_for_status()
                
                # Parse XML and extract observations
                root = etree.parse(BytesIO(response.content))
                
                # Find all Series elements (each Series = one country's time series)
                namespaces = {'ns': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/structurespecific'}
                series_elements = root.xpath('//ns:Series', namespaces=namespaces)
                
                if not series_elements:
                    # Try without namespace
                    series_elements = root.xpath('//Series')
                
                observations = []
                for series in series_elements:
                    # Get country code from Series attributes (BIS uses ISO-2)
                    series_country_iso2 = series.get('REF_AREA', '').upper()
                    # Convert BIS ISO-2 to our ISO-3 format for comparison
                    series_country_iso3 = ISO2_TO_ISO3.get(series_country_iso2, series_country_iso2)
                    
                    if series_country_iso3 != country_id.upper():
                        continue  # Skip series not matching our country
                    
                    # Extract all Obs elements within this Series
                    obs_elements = series.xpath('.//ns:Obs', namespaces=namespaces)
                    if not obs_elements:
                        obs_elements = series.xpath('.//Obs')
                    
                    for obs in obs_elements:
                        time_period = obs.get('TIME_PERIOD', '')
                        obs_value = obs.get('OBS_VALUE', '')
                        if time_period and obs_value and obs_value.lower() != 'nan':
                            try:
                                observations.append({
                                    'time': time_period,
                                    'value': float(obs_value)
                                })
                            except ValueError:
                                continue
                
                if not observations:
                    raise ValueError(f"No valid observations found in BIS response for {country_id}")
                
                df = pd.DataFrame(observations)
                
                if sample_config.enabled and len(df) > sample_config.max_records_per_country:
                    df = df.tail(sample_config.max_records_per_country)
                payload_df = df.copy()
                if hasattr(payload_df, "columns"):
                    for column in payload_df.columns:
                        payload_df[column] = payload_df[column].apply(
                            lambda v: v.isoformat() if isinstance(v, dt.date) else v
                        )
                store_raw_payload(
                    session,
                    RawBis,
                    params={"dataset": dataset_code, "country": country_id},
                    payload=payload_df.to_dict() if hasattr(payload_df, "to_dict") else None,
                )
                rows = parse_timeseries_rows(
                    df,
                    indicator_id=indicator_id,
                    country_id=country_id,
                    source="BIS",
                )
                # BIS datasets may not carry a source date; stamp ingestion time
                for row in rows:
                    row.setdefault("ingested_at", dt.datetime.now(dt.timezone.utc))
                
                # Deduplicate rows by (indicator_id, country_id, date) to prevent cardinality violations
                # Keep the last occurrence (most recent value) for each unique key
                seen = {}
                for row in rows:
                    key = (row["indicator_id"], row["country_id"], row["date"])
                    seen[key] = row
                rows = list(seen.values())
                
                if sample_config.enabled and rows:
                    validation = validate_timeseries_data(rows, expected_countries=[country_id], sample_config=sample_config)
                    if sample_config.strict_validation:
                        validation.raise_if_invalid()
                
                bulk_upsert_timeseries(session, rows)
                logger.info(f"BIS: Ingested {len(rows)} records for {dataset_code}/{country_id}")
            except IngestionError:
                raise
            except Exception as e:
                logger.error(f"BIS: Failed for {dataset_code}/{country_id}: {e}")
                if sample_config.strict_validation:
                    raise IngestionError("BIS", country_id, f"Failed for {dataset_code}: {e}")

    session.commit()

