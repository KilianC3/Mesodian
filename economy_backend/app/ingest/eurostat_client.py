import asyncio
import datetime as dt
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import httpx
import yaml
from sqlalchemy.orm import Session

from app.db.models import RawEurostat
from app.ingest.sample_mode import (
    SampleConfig,
    validate_timeseries_data,
    IngestionError,
)
from app.ingest.utils import bulk_upsert_timeseries, ensure_date, resolve_indicator_id, store_raw_payload

logger = logging.getLogger(__name__)


# ISO2 to ISO3 country code mapping for Eurostat
# Eurostat uses 2-letter codes (ISO 3166-1 alpha-2)
ISO2_TO_ISO3 = {
    "DE": "DEU", "FR": "FRA", "IT": "ITA", "ES": "ESP", "NL": "NLD",
    "BE": "BEL", "AT": "AUT", "GR": "GRC", "PL": "POL", "CZ": "CZE",
    "HU": "HUN", "RO": "ROU", "IE": "IRL", "PT": "PRT", "SE": "SWE",
    "DK": "DNK", "FI": "FIN", "SK": "SVK", "BG": "BGR", "HR": "HRV",
    "LT": "LTU", "LV": "LVA", "SI": "SVN", "EE": "EST", "LU": "LUX",
    "CY": "CYP", "MT": "MLT", "GB": "GBR", "NO": "NOR", "CH": "CHE",
    "IS": "ISL", "TR": "TUR",
}


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
EUROSTAT_CONFIG = CATALOG.get("EUROSTAT", {}) if isinstance(CATALOG, dict) else {}

EUROSTAT_SERIES: Dict[str, Dict[str, Any]] = {
    "prc_hicp_manr": {
        "canonical_indicator": "HICP_YOY",
        "frequency": "M",
        "countries": ["DE", "FR", "IT", "ES", "NL", "BE", "AT", "GR", "PL", "CZ", "HU", "RO", "IE"],
    },
    "namq_10_gdp": {
        "canonical_indicator": "GDP_GROWTH_QOQ",
        "frequency": "Q",
        "countries": ["DE", "FR", "IT", "ES", "NL", "BE"],
    },
}


async def fetch_series(dataset: str, country: str, *, sample_config: Optional[SampleConfig] = None) -> Dict[str, Any]:
    """
    Fetch EUROSTAT data using dissemination API.
    
    API docs: https://ec.europa.eu/eurostat/web/json-and-unicode-web-services/getting-started/rest-request
    Pattern: /api/dissemination/statistics/1.0/data/{dataset}?geo={country}&...
    """
    sample_config = sample_config or SampleConfig()
    
    # Use correct dissemination API endpoint
    base_url = EUROSTAT_CONFIG.get("base_url", "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data")
    url = f"{base_url}/{dataset}"
    
    # Load dataset-specific params from catalog
    datasets_config = EUROSTAT_CONFIG.get("datasets", {})
    dataset_params = datasets_config.get(dataset, {}).get("params", {})
    
    params = {
        "geo": country,
        "format": "JSON",
        "lang": "en"
    }
    params.update(dataset_params)
    
    # Add last N observations filter for sample mode
    if sample_config.enabled:
        params["lastTimePeriod"] = str(sample_config.max_records_per_country)
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            raise IngestionError("EUROSTAT", country, f"HTTP {e.response.status_code}: {e}")


def parse_eurostat(
    payload: Dict[str, Any],
    *,
    indicator_id: int,
    country_id: str,
    source: str,
    sample_config: Optional[SampleConfig] = None,
) -> List[Dict[str, Any]]:
    sample_config = sample_config or SampleConfig()
    
    if not payload or (isinstance(payload, dict) and not payload):
        if sample_config.fail_on_empty:
            raise IngestionError("EUROSTAT", country_id, "Empty payload")
        return []
    
    value_map = payload.get("value", {}) if isinstance(payload, dict) else {}
    dimension = payload.get("dimension", {}) if isinstance(payload, dict) else {}
    time_category = dimension.get("time", {}).get("category", {})
    time_index = time_category.get("index", {})
    time_labels = time_category.get("label", {})
    
    # Create reverse mapping: index -> time_period
    index_to_period = {str(idx): period for period, idx in time_index.items()}
    
    rows: List[Dict[str, Any]] = []
    for idx_str, value in value_map.items():
        try:
            # Look up the time period from the index
            time_period = index_to_period.get(str(idx_str))
            if time_period is None:
                # Fallback: maybe idx_str is already the time period
                time_period = idx_str
            date = ensure_date(time_period)
            numeric_value = float(value)
        except Exception as exc:  # pragma: no cover - data dependent
            logger.error("Failed to parse Eurostat point %s: %s", idx_str, exc)
            if sample_config.strict_validation:
                raise IngestionError("EUROSTAT", country_id, f"Parse error: {exc}")
            continue
        rows.append(
            {
                "indicator_id": indicator_id,
                "country_id": country_id,
                "date": date,
                "value": numeric_value,
                "source": source,
                "ingested_at": dt.datetime.now(dt.timezone.utc),
            }
        )
    
    # Limit records in sample mode BEFORE validation
    if sample_config.enabled and sample_config.max_records_per_country:
        rows = rows[:sample_config.max_records_per_country]
    
    if sample_config.enabled and rows:
        validation = validate_timeseries_data(rows, expected_countries=[country_id], sample_config=sample_config)
        if sample_config.strict_validation:
            validation.raise_if_invalid()
    
    return rows


def ingest_full(
    session: Session,
    *,
    country_subset: Optional[Iterable[str]] = None,
    series_subset: Optional[Iterable[str]] = None,
    sample_config: Optional[SampleConfig] = None,
) -> None:
    sample_config = sample_config or SampleConfig()
    selected_series = set(series_subset) if series_subset else None
    selected_countries = set(country_subset) if country_subset else None

    series_to_fetch = list(EUROSTAT_SERIES.items())
    if selected_series:
        series_to_fetch = [(k, v) for k, v in series_to_fetch if k in selected_series]
    if sample_config.enabled:
        series_to_fetch = series_to_fetch[:2]

    async def _run() -> None:
        for dataset_code, cfg in series_to_fetch:
            if selected_series and dataset_code not in selected_series:
                continue
            indicator_id = resolve_indicator_id(session, cfg["canonical_indicator"])
            for country_iso2 in cfg.get("countries", []):
                if selected_countries and country_iso2 not in selected_countries:
                    continue
                # Convert ISO2 to ISO3 for database storage
                country_iso3 = ISO2_TO_ISO3.get(country_iso2, country_iso2)
                try:
                    payload = await fetch_series(dataset_code, country_iso2)
                    store_raw_payload(
                        session,
                        RawEurostat,
                        params={"dataset": dataset_code, "country": country_iso2},
                        payload=payload,
                    )
                    rows = parse_eurostat(
                        payload,
                        indicator_id=indicator_id,
                        country_id=country_iso3,  # Use ISO3 for database
                        source="EUROSTAT",
                        sample_config=sample_config,
                    )
                    bulk_upsert_timeseries(session, rows)
                    logger.info(f"EUROSTAT: Ingested {len(rows)} records for {dataset_code}/{country_iso3}")
                except IngestionError:
                    raise
                except Exception as e:
                    logger.error(f"EUROSTAT: Failed for {dataset_code}/{country_iso2}: {e}")
                    if sample_config.strict_validation:
                        raise IngestionError("EUROSTAT", country_iso3, f"Failed for {dataset_code}: {e}")

    asyncio.run(_run())
    session.commit()

