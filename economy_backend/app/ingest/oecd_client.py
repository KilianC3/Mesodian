import asyncio
import datetime as dt
import logging
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy.orm import Session

from app.db.models import RawOecd
from app.ingest.base_client import fetch_sdmx_dataset
from app.ingest.sample_mode import (
    SampleConfig,
    validate_timeseries_data,
    IngestionError,
)
from app.ingest.utils import bulk_upsert_timeseries, ensure_date, resolve_indicator_id, store_raw_payload

logger = logging.getLogger(__name__)


def get_oecd_dataflows() -> str:
    """
    Fetch list of all available OECD dataflows.
    Returns raw XML response from /dataflow endpoint.
    
    Use this for discovery when adding new series.
    """
    import requests
    url = f"{OECD_BASE_URL}/dataflow"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.text


def get_oecd_contentconstraint(agency_id: str, dataset_id: str) -> str:
    """
    Check contentconstraint for a dataset to detect version changes.
    
    Args:
        agency_id: Usually "OECD" for OECD datasets
        dataset_id: Dataset/dataflow ID (e.g., "QNA", "PRICES_CPI")
    
    Returns:
        Raw XML response with constraint information
    
    Use this before large ingestions to avoid re-ingesting unchanged data.
    """
    import requests
    url = f"{OECD_BASE_URL}/contentconstraint/{agency_id}/CR_A_{dataset_id}/"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.text


# OECD SDMX API endpoint
# Updated December 2025: OECD uses SDMX 2.1 REST API at sdmx.oecd.org
# Base URL: https://sdmx.oecd.org/public/rest
# Documentation: https://sdmx.oecd.org/ and OECD Data Explorer
# 
# CRITICAL: OECD changed dataflow ID format from "PRICES_CPI" to "DSD_PRICES@DF_PRICES_ALL"
# Key format: REF_AREA.dim2.dim3.dim4.dim5.dim6.dim7.dim8 (8 dimensions total)
# Use dots (.) for wildcard dimensions
#
# Key endpoints:
# - /dataflow - list all 1450+ available dataflows
# - /data/{dataflow}/{key}?params - fetch data
# 
# Working example:
# /data/DSD_PRICES@DF_PRICES_ALL/USA........?startPeriod=2020&endPeriod=2023
OECD_BASE_URL = "https://sdmx.oecd.org/public/rest"

# OECD dataflows with new DSD_XXX@DF_YYY format (verified 2025-12-11)
OECD_SERIES: Dict[str, Dict[str, Any]] = {
    # Consumer Price Index - All items
    "PRICES_ALL": {
        "canonical_indicator": "CPI_YOY_OECD",
        "dataflow": "DSD_PRICES@DF_PRICES_ALL",
        "frequency": "M",
        "countries": ["USA", "CAN", "GBR", "FRA", "DEU", "JPN", "ITA", "ESP"],
        # Key format: REF_AREA........  (country + 7 wildcards for 8 total dimensions)
    },
    # Quarterly National Accounts - GDP
    "QNA": {
        "canonical_indicator": "GDP_QUARTERLY_OECD",
        "dataflow": "DSD_NAMAIN1@DF_QNA",
        "frequency": "Q",
        "countries": ["USA", "CAN", "GBR", "FRA", "DEU", "JPN", "ITA", "ESP"],
        # Key format: REF_AREA........  (country + 7 wildcards for 8 total dimensions)
    },
}


def parse_oecd(
    df: Any,
    *,
    indicator_id: int,
    country_id: str,
    source: str,
    sample_config: Optional[SampleConfig] = None,
) -> List[Dict[str, Any]]:
    sample_config = sample_config or SampleConfig()
    rows: List[Dict[str, Any]] = []
    if df is None or (hasattr(df, 'empty') and df.empty):
        if sample_config.fail_on_empty:
            raise IngestionError("OECD", country_id, "Empty dataframe")
        return rows
    
    # Track seen dates to deduplicate (wildcard queries return multiple series)
    seen_dates = {}
    
    for _, row in df.iterrows():  # type: ignore[call-arg]
        location = row.get("LOCATION") or row.get("geo")
        if location and str(location).upper() != country_id.upper():
            continue
        time_value = row.get("time")
        value = row.get("value")
        if time_value is None or value is None:
            continue
        try:
            date = ensure_date(time_value)
            numeric_value = float(value)
        except Exception as exc:  # pragma: no cover - data dependent
            logger.error("Skipping OECD row %s: %s", row, exc)
            if sample_config.strict_validation:
                raise IngestionError("OECD", country_id, f"Parse error: {exc}")
            continue
        
        # Deduplicate: keep first value per date (or average if you prefer)
        if date not in seen_dates:
            seen_dates[date] = numeric_value
            rows.append(
                {
                    "indicator_id": indicator_id,
                    "country_id": country_id,
                    "date": date,
                    "value": numeric_value,
                    "source": source,
                    "ingested_at": dt.datetime.utcnow(),
                }
            )
    
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
    selected_countries = set(country_subset) if country_subset else None
    selected_series = set(series_subset) if series_subset else None

    series_to_fetch = list(OECD_SERIES.items())
    if selected_series:
        series_to_fetch = [(k, v) for k, v in series_to_fetch if k in selected_series]
    if sample_config.enabled:
        series_to_fetch = series_to_fetch[:2]

    async def _run() -> None:
        for dataset_code, cfg in series_to_fetch:
            if selected_series and dataset_code not in selected_series:
                continue
            indicator_id = resolve_indicator_id(session, cfg["canonical_indicator"])
            dataflow = cfg.get("dataflow", dataset_code)
            
            for country_id in cfg.get("countries", []):
                if selected_countries and country_id not in selected_countries:
                    continue
                try:
                    # OECD new API format: {dataflow}/{country}........
                    # The key has 8 dimensions total: REF_AREA (country) + 7 wildcards
                    # Example: DSD_PRICES@DF_PRICES_ALL/USA........
                    dataset_key = f"{country_id}........"  # Country + 7 wildcards (8 dimensions total)
                    
                    # Full path: dataflow/key
                    full_path = f"{dataflow}/{dataset_key}"
                    
                    df = fetch_sdmx_dataset(
                        OECD_BASE_URL, 
                        full_path,
                        params={
                            "startPeriod": "2015",
                            "endPeriod": "2024",
                            "dimensionAtObservation": "AllDimensions"
                        },
                        sample_config=sample_config
                    )
                    store_raw_payload(
                        session,
                        RawOecd,
                        params={"dataset": dataflow, "country": country_id},
                        payload=df.to_dict() if hasattr(df, "to_dict") else None,
                    )
                    rows = parse_oecd(
                        df,
                        indicator_id=indicator_id,
                        country_id=country_id,
                        source="OECD",
                        sample_config=sample_config,
                    )
                    bulk_upsert_timeseries(session, rows)
                    logger.info(f"OECD: Ingested {len(rows)} records for {dataflow}/{country_id}")
                except IngestionError:
                    raise
                except Exception as e:
                    logger.error(f"OECD: Failed for {dataflow}/{country_id}: {e}")
                    if sample_config.strict_validation:
                        raise IngestionError("OECD", country_id, f"Failed for {dataflow}: {e}")

    asyncio.run(_run())
    session.commit()

