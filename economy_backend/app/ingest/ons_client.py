import asyncio
import datetime as dt
import logging
from typing import Any, Dict, Iterable, List, Optional

import httpx
from sqlalchemy.orm import Session

from app.db.models import RawOns
from app.ingest.utils import bulk_upsert_timeseries, ensure_date, resolve_indicator_id, store_raw_payload
from app.ingest.sample_mode import SampleConfig, validate_timeseries_data, IngestionError
from app.ingest.rate_limiter import ONS_LIMITER, rate_limit

logger = logging.getLogger(__name__)


# ONS Beta API v1 (replaced old v0 API in November 2024)
# Old v0 API was retired on November 25, 2024
# New API: https://api.beta.ons.gov.uk/v1
# Documentation: https://developer.ons.gov.uk/
# No API key required for public data
# Use dataset -> latest version -> observations flow
ONS_BASE_URL = "https://api.beta.ons.gov.uk/v1"

# ONS dataset IDs for key economic indicators
# Note: These are new dataset IDs for the beta API
# The aggregate_filter is used to select specific series from the CSV
# For CPIH: CP00 is the overall CPIH index (all items), CP01 is food & beverages, etc.
# For GDP: ABMI is the overall GDP index (2016=100)
ONS_SERIES: Dict[str, Dict[str, Any]] = {
    "CPIH": {
        "canonical_indicator": "CPIH_UK",
        "dataset_id": "cpih01",  # CPIH time series
        "aggregate_filter": "CP00",  # Overall CPIH (all items)
        "aggregate_column": "cpih1dim1aggid",  # Column name for filtering
        "frequency": "M",
    },
    "GDP": {
        "canonical_indicator": "GDP_GROWTH_UK",
        "dataset_id": "gdp-to-four-decimal-places",  # Monthly GDP estimate
        "aggregate_filter": "A--T",  # Overall GDP (all industries A-T)
        "aggregate_column": "sic-unofficial",  # Column name for filtering
        "frequency": "M",
    },
}


async def get_ons_dataset_metadata(dataset_id: str) -> Dict[str, Any]:
    """
    Get metadata for an ONS dataset including latest version link.
    
    Args:
        dataset_id: ONS dataset identifier
    
    Returns:
        Dataset metadata with links to latest version
    """
    url = f"{ONS_BASE_URL}/datasets/{dataset_id}"
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(url)
        response.raise_for_status()
        return response.json()


async def get_ons_latest_version_url(dataset_id: str) -> str:
    """
    Get the URL for the latest version of an ONS dataset.
    
    Args:
        dataset_id: ONS dataset identifier
    
    Returns:
        URL to fetch latest version data
    """
    meta = await get_ons_dataset_metadata(dataset_id)
    latest_link = meta.get("links", {}).get("latest_version", {}).get("href")
    if not latest_link:
        raise IngestionError("ONS", "GBR", f"No latest_version link for dataset {dataset_id}")
    return latest_link



async def fetch_series(
    series_id: str,
    *,
    sample_config: Optional[SampleConfig] = None
) -> Dict[str, Any]:
    """
    Fetch ONS time series data using the new beta API v1.
    
    Flow:
    1. Get dataset metadata
    2. Follow latest_version link
    3. Download CSV file (observations endpoint doesn't work in Beta API)
    4. Parse CSV and return as structured data
    
    API docs: https://developer.ons.gov.uk/
    Note: Beta API v1 provides downloadable CSVs, not JSON observations
    """
    import csv
    from io import StringIO
    
    sample_config = sample_config or SampleConfig()
    cfg = ONS_SERIES.get(series_id, {})
    dataset_id = cfg.get("dataset_id")
    aggregate_filter = cfg.get("aggregate_filter")
    aggregate_column = cfg.get("aggregate_column")
    
    if not dataset_id:
        raise IngestionError("ONS", "GBR", f"No dataset_id configured for series {series_id}")
    
    # Apply rate limiting (10 requests/minute)
    ONS_LIMITER.acquire()
    
    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        try:
            # Step 1: Get dataset metadata to find latest version link
            dataset_url = f"{ONS_BASE_URL}/datasets/{dataset_id}"
            dataset_resp = await client.get(dataset_url)
            dataset_resp.raise_for_status()
            dataset_meta = dataset_resp.json()
            
            # Step 2: Get latest version URL
            latest_link = dataset_meta.get("links", {}).get("latest_version", {}).get("href")
            if not latest_link:
                raise IngestionError("ONS", "GBR", f"No latest_version link in dataset {dataset_id}")
            
            # Step 3: Fetch latest version metadata to get CSV download link
            version_resp = await client.get(latest_link)
            version_resp.raise_for_status()
            version_data = version_resp.json()
            
            # Step 4: Get CSV download link
            csv_link = version_data.get("downloads", {}).get("csv", {}).get("href")
            if not csv_link:
                raise IngestionError("ONS", "GBR", f"No CSV download link for dataset {dataset_id}")
            
            # Step 5: Download CSV data
            csv_resp = await client.get(csv_link)
            csv_resp.raise_for_status()
            csv_text = csv_resp.text
            
            # Step 6: Parse CSV into observations format
            # CSV format: v4_0,mmm-yy,Time,uk-only,Geography,cpih1dim1aggid,Aggregate
            # Example: 142,Oct-25,Oct-25,K02000001,United Kingdom,CP0111,01.1.1 Bread and cereals
            reader = csv.DictReader(StringIO(csv_text))
            observations = []
            
            for row in reader:
                # Extract the observation value (first column)
                # The CSV uses dynamic column names based on dimension codes
                # First column is the observation value
                keys = list(row.keys())
                if not keys:
                    continue
                    
                value_key = keys[0]  # First column is the value
                time_key = keys[1]   # Second column is time code
                
                obs_value = row.get(value_key)
                time_code = row.get(time_key)
                
                # Filter by aggregate if specified (e.g., overall index)
                if aggregate_filter and aggregate_column:
                    aggregate_code = row.get(aggregate_column, "")
                    if aggregate_code != aggregate_filter:
                        continue
                
                if obs_value and time_code:
                    observations.append({
                        "time": time_code,
                        "observation": obs_value
                    })
            
            # Limit in sample mode
            if sample_config.enabled:
                observations = observations[-sample_config.max_records_per_country:]
            
            return {"observations": observations}
            
        except httpx.HTTPStatusError as e:
            raise IngestionError("ONS", "GBR", f"HTTP {e.response.status_code} for dataset {dataset_id}: {e.response.text[:200]}")
        except Exception as e:
            raise IngestionError("ONS", "GBR", f"Error parsing CSV: {str(e)}")


def parse_ons(
    payload: Dict[str, Any],
    *,
    indicator_id: int,
    country_id: str,
    source: str,
    sample_config: Optional[SampleConfig] = None,
) -> List[Dict[str, Any]]:
    """
    Parse ONS beta API v1 response with strict validation.
    
    Beta API returns observations in format:
    {
        "observations": [
            {"time": "2023-Q1", "observation": "123.4"},
            ...
        ]
    }
    Or it may use different keys like "data", "datapoints", etc.
    """
    sample_config = sample_config or SampleConfig()
    
    # Try multiple possible data keys for observations
    # Use 'is not None' to distinguish between missing key and empty list
    if "observations" in payload:
        observations = payload["observations"]
    elif "data" in payload:
        observations = payload["data"]
    elif "datapoints" in payload:
        observations = payload["datapoints"]
    elif "months" in payload:
        observations = payload["months"]
    elif "quarters" in payload:
        observations = payload["quarters"]
    elif "years" in payload:
        observations = payload["years"]
    else:
        observations = []
    
    logger.info(f"ONS parse_ons: Found {len(observations)} observations in payload")
    
    if not observations:
        if sample_config.fail_on_empty:
            raise IngestionError("ONS", country_id, "No observations in payload")
        return []
    
    rows: List[Dict[str, Any]] = []
    for entry in observations:
        # Handle different response formats
        # Beta API uses "time" and "observation" or "value"
        date_label = (
            entry.get("time") or 
            entry.get("date") or 
            entry.get("quarter") or 
            entry.get("year") or 
            entry.get("month")
        )
        value = (
            entry.get("observation") or 
            entry.get("value")
        )
        
        if date_label is None:
            logger.warning(f"ONS entry missing date: {entry}")
            if sample_config.strict_validation:
                raise IngestionError("ONS", country_id, f"Missing date in entry: {entry}")
            continue
        
        if value is None or value == "":
            logger.debug(f"ONS entry missing or empty value: {entry}")
            continue
        
        try:
            # ONS dates can be in various formats: "2023-Q1", "2023-01", "2023", "Oct-25" (MMM-YY)
            date_str = str(date_label).strip()
            
            # Parse different date formats
            if '-Q' in date_str:
                # Quarterly format: "2023-Q1"
                year_str, quarter_str = date_str.split('-Q')
                year = int(year_str)
                quarter = int(quarter_str)
                month = (quarter - 1) * 3 + 1
                date = dt.date(year, month, 1)
            elif '-' in date_str and len(date_str.split('-')) == 2:
                parts = date_str.split('-')
                # Check if it's MMM-YY format (e.g., "Oct-25")
                if len(parts[1]) == 2 and parts[0].isalpha() and len(parts[0]) == 3:
                    # MMM-YY format: "Oct-25"
                    month_str = parts[0]
                    year_str = parts[1]
                    month_map = {
                        'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
                        'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
                    }
                    month = month_map.get(month_str.upper(), 1)
                    # Year is 2-digit, assume 2000+ for YY >= 00 and < 50, 1900+ otherwise
                    year_int = int(year_str)
                    year = 2000 + year_int if year_int < 50 else 1900 + year_int
                    date = dt.date(year, month, 1)
                else:
                    # Monthly format: "2023-01"
                    year_str, month_str = parts
                    year = int(year_str)
                    month = int(month_str)
                    date = dt.date(year, month, 1)
            elif ' ' in date_str:
                # Old format: "2023 Q1" or "2023 JAN"
                year_part, period_part = date_str.split(' ', 1)
                year = int(year_part)
                
                if len(period_part) == 3 and period_part.isalpha():
                    # Monthly format: "2023 JAN"
                    month_map = {
                        'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
                        'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
                    }
                    month = month_map.get(period_part.upper(), 1)
                    date = dt.date(year, month, 1)
                elif period_part.startswith('Q'):
                    # Quarterly format: "2023 Q1"
                    quarter = int(period_part[1])
                    month = (quarter - 1) * 3 + 1
                    date = dt.date(year, month, 1)
                else:
                    date = dt.date(year, 1, 1)
            else:
                # Just a year
                date = ensure_date(date_str)
            
            numeric_value = float(value)
        except Exception as exc:
            logger.error(f"Failed to parse ONS entry {entry}: {exc}")
            if sample_config.strict_validation:
                raise IngestionError("ONS", country_id, f"Parse error: {exc}")
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
    
    # Validate in sample mode
    if sample_config.enabled:
        validation = validate_timeseries_data(
            rows,
            expected_countries=[country_id],
            sample_config=sample_config
        )
        if sample_config.strict_validation:
            validation.raise_if_invalid()
        elif validation.warnings:
            for warning in validation.warnings:
                logger.warning(f"ONS validation warning: {warning}")
    
    return rows


def ingest_full(
    session: Session,
    *,
    series_subset: Optional[Iterable[str]] = None,
    sample_config: Optional[SampleConfig] = None,
) -> None:
    """
    Ingest ONS data with optional sample mode.
    
    Args:
        session: Database session
        series_subset: Optional subset of series to ingest
        sample_config: Sample mode configuration for testing
    """
    sample_config = sample_config or SampleConfig()
    selected_series = set(series_subset) if series_subset else None

    async def _run() -> None:
        for series_id, cfg in ONS_SERIES.items():
            if selected_series and series_id not in selected_series:
                continue
            
            try:
                indicator_id = resolve_indicator_id(session, cfg["canonical_indicator"])
            except Exception as e:
                error_msg = f"Failed to resolve indicator {cfg['canonical_indicator']}: {e}"
                logger.error(error_msg)
                if sample_config.strict_validation:
                    raise IngestionError("ONS", "GBR", error_msg)
                continue
            
            try:
                payload = await fetch_series(series_id, sample_config=sample_config)
                # Skip raw payload storage - raw_ons table doesn't exist yet
                # store_raw_payload(
                #     session, RawOns, params={"series_id": series_id}, payload=payload
                # )
                rows = parse_ons(
                    payload,
                    indicator_id=indicator_id,
                    country_id="GBR",
                    source="ONS",
                    sample_config=sample_config,
                )
                bulk_upsert_timeseries(session, rows)
                logger.info(f"ONS: Ingested {len(rows)} records for series {series_id}")
            except IngestionError:
                raise
            except Exception as e:
                error_msg = f"Failed to ingest ONS series {series_id}: {e}"
                logger.error(error_msg)
                if sample_config.strict_validation:
                    raise IngestionError("ONS", "GBR", error_msg)

    asyncio.run(_run())
    session.commit()

