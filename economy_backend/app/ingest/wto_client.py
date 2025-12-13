import datetime as dt
import logging
import os
from typing import Any, Dict, Iterable, Optional, List

import httpx
from sqlalchemy.orm import Session

from app.config.country_universe import COUNTRY_UNIVERSE
from app.db.models import RawWto
from app.ingest.sample_mode import (
    SampleConfig,
    validate_timeseries_data,
    IngestionError,
)
from app.ingest.utils import (
    bulk_upsert_timeseries,
    resolve_indicator_id,
    store_raw_payload,
)
from app.ingest.rate_limiter import WTO_LIMITER

logger = logging.getLogger(__name__)


# WTO Data API
# API docs: https://apiportal.wto.org/
# Base URL: https://api.wto.org/timeseries/v1
#
# Authentication: Ocp-Apim-Subscription-Key header
#
# Key endpoints:
# - GET /indicators: List all available indicators
# - GET /data: Get time series data
#   Required params: i (indicator code), r (reporter code - M49 numeric)
#   Optional params: p (partner code), ps (product/sector), t (time period)
#
# Example indicators:
# - TP_A_0010: Simple average MFN applied tariff - all products
# - ITS_MTV_AM: Merchandise trade value (annual)
# - ITS_CS_AM: Commercial services trade (annual)
#
# Reporter codes: M49 numeric country codes (840=USA, 826=GBR, 276=DEU, etc.)
# Time period format: Annual (2021), Quarterly (2021-Q1), Monthly (2021-M01)
WTO_BASE = "https://api.wto.org/timeseries/v1"

# ISO3 to M49 code mapping (WTO uses UN M49 numeric country codes)
ISO3_TO_M49 = {
    "USA": "840",
    "GBR": "826",
    "DEU": "276",
    "FRA": "250",
    "CHN": "156",
    "IND": "356",
    "JPN": "392",
    "ITA": "380",
    "ESP": "724",
    "NLD": "528",
}

# Map WTO indicators to our canonical indicators
# Only includes indicators confirmed to work (TP_A_0010)
# NOTE: Most indicators return 400 "No indicator found" - WTO API appears limited
WTO_INDICATORS: Dict[str, Dict[str, Any]] = {
    # === TARIFFS & MARKET ACCESS ===
    # Applied tariffs (ONLY WORKING INDICATOR)
    "TP_A_0010": {
        "canonical_indicator": "WTO_TARIFF_AVG_MFN",
        "name": "Simple average MFN applied tariff - all products",
        "countries": ["USA", "GBR", "DEU", "FRA", "CHN", "IND", "JPN", "ITA", "ESP", "NLD"],
    },
}


def fetch_wto_data(
    indicator_code: str,
    reporter_m49_code: str,
    *,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
    sample_config: Optional[SampleConfig] = None,
) -> Dict[str, Any]:
    """
    Fetch data from WTO Time Series API.
    
    Args:
        indicator_code: WTO indicator code (e.g., "TP_A_0010")
        reporter_m49_code: M49 numeric country code (e.g., "840" for USA)
        start_year: Start year (optional)
        end_year: End year (optional)
        sample_config: Sample mode configuration
    
    Returns:
        JSON response from API
    
    Example:
        fetch_wto_data("TP_A_0010", "840", start_year=2015)
    """
    sample_config = sample_config or SampleConfig()
    
    # Get API key from environment
    api_key = os.getenv("WTO_API_KEY")
    
    if not api_key:
        raise IngestionError("WTO", reporter_m49_code, "Missing WTO_API_KEY in environment")
    
    # Build URL and parameters
    url = f"{WTO_BASE}/data"
    
    params = {
        "i": indicator_code,      # indicator
        "r": reporter_m49_code,   # reporter (M49 numeric code)
    }
    
    # Add time period filter if specified
    if start_year or end_year:
        # Format: t=2015,2016,2017 or t=2015-2020
        if start_year and end_year:
            params["t"] = f"{start_year}-{end_year}"
        elif start_year:
            params["t"] = f"{start_year}-{dt.datetime.now().year}"
        elif end_year:
            params["t"] = f"2000-{end_year}"
    
    try:
        # Rate limiting: 1000 requests/hour
        WTO_LIMITER.acquire()
        response = httpx.get(
            url,
            params=params,
            headers={
                "Ocp-Apim-Subscription-Key": api_key,
            },
            timeout=30.0,
            follow_redirects=True,
        )
        response.raise_for_status()
        
        data = response.json()
        
        # Check if response has data
        if not data or (isinstance(data, dict) and not data.get("Dataset")):
            if sample_config.fail_on_empty:
                raise IngestionError("WTO", reporter_m49_code, "Empty response from API")
        
        return data
        
    except httpx.HTTPStatusError as e:
        raise IngestionError("WTO", reporter_m49_code, f"HTTP {e.response.status_code}: {e.response.text[:200]}")
    except Exception as e:
        raise IngestionError("WTO", reporter_m49_code, f"Fetch error: {e}")


def _parse_wto_response(
    data: Dict[str, Any],
    *,
    indicator_id: int,
    country_id: str,
    sample_config: Optional[SampleConfig] = None,
) -> List[Dict[str, Any]]:
    """
    Parse WTO API response with strict validation.
    
    WTO response structure:
    {
      "Dataset": [
        {
          "ReportingEconomyCode": "840",
          "ReportingEconomy": "United States of America",
          "IndicatorCode": "TP_A_0010",
          "Indicator": "Simple average MFN applied tariff...",
          "TimePeriod": "2021",
          "Value": 3.5,
          "UnitCode": "PCT",
          ...
        }
      ]
    }
    """
    sample_config = sample_config or SampleConfig()
    rows: List[Dict[str, Any]] = []
    
    # Extract dataset
    dataset = data.get("Dataset", [])
    
    if not dataset:
        if sample_config.fail_on_empty:
            raise IngestionError("WTO", country_id, "Empty dataset in response")
        return rows
    
    # Limit to sample size
    if sample_config.enabled and dataset:
        dataset = dataset[-sample_config.max_records_per_country:]
    
    for record in dataset:
        time_period = record.get("Year")  # WTO uses "Year" field
        value = record.get("Value")
        
        if time_period is None or value is None:
            if sample_config.strict_validation:
                raise IngestionError("WTO", country_id, f"Missing Year or Value: {record}")
            continue
        
        try:
            # Parse time period (WTO uses Year field for annual data)
            year = int(time_period)
            date = dt.date(year, 12, 31)
            
            # Handle value
            numeric_value = float(value)
            
        except Exception as exc:
            logger.error(f"WTO: Parse error for {country_id}: {exc}")
            if sample_config.strict_validation:
                raise IngestionError("WTO", country_id, f"Parse error: {exc}")
            continue
        
        rows.append(
            {
                "indicator_id": indicator_id,
                "country_id": country_id,
                "date": date,
                "value": numeric_value,
                "source": "WTO",
                "ingested_at": dt.datetime.utcnow(),
            }
        )
    
    # Validate results
    if sample_config.enabled and rows:
        validation = validate_timeseries_data(
            rows,
            expected_countries=[country_id],
            sample_config=sample_config
        )
        if sample_config.strict_validation:
            validation.raise_if_invalid()
    
    return rows


def ingest_full(
    session: Session,
    *,
    indicator_subset: Optional[Iterable[str]] = None,
    country_subset: Optional[Iterable[str]] = None,
    sample_config: Optional[SampleConfig] = None,
) -> None:
    """Ingest data from WTO Time Series API with optional sample mode."""
    sample_config = sample_config or SampleConfig()
    selected_indicators = set(indicator_subset) if indicator_subset else None
    selected_countries = set(country_subset) if country_subset else None

    for wto_code, cfg in WTO_INDICATORS.items():
        if selected_indicators and wto_code not in selected_indicators:
            continue
        
        try:
            indicator_id = resolve_indicator_id(session, cfg["canonical_indicator"])
        except Exception as e:
            logger.error(f"WTO: Failed to resolve indicator {cfg['canonical_indicator']}: {e}")
            if sample_config.strict_validation:
                raise IngestionError("WTO", "N/A", f"Indicator resolution failed: {e}")
            continue
        
        # Get countries to fetch
        countries = cfg.get("countries", ["USA", "GBR", "DEU", "FRA", "CHN"])
        if selected_countries:
            countries = [c for c in countries if c in selected_countries]
        if sample_config.enabled:
            countries = countries[:3]  # Limit to 3 countries in sample mode
        
        # Determine time range
        current_year = dt.datetime.now().year
        start_year = current_year - 10 if not sample_config.enabled else current_year - 2
        
        for country_id in countries:
            try:
                # Map ISO3 to M49 code
                m49_code = ISO3_TO_M49.get(country_id)
                if not m49_code:
                    logger.warning(f"WTO: No M49 code mapping for {country_id}, skipping")
                    continue
                
                # Fetch data
                data = fetch_wto_data(
                    wto_code,
                    m49_code,
                    start_year=start_year,
                    sample_config=sample_config,
                )
                
                # Store raw response (sample)
                store_raw_payload(
                    session,
                    RawWto,
                    params={"indicator": wto_code, "country": country_id},
                    payload={"response": data if isinstance(data, dict) else {"error": "Invalid response type"}},
                )
                
                # Parse and insert
                rows = _parse_wto_response(
                    data,
                    indicator_id=indicator_id,
                    country_id=country_id,
                    sample_config=sample_config,
                )
                
                # Deduplicate rows by (indicator_id, country_id, date) - keep last value
                # WTO API can return multiple values for same date (different product codes, flows, etc.)
                seen_keys = {}
                for row in rows:
                    key = (row["indicator_id"], row["country_id"], row["date"])
                    seen_keys[key] = row  # Overwrites with latest value
                deduped_rows = list(seen_keys.values())
                
                if len(rows) != len(deduped_rows):
                    logger.warning(f"WTO: Deduplicated {len(rows)} → {len(deduped_rows)} rows for {country_id}/{wto_code}")
                
                bulk_upsert_timeseries(session, deduped_rows)
                logger.info(f"WTO: Ingested {len(deduped_rows)} records for {country_id}/{wto_code}")
                
            except IngestionError as ie:
                # Log but continue for known issues
                if "Empty" in str(ie) or "No data" in str(ie):
                    logger.warning(f"WTO: No data available for {country_id}/{wto_code}")
                else:
                    logger.error(f"WTO: {ie}")
                    if sample_config.strict_validation:
                        raise
            except Exception as e:
                logger.error(f"WTO: Failed for {country_id}/{wto_code}: {e}")
                if sample_config.strict_validation:
                    raise IngestionError("WTO", country_id, f"Ingestion failed: {e}")
    
    session.commit()
