import datetime as dt
import logging
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import yaml
from sqlalchemy.orm import Session

from app.config.country_universe import COUNTRY_UNIVERSE
from app.db.models import RawEmber
from app.ingest.sample_mode import (
    SampleConfig,
    validate_timeseries_data,
    IngestionError,
)
from app.ingest.utils import (
    bulk_upsert_timeseries,
    ensure_date,
    resolve_indicator_id,
    store_raw_payload,
)
from app.ingest.rate_limiter import EMBER_LIMITER

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
EMBER_CONFIG = CATALOG.get("EMBER", {}) if isinstance(CATALOG, dict) else {}

# New EMBER API v1 endpoint (Dec 2025)
# API docs: https://api.ember-energy.org/v1/docs
EMBER_BASE_URL = "https://api.ember-energy.org/v1"

# Map EMBER series names to our indicator canonical codes
SERIES_TO_INDICATOR: Dict[str, str] = {
    "Solar": "EMBER_ELECTRICITY_SOLAR",
    "Wind": "EMBER_ELECTRICITY_WIND",
    "Coal": "EMBER_ELECTRICITY_COAL",
    "Gas": "EMBER_ELECTRICITY_GAS",
    "Hydro": "EMBER_ELECTRICITY_HYDRO",
}


def fetch_ember_data(
    country_codes: List[str],
    start_year: int,
    end_year: int,
    api_key: str,
    *,
    sample_config: Optional[SampleConfig] = None,
) -> Dict[str, Any]:
    """Fetch electricity generation data from EMBER API v1.
    
    API endpoint: GET /v1/electricity-generation/yearly
    Params: entity_code (comma-separated ISO3 codes), start_date (YYYY), end_date (YYYY), api_key
    Returns: JSON with 'data' array and 'stats' metadata
    """
    sample_config = sample_config or SampleConfig()
    
    try:
        import httpx
    except ImportError as exc:
        raise RuntimeError("httpx is required to fetch EMBER data") from exc
    
    # Build comma-separated list of country codes
    entity_codes = ",".join(country_codes)
    
    url = f"{EMBER_BASE_URL}/electricity-generation/yearly"
    params = {
        "entity_code": entity_codes,
        "start_date": str(start_year),
        "end_date": str(end_year),
        "is_aggregate_series": "false",  # Get individual fuel types, not aggregates
        "api_key": api_key,
    }
    
    try:
        # Rate limiting: 1000 requests/day
        EMBER_LIMITER.acquire()
        with httpx.Client(timeout=60.0) as client:
            response = client.get(url, params=params)
            response.raise_for_status()
            return response.json()
    except httpx.HTTPStatusError as e:
        raise IngestionError("EMBER", entity_codes, f"HTTP {e.response.status_code}: {e.response.text[:200]}")
    except Exception as e:
        raise IngestionError("EMBER", entity_codes, f"Fetch error: {e}")


def parse_ember_response(
    response: Dict[str, Any],
    *,
    indicator_map: Dict[str, int],
    sample_config: Optional[SampleConfig] = None,
) -> List[Dict[str, Any]]:
    """Parse EMBER API response into timeseries rows.
    
    Response structure:
    {
        "stats": {...},
        "data": [
            {
                "entity": "United States",
                "entity_code": "USA",
                "date": "2020",
                "series": "Solar",
                "generation_twh": 130.72,
                "share_of_generation_pct": 3.23
            },
            ...
        ]
    }
    """
    sample_config = sample_config or SampleConfig()
    rows: List[Dict[str, Any]] = []
    
    data_records = response.get("data", [])
    if not data_records:
        if sample_config.fail_on_empty:
            raise IngestionError("EMBER", "N/A", "Empty response from API")
        return rows
    
    for record in data_records:
        country_code = record.get("entity_code")
        series = record.get("series")
        
        # Skip aggregate series (Clean, Total generation, etc.)
        if record.get("is_aggregate_series", False):
            continue
        
        # Only process series we have indicators for
        if series not in indicator_map:
            continue
        
        try:
            # Date is just the year (e.g., "2020")
            year = str(record.get("date"))
            date = ensure_date(f"{year}-01-01")  # Convert to Jan 1 of that year
            
            # Use generation_twh as the value
            value = float(record.get("generation_twh", 0))
            
            rows.append({
                "indicator_id": indicator_map[series],
                "country_id": country_code,
                "date": date,
                "value": value,
                "source": "EMBER",
                "ingested_at": dt.datetime.now(dt.timezone.utc),
            })
        except Exception as exc:
            logger.error(f"EMBER: Parse error for {country_code} {series}: {exc}")
            if sample_config.strict_validation:
                raise IngestionError("EMBER", country_code, f"Parse error: {exc}")
            continue
    
    # Log summary in sample mode
    if sample_config.enabled and rows:
        logger.info(f"EMBER: Parsed {len(rows)} records from API response")
    
    return rows



def ingest_full(
    session: Session,
    *,
    country_subset: Optional[Iterable[str]] = None,
    sample_config: Optional[SampleConfig] = None,
) -> None:
    """Ingest EMBER electricity generation data using the new API v1.
    
    Args:
        session: Database session
        country_subset: Optional list of country codes (ISO3) to ingest
        sample_config: Optional sample mode configuration
    
    Requires:
        EMBER_API_KEY environment variable or api_key in providers.yaml
        API key can be obtained from https://ember-energy.org/data/api/
    """
    sample_config = sample_config or SampleConfig()
    
    # Get API key from environment or config
    api_key = os.getenv("EMBER_API_KEY") or EMBER_CONFIG.get("api_key")
    if not api_key:
        raise IngestionError(
            "EMBER", 
            "N/A", 
            "EMBER_API_KEY environment variable or api_key in providers.yaml is required. "
            "Get an API key at https://ember-energy.org/data/api/"
        )
    
    # Determine countries to query
    if country_subset:
        countries = list(country_subset)
    elif sample_config.enabled:
        # In sample mode, use first 5 countries
        countries = list(COUNTRY_UNIVERSE)[:5]
    else:
        # Full mode: use all countries
        countries = list(COUNTRY_UNIVERSE)
    
    # Build indicator mapping
    indicator_map: Dict[str, int] = {}
    for series_name, canonical in SERIES_TO_INDICATOR.items():
        try:
            indicator_map[series_name] = resolve_indicator_id(session, canonical)
        except ValueError:
            logger.warning(f"EMBER: Skipping series {series_name} due to missing indicator {canonical}")
    
    if not indicator_map:
        raise IngestionError("EMBER", "N/A", "No valid indicators found for EMBER series")
    
    # Fetch data from API
    # EMBER has data from ~2000 to present, we'll query recent years
    current_year = dt.datetime.now().year
    start_year = current_year - 5 if sample_config.enabled else 2000
    end_year = current_year
    
    logger.info(f"EMBER: Fetching data for {len(countries)} countries from {start_year} to {end_year}")
    
    try:
        response = fetch_ember_data(
            country_codes=countries,
            start_year=start_year,
            end_year=end_year,
            api_key=api_key,
            sample_config=sample_config,
        )
        
        # Store raw response
        store_raw_payload(
            session,
            RawEmber,
            params={"countries": ",".join(countries[:5]), "years": f"{start_year}-{end_year}"},
            payload=response,
        )
        
        # Parse and insert data
        rows = parse_ember_response(
            response,
            indicator_map=indicator_map,
            sample_config=sample_config,
        )
        
        if rows:
            bulk_upsert_timeseries(session, rows)
            logger.info(f"EMBER: Successfully ingested {len(rows)} records")
        else:
            logger.warning("EMBER: No data returned from API")
        
        session.commit()
        
    except IngestionError:
        raise
    except Exception as e:
        logger.error(f"EMBER: Ingestion failed: {e}")
        if sample_config.strict_validation:
            raise IngestionError("EMBER", "N/A", f"Ingestion failed: {e}")
