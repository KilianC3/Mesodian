import datetime as dt
import logging
from io import StringIO
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import pandas as pd
import yaml
from sqlalchemy.orm import Session

from app.config.country_universe import COUNTRY_UNIVERSE
from app.db.models import CountryYearFeatures, RawGdelt
from app.ingest.sample_mode import (
    SampleConfig,
    validate_timeseries_data,
    IngestionError,
)
from app.ingest.utils import bulk_upsert_timeseries, resolve_indicator_id, store_raw_payload

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
GDELT_CONFIG = CATALOG.get("GDELT", {}) if isinstance(CATALOG, dict) else {}

GDELT_EVENTS_URL = GDELT_CONFIG.get("base_url", "https://api.gdeltproject.org/api/v2/doc/doc")


def fetch_gdelt_events(params: Optional[Dict[str, str]] = None, *, sample_config: Optional[SampleConfig] = None) -> pd.DataFrame:
    """Fetch GDELT events with optional sample mode limiting.
    
    API docs: https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/
    Query format: keyword searches (country:XXX doesn't work anymore)
    """
    sample_config = sample_config or SampleConfig()
    try:
        import httpx
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("httpx required for GDELT ingestion") from exc

    # Build proper query - GDELT Doc API requires keyword searches now
    default_query = "economy trade finance gdp"
    request_params = {
        "query": params.get("query", default_query) if params else default_query,
        "mode": "ArtList",
        "format": "csv",
        "maxrecords": str(sample_config.max_records_per_country * 5) if sample_config.enabled else "250",
    }
    
    try:
        response = httpx.get(GDELT_EVENTS_URL, params=request_params, timeout=60.0)
        response.raise_for_status()
        if not response.text or len(response.text) < 10:
            raise IngestionError("GDELT", "N/A", "Empty response from API")
        return pd.read_csv(StringIO(response.text))
    except Exception as e:
        raise IngestionError("GDELT", "N/A", f"Fetch error: {e}")


def ingest_full(
    session: Session,
    *,
    country_subset: Optional[Iterable[str]] = None,
    year_subset: Optional[Iterable[int]] = None,
    sample_config: Optional[SampleConfig] = None,
) -> None:
    """Ingest GDELT data with optional sample mode."""
    sample_config = sample_config or SampleConfig()
    selected_countries = set(country_subset) if country_subset else set(COUNTRY_UNIVERSE)
    if sample_config.enabled:
        selected_countries = set(list(selected_countries)[:5])
    
    selected_years = set(year_subset) if year_subset else None

    try:
        # Pass query parameters to get actual data
        query_terms = "economy trade finance gdp"
        df = fetch_gdelt_events(params={"query": query_terms}, sample_config=sample_config)
        
        if df.empty:
            if sample_config.fail_on_empty:
                raise IngestionError("GDELT", "N/A", "Empty dataframe returned")
            logger.warning("GDELT: No data returned, skipping")
            return
        
        # Replace NaN with None for JSON serialization
        df = df.where(pd.notnull(df), None)
        
        store_raw_payload(session, RawGdelt, params={"count": len(df)}, payload=df.to_dict())
        
        logger.info(f"GDELT: Stored {len(df)} news articles (raw data for sentiment analysis)")
        
    except Exception as e:
        logger.error(f"GDELT: Failed to fetch/store: {e}")
        if sample_config.strict_validation:
            raise IngestionError("GDELT", "N/A", f"Init error: {e}")
        return

    session.commit()

