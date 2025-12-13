import asyncio
import datetime as dt
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, List
import yaml

from sqlalchemy.orm import Session

from app.config.country_universe import COUNTRY_UNIVERSE
from app.db.models import RawPatentsView
from app.ingest.base_client import get_provider_client
from app.ingest.sample_mode import (
    SampleConfig,
    validate_timeseries_data,
    IngestionError,
)
from app.ingest.utils import bulk_upsert_timeseries, resolve_indicator_id, store_raw_payload

logger = logging.getLogger(__name__)


def _load_provider_catalog() -> Dict[str, Any]:
    """Load PatentsView catalog for CPC class definitions."""
    catalog_path = Path(__file__).parents[2] / "config" / "catalogs" / "providers.yaml"
    with open(catalog_path) as f:
        return yaml.safe_load(f).get("PATENTSVIEW", {})


_catalog = _load_provider_catalog()
PATENTSVIEW_BASE_URL = _catalog.get("base_url", "https://api.patentsview.org/patents/query")
PATENTSVIEW_CPC_CLASSES = _catalog.get("cpc_classes", {})

PATENTSVIEW_CONFIG: Dict[str, Any] = {
    "indicator": "PATENTS_COUNT",
    "start_year": 2018,
    "end_year": 2023,
}


async def fetch_patents(
    technology: str, country: str, year: int, *, client=None, sample_config: Optional[SampleConfig] = None
) -> Dict[str, Any]:
    """Fetch patent counts from PatentsView API v1 with optional sample mode limiting.
    
    API docs: https://search.patentsview.org/docs/docs/API%20Endpoints/Patents/
    Correct endpoint: POST /api/v1/patent/
    Query format: JSON body with q (query), f (fields), o (options)
    """
    sample_config = sample_config or SampleConfig()
    if client is None:
        client = get_provider_client("PATENTSVIEW", PATENTSVIEW_BASE_URL)

    # PatentsView API query format
    body = {
        "q": {
            "_and": [
                {"_gte": {"patent_date": f"{year}-01-01"}},
                {"_lte": {"patent_date": f"{year}-12-31"}},
                {"_contains": {"assignee_country": country.upper()}},
            ]
        },
        "f": ["patent_number", "patent_date", "assignee_country"],
        "o": {
            "per_page": sample_config.max_records_per_country if sample_config.enabled else 100,
            "page": 1,
        },
    }

    try:
        import httpx
        async with httpx.AsyncClient(timeout=30.0) as http_client:
            response = await http_client.post(f"{PATENTSVIEW_BASE_URL}/query", json=body)
            response.raise_for_status()
            return response.json()
    except Exception as e:
        raise IngestionError("PATENTSVIEW", country, f"Fetch error: {e}")


def _parse_patents(
    raw: Dict[str, Any],
    *,
    indicator_id: int,
    country_id: str,
    year: int,
    sample_config: Optional[SampleConfig] = None,
) -> List[Dict[str, Any]]:
    """Parse PatentsView response with strict validation."""
    sample_config = sample_config or SampleConfig()
    
    if not raw:
        if sample_config.fail_on_empty:
            raise IngestionError("PATENTSVIEW", country_id, "Empty response from API")
        return []
    
    # PatentsView returns "total_patent_count" or "count" in response
    total = raw.get("total_patent_count") or raw.get("count") or len(raw.get("patents", []))
    
    if total == 0 and sample_config.strict_validation:
        logger.warning(f"PATENTSVIEW: Zero patents found for {country_id} year {year}")
    
    rows = [
        {
            "indicator_id": indicator_id,
            "country_id": country_id,
            "date": dt.date(year, 12, 31),
            "value": float(total),
            "source": "PATENTSVIEW",
            "ingested_at": dt.datetime.now(dt.timezone.utc),
        }
    ]
    
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
    country_subset: Optional[Iterable[str]] = None,
    technology_subset: Optional[Iterable[str]] = None,
    year_subset: Optional[Iterable[int]] = None,
    sample_config: Optional[SampleConfig] = None,
) -> None:
    """Ingest PatentsView data with optional sample mode."""
    sample_config = sample_config or SampleConfig()
    
    try:
        indicator_id = resolve_indicator_id(session, PATENTSVIEW_CONFIG["indicator"])
    except Exception as e:
        logger.error(f"PATENTSVIEW: Failed to resolve indicator: {e}")
        if sample_config.strict_validation:
            raise IngestionError("PATENTSVIEW", "N/A", f"Indicator resolution failed: {e}")
        return
    
    selected_countries = set(country_subset) if country_subset else set(COUNTRY_UNIVERSE)
    if sample_config.enabled:
        # Limit to 5 countries in sample mode
        selected_countries = set(list(selected_countries)[:5])
    
    # Load CPC classes from catalog (tech themes)
    cpc_classes = list(PATENTSVIEW_CPC_CLASSES.items())
    if technology_subset:
        cpc_classes = [(name, code) for name, code in cpc_classes if name in technology_subset]
    if sample_config.enabled:
        cpc_classes = cpc_classes[:2]  # Limit to 2 CPC classes in sample mode
    
    years = list(range(PATENTSVIEW_CONFIG["start_year"], PATENTSVIEW_CONFIG["end_year"] + 1))
    if year_subset:
        years = [y for y in years if y in set(year_subset)]
    if sample_config.enabled:
        years = years[-sample_config.max_years:]  # Only most recent years in sample mode

    async def _run() -> None:
        for tech_name, cpc_code in cpc_classes:
            for country_id in selected_countries:
                for year in years:
                    try:
                        raw = await fetch_patents(tech_name, country_id, year, sample_config=sample_config)
                        store_raw_payload(
                            session,
                            RawPatentsView,
                            params={"technology": tech_name, "country": country_id, "year": year},
                            payload=raw,
                        )
                        rows = _parse_patents(
                            raw,
                            indicator_id=indicator_id,
                            country_id=country_id,
                            year=year,
                            sample_config=sample_config,
                        )
                        bulk_upsert_timeseries(session, rows)
                        logger.info(f"PATENTSVIEW: Ingested {len(rows)} records for {country_id}/{tech}/{year}")
                    except IngestionError:
                        raise
                    except Exception as e:
                        error_msg = f"Failed for {country_id}/{tech}/{year}: {e}"
                        logger.error(f"PATENTSVIEW: {error_msg}")
                        if sample_config.strict_validation:
                            raise IngestionError("PATENTSVIEW", country_id, error_msg)
                    await asyncio.sleep(0.2)

    asyncio.run(_run())
    session.commit()

