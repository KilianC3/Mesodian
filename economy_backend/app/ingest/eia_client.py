import asyncio
import datetime as dt
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import yaml
from sqlalchemy.orm import Session

from app.config import get_settings
from app.config.country_universe import COUNTRY_UNIVERSE
from app.db.models import RawEia
from app.ingest.base_client import AsyncHttpClient, ProviderLimits, PROVIDER_LIMITS
from app.ingest.sample_mode import (
    SampleConfig,
    validate_timeseries_data,
    IngestionError,
)
from app.ingest.utils import bulk_upsert_timeseries, ensure_date, resolve_indicator_id, store_raw_payload
from app.ingest.rate_limiter import EIA_LIMITER

logger = logging.getLogger(__name__)


# Configure sensible defaults for EIA
PROVIDER_LIMITS.setdefault("EIA", ProviderLimits(max_retries=4, backoff_base_seconds=0.5, timeout_seconds=20.0))


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
EIA_CONFIG = CATALOG.get("EIA", {}) if isinstance(CATALOG, dict) else {}

# Use series IDs from catalog
EIA_SERIES_CONFIG: Dict[str, Dict[str, Any]] = {
    EIA_CONFIG.get("series_ids", {}).get("BRENT_SPOT", "PET.RBRTE.D"): {
        "canonical_indicator": "BRENT_OIL_SPOT_PRICE",
        "countries": ["WLD"],  # Global/World data
    },
    EIA_CONFIG.get("series_ids", {}).get("WTI_SPOT", "PET.RWTC.D"): {
        "canonical_indicator": "EIA_WTI_PRICE",
        "countries": ["USA"],
    },
    EIA_CONFIG.get("series_ids", {}).get("HENRY_HUB", "NG.RNGWHHD.D"): {
        "canonical_indicator": "HENRY_HUB_GAS_PRICE",
        "countries": ["USA"],  # US natural gas hub
    },
    EIA_CONFIG.get("series_ids", {}).get("WORLD_LIQUIDS_PRODUCTION", "INTL.53-1-WORL-TBPD.A"): {
        "canonical_indicator": "EIA_ENERGY_CONSUMPTION_TOTAL",
        "countries": ["WLD"],  # Global/World data
    },
}


async def fetch_series(series_id: str, *, sample_config: Optional[SampleConfig] = None) -> Dict[str, Any]:
    """Fetch a single EIA series payload from v2 API with optional sample mode limiting.
    
    Correct endpoint: /v2/seriesid/{series_id}
    Returns: {\"response\": {\"data\": [{\"period\": \"2023\", \"value\": 123.4}, ...]}}
    """
    sample_config = sample_config or SampleConfig()
    settings = get_settings()
    
    # EIA v2 API params
    params = {
        "api_key": settings.eia_api_key,
    }
    
    # Add limiting for sample mode - v2 API uses "length" parameter
    if sample_config.enabled:
        params["length"] = str(sample_config.max_records_per_country)
    
    try:
        # Rate limiting: 1000 requests/hour
        EIA_LIMITER.acquire()
        async with AsyncHttpClient("https://api.eia.gov", **PROVIDER_LIMITS["EIA"].__dict__) as client:
            # Correct v2 endpoint path
            return await client.get_json(f"/v2/seriesid/{series_id}", params=params)
    except Exception as e:
        raise IngestionError("EIA", "N/A", f"Fetch error for series {series_id}: {e}")


def _parse_series_payload(
    payload: Dict[str, Any],
    *,
    indicator_id: int,
    country_id: str,
    source: str,
    sample_config: Optional[SampleConfig] = None,
) -> List[Dict[str, Any]]:
    """Parse EIA v2 API response with strict validation.
    
    EIA v2 response: {\"response\": {\"data\": [{\"period\": \"2023-01\", \"value\": 123.4}, ...]}}
    EIA v1 response: {\"series\": [{\"data\": [[\"2023\", 123.4], ...]}]}
    """
    sample_config = sample_config or SampleConfig()
    rows: List[Dict[str, Any]] = []
    
    if not payload:
        if sample_config.fail_on_empty:
            raise IngestionError("EIA", country_id, "Empty payload returned")
        return rows
    
    # Try v2 API structure first
    response = payload.get("response", {})
    data = response.get("data", [])
    
    if data:
        # v2 format: list of dicts with \"period\" and \"value\" keys
        if sample_config.enabled:
            data = data[:sample_config.max_records_per_country]
        
        for entry in data:
            if isinstance(entry, dict):
                period = entry.get("period")
                value = entry.get("value")
                if period and value is not None:
                    try:
                        date = ensure_date(str(period))
                        rows.append({
                            "indicator_id": indicator_id,
                            "country_id": country_id,
                            "date": date,
                            "value": float(value),
                            "source": source,
                            "ingested_at": dt.datetime.now(dt.timezone.utc),
                        })
                    except Exception as exc:
                        logger.error(f"EIA v2: Parse error for {country_id}: {exc}")
                        if sample_config.strict_validation:
                            raise IngestionError("EIA", country_id, f"Parse error: {exc}")
    else:
        # Try v1 structure: array of [date, value] pairs
        for series in payload.get("series", []):
            series_data = series.get("data", [])
            for entry in series_data[:sample_config.max_records_per_country if sample_config.enabled else len(series_data)]:
                if isinstance(entry, (list, tuple)) and len(entry) >= 2:
                    try:
                        date = ensure_date(str(entry[0]))
                        value = float(entry[1])
                        rows.append({
                            "indicator_id": indicator_id,
                            "country_id": country_id,
                            "date": date,
                            "value": value,
                            "source": source,
                            "ingested_at": dt.datetime.now(dt.timezone.utc),
                        })
                    except Exception as exc:
                        logger.error(f"EIA v1: Parse error for {country_id}: {exc}")
                        if sample_config.strict_validation:
                            raise IngestionError("EIA", country_id, f"Parse error: {exc}")
    
    if not rows and sample_config.fail_on_empty:
        raise IngestionError("EIA", country_id, "No parseable data in response")
    
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
    series_subset: Optional[Iterable[str]] = None,
    sample_config: Optional[SampleConfig] = None,
) -> None:
    """Ingest configured EIA series into TimeSeriesValue with optional sample mode."""
    sample_config = sample_config or SampleConfig()
    selected_countries = set(country_subset) if country_subset else None
    selected_series = set(series_subset) if series_subset else None
    
    # Limit series in sample mode
    series_to_fetch = list(EIA_SERIES_CONFIG.items())
    if selected_series:
        series_to_fetch = [(k, v) for k, v in series_to_fetch if k in selected_series]
    if sample_config.enabled:
        series_to_fetch = series_to_fetch[:2]  # Limit to 2 series in sample mode

    async def _run() -> None:
        for series_id, cfg in series_to_fetch:
            try:
                indicator_id = resolve_indicator_id(session, cfg["canonical_indicator"])
            except Exception as e:
                logger.error(f"EIA: Failed to resolve indicator {cfg['canonical_indicator']}: {e}")
                if sample_config.strict_validation:
                    raise IngestionError("EIA", "N/A", f"Indicator resolution failed: {e}")
                continue
            
            countries = cfg.get("countries", COUNTRY_UNIVERSE)
            if selected_countries:
                countries = [c for c in countries if c in selected_countries]
            if sample_config.enabled:
                countries = countries[:3]  # Limit to 3 countries per series in sample mode
            
            for country_id in countries:
                try:
                    payload = await fetch_series(series_id, sample_config=sample_config)
                    store_raw_payload(
                        session,
                        RawEia,
                        params={"series_id": series_id, "country_id": country_id},
                        payload=payload,
                    )
                    rows = _parse_series_payload(
                        payload,
                        indicator_id=indicator_id,
                        country_id=country_id,
                        source="EIA",
                        sample_config=sample_config,
                    )
                    bulk_upsert_timeseries(session, rows)
                    logger.info(f"EIA: Ingested {len(rows)} records for {country_id}/{series_id}")
                except IngestionError:
                    raise
                except Exception as e:
                    error_msg = f"Failed to ingest EIA series {series_id} for {country_id}: {e}"
                    logger.error(error_msg)
                    if sample_config.strict_validation:
                        raise IngestionError("EIA", country_id, error_msg)

    asyncio.run(_run())
    session.commit()

