import asyncio
import datetime as dt
import logging
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy.orm import Session

from app.config import get_settings
from app.config.country_universe import COUNTRY_UNIVERSE
from app.db.models import RawEia
from app.ingest.base_client import AsyncHttpClient, ProviderLimits, PROVIDER_LIMITS
from app.ingest.utils import bulk_upsert_timeseries, ensure_date, resolve_indicator_id, store_raw_payload

logger = logging.getLogger(__name__)


# Configure sensible defaults for EIA
PROVIDER_LIMITS.setdefault("EIA", ProviderLimits(max_retries=4, backoff_base_seconds=0.5, timeout_seconds=20.0))


EIA_SERIES_CONFIG: Dict[str, Dict[str, Any]] = {
    "TOTAL.CONS_TOT.A": {
        "canonical_indicator": "EIA_ENERGY_CONSUMPTION_TOTAL",
        "countries": COUNTRY_UNIVERSE,
    },
    "TOTAL.PROD_TOT.A": {
        "canonical_indicator": "EIA_ENERGY_PRODUCTION_TOTAL",
        "countries": COUNTRY_UNIVERSE,
    },
    "PET.RWTC.D": {
        "canonical_indicator": "EIA_WTI_PRICE",
        "countries": ["USA"],
    },
}


async def fetch_series(series_id: str) -> Dict[str, Any]:
    """Fetch a single EIA series payload."""

    settings = get_settings()
    params = {"api_key": settings.eia_api_key, "series_id": series_id}
    async with AsyncHttpClient("https://api.eia.gov", **PROVIDER_LIMITS["EIA"].__dict__) as client:
        return await client.get_json("/series/", params=params)


def _parse_series_payload(
    payload: Dict[str, Any], *, indicator_id: int, country_id: str, source: str
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for series in payload.get("series", []):
        for entry in series.get("data", []):
            try:
                if not entry or len(entry) < 2:
                    continue
                date = ensure_date(entry[0])
                value = float(entry[1])
            except Exception as exc:  # pragma: no cover - data dependent
                logger.warning("Skipping EIA data point due to parse error: %s", exc)
                continue
            rows.append(
                {
                    "indicator_id": indicator_id,
                    "country_id": country_id,
                    "date": date,
                    "value": value,
                    "source": source,
                    "ingested_at": dt.datetime.utcnow(),
                }
            )
    return rows


def ingest_full(
    session: Session,
    *,
    country_subset: Optional[Iterable[str]] = None,
    series_subset: Optional[Iterable[str]] = None,
) -> None:
    """Ingest configured EIA series into TimeSeriesValue."""

    selected_countries = set(country_subset) if country_subset else None
    selected_series = set(series_subset) if series_subset else None

    async def _run() -> None:
        for series_id, cfg in EIA_SERIES_CONFIG.items():
            if selected_series and series_id not in selected_series:
                continue
            indicator_id = resolve_indicator_id(session, cfg["canonical_indicator"])
            for country_id in cfg.get("countries", COUNTRY_UNIVERSE):
                if selected_countries and country_id not in selected_countries:
                    continue
                try:
                    payload = await fetch_series(series_id)
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
                    )
                    bulk_upsert_timeseries(session, rows)
                except Exception:
                    logger.exception("Failed to ingest EIA series %s for %s", series_id, country_id)

    asyncio.run(_run())
    session.commit()

