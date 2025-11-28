import asyncio
import datetime as dt
import logging
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy.orm import Session

from app.config import get_settings
from app.db.models import RawFred
from app.ingest.base_client import get_provider_client
from app.ingest.utils import bulk_upsert_timeseries, ensure_date, resolve_indicator_id, store_raw_payload

logger = logging.getLogger(__name__)


FRED_SERIES_CONFIG: Dict[str, Dict[str, Any]] = {
    "CPIAUCSL": {
        "canonical_indicator": "CPI_USA_MONTHLY",
        "countries": ["USA"],
        "frequency": "M",
        "unit": "Index",
    },
    "UNRATE": {
        "canonical_indicator": "UNEMP_RATE_USA",
        "countries": ["USA"],
        "frequency": "M",
        "unit": "Percent",
    },
}


async def fetch_series(
    series_id: str,
    *,
    observation_start: Optional[str] = None,
    observation_end: Optional[str] = None,
) -> Dict[str, Any]:
    settings = get_settings()
    params = {
        "series_id": series_id,
        "api_key": settings.fred_api_key,
        "file_type": "json",
    }
    if observation_start:
        params["observation_start"] = observation_start
    if observation_end:
        params["observation_end"] = observation_end

    async with get_provider_client("FRED", "https://api.stlouisfed.org/fred") as client:
        return await client.get_json("/series/observations", params=params)


def parse_observations(
    payload: Dict[str, Any],
    *,
    indicator_id: int,
    country_id: str,
    source: str,
) -> List[Dict[str, Any]]:
    observations = payload.get("observations", [])
    rows: List[Dict[str, Any]] = []
    for obs in observations:
        try:
            date = ensure_date(obs.get("date"))
            value_str = obs.get("value")
            if value_str in {"." , None, ""}:
                continue
            value = float(value_str)
        except Exception as exc:  # pragma: no cover - data dependent
            logger.warning("Skipping observation due to parse error: %s", exc)
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
    selected_countries = set(country_subset) if country_subset else None
    selected_series = set(series_subset) if series_subset else None

    async def _run() -> None:
        for series_id, cfg in FRED_SERIES_CONFIG.items():
            if selected_series and series_id not in selected_series:
                continue
            indicator_id = resolve_indicator_id(session, cfg["canonical_indicator"])
            for country_id in cfg.get("countries", []):
                if selected_countries and country_id not in selected_countries:
                    continue
                payload = await fetch_series(series_id)
                store_raw_payload(
                    session,
                    RawFred,
                    params={"series_id": series_id, "country_id": country_id},
                    payload=payload,
                )
                rows = parse_observations(
                    payload,
                    indicator_id=indicator_id,
                    country_id=country_id,
                    source="FRED",
                )
                bulk_upsert_timeseries(session, rows)

    asyncio.run(_run())
    session.commit()

