import asyncio
import datetime as dt
import logging
from typing import Any, Dict, Iterable, List, Optional

import httpx
from sqlalchemy.orm import Session

from app.db.models import RawOns
from app.ingest.utils import bulk_upsert_timeseries, ensure_date, resolve_indicator_id, store_raw_payload

logger = logging.getLogger(__name__)


ONS_SERIES: Dict[str, Dict[str, Any]] = {
    "CPIH": {"canonical_indicator": "CPIH_UK", "frequency": "M"},
    "GDP": {"canonical_indicator": "GDP_GROWTH_UK", "frequency": "Q"},
}


async def fetch_series(series_id: str) -> Dict[str, Any]:
    url = f"https://api.ons.gov.uk/timeseries/{series_id}/dataset/CP/json"
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.get(url)
        response.raise_for_status()
        return response.json()


def parse_ons(
    payload: Dict[str, Any],
    *,
    indicator_id: int,
    country_id: str,
    source: str,
) -> List[Dict[str, Any]]:
    data = payload.get("months") or payload.get("quarters") or payload.get("data") or []
    rows: List[Dict[str, Any]] = []
    for entry in data:
        date_label = entry.get("date") or entry.get("month") or entry.get("quarter")
        value = entry.get("value")
        if date_label is None or value is None:
            continue
        try:
            date = ensure_date(date_label)
            numeric_value = float(value)
        except Exception as exc:  # pragma: no cover - data dependent
            logger.warning("Skipping ONS entry %s: %s", entry, exc)
            continue
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
    return rows


def ingest_full(
    session: Session,
    *,
    series_subset: Optional[Iterable[str]] = None,
) -> None:
    selected_series = set(series_subset) if series_subset else None

    async def _run() -> None:
        for series_id, cfg in ONS_SERIES.items():
            if selected_series and series_id not in selected_series:
                continue
            indicator_id = resolve_indicator_id(session, cfg["canonical_indicator"])
            payload = await fetch_series(series_id)
            store_raw_payload(
                session, RawOns, params={"series_id": series_id}, payload=payload
            )
            rows = parse_ons(
                payload,
                indicator_id=indicator_id,
                country_id="GBR",
                source="ONS",
            )
            bulk_upsert_timeseries(session, rows)

    asyncio.run(_run())
    session.commit()

