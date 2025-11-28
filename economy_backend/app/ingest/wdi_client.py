import asyncio
import datetime as dt
import logging
from typing import Any, Dict, Iterable, List, Optional

import httpx
from sqlalchemy.orm import Session

from app.config.country_universe import COUNTRY_UNIVERSE
from app.db.models import RawWdi
from app.ingest.utils import bulk_upsert_timeseries, ensure_date, resolve_indicator_id, store_raw_payload

logger = logging.getLogger(__name__)


WDI_INDICATORS: Dict[str, Dict[str, str]] = {
    "NY.GDP.MKTP.KD": {"canonical_indicator": "GDP_REAL"},
    "FP.CPI.TOTL.ZG": {"canonical_indicator": "CPI_YOY"},
    "SL.UEM.TOTL.ZS": {"canonical_indicator": "UNEMP_RATE"},
}


async def fetch_indicator(country: str, indicator: str) -> Dict[str, Any]:
    url = f"https://api.worldbank.org/v2/country/{country}/indicator/{indicator}"
    params = {"format": "json", "per_page": 20000}
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.get(url, params=params)
        response.raise_for_status()
        return response.json()


def parse_wdi(
    payload: Dict[str, Any],
    *,
    indicator_id: int,
    country_id: str,
    source: str,
) -> List[Dict[str, Any]]:
    if not isinstance(payload, list) or len(payload) < 2:
        return []
    data = payload[1] or []
    rows: List[Dict[str, Any]] = []
    for entry in data:
        value = entry.get("value")
        year = entry.get("date")
        if value is None or year is None:
            continue
        try:
            date = ensure_date(f"{year}-12-31")
            numeric_value = float(value)
        except Exception as exc:  # pragma: no cover - data dependent
            logger.warning("Failed to parse WDI entry %s: %s", entry, exc)
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
    country_subset: Optional[Iterable[str]] = None,
    indicator_subset: Optional[Iterable[str]] = None,
) -> None:
    selected_countries = set(country_subset) if country_subset else set(COUNTRY_UNIVERSE)
    selected_indicators = set(indicator_subset) if indicator_subset else set(WDI_INDICATORS)

    async def _run() -> None:
        for indicator_code, cfg in WDI_INDICATORS.items():
            if indicator_code not in selected_indicators:
                continue
            indicator_id = resolve_indicator_id(session, cfg["canonical_indicator"])
            for country_id in selected_countries:
                payload = await fetch_indicator(country_id.lower(), indicator_code)
                store_raw_payload(
                    session,
                    RawWdi,
                    params={"indicator": indicator_code, "country": country_id},
                    payload=payload,
                )
                rows = parse_wdi(
                    payload,
                    indicator_id=indicator_id,
                    country_id=country_id,
                    source="WDI",
                )
                bulk_upsert_timeseries(session, rows)

    asyncio.run(_run())
    session.commit()

