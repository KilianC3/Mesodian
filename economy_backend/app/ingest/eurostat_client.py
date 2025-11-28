import asyncio
import datetime as dt
import logging
from typing import Any, Dict, Iterable, List, Optional

import httpx
from sqlalchemy.orm import Session

from app.db.models import RawEurostat
from app.ingest.utils import bulk_upsert_timeseries, ensure_date, resolve_indicator_id, store_raw_payload

logger = logging.getLogger(__name__)


EUROSTAT_SERIES: Dict[str, Dict[str, Any]] = {
    "teicp010": {
        "canonical_indicator": "HICP_YOY",
        "frequency": "M",
        "countries": ["DEU", "FRA", "ITA", "ESP", "NLD", "BEL"],
    },
    "teina011": {
        "canonical_indicator": "GDP_GROWTH_QOQ",
        "frequency": "Q",
        "countries": ["DEU", "FRA", "ITA", "ESP"],
    },
}


async def fetch_series(dataset: str, country: str) -> Dict[str, Any]:
    url = f"https://ec.europa.eu/eurostat/api/discover/tables/{dataset}"
    params = {"geo": country, "format": "JSON"}
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.get(url, params=params)
        response.raise_for_status()
        return response.json()


def parse_eurostat(
    payload: Dict[str, Any],
    *,
    indicator_id: int,
    country_id: str,
    source: str,
) -> List[Dict[str, Any]]:
    value_map = payload.get("value", {}) if isinstance(payload, dict) else {}
    dimension = payload.get("dimension", {}) if isinstance(payload, dict) else {}
    time_labels = dimension.get("time", {}).get("category", {}).get("label", {})
    rows: List[Dict[str, Any]] = []
    for idx_str, value in value_map.items():
        try:
            time_key = time_labels[str(idx_str)]
            date = ensure_date(time_key)
            numeric_value = float(value)
        except Exception as exc:  # pragma: no cover - data dependent
            logger.warning("Failed to parse Eurostat point %s: %s", idx_str, exc)
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
    series_subset: Optional[Iterable[str]] = None,
) -> None:
    selected_series = set(series_subset) if series_subset else None
    selected_countries = set(country_subset) if country_subset else None

    async def _run() -> None:
        for dataset_code, cfg in EUROSTAT_SERIES.items():
            if selected_series and dataset_code not in selected_series:
                continue
            indicator_id = resolve_indicator_id(session, cfg["canonical_indicator"])
            for country_id in cfg.get("countries", []):
                if selected_countries and country_id not in selected_countries:
                    continue
                payload = await fetch_series(dataset_code, country_id)
                store_raw_payload(
                    session,
                    RawEurostat,
                    params={"dataset": dataset_code, "country": country_id},
                    payload=payload,
                )
                rows = parse_eurostat(
                    payload,
                    indicator_id=indicator_id,
                    country_id=country_id,
                    source="EUROSTAT",
                )
                bulk_upsert_timeseries(session, rows)

    asyncio.run(_run())
    session.commit()

