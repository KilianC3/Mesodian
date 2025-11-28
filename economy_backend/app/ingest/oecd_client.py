import asyncio
import datetime as dt
import logging
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy.orm import Session

from app.db.models import RawOecd
from app.ingest.base_client import fetch_sdmx_dataset
from app.ingest.utils import bulk_upsert_timeseries, ensure_date, resolve_indicator_id, store_raw_payload

logger = logging.getLogger(__name__)


OECD_BASE_URL = "https://stats.oecd.org/sdmx-json"

OECD_SERIES: Dict[str, Dict[str, Any]] = {
    "DP_LIVE/.GDP.A": {
        "canonical_indicator": "GDP_PER_CAPITA_OECD",
        "countries": ["USA", "CAN", "GBR", "FRA", "DEU", "JPN"],
    },
    "MEI_CPI/M.CPI.TOT.AGRWTH.Q": {
        "canonical_indicator": "CPI_YOY_OECD",
        "countries": ["USA", "CAN", "GBR", "FRA", "DEU", "JPN"],
    },
}


def parse_oecd(
    df: Any,
    *,
    indicator_id: int,
    country_id: str,
    source: str,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if df is None:
        return rows
    for _, row in df.iterrows():  # type: ignore[call-arg]
        location = row.get("LOCATION") or row.get("geo")
        if location and str(location).upper() != country_id.upper():
            continue
        time_value = row.get("time")
        value = row.get("value")
        if time_value is None or value is None:
            continue
        try:
            date = ensure_date(time_value)
            numeric_value = float(value)
        except Exception as exc:  # pragma: no cover - data dependent
            logger.warning("Skipping OECD row %s: %s", row, exc)
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
    selected_countries = set(country_subset) if country_subset else None
    selected_series = set(series_subset) if series_subset else None

    async def _run() -> None:
        for dataset_code, cfg in OECD_SERIES.items():
            if selected_series and dataset_code not in selected_series:
                continue
            indicator_id = resolve_indicator_id(session, cfg["canonical_indicator"])
            for country_id in cfg.get("countries", []):
                if selected_countries and country_id not in selected_countries:
                    continue
                df = fetch_sdmx_dataset(OECD_BASE_URL, dataset_code, params={"contentType": "csv"})
                store_raw_payload(
                    session,
                    RawOecd,
                    params={"dataset": dataset_code, "country": country_id},
                    payload=df.to_dict() if hasattr(df, "to_dict") else None,
                )
                rows = parse_oecd(
                    df,
                    indicator_id=indicator_id,
                    country_id=country_id,
                    source="OECD",
                )
                bulk_upsert_timeseries(session, rows)

    asyncio.run(_run())
    session.commit()

