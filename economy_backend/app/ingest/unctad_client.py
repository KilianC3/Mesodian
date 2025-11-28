import datetime as dt
import logging
from typing import Any, Dict, Iterable, Optional

import httpx
import pandas as pd
from sqlalchemy.orm import Session

from app.config.country_universe import COUNTRY_UNIVERSE
from app.db.models import RawUnctad
from app.ingest.utils import (
    bulk_upsert_timeseries,
    resolve_indicator_id,
    store_raw_payload,
)

logger = logging.getLogger(__name__)


UNCTAD_BASE_URL = "https://unctadstat.unctad.org/EN/DataDownload/"

UNCTAD_SERIES: Dict[str, Dict[str, Any]] = {
    "FDI_FLOW_INWARD": {
        "indicator": "FDI_FLOWS_INWARD",
        "canonical_indicator": "UNCTAD_FDI_FLOW_INWARD",
    },
    "FDI_STOCK_INWARD": {
        "indicator": "FDI_STOCKS_INWARD",
        "canonical_indicator": "UNCTAD_FDI_STOCK_INWARD",
    },
}


def fetch_unctad(indicator: str, country: str) -> Dict[str, Any]:
    url = f"https://unctadstat.unctad.org/EN/DownloadHandler.ashx"
    params = {"fileName": indicator, "country": country}
    response = httpx.get(url, params=params, timeout=30.0)
    response.raise_for_status()
    return response.json()


def _unctad_to_dataframe(raw: Dict[str, Any]) -> pd.DataFrame:
    data = raw.get("data", []) if raw else []
    return pd.DataFrame(data)


def _parse_unctad(df: pd.DataFrame, *, indicator_id: int, country_id: str, source: str):
    rows = []
    if df is None or df.empty:
        return rows
    for _, row in df.iterrows():
        if str(row.get("Country")) != country_id:
            continue
        year = row.get("Year")
        value = row.get("Value")
        if year is None or value is None:
            continue
        try:
            date = dt.date(int(year), 12, 31)
            numeric_value = float(value)
        except Exception as exc:  # pragma: no cover - data dependent
            logger.warning("Skipping UNCTAD row %s: %s", row, exc)
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
    indicator_subset: Optional[Iterable[str]] = None,
    country_subset: Optional[Iterable[str]] = None,
) -> None:
    selected_indicators = set(indicator_subset) if indicator_subset else None
    selected_countries = set(country_subset) if country_subset else None

    for key, cfg in UNCTAD_SERIES.items():
        if selected_indicators and key not in selected_indicators:
            continue
        indicator_id = resolve_indicator_id(session, cfg["canonical_indicator"])
        for country_id in COUNTRY_UNIVERSE:
            if selected_countries and country_id not in selected_countries:
                continue
            raw = fetch_unctad(cfg["indicator"], country_id)
            store_raw_payload(
                session,
                RawUnctad,
                params={"indicator": key, "country": country_id},
                payload=raw,
            )
            df = _unctad_to_dataframe(raw)
            rows = _parse_unctad(df, indicator_id=indicator_id, country_id=country_id, source="UNCTAD")
            bulk_upsert_timeseries(session, rows)

    session.commit()

