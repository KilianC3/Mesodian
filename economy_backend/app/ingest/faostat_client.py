import datetime as dt
import logging
from typing import Any, Dict, Iterable, List, Optional

import httpx
import pandas as pd
from sqlalchemy.orm import Session

from app.config.country_universe import COUNTRY_UNIVERSE
from app.db.models import RawFaostat
from app.ingest.utils import (
    bulk_upsert_timeseries,
    ensure_date,
    resolve_indicator_id,
    store_raw_payload,
)

logger = logging.getLogger(__name__)


FAOSTAT_BASE_URL = "https://fenixservices.fao.org/faostat/api/v1/en"

FAOSTAT_CONFIG: Dict[str, Dict[str, Any]] = {
    "production": {
        "domain": "QCL",
        "element_code": "5510",  # production
        "item_codes": ["15", "27"],  # cereals, meat examples
        "canonical_indicator": "FAOSTAT_PRODUCTION",
    },
    "yield": {
        "domain": "QCL",
        "element_code": "5419",
        "item_codes": ["15"],
        "canonical_indicator": "FAOSTAT_YIELD",
    },
}


def fetch_faostat(domain: str, params: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{FAOSTAT_BASE_URL}/{domain}"
    response = httpx.get(url, params=params, timeout=30.0)
    response.raise_for_status()
    return response.json()


def _faostat_payload_to_dataframe(raw: Dict[str, Any]) -> pd.DataFrame:
    data = raw.get("data", []) if raw else []
    return pd.DataFrame(data)


def _parse_faostat(
    df: pd.DataFrame, *, indicator_id: int, country_id: str, source: str
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if df is None or df.empty:
        return rows

    for _, row in df.iterrows():
        if str(row.get("Area Code (M49)", "")).upper() != country_id.upper():
            continue
        value = row.get("Value")
        year = row.get("Year") or row.get("year")
        if value is None or year is None:
            continue
        try:
            date = ensure_date(f"{int(year)}-12-31")
            numeric_value = float(value)
        except Exception as exc:  # pragma: no cover - data dependent
            logger.warning("Skipping FAOSTAT row %s: %s", row, exc)
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

    for name, cfg in FAOSTAT_CONFIG.items():
        if selected_indicators and name not in selected_indicators:
            continue
        indicator_id = resolve_indicator_id(session, cfg["canonical_indicator"])
        for country_id in COUNTRY_UNIVERSE:
            if selected_countries and country_id not in selected_countries:
                continue
            params = {
                "area_code": country_id,
                "item_code": ",".join(cfg["item_codes"]),
                "element_code": cfg["element_code"],
                "show_codes": True,
            }
            raw = fetch_faostat(cfg["domain"], params=params)
            store_raw_payload(
                session,
                RawFaostat,
                params={"indicator": name, "country": country_id},
                payload=raw,
            )
            df = _faostat_payload_to_dataframe(raw)
            rows = _parse_faostat(df, indicator_id=indicator_id, country_id=country_id, source="FAOSTAT")
            bulk_upsert_timeseries(session, rows)

    session.commit()

