import datetime as dt
import logging
from io import StringIO
from typing import Iterable, Optional

import pandas as pd
from sqlalchemy.orm import Session

from app.config.country_universe import COUNTRY_UNIVERSE
from app.db.models import RawGcp
from app.ingest.utils import bulk_upsert_timeseries, resolve_indicator_id, store_raw_payload

logger = logging.getLogger(__name__)


CO2_DATA_URL = "https://raw.githubusercontent.com/owid/co2-data/master/owid-co2-data.csv"


def fetch_co2_dataset() -> pd.DataFrame:
    try:
        import httpx
    except ImportError as exc:  # pragma: no cover - dependency missing
        raise RuntimeError("httpx is required to fetch CO2 data") from exc

    response = httpx.get(CO2_DATA_URL, timeout=60.0)
    response.raise_for_status()
    return pd.read_csv(StringIO(response.text))


def ingest_full(
    session: Session,
    *,
    country_subset: Optional[Iterable[str]] = None,
    year_subset: Optional[Iterable[int]] = None,
) -> None:
    selected_countries = set(country_subset) if country_subset else set(COUNTRY_UNIVERSE)
    selected_years = set(year_subset) if year_subset else None

    df = fetch_co2_dataset()
    df = df.rename(columns={"iso_code": "country", "year": "date"})
    df = df[df["country"].isin(selected_countries)]
    if selected_years:
        df = df[df["date"].isin(selected_years)]

    indicator_total = resolve_indicator_id(session, "CO2_TOTAL")
    indicator_per_capita = resolve_indicator_id(session, "CO2_PER_CAPITA")

    store_raw_payload(session, RawGcp, params={"type": "co2"}, payload=df.to_dict())

    rows = []
    for _, row in df.iterrows():
        try:
            date = dt.date(int(row["date"]), 12, 31)
        except Exception as exc:  # pragma: no cover - data dependent
            logger.warning("Skipping CO2 row due to bad date: %s", exc)
            continue
        for indicator_id, column in [
            (indicator_total, "co2"),
            (indicator_per_capita, "co2_per_capita"),
        ]:
            value = row.get(column)
            if pd.isna(value):
                continue
            rows.append(
                {
                    "indicator_id": indicator_id,
                    "country_id": row["country"],
                    "date": date,
                    "value": float(value),
                    "source": "OWID_CO2",
                    "ingested_at": dt.datetime.utcnow(),
                }
            )

    bulk_upsert_timeseries(session, rows)
    session.commit()

