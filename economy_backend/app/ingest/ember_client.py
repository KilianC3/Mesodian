import datetime as dt
import logging
from typing import Dict, Iterable, List, Optional

import pandas as pd
from io import StringIO
from sqlalchemy.orm import Session

from app.config.country_universe import COUNTRY_UNIVERSE
from app.db.models import RawEmber
from app.ingest.utils import (
    bulk_upsert_timeseries,
    ensure_date,
    resolve_indicator_id,
    store_raw_payload,
)

logger = logging.getLogger(__name__)


EMBER_FILES: Dict[str, Dict[str, str]] = {
    "yearly_generation": {
        "url": "https://ember-data.s3.eu-west-2.amazonaws.com/Exports/Global/Yearly%20Generation%20v23.1.csv",
        "country_column": "Country code",
        "tech_column": "Technology",
        "value_column": "TWh",
        "date_column": "Year",
    }
}

TECH_TO_INDICATOR: Dict[str, str] = {
    "Solar": "EMBER_ELECTRICITY_SOLAR",
    "Wind": "EMBER_ELECTRICITY_WIND",
    "Coal": "EMBER_ELECTRICITY_COAL",
    "Gas": "EMBER_ELECTRICITY_GAS",
    "Hydro": "EMBER_ELECTRICITY_HYDRO",
}


def fetch_csv(url: str) -> pd.DataFrame:
    try:
        import httpx
    except ImportError as exc:  # pragma: no cover - dependency missing
        raise RuntimeError("httpx is required to fetch Ember data") from exc

    response = httpx.get(url, timeout=30.0)
    response.raise_for_status()
    return pd.read_csv(StringIO(response.text))


def parse_rows(
    df: pd.DataFrame,
    *,
    indicator_map: Dict[str, int],
    selected_countries: Optional[set],
    selected_techs: Optional[set],
) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for _, row in df.iterrows():
        country = str(row.get("country"))
        tech = row.get("technology")
        if tech not in indicator_map:
            continue
        if selected_countries and country not in selected_countries:
            continue
        if selected_techs and tech not in selected_techs:
            continue
        try:
            date = ensure_date(str(row.get("date")))
            value = float(row.get("value"))
        except Exception as exc:  # pragma: no cover - data dependent
            logger.warning("Skipping Ember row due to parse error: %s", exc)
            continue
        rows.append(
            {
                "indicator_id": indicator_map[tech],
                "country_id": country,
                "date": date,
                "value": value,
                "source": "EMBER",
                "ingested_at": dt.datetime.utcnow(),
            }
        )
    return rows


def ingest_full(
    session: Session,
    *,
    country_subset: Optional[Iterable[str]] = None,
    technology_subset: Optional[Iterable[str]] = None,
    file_subset: Optional[Iterable[str]] = None,
) -> None:
    selected_countries = set(country_subset) if country_subset else None
    selected_techs = set(technology_subset) if technology_subset else None
    selected_files = set(file_subset) if file_subset else None

    for file_key, cfg in EMBER_FILES.items():
        if selected_files and file_key not in selected_files:
            continue
        df = fetch_csv(cfg["url"])
        rename_map = {
            cfg["country_column"]: "country",
            cfg["tech_column"]: "technology",
            cfg["value_column"]: "value",
            cfg["date_column"]: "date",
        }
        df = df.rename(columns=rename_map)
        df = df[df["country"].isin(COUNTRY_UNIVERSE)]

        indicator_map: Dict[str, int] = {}
        for tech, canonical in TECH_TO_INDICATOR.items():
            try:
                indicator_map[tech] = resolve_indicator_id(session, canonical)
            except ValueError:
                logger.warning("Skipping Ember tech %s due to missing indicator", tech)

        store_raw_payload(
            session,
            RawEmber,
            params={"file": file_key},
            payload=df.to_dict(),
        )

        rows = parse_rows(
            df,
            indicator_map=indicator_map,
            selected_countries=selected_countries,
            selected_techs=selected_techs,
        )
        bulk_upsert_timeseries(session, rows)

    session.commit()

