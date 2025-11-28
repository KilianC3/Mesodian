import datetime as dt
import logging
from io import StringIO
from typing import Dict, Iterable, Optional

import pandas as pd
from sqlalchemy.orm import Session

from app.config.country_universe import COUNTRY_UNIVERSE
from app.db.models import CountryYearFeatures, RawGdelt
from app.ingest.utils import bulk_upsert_timeseries, resolve_indicator_id, store_raw_payload

logger = logging.getLogger(__name__)


GDELT_EVENTS_URL = "https://api.gdeltproject.org/api/v2/events/geo"


def fetch_gdelt_events(params: Optional[Dict[str, str]] = None) -> pd.DataFrame:
    try:
        import httpx
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("httpx required for GDELT ingestion") from exc

    response = httpx.get(GDELT_EVENTS_URL, params=params or {"format": "csv"}, timeout=60.0)
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

    df = fetch_gdelt_events()
    store_raw_payload(session, RawGdelt, params={}, payload=df.to_dict())

    indicator_events = resolve_indicator_id(session, "GDELT_EVENT_COUNT")
    indicator_tone = resolve_indicator_id(session, "GDELT_GOLDSTEIN")

    rows = []
    stress: Dict[tuple, float] = {}

    for _, row in df.iterrows():
        country = str(row.get("Actor1Geo_CountryCode"))
        if country not in selected_countries:
            continue
        sql_date = str(row.get("SQLDATE"))
        try:
            date = dt.datetime.strptime(sql_date, "%Y%m%d").date()
        except Exception:
            continue
        if selected_years and date.year not in selected_years:
            continue
        rows.append(
            {
                "indicator_id": indicator_events,
                "country_id": country,
                "date": date,
                "value": float(row.get("NumEvents", 0)),
                "source": "GDELT",
                "ingested_at": dt.datetime.utcnow(),
            }
        )
        rows.append(
            {
                "indicator_id": indicator_tone,
                "country_id": country,
                "date": date,
                "value": float(row.get("GoldsteinScale", 0)),
                "source": "GDELT",
                "ingested_at": dt.datetime.utcnow(),
            }
        )
        key = (country, date.year)
        stress[key] = stress.get(key, 0.0) + float(row.get("NumEvents", 0))

    bulk_upsert_timeseries(session, rows)

    for (country, year), value in stress.items():
        feature = (
            session.query(CountryYearFeatures)
            .filter(CountryYearFeatures.country_id == country, CountryYearFeatures.year == year)
            .one_or_none()
        )
        if feature:
            feature.event_stress_pulse = value
        else:
            session.add(CountryYearFeatures(country_id=country, year=year, event_stress_pulse=value))

    session.commit()

