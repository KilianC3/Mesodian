import asyncio
import datetime as dt
import logging
from typing import Any, Dict, Iterable, Optional

from sqlalchemy.orm import Session

from app.config.country_universe import COUNTRY_UNIVERSE
from app.db.models import RawPatentsView
from app.ingest.base_client import get_provider_client
from app.ingest.utils import bulk_upsert_timeseries, resolve_indicator_id, store_raw_payload

logger = logging.getLogger(__name__)


PATENTSVIEW_BASE_URL = "https://search.patentsview.org/api/v1"

PATENTSVIEW_CONFIG: Dict[str, Any] = {
    "technology_fields": ["computers", "energy"],
    "indicator": "PATENTS_COUNT",
    "start_year": 2018,
    "end_year": 2023,
}


async def fetch_patents(
    technology: str, country: str, year: int, *, client=None
) -> Dict[str, Any]:
    if client is None:
        client = get_provider_client("COMTRADE", PATENTSVIEW_BASE_URL)

    params = {
        "q": {
            "_and": [
                {"app_date": {"from": f"{year}-01-01", "to": f"{year}-12-31"}},
                {"inventor_country": country.upper()},
                {"cpc_subsection_title": {"text": technology}},
            ]
        },
        "f": ["patent_id"],
        "size": 0,
    }

    async with client as http_client:
        return await http_client.get_json("/patents", params=params)


def _parse_patents(
    raw: Dict[str, Any], *, indicator_id: int, country_id: str, year: int
):
    if not raw:
        return []
    total = raw.get("total_patent_count") or raw.get("total_found") or raw.get("total", 0)
    return [
        {
            "indicator_id": indicator_id,
            "country_id": country_id,
            "date": dt.date(year, 12, 31),
            "value": float(total),
            "source": "PATENTSVIEW",
            "ingested_at": dt.datetime.utcnow(),
        }
    ]


def ingest_full(
    session: Session,
    *,
    country_subset: Optional[Iterable[str]] = None,
    technology_subset: Optional[Iterable[str]] = None,
    year_subset: Optional[Iterable[int]] = None,
) -> None:
    indicator_id = resolve_indicator_id(session, PATENTSVIEW_CONFIG["indicator"])
    selected_countries = set(country_subset) if country_subset else set(COUNTRY_UNIVERSE)
    technologies = [
        t
        for t in PATENTSVIEW_CONFIG["technology_fields"]
        if not technology_subset or t in technology_subset
    ]
    years = list(range(PATENTSVIEW_CONFIG["start_year"], PATENTSVIEW_CONFIG["end_year"] + 1))
    if year_subset:
        years = [y for y in years if y in set(year_subset)]

    async def _run() -> None:
        for tech in technologies:
            for country_id in selected_countries:
                for year in years:
                    raw = await fetch_patents(tech, country_id, year)
                    store_raw_payload(
                        session,
                        RawPatentsView,
                        params={"technology": tech, "country": country_id, "year": year},
                        payload=raw,
                    )
                    rows = _parse_patents(raw, indicator_id=indicator_id, country_id=country_id, year=year)
                    bulk_upsert_timeseries(session, rows)
                    await asyncio.sleep(0.2)

    asyncio.run(_run())
    session.commit()

