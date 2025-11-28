import asyncio
import datetime as dt
import logging
from typing import Any, Dict, Iterable, Optional

from sqlalchemy.orm import Session

from app.config.country_universe import COUNTRY_UNIVERSE
from app.db.models import RawOpenAlex
from app.ingest.base_client import get_provider_client
from app.ingest.utils import bulk_upsert_timeseries, resolve_indicator_id, store_raw_payload

logger = logging.getLogger(__name__)


OPENALEX_BASE_URL = "https://api.openalex.org"

OPENALEX_CONFIG: Dict[str, Any] = {
    "concepts": ["C154945302"],
    "indicator": "OPENALEX_WORKS_COUNT",
    "start_year": 2018,
    "end_year": 2023,
}


async def fetch_openalex_works(concept_id: str, country: str, year: int, *, client=None) -> Dict[str, Any]:
    if client is None:
        client = get_provider_client("COMTRADE", OPENALEX_BASE_URL)

    params = {
        "filter": f"concepts.id:{concept_id},authorships.institutions.country_code:{country.lower()},from_publication_date:{year}-01-01,to_publication_date:{year}-12-31",
        "per_page": 1,
    }

    async with client as http_client:
        return await http_client.get_json("/works", params=params)


def _parse_openalex(
    raw: Dict[str, Any], *, indicator_id: int, country_id: str, year: int
):
    results = raw.get("meta", {}) if raw else {}
    count = results.get("count")
    if count is None:
        return []
    return [
        {
            "indicator_id": indicator_id,
            "country_id": country_id,
            "date": dt.date(year, 12, 31),
            "value": float(count),
            "source": "OPENALEX",
            "ingested_at": dt.datetime.utcnow(),
        }
    ]


def ingest_full(
    session: Session,
    *,
    country_subset: Optional[Iterable[str]] = None,
    concept_subset: Optional[Iterable[str]] = None,
    year_subset: Optional[Iterable[int]] = None,
) -> None:
    indicator_id = resolve_indicator_id(session, OPENALEX_CONFIG["indicator"])
    selected_countries = set(country_subset) if country_subset else set(COUNTRY_UNIVERSE)
    concepts = [
        c for c in OPENALEX_CONFIG["concepts"] if not concept_subset or c in concept_subset
    ]
    years = list(range(OPENALEX_CONFIG["start_year"], OPENALEX_CONFIG["end_year"] + 1))
    if year_subset:
        years = [y for y in years if y in set(year_subset)]

    async def _run() -> None:
        for concept in concepts:
            for country_id in selected_countries:
                for year in years:
                    raw = await fetch_openalex_works(concept, country_id, year)
                    store_raw_payload(
                        session,
                        RawOpenAlex,
                        params={"concept": concept, "country": country_id, "year": year},
                        payload=raw,
                    )
                    rows = _parse_openalex(raw, indicator_id=indicator_id, country_id=country_id, year=year)
                    bulk_upsert_timeseries(session, rows)
                    await asyncio.sleep(0.2)

    asyncio.run(_run())
    session.commit()

