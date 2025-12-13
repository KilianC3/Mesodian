import asyncio
import datetime as dt
import logging
from typing import Any, Dict, Iterable, Optional

from sqlalchemy.orm import Session

from app.config.country_universe import COUNTRY_UNIVERSE
from app.db.models import RawOpenAlex
from app.ingest.base_client import get_provider_client
from app.ingest.sample_mode import (
    SampleConfig,
    validate_timeseries_data,
    IngestionError,
)
from app.ingest.utils import bulk_upsert_timeseries, resolve_indicator_id, store_raw_payload

logger = logging.getLogger(__name__)


# ISO3 to ISO2 country code mapping for OpenAlex API
# OpenAlex uses 2-letter country codes (ISO 3166-1 alpha-2)
ISO3_TO_ISO2 = {
    "USA": "US", "CAN": "CA", "MEX": "MX", "DEU": "DE", "FRA": "FR",
    "ITA": "IT", "ESP": "ES", "NLD": "NL", "GBR": "GB", "IRL": "IE",
    "BEL": "BE", "AUT": "AT", "CHE": "CH", "SWE": "SE", "NOR": "NO",
    "DNK": "DK", "FIN": "FI", "POL": "PL", "CZE": "CZ", "HUN": "HU",
    "ROU": "RO", "BGR": "BG", "HRV": "HR", "SVK": "SK", "SVN": "SI",
    "LTU": "LT", "LVA": "LV", "EST": "EE", "JPN": "JP", "CHN": "CN",
    "KOR": "KR", "IND": "IN", "IDN": "ID", "THA": "TH", "MYS": "MY",
    "SGP": "SG", "PHL": "PH", "VNM": "VN", "AUS": "AU", "NZL": "NZ",
    "ZAF": "ZA", "EGY": "EG", "NGA": "NG", "KEN": "KE", "GHA": "GH",
    "MAR": "MA", "TUN": "TN", "ARG": "AR", "BRA": "BR", "CHL": "CL",
    "COL": "CO", "PER": "PE", "URY": "UY", "VEN": "VE", "TUR": "TR",
    "ISR": "IL", "SAU": "SA", "ARE": "AE", "QAT": "QA", "KWT": "KW",
}


OPENALEX_BASE_URL = "https://api.openalex.org"

OPENALEX_CONFIG: Dict[str, Any] = {
    "concepts": ["C154945302"],
    "indicator": "OPENALEX_WORKS_COUNT",
    "start_year": 2018,
    "end_year": 2023,
}


async def fetch_openalex_works(
    concept_id: str,
    country: str,
    year: int,
    *,
    client=None,
    sample_config: Optional[SampleConfig] = None,
) -> Dict[str, Any]:
    """Fetch OpenAlex works count for a concept/country/year.
    
    API docs: https://docs.openalex.org/
    Endpoint: GET /works?filter={...}
    Returns: {\"meta\": {\"count\": 123}, \"results\": [...]}
    
    Note: OpenAlex uses ISO 3166-1 alpha-2 (2-letter) country codes.
    """
    sample_config = sample_config or SampleConfig()
    if client is None:
        client = get_provider_client("OPENALEX", OPENALEX_BASE_URL)
    
    # Convert ISO3 country code to ISO2 for OpenAlex API
    country_iso2 = ISO3_TO_ISO2.get(country.upper(), country.upper()[:2])
    
    # OpenAlex uses simple GET with filter parameter
    # Only request count, not full works
    per_page = 1  # We only care about meta.count
    params = {
        "filter": f"concepts.id:{concept_id},authorships.institutions.country_code:{country_iso2},publication_year:{year}",
        "per_page": per_page,
    }

    try:
        async with client as http_client:
            return await http_client.get_json("/works", params=params)
    except Exception as e:
        raise IngestionError("OPENALEX", country, f"Fetch error: {e}")


def _parse_openalex(
    raw: Dict[str, Any],
    *,
    indicator_id: int,
    country_id: str,
    year: int,
    sample_config: Optional[SampleConfig] = None,
):
    sample_config = sample_config or SampleConfig()
    results = raw.get("meta", {}) if raw else {}
    count = results.get("count")
    
    if count is None:
        if sample_config.enabled:
            logger.error("OpenAlex count is None for country=%s, year=%d", country_id, year)
            if sample_config.strict_validation:
                raise IngestionError("OPENALEX", country_id, f"No count for year {year}")
        return []
    
    rows = [
        {
            "indicator_id": indicator_id,
            "country_id": country_id,
            "date": dt.date(year, 12, 31),
            "value": float(count),
            "source": "OPENALEX",
            "ingested_at": dt.datetime.now(dt.timezone.utc),
        }
    ]
    
    # Validate before returning
    if sample_config.enabled and rows:
        validation = validate_timeseries_data(
            rows,
            expected_countries=[country_id],
            sample_config=sample_config,
        )
        logger.info("OpenAlex parsed %d records for country=%s", validation.record_count, country_id)
        if sample_config.strict_validation:
            validation.raise_if_invalid()
    
    return rows


def ingest_full(
    session: Session,
    *,
    country_subset: Optional[Iterable[str]] = None,
    concept_subset: Optional[Iterable[str]] = None,
    year_subset: Optional[Iterable[int]] = None,
    sample_config: Optional[SampleConfig] = None,
) -> None:
    sample_config = sample_config or SampleConfig()
    indicator_id = resolve_indicator_id(session, OPENALEX_CONFIG["indicator"])
    selected_countries = set(country_subset) if country_subset else set(COUNTRY_UNIVERSE)
    concepts = [
        c for c in OPENALEX_CONFIG["concepts"] if not concept_subset or c in concept_subset
    ]
    years = list(range(OPENALEX_CONFIG["start_year"], OPENALEX_CONFIG["end_year"] + 1))
    if year_subset:
        years = [y for y in years if y in set(year_subset)]
    
    # Limit in sample mode
    if sample_config.enabled:
        concepts = concepts[:1]
        selected_countries = set(list(selected_countries)[:2])
        years = years[:2]

    async def _run() -> None:
        for concept in concepts:
            for country_id in selected_countries:
                for year in years:
                    try:
                        raw = await fetch_openalex_works(
                            concept,
                            country_id,
                            year,
                            sample_config=sample_config,
                        )
                        store_raw_payload(
                            session,
                            RawOpenAlex,
                            params={"concept": concept, "country": country_id, "year": year},
                            payload=raw,
                        )
                        rows = _parse_openalex(
                            raw,
                            indicator_id=indicator_id,
                            country_id=country_id,
                            year=year,
                            sample_config=sample_config,
                        )
                        bulk_upsert_timeseries(session, rows)
                        await asyncio.sleep(0.2)
                    except IngestionError:
                        raise
                    except Exception as e:
                        logger.error("OpenAlex ingestion failed for %s/%s/%d: %s", country_id, concept, year, e)
                        if sample_config.strict_validation:
                            raise IngestionError("OPENALEX", country_id, f"Ingestion failed: {e}")

    asyncio.run(_run())
    session.commit()

