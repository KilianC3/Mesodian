from __future__ import annotations

import logging
from typing import Dict, Iterable, Optional, TYPE_CHECKING

from sqlalchemy.orm import Session

from app.config.country_universe import COUNTRY_UNIVERSE
from app.config.regions import COUNTRY_REGION_MAP, get_region_for_country
from app.db.models import Country
from app.extern.worldbank_income import (
    fetch_worldbank_country_names,
    fetch_worldbank_income_table,
)

if TYPE_CHECKING:
    from app.db.engine import db_session  # pragma: no cover

logger = logging.getLogger(__name__)

# Fallback names to use when the World Bank API lacks entries for a country we track.
FALLBACK_COUNTRY_NAMES: Dict[str, str] = {
    "USA": "United States",
    "CAN": "Canada",
    "MEX": "Mexico",
    "DEU": "Germany",
    "FRA": "France",
    "ITA": "Italy",
    "ESP": "Spain",
    "NLD": "Netherlands",
    "GBR": "United Kingdom",
    "IRL": "Ireland",
    "CHN": "China",
    "JPN": "Japan",
    "KOR": "South Korea",
    "IND": "India",
    "IDN": "Indonesia",
    "AUS": "Australia",
    "BRA": "Brazil",
    "ARG": "Argentina",
    "SAU": "Saudi Arabia",
    "ARE": "United Arab Emirates",
    "TUR": "Turkey",
    "EGY": "Egypt",
    "ZAF": "South Africa",
    "NGA": "Nigeria",
    "BEL": "Belgium",
    "CHE": "Switzerland",
    "SWE": "Sweden",
    "NOR": "Norway",
    "DNK": "Denmark",
    "POL": "Poland",
    "AUT": "Austria",
    "CZE": "Czechia",
    "HUN": "Hungary",
    "ROU": "Romania",
    "SGP": "Singapore",
    "MYS": "Malaysia",
    "THA": "Thailand",
    "VNM": "Vietnam",
    "PHL": "Philippines",
    "PAK": "Pakistan",
    "BGD": "Bangladesh",
    "HKG": "Hong Kong",
    "TWN": "Taiwan",
    "QAT": "Qatar",
    "KWT": "Kuwait",
    "ISR": "Israel",
    "MAR": "Morocco",
    "CHL": "Chile",
    "COL": "Colombia",
    "PER": "Peru",
    "KEN": "Kenya",
    "GHA": "Ghana",
    "ETH": "Ethiopia",
    "NZL": "New Zealand",
}


def _validate_region_coverage(country_codes: Iterable[str]) -> None:
    missing = sorted(code for code in country_codes if code not in COUNTRY_REGION_MAP)
    if missing:
        raise ValueError(
            "Missing region mapping for ISO3 codes: " + ", ".join(missing)
        )


def _resolve_country_name(iso3: str, names_by_iso3: Dict[str, str]) -> str:
    name = names_by_iso3.get(iso3)
    if name:
        return name

    if iso3 in FALLBACK_COUNTRY_NAMES:
        logger.warning("Using fallback country name for %s", iso3)
        return FALLBACK_COUNTRY_NAMES[iso3]

    logger.warning("No country name found for %s; defaulting to ISO code", iso3)
    return iso3


def _upsert_country(
    session: Session,
    *,
    iso3: str,
    name: str,
    region: str,
    income_group: str,
) -> None:
    session.merge(
        Country(
            id=iso3,
            name=name,
            region=region,
            income_group=income_group,
        )
    )


def seed_or_refresh_countries(session: Optional[Session] = None) -> None:
    """Seed or refresh country metadata from configured sources."""

    _validate_region_coverage(COUNTRY_UNIVERSE)

    income_by_iso3 = fetch_worldbank_income_table()
    names_by_iso3 = fetch_worldbank_country_names()

    context = None
    if session is None:
        from app.db.engine import db_session

        context = db_session()
        session = context.__enter__()

    try:
        for iso3 in COUNTRY_UNIVERSE:
            region = get_region_for_country(iso3)
            income_group = income_by_iso3.get(iso3)
            if income_group is None:
                logger.warning(
                    "World Bank income classification missing for %s; using 'Unknown'", iso3
                )
                income_group = "Unknown"

            name = _resolve_country_name(iso3, names_by_iso3)

            _upsert_country(
                session,
                iso3=iso3,
                name=name,
                region=region,
                income_group=income_group,
            )

        session.commit()
    finally:
        if context is not None:
            context.__exit__(None, None, None)


def seed_countries() -> None:
    seed_or_refresh_countries()
    print(f"Seeded {len(COUNTRY_UNIVERSE)} countries into warehouse.country")


if __name__ == "__main__":
    seed_or_refresh_countries()

