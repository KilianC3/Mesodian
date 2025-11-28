from __future__ import annotations

from typing import Dict

from sqlalchemy.orm import Session

from app.config.country_universe import COUNTRY_UNIVERSE
from app.db.engine import db_session
from app.db.models import Country

COUNTRY_METADATA: Dict[str, Dict[str, str]] = {
    "USA": {"name": "United States", "region": "North America", "income_group": "High income"},
    "CAN": {"name": "Canada", "region": "North America", "income_group": "High income"},
    "MEX": {"name": "Mexico", "region": "Latin America & Caribbean", "income_group": "Upper middle income"},
    "DEU": {"name": "Germany", "region": "Europe", "income_group": "High income"},
    "FRA": {"name": "France", "region": "Europe", "income_group": "High income"},
    "ITA": {"name": "Italy", "region": "Europe", "income_group": "High income"},
    "ESP": {"name": "Spain", "region": "Europe", "income_group": "High income"},
    "NLD": {"name": "Netherlands", "region": "Europe", "income_group": "High income"},
    "GBR": {"name": "United Kingdom", "region": "Europe", "income_group": "High income"},
    "IRL": {"name": "Ireland", "region": "Europe", "income_group": "High income"},
    "CHN": {"name": "China", "region": "East Asia & Pacific", "income_group": "Upper middle income"},
    "JPN": {"name": "Japan", "region": "East Asia & Pacific", "income_group": "High income"},
    "KOR": {"name": "Korea, Rep.", "region": "East Asia & Pacific", "income_group": "High income"},
    "IND": {"name": "India", "region": "South Asia", "income_group": "Lower middle income"},
    "IDN": {"name": "Indonesia", "region": "East Asia & Pacific", "income_group": "Lower middle income"},
    "AUS": {"name": "Australia", "region": "East Asia & Pacific", "income_group": "High income"},
    "BRA": {"name": "Brazil", "region": "Latin America & Caribbean", "income_group": "Upper middle income"},
    "ARG": {"name": "Argentina", "region": "Latin America & Caribbean", "income_group": "Upper middle income"},
    "SAU": {"name": "Saudi Arabia", "region": "Middle East & North Africa", "income_group": "High income"},
    "ARE": {"name": "United Arab Emirates", "region": "Middle East & North Africa", "income_group": "High income"},
    "TUR": {"name": "Turkey", "region": "Europe & Central Asia", "income_group": "Upper middle income"},
    "EGY": {"name": "Egypt, Arab Rep.", "region": "Middle East & North Africa", "income_group": "Lower middle income"},
    "ZAF": {"name": "South Africa", "region": "Sub-Saharan Africa", "income_group": "Upper middle income"},
    "NGA": {"name": "Nigeria", "region": "Sub-Saharan Africa", "income_group": "Lower middle income"},
    "BEL": {"name": "Belgium", "region": "Europe", "income_group": "High income"},
    "CHE": {"name": "Switzerland", "region": "Europe", "income_group": "High income"},
    "SWE": {"name": "Sweden", "region": "Europe", "income_group": "High income"},
    "NOR": {"name": "Norway", "region": "Europe", "income_group": "High income"},
    "DNK": {"name": "Denmark", "region": "Europe", "income_group": "High income"},
    "POL": {"name": "Poland", "region": "Europe", "income_group": "High income"},
    "AUT": {"name": "Austria", "region": "Europe", "income_group": "High income"},
    "CZE": {"name": "Czechia", "region": "Europe", "income_group": "High income"},
    "HUN": {"name": "Hungary", "region": "Europe", "income_group": "High income"},
    "ROU": {"name": "Romania", "region": "Europe", "income_group": "Upper middle income"},
    "SGP": {"name": "Singapore", "region": "East Asia & Pacific", "income_group": "High income"},
    "MYS": {"name": "Malaysia", "region": "East Asia & Pacific", "income_group": "Upper middle income"},
    "THA": {"name": "Thailand", "region": "East Asia & Pacific", "income_group": "Upper middle income"},
    "VNM": {"name": "Vietnam", "region": "East Asia & Pacific", "income_group": "Lower middle income"},
    "PHL": {"name": "Philippines", "region": "East Asia & Pacific", "income_group": "Lower middle income"},
    "PAK": {"name": "Pakistan", "region": "South Asia", "income_group": "Lower middle income"},
    "BGD": {"name": "Bangladesh", "region": "South Asia", "income_group": "Lower middle income"},
    "HKG": {"name": "Hong Kong SAR, China", "region": "East Asia & Pacific", "income_group": "High income"},
    "TWN": {"name": "Taiwan", "region": "East Asia & Pacific", "income_group": "High income"},
    "QAT": {"name": "Qatar", "region": "Middle East & North Africa", "income_group": "High income"},
    "KWT": {"name": "Kuwait", "region": "Middle East & North Africa", "income_group": "High income"},
    "ISR": {"name": "Israel", "region": "Middle East & North Africa", "income_group": "High income"},
    "MAR": {"name": "Morocco", "region": "Middle East & North Africa", "income_group": "Lower middle income"},
    "CHL": {"name": "Chile", "region": "Latin America & Caribbean", "income_group": "High income"},
    "COL": {"name": "Colombia", "region": "Latin America & Caribbean", "income_group": "Upper middle income"},
    "PER": {"name": "Peru", "region": "Latin America & Caribbean", "income_group": "Upper middle income"},
    "KEN": {"name": "Kenya", "region": "Sub-Saharan Africa", "income_group": "Lower middle income"},
    "GHA": {"name": "Ghana", "region": "Sub-Saharan Africa", "income_group": "Lower middle income"},
    "ETH": {"name": "Ethiopia", "region": "Sub-Saharan Africa", "income_group": "Low income"},
    "NZL": {"name": "New Zealand", "region": "East Asia & Pacific", "income_group": "High income"},
}


def _validate_metadata() -> None:
    missing = set(COUNTRY_UNIVERSE) - COUNTRY_METADATA.keys()
    if missing:
        missing_codes = ", ".join(sorted(missing))
        raise ValueError(f"Missing country metadata for: {missing_codes}")



def upsert_countries(session: Session) -> None:
    """Insert or update country records to match the configured universe."""

    _validate_metadata()
    for iso3 in COUNTRY_UNIVERSE:
        details = COUNTRY_METADATA[iso3]
        session.merge(
            Country(
                id=iso3,
                name=details["name"],
                region=details["region"],
                income_group=details["income_group"],
            )
        )
    session.commit()



def seed_countries() -> None:
    with db_session() as session:
        upsert_countries(session)
    print(f"Seeded {len(COUNTRY_UNIVERSE)} countries into warehouse.country")


if __name__ == "__main__":
    seed_countries()
