"""
Canonical list of supported country ISO3 codes used across ingestion, feature
engineering, and metrics pipelines. This universe guides fixtures and
validations without duplicating lists in downstream modules.
"""

from typing import List

COUNTRY_UNIVERSE = [
    "USA",
    "CAN",
    "MEX",
    "DEU",
    "FRA",
    "ITA",
    "ESP",
    "NLD",
    "GBR",
    "IRL",
    "CHN",
    "JPN",
    "KOR",
    "IND",
    "IDN",
    "AUS",
    "BRA",
    "ARG",
    "SAU",
    "ARE",
    "TUR",
    "EGY",
    "ZAF",
    "NGA",
    "BEL",
    "CHE",
    "SWE",
    "NOR",
    "DNK",
    "POL",
    "AUT",
    "CZE",
    "HUN",
    "ROU",
    "SGP",
    "MYS",
    "THA",
    "VNM",
    "PHL",
    "PAK",
    "BGD",
    "HKG",
    "TWN",
    "QAT",
    "KWT",
    "ISR",
    "MAR",
    "CHL",
    "COL",
    "PER",
    "KEN",
    "GHA",
    "ETH",
    "NZL",
]


def get_country_universe() -> List[str]:
    """Return a copy of the configured country universe."""

    return list(COUNTRY_UNIVERSE)
