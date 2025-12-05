"""
Static configuration listing the supported country universe.

Used by feature builders and metrics pipelines to iterate deterministically
over the set of countries included in the analytics warehouse.
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
