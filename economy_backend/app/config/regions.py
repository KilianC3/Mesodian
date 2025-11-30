"""Custom region groupings and country mappings."""

from __future__ import annotations

from typing import Dict, Tuple

REGION_LABELS: Tuple[str, ...] = (
    "North America",
    "Latin America and Caribbean",
    "Western and Northern Europe",
    "Central and Eastern Europe",
    "Sub Saharan Africa",
    "Middle East and North Africa",
    "Advanced Asia",
    "Emerging Asia",
    "Oceania",
)

COUNTRY_REGION_MAP: Dict[str, str] = {
    # North America
    "USA": "North America",
    "CAN": "North America",
    "MEX": "North America",
    # Latin America and Caribbean
    "BRA": "Latin America and Caribbean",
    "ARG": "Latin America and Caribbean",
    "CHL": "Latin America and Caribbean",
    "COL": "Latin America and Caribbean",
    "PER": "Latin America and Caribbean",
    # Western and Northern Europe
    "GBR": "Western and Northern Europe",
    "IRL": "Western and Northern Europe",
    "DEU": "Western and Northern Europe",
    "FRA": "Western and Northern Europe",
    "ITA": "Western and Northern Europe",
    "ESP": "Western and Northern Europe",
    "NLD": "Western and Northern Europe",
    "BEL": "Western and Northern Europe",
    "CHE": "Western and Northern Europe",
    "SWE": "Western and Northern Europe",
    "NOR": "Western and Northern Europe",
    "DNK": "Western and Northern Europe",
    "AUT": "Western and Northern Europe",
    "GRC": "Western and Northern Europe",
    # Central and Eastern Europe
    "POL": "Central and Eastern Europe",
    "CZE": "Central and Eastern Europe",
    "HUN": "Central and Eastern Europe",
    "ROU": "Central and Eastern Europe",
    "RUS": "Central and Eastern Europe",
    # Sub Saharan Africa
    "ZAF": "Sub Saharan Africa",
    "NGA": "Sub Saharan Africa",
    "KEN": "Sub Saharan Africa",
    "GHA": "Sub Saharan Africa",
    "ETH": "Sub Saharan Africa",
    "AGO": "Sub Saharan Africa",
    # Middle East and North Africa
    "SAU": "Middle East and North Africa",
    "ARE": "Middle East and North Africa",
    "QAT": "Middle East and North Africa",
    "KWT": "Middle East and North Africa",
    "ISR": "Middle East and North Africa",
    "TUR": "Middle East and North Africa",
    "EGY": "Middle East and North Africa",
    "MAR": "Middle East and North Africa",
    "IRN": "Middle East and North Africa",
    "IRQ": "Middle East and North Africa",
    "DZA": "Middle East and North Africa",
    # Advanced Asia
    "JPN": "Advanced Asia",
    "KOR": "Advanced Asia",
    "HKG": "Advanced Asia",
    "TWN": "Advanced Asia",
    "SGP": "Advanced Asia",
    # Emerging Asia
    "CHN": "Emerging Asia",
    "IND": "Emerging Asia",
    "IDN": "Emerging Asia",
    "MYS": "Emerging Asia",
    "THA": "Emerging Asia",
    "VNM": "Emerging Asia",
    "PHL": "Emerging Asia",
    "PAK": "Emerging Asia",
    "BGD": "Emerging Asia",
    # Oceania
    "AUS": "Oceania",
    "NZL": "Oceania",
}


def get_region_for_country(iso3: str) -> str:
    """Return the custom region label for the provided ISO3 code."""

    iso3_upper = iso3.upper()
    if iso3_upper not in COUNTRY_REGION_MAP:
        raise ValueError(f"No region mapping defined for ISO3 code '{iso3}'")
    return COUNTRY_REGION_MAP[iso3_upper]

