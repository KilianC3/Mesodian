"""Utilities for fetching World Bank income classifications."""

from __future__ import annotations

from functools import lru_cache
from typing import Dict, List

import httpx

WB_COUNTRY_API_URL = "https://api.worldbank.org/v2/country/all"


class WorldBankAPIError(RuntimeError):
    """Custom error raised when the World Bank API response is invalid."""


@lru_cache(maxsize=1)
def _fetch_worldbank_countries() -> List[dict]:
    """Fetch all country entries from the World Bank API with pagination."""

    params = {"format": "json", "per_page": 400, "page": 1}
    entries: List[dict] = []

    with httpx.Client(timeout=30.0) as client:
        while True:
            response = client.get(WB_COUNTRY_API_URL, params=params)
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:  # pragma: no cover - network safety
                raise WorldBankAPIError(
                    f"World Bank API request failed with status {response.status_code}: {response.text}"
                ) from exc

            try:
                payload = response.json()
            except ValueError as exc:  # pragma: no cover - network safety
                raise WorldBankAPIError("World Bank API response is not valid JSON") from exc

            if not isinstance(payload, list) or len(payload) != 2:
                raise WorldBankAPIError("Unexpected World Bank API response format")

            metadata, data = payload
            if not isinstance(metadata, dict) or "page" not in metadata or "pages" not in metadata:
                raise WorldBankAPIError("World Bank API metadata missing pagination details")
            if not isinstance(data, list):
                raise WorldBankAPIError("World Bank API data section is not a list")

            entries.extend(data)

            current_page = int(metadata.get("page", 1))
            total_pages = int(metadata.get("pages", 1))
            if current_page >= total_pages:
                break
            params["page"] = current_page + 1

    return entries


def fetch_worldbank_income_table() -> Dict[str, str]:
    """Return a mapping of ISO3 country code to income level label from the World Bank."""

    country_entries = _fetch_worldbank_countries()
    income_by_iso3: Dict[str, str] = {}
    for entry in country_entries:
        iso3 = (entry.get("iso3Code") or "").upper()
        income_level = (entry.get("incomeLevel") or {}).get("value")
        if iso3 and income_level:
            income_by_iso3[iso3] = income_level
    return income_by_iso3


def fetch_worldbank_country_names() -> Dict[str, str]:
    """Return a mapping of ISO3 country code to the World Bank country name."""

    country_entries = _fetch_worldbank_countries()
    names_by_iso3: Dict[str, str] = {}
    for entry in country_entries:
        iso3 = (entry.get("iso3Code") or "").upper()
        name = entry.get("name")
        if iso3 and name:
            names_by_iso3[iso3] = name
    return names_by_iso3

