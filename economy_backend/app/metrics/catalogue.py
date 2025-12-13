"""Canonical metric catalogue for the platform.

This module centralises all metric and index codes that can be written by the
system. Downstream ingestion, feature engineering, cycle construction, and API
layers should import codes from here rather than defining ad-hoc strings. The
catalogue also exposes helper utilities for basic validation and scope lookups.
"""

from __future__ import annotations

from enum import Enum
from typing import Dict, Iterable, Set


class MetricScope(str, Enum):
    """Enumeration of supported metric scopes."""

    GLOBAL = "global"
    REGIONAL = "regional"
    COUNTRY = "country"
    WEB = "web"
    EDGE = "edge"


class Frequency(str, Enum):
    """Frequency helper used across cycles and metrics."""

    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    ANNUAL = "annual"


# Global cycle indices stored in warehouse.global_cycle_index
GLOBAL_INDEX_CODES: Set[str] = {
    "GBC",  # Global Business Cycle Index
    "GTMC",  # Global Trade and Manufacturing Cycle Index
    "COM_ENERGY",  # Global Energy Commodity Cycle Index
    "COM_METALS",  # Global Metals Commodity Cycle Index
    "COM_AGRI",  # Global Agriculture Commodity Cycle Index
    "GIC",  # Global Inflation Cycle Index
    "GFC",  # Global Financial Conditions Index
}


# Regional cycle indices mirroring the global set
REGIONAL_INDEX_CODES: Set[str] = {
    "RBC",
    "RTMC",
    "R_COM_ENERGY",
    "R_COM_METALS",
    "R_COM_AGRI",
    "R_GIC",
    "R_GFC",
}


# Country level resilience, risk, policy and ESG metrics
# Consolidated from 24 → 18 codes (removed redundant composites & sub-components)
COUNTRY_METRIC_CODES: Set[str] = {
    # Core resilience (sub-components moved to metadata)
    "CR_RESILIENCE",
    # Risk indicators
    "RISK_FOOD",
    "RISK_ENERGY",
    "RISKOP_TRANSITION",
    "PHYSICAL_CLIMATE_WATER_STRESS",
    "CRITICAL_IMPORT_DEPENDENCE",
    "CLIMATE_TOTAL_RISK",
    # Household vulnerability (consolidated from 3 redundant metrics)
    "HOUSEHOLD_VULNERABILITY",
    # Macro/policy indicators
    "POLICY_STANCE",
    "RECESSION_INDICATOR",
    "INFLATION_PRESSURE",
    "CREDIT_EXCESS",
    # Data quality
    "DQ_COVERAGE",
    "DQ_FRESHNESS",
    # Network
    "NET_SYS_IMPORTANCE",
    # ESG (keep total only in primary view)
    "ESG_TOTAL_SOV",
}


# Web-level metrics attached to graph webs
# Consolidated: removed theoretical metrics with low actionability
WEB_METRIC_CODES: Set[str] = {
    "WEB_RISK_SCORE",
    "WEB_CONCENTRATION",
    "WEB_STRATEGIC_IMPORTANCE",
    "WEB_RESILIENCE",
}


# Edge-level (relationship) metrics
# Consolidated: removed EDGE_SUBSTITUTABILITY (low signal)
EDGE_METRIC_CODES: Set[str] = {
    "EDGE_DEPENDENCE",
    "EDGE_CRITICALITY",
}


# Analyst-focused equity metrics (new - Week 1)
ANALYST_METRIC_CODES: Set[str] = {
    "EARNINGS_SURPRISE_3M",       # Earnings actual vs estimate deviation (3mo rolling)
    "PE_PERCENTILE",              # P/E ratio percentile vs 5-year history
    "SECTOR_ROTATION_SIGNAL",     # Relative strength: sector vs broad market
    "EM_RISK_PREMIUM",            # EM equity yield spread over US
    "VALUATION_SPREAD",           # Cross-country valuation dispersion
    "SENTIMENT_EXTREME",          # VIX percentile + put/call ratio signal
    "INSIDER_BUYING_SURGE",       # Insider buy/sell ratio acceleration
    "ETF_FLOW_5D",                # 5-day ETF net flows (sector rotation proxy)
    "POLICY_SURPRISE",            # Central bank dovish/hawkish vs consensus
    "LIQUIDITY_SCORE",            # Market depth & bid-ask spread composite
}


METRIC_SCOPE_MAP: Dict[str, MetricScope] = {
    **{code: MetricScope.GLOBAL for code in GLOBAL_INDEX_CODES},
    **{code: MetricScope.REGIONAL for code in REGIONAL_INDEX_CODES},
    **{code: MetricScope.COUNTRY for code in COUNTRY_METRIC_CODES},
    **{code: MetricScope.WEB for code in WEB_METRIC_CODES},
    **{code: MetricScope.EDGE for code in EDGE_METRIC_CODES},
    **{code: MetricScope.COUNTRY for code in ANALYST_METRIC_CODES},  # Analyst metrics are country-scoped
}


def is_valid_metric_code(code: str) -> bool:
    """Return True if the supplied code exists in the canonical catalogue."""

    return code in METRIC_SCOPE_MAP


def get_metric_scope(code: str) -> MetricScope:
    """Lookup the declared scope for a metric code.

    Raises a ValueError if the code is unknown to ensure callers only work with
    catalogue-approved metrics.
    """

    try:
        return METRIC_SCOPE_MAP[code]
    except KeyError as exc:  # pragma: no cover - explicit failure path
        raise ValueError(f"Unknown metric code: {code}") from exc


def iter_all_metric_codes() -> Iterable[str]:
    """Yield all known metric codes.

    This is useful for validation or for populating test fixtures that need a
    complete list of supported metrics.
    """

    return METRIC_SCOPE_MAP.keys()

