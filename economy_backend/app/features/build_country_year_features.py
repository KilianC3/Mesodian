from __future__ import annotations

"""
Country-year feature aggregation for the warehouse layer.

This feature module consolidates macro indicators, shipping intensity, event
stress proxies, and data-quality scores into ``country_year_features`` rows.
It is invoked by offline batch jobs or CLI execution when building training
panels for metrics and graph enrichment.
"""

import datetime as dt
import os
import sys
from typing import Dict, Optional

import pandas as pd
from sqlalchemy.orm import Session

from app.config.country_universe import COUNTRY_UNIVERSE
from app.db.models import CountryYearFeatures, TimeSeriesValue
from app.features.data_quality import compute_data_coverage_score, compute_data_freshness_score
from app.features.shipping_features import get_shipping_features_for_country
from app.features.transforms import compute_yoy_growth
from app.ingest.utils import resolve_indicator_id


FEATURE_INDICATORS: Dict[str, str] = {
    "gdp_real": "GDP_REAL",
    "inflation_cpi": "CPI_YOY",
    "ca_pct_gdp": "CA_PCT_GDP",
    "debt_pct_gdp": "DEBT_PCT_GDP",
    "unemployment_rate": "UNEMP_RATE",
    "co2_per_capita": "CO2_PER_CAPITA",
    "energy_import_dep": "ENERGY_IMPORT_DEP",
    "food_import_dep": "FOOD_IMPORT_DEP",
}

STRESS_INDICATORS = ["GDELT_EVENT_COUNT", "POLICY_RATE_CHANGE_FLAG"]


def _fetch_series(
    session: Session,
    country_id: str,
    canonical_code: str,
    start_date: dt.date,
    end_date: dt.date,
) -> Optional[pd.Series]:
    try:
        indicator_id = resolve_indicator_id(session, canonical_code)
    except ValueError:
        return None

    rows = (
        session.query(TimeSeriesValue)
        .filter(
            TimeSeriesValue.indicator_id == indicator_id,
            TimeSeriesValue.country_id == country_id,
            TimeSeriesValue.date >= start_date,
            TimeSeriesValue.date <= end_date,
        )
        .order_by(TimeSeriesValue.date.asc())
        .all()
    )
    if not rows:
        return None
    dates = pd.to_datetime([row.date for row in rows])
    values = [float(row.value) for row in rows]
    return pd.Series(values, index=dates)


def _get_value_for_year(series: pd.Series, year: int) -> Optional[float]:
    if series is None or series.empty:
        return None
    s = series.sort_index()
    in_year = s[s.index.year == year]
    if not in_year.empty:
        return float(in_year.iloc[-1])
    up_to_year = s[s.index.year <= year]
    if not up_to_year.empty:
        return float(up_to_year.iloc[-1])
    return None


def _update_latest_date(current: Optional[dt.date], series: Optional[pd.Series]) -> Optional[dt.date]:
    if series is None or series.empty:
        return current
    latest = pd.to_datetime(series.index).max().date()
    if current is None or latest > current:
        return latest
    return current


def build_country_year_features(session: Session, year: int) -> None:
    """
    Aggregate yearly country features and persist them to the warehouse.

    Args:
        session: Open SQLAlchemy session bound to the warehouse database.
        year: Target year for which to compute macro, shipping, and quality features.
    """

    start_date = dt.date(year - 5, 1, 1)
    end_date = dt.date(year, 12, 31)
    now = dt.date.today()

    for country_id in COUNTRY_UNIVERSE:
        latest_obs: Optional[dt.date] = None
        computed: Dict[str, Optional[float]] = {}

        # Core macro indicators
        gdp_series = _fetch_series(session, country_id, FEATURE_INDICATORS["gdp_real"], start_date, end_date)
        latest_obs = _update_latest_date(latest_obs, gdp_series)
        gdp_real = _get_value_for_year(gdp_series, year)
        computed["gdp_real"] = gdp_real

        gdp_growth = None
        if gdp_series is not None and not gdp_series.empty:
            yoy = compute_yoy_growth(gdp_series)
            gdp_growth = _get_value_for_year(yoy, year)
            latest_obs = _update_latest_date(latest_obs, yoy)
        computed["gdp_growth"] = gdp_growth

        for feature_name, indicator_code in FEATURE_INDICATORS.items():
            if feature_name == "gdp_real":
                continue  # already handled
            series = _fetch_series(session, country_id, indicator_code, start_date, end_date)
            latest_obs = _update_latest_date(latest_obs, series)
            computed[feature_name] = _get_value_for_year(series, year)

        # Shipping features
        shipping = get_shipping_features_for_country(session, country_id, year)
        computed.update(shipping)

        # Event stress pulse from GDELT and policy events
        event_stress: Optional[float] = None
        for indicator_code in STRESS_INDICATORS:
            series = _fetch_series(session, country_id, indicator_code, start_date, end_date)
            latest_obs = _update_latest_date(latest_obs, series)
            if series is not None:
                year_values = series[series.index.year == year]
                value = float(year_values.sum()) if not year_values.empty else None
                if value is not None:
                    event_stress = (event_stress or 0.0) + value

        # Data quality metrics
        coverage_score = compute_data_coverage_score(computed)
        freshness_score = compute_data_freshness_score(latest_obs, now)

        existing = (
            session.query(CountryYearFeatures)
            .filter(CountryYearFeatures.country_id == country_id, CountryYearFeatures.year == year)
            .one_or_none()
        )
        if existing:
            feature_row = existing
        else:
            feature_row = CountryYearFeatures(country_id=country_id, year=year)
            session.add(feature_row)

        feature_row.gdp_real = computed.get("gdp_real")
        feature_row.gdp_growth = computed.get("gdp_growth")
        feature_row.inflation_cpi = computed.get("inflation_cpi")
        feature_row.ca_pct_gdp = computed.get("ca_pct_gdp")
        feature_row.debt_pct_gdp = computed.get("debt_pct_gdp")
        feature_row.unemployment_rate = computed.get("unemployment_rate")
        feature_row.co2_per_capita = computed.get("co2_per_capita")
        feature_row.energy_import_dep = computed.get("energy_import_dep")
        feature_row.food_import_dep = computed.get("food_import_dep")
        feature_row.shipping_activity_level = computed.get("shipping_activity_level")
        feature_row.shipping_activity_change = computed.get("shipping_activity_change")
        feature_row.event_stress_pulse = event_stress
        feature_row.data_coverage_score = coverage_score
        feature_row.data_freshness_score = freshness_score

    session.commit()


if __name__ == "__main__":
    target_year = dt.date.today().year
    if len(sys.argv) > 1:
        target_year = int(sys.argv[1])
    elif os.getenv("YEAR"):
        target_year = int(os.getenv("YEAR"))

    from app.db.engine import db_session

    with db_session() as session:
        build_country_year_features(session, target_year)
