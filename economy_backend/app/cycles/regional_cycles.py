"""Regional cycle utilities built as lightweight aggregates of country data."""
from __future__ import annotations

import datetime as dt
from typing import Dict, Iterable

import pandas as pd
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.db.models import Country, CountryYearFeatures, GlobalCycleIndex
from app.metrics.catalogue import Frequency


def _write_cycle(
    session: Session,
    date: dt.date,
    frequency: Frequency,
    scope: str,
    region: str,
    cycle_type: str,
    score: float,
    method_version: str,
) -> None:
    existing = (
        session.query(GlobalCycleIndex)
        .filter(
            GlobalCycleIndex.date == date,
            GlobalCycleIndex.frequency == frequency.value,
            GlobalCycleIndex.scope == scope,
            GlobalCycleIndex.region == region if hasattr(GlobalCycleIndex, "region") else True,
            GlobalCycleIndex.cycle_type == cycle_type,
            GlobalCycleIndex.method_version == method_version,
        )
        .one_or_none()
    )
    if existing:
        existing.cycle_score = score
        existing.cycle_regime = "expansion" if score >= 0 else "contraction"
    else:
        row = GlobalCycleIndex(
            date=date,
            frequency=frequency.value,
            scope=scope,
            cycle_type=cycle_type,
            cycle_score=score,
            cycle_regime="expansion" if score >= 0 else "contraction",
            method_version=method_version,
            coverage_gdp_share=None,
        )
        session.add(row)


def compute_regional_cycles(session: Session, year: int) -> None:
    countries = session.query(Country).all()
    if not countries:
        return
    by_region: Dict[str, Iterable[str]] = {}
    for country in countries:
        by_region.setdefault(country.region, set()).add(country.id)

    for region, codes in by_region.items():
        rows = (
            session.query(CountryYearFeatures)
            .filter(CountryYearFeatures.country_id.in_(list(codes)), CountryYearFeatures.year == year)
            .all()
        )
        if not rows:
            continue
        gdp_growth = pd.Series({row.country_id: float(row.gdp_growth or 0.0) for row in rows})
        inflation = pd.Series({row.country_id: float(row.inflation_cpi or 0.0) for row in rows})
        trade = pd.Series({row.country_id: float(row.shipping_activity_change or 0.0) for row in rows})

        date = dt.date(year, 12, 31)
        _write_cycle(session, date, Frequency.ANNUAL, "region", region, "business", gdp_growth.mean(), "v1")
        _write_cycle(session, date, Frequency.ANNUAL, "region", region, "inflation", inflation.mean(), "v1")
        _write_cycle(session, date, Frequency.ANNUAL, "region", region, "trade", trade.mean(), "v1")
    session.commit()


def compute_all_regional_cycles(session: Session, year: int) -> None:
    compute_regional_cycles(session, year)
