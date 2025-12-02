"""Yield-curve style recession indicator using growth momentum."""
from __future__ import annotations

from sqlalchemy.orm import Session

from app.db.models import CountryYearFeatures
from app.metrics.utils import get_or_create_country_node, upsert_node_metric


def _clamp(value: float) -> float:
    return max(0.0, min(100.0, value))


def compute_recession_indicator_for_year(session: Session, year: int) -> None:
    rows = (
        session.query(CountryYearFeatures)
        .filter(CountryYearFeatures.year == year)
        .all()
    )
    for row in rows:
        node = get_or_create_country_node(session, row.country_id)
        growth = float(row.gdp_growth or 0.0)
        unemployment = float(row.unemployment_rate or 0.0)
        probability = 50.0 - 5.0 * growth + 2.0 * unemployment
        upsert_node_metric(session, node.id, "RECESSION_INDICATOR", year, _clamp(probability))
    session.commit()
