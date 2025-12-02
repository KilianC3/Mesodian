"""Household financial stress score combining unemployment and inflation."""
from __future__ import annotations

from sqlalchemy.orm import Session

from app.db.models import CountryYearFeatures
from app.metrics.utils import get_or_create_country_node, upsert_node_metric


def _clamp(value: float) -> float:
    return max(0.0, min(100.0, value))


def compute_household_stress_for_year(session: Session, year: int) -> None:
    rows = (
        session.query(CountryYearFeatures)
        .filter(CountryYearFeatures.year == year)
        .all()
    )
    for row in rows:
        node = get_or_create_country_node(session, row.country_id)
        inflation = float(row.inflation_cpi or 0.0)
        unemployment = float(row.unemployment_rate or 0.0)
        stress = 0.6 * inflation + 0.4 * unemployment
        upsert_node_metric(session, node.id, "HH_STRESS", year, _clamp(stress))
    session.commit()
