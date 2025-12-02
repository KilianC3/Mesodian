"""Policy stance score inspired by Taylor-rule gaps."""
from __future__ import annotations

from sqlalchemy.orm import Session

from app.db.models import CountryYearFeatures
from app.metrics.utils import get_or_create_country_node, upsert_node_metric


def _clamp(value: float) -> float:
    return max(0.0, min(100.0, value))


def compute_policy_stance_for_year(session: Session, year: int) -> None:
    rows = (
        session.query(CountryYearFeatures)
        .filter(CountryYearFeatures.year == year)
        .all()
    )
    for row in rows:
        node = get_or_create_country_node(session, row.country_id)
        inflation_gap = float(row.inflation_cpi or 0.0) - 2.0
        growth_gap = float(row.gdp_growth or 0.0) - 2.0
        stance = 50.0 + 5.0 * growth_gap - 3.0 * inflation_gap
        upsert_node_metric(session, node.id, "POLICY_STANCE", year, _clamp(stance))
    session.commit()
