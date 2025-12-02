"""Credit excess metric using debt ratios as a proxy."""
from __future__ import annotations

from sqlalchemy.orm import Session

from app.db.models import CountryYearFeatures
from app.metrics.utils import get_or_create_country_node, upsert_node_metric


def _clamp(value: float) -> float:
    return max(0.0, min(100.0, value))


def compute_credit_excess_for_year(session: Session, year: int) -> None:
    rows = (
        session.query(CountryYearFeatures)
        .filter(CountryYearFeatures.year == year)
        .all()
    )
    for row in rows:
        node = get_or_create_country_node(session, row.country_id)
        debt = float(row.debt_pct_gdp or 0.0)
        credit_excess = _clamp(debt / 2.0)
        upsert_node_metric(session, node.id, "CREDIT_EXCESS", year, credit_excess)
    session.commit()
