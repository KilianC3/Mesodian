"""Housing stress metric derived from macro imbalances."""
from __future__ import annotations

from sqlalchemy.orm import Session

from app.db.models import CountryYearFeatures
from app.metrics.utils import get_or_create_country_node, upsert_node_metric


def _clamp(value: float) -> float:
    return max(0.0, min(100.0, value))


def compute_housing_stress_for_year(session: Session, year: int) -> None:
    """Compute HOUSING_STRESS using inflation and growth proxies."""

    rows = (
        session.query(CountryYearFeatures)
        .filter(CountryYearFeatures.year == year)
        .all()
    )
    for row in rows:
        node = get_or_create_country_node(session, row.country_id)
        inflation = float(row.inflation_cpi or 0.0)
        growth = float(row.gdp_growth or 0.0)
        debt = float(row.debt_pct_gdp or 0.0)
        stress = 0.5 * inflation + 0.3 * max(0.0, -growth) + 0.2 * max(0.0, (debt - 50.0) / 2)
        upsert_node_metric(session, node.id, "HOUSING_STRESS", year, _clamp(stress))
    session.commit()
