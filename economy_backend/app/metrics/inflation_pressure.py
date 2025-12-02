"""Inflation pressure score combining prices and labour slack."""
from __future__ import annotations

from sqlalchemy.orm import Session

from app.db.models import CountryYearFeatures
from app.metrics.utils import get_or_create_country_node, upsert_node_metric


def _clamp(value: float) -> float:
    return max(0.0, min(100.0, value))


def compute_inflation_pressure_for_year(session: Session, year: int) -> None:
    rows = (
        session.query(CountryYearFeatures)
        .filter(CountryYearFeatures.year == year)
        .all()
    )
    for row in rows:
        node = get_or_create_country_node(session, row.country_id)
        inflation = float(row.inflation_cpi or 0.0)
        unemployment = float(row.unemployment_rate or 0.0)
        pressure = 10.0 * inflation - 2.0 * max(0.0, unemployment - 4.0)
        upsert_node_metric(session, node.id, "INFLATION_PRESSURE", year, _clamp(pressure))
    session.commit()
