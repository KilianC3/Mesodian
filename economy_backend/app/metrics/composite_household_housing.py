"""Composite metric combining household and housing stress signals."""
from __future__ import annotations

from sqlalchemy.orm import Session

from app.db.models import NodeMetric
from app.metrics.utils import upsert_node_metric


def compute_household_housing_composite_for_year(session: Session, year: int) -> None:
    node_ids = {
        nm.node_id
        for nm in session.query(NodeMetric)
        .filter(NodeMetric.metric_code.in_(["HOUSING_STRESS", "HH_STRESS"]), NodeMetric.as_of_year == year)
        .all()
    }

    for node_id in node_ids:
        housing = (
            session.query(NodeMetric)
            .filter_by(node_id=node_id, metric_code="HOUSING_STRESS", as_of_year=year)
            .one_or_none()
        )
        household = (
            session.query(NodeMetric)
            .filter_by(node_id=node_id, metric_code="HH_STRESS", as_of_year=year)
            .one_or_none()
        )
        values = [float(m.value) for m in (housing, household) if m and m.value is not None]
        if not values:
            continue
        composite = sum(values) / len(values)
        upsert_node_metric(session, node_id, "HOUSEHOLD_HOUSING_STRESS", year, composite)
    session.commit()
