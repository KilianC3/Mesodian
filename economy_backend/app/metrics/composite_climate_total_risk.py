"""Composite total climate risk metric."""
from __future__ import annotations

from sqlalchemy.orm import Session

from app.db.models import NodeMetric
from app.metrics.utils import upsert_node_metric


def compute_climate_total_risk_for_year(session: Session, year: int) -> None:
    node_ids = {
        nm.node_id
        for nm in session.query(NodeMetric)
        .filter(NodeMetric.metric_code.in_(["RISKOP_TRANSITION", "PHYSICAL_CLIMATE_WATER_STRESS"]), NodeMetric.as_of_year == year)
        .all()
    }

    for node_id in node_ids:
        transition = (
            session.query(NodeMetric)
            .filter_by(node_id=node_id, metric_code="RISKOP_TRANSITION", as_of_year=year)
            .one_or_none()
        )
        physical = (
            session.query(NodeMetric)
            .filter_by(node_id=node_id, metric_code="PHYSICAL_CLIMATE_WATER_STRESS", as_of_year=year)
            .one_or_none()
        )
        values = [float(m.value) for m in (transition, physical) if m and m.value is not None]
        if not values:
            continue
        composite = sum(values) / len(values)
        upsert_node_metric(session, node_id, "CLIMATE_TOTAL_RISK", year, composite)
    session.commit()
