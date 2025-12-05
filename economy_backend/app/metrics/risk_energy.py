"""
Country-level energy dependence and diversification metrics built from trade
flows and country feature panels. Outputs populate `node_metric` for
downstream dashboards and web scoring.
"""

from __future__ import annotations

from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.db.models import CountryYearFeatures, NodeMetric, TradeFlow
from app.metrics.utils import get_or_create_country_node


def _compute_trade_concentration(
    session: Session, country_id: str, year: int, *, hs_section: Optional[str] = None
) -> Optional[float]:
    query = (
        session.query(TradeFlow.partner_country_id, func.sum(TradeFlow.value_usd))
        .filter(TradeFlow.reporter_country_id == country_id, TradeFlow.year == year)
        .group_by(TradeFlow.partner_country_id)
    )
    if hs_section:
        query = query.filter(TradeFlow.hs_section == hs_section)

    rows = query.all()
    totals = [float(val) for _, val in rows if val is not None and float(val) > 0]
    total_value = sum(totals)
    if total_value <= 0:
        return None
    return sum((val / total_value) ** 2 for val in totals)


def _clamp_unit(value: float) -> float:
    return min(1.0, max(0.0, value))


def compute_energy_risk_for_year(session: Session, year: int) -> None:
    rows = (
        session.query(CountryYearFeatures)
        .filter(CountryYearFeatures.year == year)
        .all()
    )
    for row in rows:
        node = get_or_create_country_node(session, row.country_id)
        import_dep = (
            float(row.energy_import_dep) if row.energy_import_dep is not None else 0.0
        )
        import_dep_norm = _clamp_unit(import_dep / 100.0)

        concentration = _compute_trade_concentration(session, row.country_id, year, hs_section=None)
        concentration_norm = concentration if concentration is not None else 0.0

        stress = float(row.event_stress_pulse) if row.event_stress_pulse is not None else 0.0
        stress_norm = _clamp_unit(stress / 10.0)

        shipping_change = (
            float(row.shipping_activity_change) if row.shipping_activity_change is not None else 0.0
        )
        shipping_penalty = _clamp_unit(max(0.0, -shipping_change) / 100.0)

        risk_unit = _clamp_unit(
            0.55 * import_dep_norm + 0.25 * concentration_norm + 0.15 * stress_norm + 0.05 * shipping_penalty
        )
        risk_score = risk_unit * 100.0

        metric = (
            session.query(NodeMetric)
            .filter(
                NodeMetric.node_id == node.id,
                NodeMetric.metric_code == "RISK_ENERGY",
                NodeMetric.as_of_year == year,
            )
            .one_or_none()
        )
        if metric:
            metric.value = risk_score
        else:
            next_id = session.query(func.coalesce(func.max(NodeMetric.id), 0)).scalar() or 0
            metric = NodeMetric(
                id=int(next_id) + 1,
                node_id=node.id,
                metric_code="RISK_ENERGY",
                as_of_year=year,
                value=risk_score,
            )
            session.add(metric)
            session.flush()

    session.commit()
