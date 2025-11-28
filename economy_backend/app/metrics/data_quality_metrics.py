from __future__ import annotations

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.db.models import Country, CountryYearFeatures, Node, NodeMetric


def _get_or_create_country_node(session: Session, country_id: str) -> Node:
    node = (
        session.query(Node)
        .filter(Node.ref_type == "country", Node.ref_id == country_id)
        .one_or_none()
    )
    if node:
        return node

    country = session.query(Country).filter(Country.id == country_id).one_or_none()
    next_id = session.query(func.coalesce(func.max(Node.id), 0)).scalar() or 0
    node = Node(
        id=int(next_id) + 1,
        node_type="country",
        ref_type="country",
        ref_id=country_id,
        label=country.name if country else country_id,
    )
    session.add(node)
    session.flush()
    return node


def write_data_quality_metrics_for_year(session: Session, year: int) -> None:
    rows = (
        session.query(CountryYearFeatures)
        .filter(CountryYearFeatures.year == year)
        .all()
    )
    for row in rows:
        node = _get_or_create_country_node(session, row.country_id)
        coverage = float(row.data_coverage_score) if row.data_coverage_score is not None else None
        freshness = (
            float(row.data_freshness_score) if row.data_freshness_score is not None else None
        )

        for code, value in {
            "DATA_COVERAGE": coverage,
            "DATA_FRESHNESS": freshness,
        }.items():
            metric = (
                session.query(NodeMetric)
                .filter(
                    NodeMetric.node_id == node.id,
                    NodeMetric.metric_code == code,
                    NodeMetric.as_of_year == year,
                )
                .one_or_none()
            )
            if metric:
                metric.value = value
            else:
                next_id = session.query(func.coalesce(func.max(NodeMetric.id), 0)).scalar() or 0
                metric = NodeMetric(
                    id=int(next_id) + 1,
                    node_id=node.id,
                    metric_code=code,
                    as_of_year=year,
                    value=value,
                )
                session.add(metric)
                session.flush()

    session.commit()
