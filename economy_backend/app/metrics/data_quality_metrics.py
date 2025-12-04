from __future__ import annotations

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.db.models import CountryYearFeatures, NodeMetric
from app.metrics.utils import get_or_create_country_node


def write_data_quality_metrics_for_year(session: Session, year: int) -> None:
    rows = (
        session.query(CountryYearFeatures)
        .filter(CountryYearFeatures.year == year)
        .all()
    )
    for row in rows:
        node = get_or_create_country_node(session, row.country_id)
        coverage = float(row.data_coverage_score) if row.data_coverage_score is not None else None
        freshness = (
            float(row.data_freshness_score) if row.data_freshness_score is not None else None
        )

        for code, value in {
            "DQ_COVERAGE": coverage,
            "DQ_FRESHNESS": freshness,
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
