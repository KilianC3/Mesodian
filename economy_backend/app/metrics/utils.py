from __future__ import annotations

"""Shared helpers for metrics computation.

These utilities provide consistent node creation and metric upsert logic across
country-level metric modules. They intentionally avoid any complex caching to
keep the functions side-effect free and easy to test.
"""

from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.db.models import Country, Node, NodeMetric


def get_or_create_country_node(session: Session, country_id: str) -> Node:
    """Return a Node representing the given country, creating it if needed."""

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


def upsert_node_metric(
    session: Session, node_id: int, metric_code: str, year: int, value: Optional[float]
) -> NodeMetric:
    """Insert or update a NodeMetric value for a node and year."""

    metric = (
        session.query(NodeMetric)
        .filter(
            NodeMetric.node_id == node_id,
            NodeMetric.metric_code == metric_code,
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
            node_id=node_id,
            metric_code=metric_code,
            as_of_year=year,
            value=value,
        )
        session.add(metric)
        session.flush()
    return metric
