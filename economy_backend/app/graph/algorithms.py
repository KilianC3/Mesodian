"""
Graph analytics helpers that compute centrality and propagation metrics on the
stored webs. Functions in this module read graph tables via SQLAlchemy sessions
and persist results into metric tables for downstream reporting.
"""

from __future__ import annotations

"""
Graph algorithms for computing centrality metrics on trade webs.

This graph-layer module calculates simple degree/partner centrality for country
nodes based on flow edges and writes results into ``graph.node_metric``. It is
invoked by metric orchestrators when updating web metrics.
"""

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.db.models import Edge, EdgeType, Node, NodeMetric, NodeType


def compute_trade_centrality(session: Session, year: int) -> None:
    """Compute partner counts and weighted degree centrality for trade webs."""
    country_nodes = session.query(Node).filter(Node.node_type == NodeType.COUNTRY).all()
    if not country_nodes:
        return

    edges = session.query(Edge).filter(Edge.edge_type == EdgeType.FLOW).all()

    partners = {node.id: set() for node in country_nodes}
    weighted_degree = {node.id: 0.0 for node in country_nodes}

    for edge in edges:
        attrs = edge.meta_json or {}
        if attrs.get("year") != year:
            continue

        weight = float(edge.weight_value) if edge.weight_value is not None else 0.0

        if edge.source_node_id in weighted_degree:
            weighted_degree[edge.source_node_id] += weight
            if edge.target_node_id in partners:
                partners[edge.source_node_id].add(edge.target_node_id)
        if edge.target_node_id in weighted_degree:
            weighted_degree[edge.target_node_id] += weight
            if edge.source_node_id in partners:
                partners[edge.target_node_id].add(edge.source_node_id)

    weights = list(weighted_degree.values())
    min_weight = min(weights) if weights else 0.0
    max_weight = max(weights) if weights else 0.0
    span = max_weight - min_weight

    def _normalize(value: float) -> float:
        if span == 0:
            return 0.0
        return ((value - min_weight) / span) * 100

    next_metric_id = session.query(func.coalesce(func.max(NodeMetric.id), 0)).scalar() or 0

    for node in country_nodes:
        metric = (
            session.query(NodeMetric)
            .filter(
                NodeMetric.node_id == node.id,
                NodeMetric.metric_code == "NET_SYS_IMPORTANCE",
                NodeMetric.as_of_year == year,
            )
            .one_or_none()
        )

        value = _normalize(weighted_degree.get(node.id, 0.0))
        if metric:
            metric.value = value
        else:
            next_metric_id += 1
            metric = NodeMetric(
                id=next_metric_id,
                node_id=node.id,
                metric_code="NET_SYS_IMPORTANCE",
                as_of_year=year,
                value=value,
            )
            session.add(metric)

    session.commit()
