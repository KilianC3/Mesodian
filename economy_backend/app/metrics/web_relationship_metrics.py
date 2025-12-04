from __future__ import annotations

"""Edge-level (relationship) metrics for webs."""

from collections import defaultdict
from typing import DefaultDict, List

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.db.models import Edge, EdgeMetric
from app.metrics.catalogue import EDGE_METRIC_CODES


def _bounded(value: float) -> float:
    return max(0.0, min(100.0, value))


def _upsert_edge_metric(
    session: Session,
    source: int,
    target: int,
    web_code: str,
    metric_code: str,
    year: int,
    value: float,
) -> None:
    if metric_code not in EDGE_METRIC_CODES:
        raise ValueError(f"Unknown edge metric code {metric_code}")
    metric = (
        session.query(EdgeMetric)
        .filter(
            EdgeMetric.source_node_id == source,
            EdgeMetric.target_node_id == target,
            EdgeMetric.web_code == web_code,
            EdgeMetric.metric_code == metric_code,
            EdgeMetric.year == year,
        )
        .one_or_none()
    )
    if metric:
        metric.value = value
    else:
        next_id = session.query(func.coalesce(func.max(EdgeMetric.id), 0)).scalar() or 0
        session.add(
            EdgeMetric(
                id=int(next_id) + 1,
                source_node_id=source,
                target_node_id=target,
                web_code=web_code,
                metric_code=metric_code,
                year=year,
                value=value,
            )
        )
    session.flush()


def compute_edge_relationship_metrics_for_year(session: Session, year: int) -> None:
    """Compute dependence, criticality, and substitutability for web edges."""

    edges = session.query(Edge).all()
    if not edges:
        return

    webs: DefaultDict[str, List[Edge]] = defaultdict(list)
    for edge in edges:
        attrs = edge.meta_json or {}
        if attrs.get("year") != year:
            continue
        web_code = edge.web_code or attrs.get("web_code") or (edge.layer_id.value if edge.layer_id else "generic")
        webs[web_code].append(edge)

    for web_code, web_edges in webs.items():
        total_flow = sum(float(e.weight_value or 0.0) for e in web_edges)
        outgoing: DefaultDict[int, float] = defaultdict(float)
        incoming: DefaultDict[int, float] = defaultdict(float)
        degree: DefaultDict[int, int] = defaultdict(int)
        for edge in web_edges:
            weight = float(edge.weight_value or 0.0)
            outgoing[edge.source_node_id] += weight
            incoming[edge.target_node_id] += weight
            degree[edge.source_node_id] += 1
            degree[edge.target_node_id] += 1

        for edge in web_edges:
            weight = float(edge.weight_value or 0.0)
            share_out = weight / outgoing[edge.source_node_id] if outgoing[edge.source_node_id] else 0.0
            share_in = weight / incoming[edge.target_node_id] if incoming[edge.target_node_id] else 0.0
            dependence = _bounded(((share_out + share_in) / 2.0) * 100.0)
            _upsert_edge_metric(session, edge.source_node_id, edge.target_node_id, web_code, "EDGE_DEPENDENCE", year, dependence)

            weight_share = weight / total_flow if total_flow else 0.0
            betweenness_proxy = 1.0 / (1 + min(degree[edge.source_node_id], degree[edge.target_node_id]))
            criticality = _bounded((weight_share * 0.7 + betweenness_proxy * 0.3) * 100.0)
            _upsert_edge_metric(session, edge.source_node_id, edge.target_node_id, web_code, "EDGE_CRITICALITY", year, criticality)

            alternative_suppliers = degree[edge.target_node_id] - 1
            substitutability = (
                (alternative_suppliers / (alternative_suppliers + 1)) * 100.0
                if alternative_suppliers >= 0
                else 0.0
            )
            adjusted = _bounded((substitutability + (1 - share_in) * 100.0) / 2.0)
            _upsert_edge_metric(session, edge.source_node_id, edge.target_node_id, web_code, "EDGE_SUBSTITUTABILITY", year, adjusted)

    session.commit()
