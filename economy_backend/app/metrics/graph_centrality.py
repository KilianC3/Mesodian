"""Graph centrality computation and structural role assignment."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

import networkx as nx
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.db.models import Edge, LayerId, Node, NodeMetric, StructuralRole


@dataclass(frozen=True)
class CentralityResults:
    """Container for centrality measures used downstream for role assignment."""

    degree_out: Dict[int, float]
    degree_in: Dict[int, float]
    eigenvector: Dict[int, float]
    betweenness: Dict[int, float]
    degree_counts: Dict[int, int]


def _coerce_layer(layer_id: LayerId | str) -> LayerId:
    """Ensure the provided layer identifier is a valid LayerId enum."""

    if isinstance(layer_id, LayerId):
        return layer_id
    return LayerId(layer_id)


def _upsert_node_metric(
    session: Session,
    *,
    node_id: int,
    metric_code: str,
    year: int,
    value: float,
    next_metric_id: int,
) -> int:
    """Insert or update a node metric and return the next available id."""

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
        return next_metric_id

    next_metric_id += 1
    session.add(
        NodeMetric(
            id=next_metric_id,
            node_id=node_id,
            metric_code=metric_code,
            as_of_year=year,
            value=value,
        )
    )
    return next_metric_id


def compute_centrality_for_layer(
    session: Session, year: int, layer_id: LayerId | str, web_code: Optional[str] = None
) -> CentralityResults:
    """Compute degree, eigenvector, and betweenness centrality for a layer.

    The function builds a directed weighted graph of nodes connected by edges in
    the requested layer and writes the resulting metrics to ``graph.node_metric``
    with metric codes prefixed by the layer name. Metrics are upserted so repeat
    runs remain idempotent.
    """

    layer = _coerce_layer(layer_id)
    edges_query = session.query(Edge).filter(Edge.layer_id == layer)
    if web_code:
        edges_query = edges_query.filter(Edge.web_code == web_code)
    edges = edges_query.all()

    graph = nx.DiGraph()
    for edge in edges:
        # Prefer explicit year filters stored in meta_json; allow edges without
        # a year tag to participate when computing a single-year snapshot.
        if edge.meta_json and "year" in edge.meta_json and edge.meta_json.get("year") != year:
            continue
        weight = float(edge.weight_value) if edge.weight_value is not None else 1.0
        graph.add_edge(edge.source_node_id, edge.target_node_id, weight=weight)

    if not graph.nodes:
        return CentralityResults({}, {}, {}, {}, {})

    degree_out: Dict[int, float] = dict(graph.out_degree(weight="weight"))
    degree_in: Dict[int, float] = dict(graph.in_degree(weight="weight"))
    degree_counts: Dict[int, int] = dict(graph.degree())

    eigenvector: Dict[int, float]
    try:
        eigenvector = nx.eigenvector_centrality(
            graph.to_undirected(), max_iter=1000, weight="weight"
        )
    except nx.PowerIterationFailedConvergence:
        eigenvector = {node: 0.0 for node in graph.nodes}

    betweenness = nx.betweenness_centrality(graph, weight="weight", normalized=True)

    if not graph.nodes:
        return CentralityResults(degree_out, degree_in, eigenvector, betweenness, degree_counts)

    next_metric_id = session.query(func.coalesce(func.max(NodeMetric.id), 0)).scalar() or 0
    layer_suffix = layer.value
    metric_definitions = {
        f"CENT_DEGREE_OUT_{layer_suffix}": degree_out,
        f"CENT_DEGREE_IN_{layer_suffix}": degree_in,
        f"CENT_EIG_{layer_suffix}": eigenvector,
        f"CENT_BETWEEN_{layer_suffix}": betweenness,
    }

    for metric_code, values in metric_definitions.items():
        for node_id, metric_value in values.items():
            next_metric_id = _upsert_node_metric(
                session,
                node_id=node_id,
                metric_code=metric_code,
                year=year,
                value=float(metric_value),
                next_metric_id=next_metric_id,
            )

    session.commit()
    return CentralityResults(degree_out, degree_in, eigenvector, betweenness, degree_counts)


def _assign_structural_roles(session: Session, results: CentralityResults) -> None:
    """Map centrality scores into structural roles and update nodes.

    The rule set is deterministic and based on relative thresholds:
    - ``CORE``: nodes with eigenvector centrality and weighted degree strength
      at or above 80% of the layer maxima.
    - ``BRIDGE``/``BOTTLENECK``: nodes whose betweenness is at or above 80% of
      the layer maximum. Nodes with only one connection are treated as
      bottlenecks, otherwise bridges.
    - ``LEAF``: nodes with only a single connection and no elevated centrality.
    - ``PERIPHERY``: all remaining nodes.
    """

    if not results.degree_counts:
        return

    strength = {
        node_id: results.degree_out.get(node_id, 0.0) + results.degree_in.get(node_id, 0.0)
        for node_id in results.degree_counts
    }
    max_eigen = max(results.eigenvector.values(), default=0.0)
    max_strength = max(strength.values(), default=0.0)
    max_betweenness = max(results.betweenness.values(), default=0.0)

    eigen_threshold = max_eigen * 0.8
    strength_threshold = max_strength * 0.8
    betweenness_threshold = max_betweenness * 0.8

    nodes = session.query(Node).filter(Node.id.in_(results.degree_counts.keys())).all()
    for node in nodes:
        node_strength = strength.get(node.id, 0.0)
        node_betweenness = results.betweenness.get(node.id, 0.0)
        node_eigen = results.eigenvector.get(node.id, 0.0)
        degree_count = results.degree_counts.get(node.id, 0)

        if node_eigen > 0 and node_strength > 0 and node_eigen >= eigen_threshold and node_strength >= strength_threshold:
            node.structural_role = StructuralRole.CORE
        elif node_betweenness > 0 and node_betweenness >= betweenness_threshold:
            node.structural_role = (
                StructuralRole.BRIDGE if degree_count > 1 else StructuralRole.BOTTLENECK
            )
        elif degree_count <= 1 and node_betweenness < betweenness_threshold and node_eigen < eigen_threshold:
            node.structural_role = StructuralRole.LEAF
        else:
            node.structural_role = StructuralRole.PERIPHERY

    session.commit()


def compute_graph_centrality_and_roles(session: Session, year: int) -> None:
    """Compute centrality metrics for key layers and update structural roles."""

    layer_results = {}
    for layer in (LayerId.TRADE, LayerId.FINANCIAL):
        layer_results[layer] = compute_centrality_for_layer(session, year, layer)

    trade_results = layer_results.get(LayerId.TRADE)
    if trade_results:
        _assign_structural_roles(session, trade_results)
