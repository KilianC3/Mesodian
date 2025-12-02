from __future__ import annotations

"""Computation of web-level metrics for trade and other network webs."""

from collections import defaultdict, deque
from collections import defaultdict, deque
from math import log1p
from typing import DefaultDict, Dict, Iterable, List, Set

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.db.models import Edge, WebMetric
from app.metrics.catalogue import WEB_METRIC_CODES


def _upsert_web_metric(session: Session, web_code: str, metric_code: str, year: int, value: float) -> None:
    metric = (
        session.query(WebMetric)
        .filter(
            WebMetric.web_code == web_code,
            WebMetric.metric_code == metric_code,
            WebMetric.as_of_year == year,
        )
        .one_or_none()
    )
    if metric:
        metric.value = value
    else:
        next_id = session.query(func.coalesce(func.max(WebMetric.id), 0)).scalar() or 0
        session.add(
            WebMetric(
                id=int(next_id) + 1,
                web_code=web_code,
                metric_code=metric_code,
                as_of_year=year,
                value=value,
            )
        )
    session.flush()


def _connected_components(nodes: Set[int], adjacency: Dict[int, Set[int]]) -> int:
    seen: Set[int] = set()
    components = 0
    for node in nodes:
        if node in seen:
            continue
        components += 1
        queue = deque([node])
        while queue:
            current = queue.popleft()
            if current in seen:
                continue
            seen.add(current)
            for neighbor in adjacency.get(current, set()):
                if neighbor not in seen:
                    queue.append(neighbor)
    return components


def _bounded_0_100(value: float) -> float:
    return max(0.0, min(100.0, value))


def _collect_webs(session: Session, year: int) -> Dict[str, List[Edge]]:
    webs: DefaultDict[str, List[Edge]] = defaultdict(list)
    edges = session.query(Edge).all()
    for edge in edges:
        attrs = edge.attrs or {}
        if attrs.get("year") != year:
            continue
        web_code = attrs.get("web_code") or edge.edge_type or "generic"
        webs[web_code].append(edge)
    return webs


def _node_sets(edges: List[Edge]) -> Set[int]:
    nodes: Set[int] = set()
    for edge in edges:
        nodes.add(edge.source_node_id)
        nodes.add(edge.target_node_id)
    return nodes


def compute_web_metrics_for_year(session: Session, year: int) -> None:
    webs = _collect_webs(session, year)
    if not webs:
        return

    for web_code, edges in webs.items():
        nodes = _node_sets(edges)
        total_flow = sum(float(edge.weight or 0.0) for edge in edges)

        degree: DefaultDict[int, float] = defaultdict(float)
        adjacency: DefaultDict[int, Set[int]] = defaultdict(set)
        for edge in edges:
            weight = float(edge.weight or 0.0)
            degree[edge.source_node_id] += weight
            degree[edge.target_node_id] += weight
            adjacency[edge.source_node_id].add(edge.target_node_id)
            adjacency[edge.target_node_id].add(edge.source_node_id)

        required_codes = [
            "WEB_RISK_SCORE",
            "WEB_CONCENTRATION",
            "WEB_STRATEGIC_IMPORTANCE",
            "WEB_RESILIENCE",
            "WEB_PROPAGATION",
            "WEB_FRAGMENTATION",
        ]
        if any(code not in WEB_METRIC_CODES for code in required_codes):
            raise ValueError("Web metric code not in canonical catalogue")

        # WEB_RISK_SCORE: higher when mean degree-adjusted risk is elevated
        mean_degree = sum(degree.values()) / len(nodes) if nodes else 0.0
        risk_score = _bounded_0_100(100.0 - mean_degree)
        _upsert_web_metric(session, web_code, "WEB_RISK_SCORE", year, risk_score)

        # WEB_CONCENTRATION: HHI on node degree share
        if total_flow > 0:
            shares = [(deg / total_flow) ** 2 for deg in degree.values() if total_flow > 0]
            concentration = _bounded_0_100(sum(shares) * 10000)
        else:
            concentration = 0.0
        _upsert_web_metric(session, web_code, "WEB_CONCENTRATION", year, concentration)

        # WEB_STRATEGIC_IMPORTANCE combines size, concentration, and node breadth
        strategic_importance = _bounded_0_100(
            (log1p(total_flow) * 10.0) + (len(nodes) * 2.0) + (concentration * 0.3)
        )
        _upsert_web_metric(session, web_code, "WEB_STRATEGIC_IMPORTANCE", year, strategic_importance)

        # WEB_RESILIENCE approximated by loss when removing the top node
        if total_flow > 0 and degree:
            top_node = max(degree, key=degree.get)
            removed_flow = degree[top_node]
            resilience = _bounded_0_100((1 - (removed_flow / total_flow)) * 100.0)
        else:
            resilience = 0.0
        _upsert_web_metric(session, web_code, "WEB_RESILIENCE", year, resilience)

        # WEB_PROPAGATION using average degree over maximum observed
        max_degree = max(degree.values()) if degree else 0.0
        avg_degree = (sum(degree.values()) / len(degree)) if degree else 0.0
        propagation = 0.0
        if max_degree > 0:
            propagation = _bounded_0_100((avg_degree / max_degree) * 100.0)
        _upsert_web_metric(session, web_code, "WEB_PROPAGATION", year, propagation)

        # WEB_FRAGMENTATION based on connected components
        components = _connected_components(nodes, adjacency) if nodes else 0
        if len(nodes) <= 1:
            fragmentation = 0.0
        else:
            fragmentation = _bounded_0_100(((components - 1) / (len(nodes) - 1)) * 100.0)
        _upsert_web_metric(session, web_code, "WEB_FRAGMENTATION", year, fragmentation)

    session.commit()
