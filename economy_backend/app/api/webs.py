"""Graph and web endpoints."""

from collections import deque
from typing import Deque, Dict, List, Optional, Set, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.config.country_universe import COUNTRY_UNIVERSE
from app.db.engine import get_db
from app.db.models import Edge, Node


router = APIRouter()


def _edge_matches_year(edge: Edge, year: int) -> bool:
    if edge.attrs and isinstance(edge.attrs, dict):
        edge_year = edge.attrs.get("year")
        if edge_year is not None:
            return int(edge_year) == int(year)
    return True


def _serialize_node(node: Node) -> Dict:
    return {"id": node.id, "country_id": node.ref_id, "label": node.label}


@router.get("/webs/trade/{year}")
def trade_web(
    year: int,
    min_value_usd: float = Query(default=0.0),
    top_n_edges: Optional[int] = Query(default=None),
    db: Session = Depends(get_db),
) -> Dict:
    edge_query = (
        db.query(Edge)
        .filter(Edge.edge_type == "trade_exposure")
        .order_by(Edge.weight.desc())
    )
    if top_n_edges:
        edge_query = edge_query.limit(top_n_edges)

    edges = [edge for edge in edge_query.all() if _edge_matches_year(edge, year)]
    edges = [edge for edge in edges if edge.weight is None or float(edge.weight) >= min_value_usd]

    node_ids: Set[int] = set()
    for edge in edges:
        node_ids.add(edge.source_node_id)
        node_ids.add(edge.target_node_id)

    nodes = db.query(Node).filter(Node.id.in_(node_ids)).all() if node_ids else []
    node_map = {node.id: node for node in nodes}

    return {
        "year": year,
        "nodes": [_serialize_node(node) for node in nodes],
        "edges": [
            {
                "source": edge.source_node_id,
                "target": edge.target_node_id,
                "weight": float(edge.weight) if edge.weight is not None else None,
            }
            for edge in edges
            if edge.source_node_id in node_map and edge.target_node_id in node_map
        ],
    }


def _filter_edges_by_year(edges: List[Edge], year: int) -> List[Edge]:
    return [edge for edge in edges if _edge_matches_year(edge, year)]


@router.get("/webs/country/{country_id}/{year}")
def country_web(
    country_id: str,
    year: int,
    depth: int = Query(default=1, ge=1),
    edge_type: str = Query(default="trade_exposure"),
    db: Session = Depends(get_db),
) -> Dict:
    country_id = country_id.upper()
    if country_id not in COUNTRY_UNIVERSE:
        raise HTTPException(status_code=404, detail="Country not supported")

    root_node = (
        db.query(Node)
        .filter(Node.ref_type == "country", Node.ref_id == country_id)
        .first()
    )
    if not root_node:
        raise HTTPException(status_code=404, detail="Country node not found")

    all_edges = db.query(Edge).filter(Edge.edge_type == edge_type).all()
    filtered_edges = _filter_edges_by_year(all_edges, year)

    adjacency: Dict[int, List[int]] = {}
    for edge in filtered_edges:
        adjacency.setdefault(edge.source_node_id, []).append(edge.target_node_id)
        adjacency.setdefault(edge.target_node_id, []).append(edge.source_node_id)

    visited: Set[int] = set()
    nodes_to_visit: Deque[Tuple[int, int]] = deque([(root_node.id, 0)])
    collected_ids: Set[int] = set()

    while nodes_to_visit:
        node_id, dist = nodes_to_visit.popleft()
        if node_id in visited or dist > depth:
            continue
        visited.add(node_id)
        collected_ids.add(node_id)
        if dist == depth:
            continue
        for neighbor in adjacency.get(node_id, []):
            if neighbor not in visited:
                nodes_to_visit.append((neighbor, dist + 1))

    nodes = db.query(Node).filter(Node.id.in_(collected_ids)).all() if collected_ids else []
    node_map = {node.id: node for node in nodes}
    edges_payload = [
        {
            "source": edge.source_node_id,
            "target": edge.target_node_id,
            "weight": float(edge.weight) if edge.weight is not None else None,
        }
        for edge in filtered_edges
        if edge.source_node_id in collected_ids and edge.target_node_id in collected_ids
    ]

    return {
        "country_id": country_id,
        "year": year,
        "edge_type": edge_type,
        "nodes": [_serialize_node(node) for node in nodes],
        "edges": edges_payload,
    }


@router.get("/webs/path/{source_country_id}/{target_country_id}/{year}")
def country_path(
    source_country_id: str,
    target_country_id: str,
    year: int,
    edge_type: str = Query(default="trade_exposure"),
    db: Session = Depends(get_db),
) -> Dict:
    source_country_id = source_country_id.upper()
    target_country_id = target_country_id.upper()
    for cid in (source_country_id, target_country_id):
        if cid not in COUNTRY_UNIVERSE:
            raise HTTPException(status_code=404, detail="Country not supported")

    nodes = db.query(Node).filter(Node.ref_type == "country").all()
    node_by_country = {node.ref_id: node for node in nodes}
    if source_country_id not in node_by_country or target_country_id not in node_by_country:
        raise HTTPException(status_code=404, detail="Country node not found")

    source_node = node_by_country[source_country_id]
    target_node = node_by_country[target_country_id]

    all_edges = db.query(Edge).filter(Edge.edge_type == edge_type).all()
    edges = _filter_edges_by_year(all_edges, year)

    adjacency: Dict[int, List[int]] = {}
    for edge in edges:
        adjacency.setdefault(edge.source_node_id, []).append(edge.target_node_id)
        adjacency.setdefault(edge.target_node_id, []).append(edge.source_node_id)

    queue: Deque[int] = deque([source_node.id])
    parents: Dict[int, Optional[int]] = {source_node.id: None}

    while queue:
        current = queue.popleft()
        if current == target_node.id:
            break
        for neighbor in adjacency.get(current, []):
            if neighbor not in parents:
                parents[neighbor] = current
                queue.append(neighbor)

    if target_node.id not in parents:
        return {
            "source_country_id": source_country_id,
            "target_country_id": target_country_id,
            "year": year,
            "path": [],
            "connected": False,
        }

    # Reconstruct path
    path_nodes: List[int] = []
    current = target_node.id
    while current is not None:
        path_nodes.append(current)
        current = parents.get(current)
    path_nodes.reverse()

    id_to_node = {node.id: node for node in nodes}
    return {
        "source_country_id": source_country_id,
        "target_country_id": target_country_id,
        "year": year,
        "path": [
            {"country_id": id_to_node[node_id].ref_id}
            for node_id in path_nodes
            if node_id in id_to_node
        ],
        "connected": True,
    }


webs_router = router
