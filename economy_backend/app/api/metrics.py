"""Metrics endpoints."""

from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.config.country_universe import COUNTRY_UNIVERSE
from app.db.engine import get_db
from app.db.models import Node, NodeMetric, WebMetric


router = APIRouter()


def _to_float(value):
    return float(value) if value is not None else None


def _get_country_node(db: Session, country_id: str) -> Node:
    node = (
        db.query(Node)
        .filter(Node.ref_type == "country", Node.ref_id == country_id)
        .first()
    )
    if not node:
        raise HTTPException(status_code=404, detail="Country node not found")
    return node


@router.get("/metrics/country/{country_id}/{year}")
def country_metrics(country_id: str, year: int, db: Session = Depends(get_db)) -> Dict:
    country_id = country_id.upper()
    if country_id not in COUNTRY_UNIVERSE:
        raise HTTPException(status_code=404, detail="Country not supported")

    node = _get_country_node(db, country_id)
    metrics = (
        db.query(NodeMetric)
        .filter(NodeMetric.node_id == node.id, NodeMetric.as_of_year == year)
        .all()
    )
    if not metrics:
        raise HTTPException(status_code=404, detail="Metrics not found")

    return {
        "country_id": country_id,
        "year": year,
        "metrics": {metric.metric_code: _to_float(metric.value) for metric in metrics},
    }


@router.get("/metrics/metric/{metric_code}")
def metric_values(
    metric_code: str,
    year: int = Query(...),
    min_value: Optional[float] = Query(default=None),
    max_value: Optional[float] = Query(default=None),
    system_layer: Optional[str] = Query(default=None),
    db: Session = Depends(get_db),
) -> Dict:
    query = (
        db.query(NodeMetric, Node)
        .join(Node, NodeMetric.node_id == Node.id)
        .filter(
            NodeMetric.metric_code == metric_code,
            NodeMetric.as_of_year == year,
            Node.ref_type == "country",
        )
    )
    if system_layer:
        query = query.filter(Node.system_layer == system_layer)
    if min_value is not None:
        query = query.filter(NodeMetric.value >= min_value)
    if max_value is not None:
        query = query.filter(NodeMetric.value <= max_value)

    records = query.order_by(Node.ref_id).all()
    return {
        "metric_code": metric_code,
        "year": year,
        "values": [
            {"country_id": node.ref_id, "value": _to_float(metric.value)}
            for metric, node in records
        ],
    }


@router.get("/metrics/web/{web_code}/{year}")
def web_metrics(web_code: str, year: int, db: Session = Depends(get_db)) -> Dict:
    metrics = (
        db.query(WebMetric)
        .filter(WebMetric.web_code == web_code, WebMetric.as_of_year == year)
        .all()
    )
    if not metrics:
        raise HTTPException(status_code=404, detail="Web metrics not found")

    return {
        "web_code": web_code,
        "year": year,
        "metrics": {metric.metric_code: _to_float(metric.value) for metric in metrics},
    }


metrics_router = router
