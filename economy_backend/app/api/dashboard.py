"""Composite dashboard endpoints."""

from typing import Dict

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.config.country_universe import COUNTRY_UNIVERSE
from app.db.engine import get_db
from app.db.models import Country, CountryYearFeatures, Node, NodeMetric, TradeFlow


router = APIRouter()


def _to_float(value):
    return float(value) if value is not None else None


def _load_country(db: Session, country_id: str) -> Country:
    country = db.query(Country).filter(Country.id == country_id).first()
    if not country:
        raise HTTPException(status_code=404, detail="Country not found")
    return country


def _load_features(db: Session, country_id: str, year: int) -> Dict:
    features = (
        db.query(CountryYearFeatures)
        .filter(CountryYearFeatures.country_id == country_id, CountryYearFeatures.year == year)
        .first()
    )
    if not features:
        return {}
    return {
        "gdp_real": _to_float(features.gdp_real),
        "gdp_growth": _to_float(features.gdp_growth),
        "inflation_cpi": _to_float(features.inflation_cpi),
        "ca_pct_gdp": _to_float(features.ca_pct_gdp),
        "debt_pct_gdp": _to_float(features.debt_pct_gdp),
        "unemployment_rate": _to_float(features.unemployment_rate),
        "co2_per_capita": _to_float(features.co2_per_capita),
        "energy_import_dep": _to_float(features.energy_import_dep),
        "food_import_dep": _to_float(features.food_import_dep),
        "shipping_activity_level": _to_float(features.shipping_activity_level),
        "shipping_activity_change": _to_float(features.shipping_activity_change),
        "event_stress_pulse": _to_float(features.event_stress_pulse),
        "data_coverage_score": _to_float(features.data_coverage_score),
        "data_freshness_score": _to_float(features.data_freshness_score),
    }


def _load_country_metrics(db: Session, country_id: str, year: int) -> Dict:
    node = (
        db.query(Node)
        .filter(Node.ref_type == "country", Node.ref_id == country_id)
        .first()
    )
    if not node:
        return {}
    metrics = (
        db.query(NodeMetric)
        .filter(NodeMetric.node_id == node.id, NodeMetric.as_of_year == year)
        .all()
    )
    return {metric.metric_code: _to_float(metric.value) for metric in metrics}


def _load_trade_summary(db: Session, country_id: str, year: int) -> Dict:
    partner_totals = (
        db.query(TradeFlow.partner_country_id, func.sum(TradeFlow.value_usd))
        .filter(TradeFlow.reporter_country_id == country_id, TradeFlow.year == year)
        .group_by(TradeFlow.partner_country_id)
        .order_by(func.sum(TradeFlow.value_usd).desc())
        .limit(5)
        .all()
    )

    node = (
        db.query(Node)
        .filter(Node.ref_type == "country", Node.ref_id == country_id)
        .first()
    )
    net_importance = None
    if node:
        metric = (
            db.query(NodeMetric)
            .filter(
                NodeMetric.node_id == node.id,
                NodeMetric.metric_code == "NET_SYS_IMPORTANCE",
                NodeMetric.as_of_year == year,
            )
            .first()
        )
        if metric:
            net_importance = _to_float(metric.value)

    return {
        "net_system_importance": net_importance,
        "top_partners": [
            {
                "partner_country_id": partner,
                "value_usd": _to_float(value),
            }
            for partner, value in partner_totals
        ],
    }


@router.get("/dashboard/country/{country_id}/{year}")
def country_dashboard(country_id: str, year: int, db: Session = Depends(get_db)) -> Dict:
    country_id = country_id.upper()
    if country_id not in COUNTRY_UNIVERSE:
        raise HTTPException(status_code=404, detail="Country not supported")

    country = _load_country(db, country_id)
    features = _load_features(db, country_id, year)
    metrics = _load_country_metrics(db, country_id, year)
    trade_summary = _load_trade_summary(db, country_id, year)

    return {
        "country_id": country_id,
        "year": year,
        "country": {
            "name": country.name,
            "region": country.region,
            "income_group": country.income_group,
        },
        "features": features,
        "metrics": metrics,
        "trade_summary": trade_summary,
    }


dashboard_router = router
