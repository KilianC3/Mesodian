from __future__ import annotations

import math
from typing import Dict, Iterable, List, Optional, Tuple

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.db.models import (
    Country,
    CountryYearFeatures,
    Node,
    NodeMetric,
    NodeMetricContrib,
)


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
    label = country.name if country else country_id
    node = Node(
        id=int(next_id) + 1,
        node_type="country",
        ref_type="country",
        ref_id=country_id,
        label=label,
    )
    session.add(node)
    session.flush()
    return node


def _standardize(values: Dict[str, Optional[float]]) -> Dict[str, Optional[float]]:
    observed = [float(v) for v in values.values() if v is not None]
    if not observed:
        return {k: None for k in values}
    mean = sum(observed) / len(observed)
    variance = sum((v - mean) ** 2 for v in observed) / len(observed)
    std = math.sqrt(variance)
    if std == 0:
        return {k: 0.0 if v is not None else None for k, v in values.items()}
    return {k: ((float(v) - mean) / std) if v is not None else None for k, v in values.items()}


def _weighted_average(contribs: List[Tuple[float, float]]) -> Optional[float]:
    if not contribs:
        return None
    total_weight = sum(abs(w) for w, _ in contribs)
    if total_weight == 0:
        return None
    weighted_sum = sum(w * v for w, v in contribs)
    return weighted_sum / total_weight


def _upsert_node_metric(
    session: Session, node_id: int, metric_code: str, year: int, value: Optional[float]
) -> NodeMetric:
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


def compute_country_resilience_for_year(session: Session, year: int) -> None:
    rows = (
        session.query(CountryYearFeatures)
        .filter(CountryYearFeatures.year == year)
        .all()
    )
    if not rows:
        return

    # Collect feature values for standardisation across countries
    feature_names: Iterable[str] = [
        "gdp_growth",
        "inflation_cpi",
        "ca_pct_gdp",
        "debt_pct_gdp",
        "unemployment_rate",
        "co2_per_capita",
        "energy_import_dep",
        "food_import_dep",
        "shipping_activity_change",
        "event_stress_pulse",
    ]

    feature_matrix: Dict[str, Dict[str, Optional[float]]] = {
        name: {} for name in feature_names
    }
    for row in rows:
        for name in feature_names:
            feature_matrix[name][row.country_id] = getattr(row, name)

    standardized: Dict[str, Dict[str, Optional[float]]] = {
        name: _standardize(values) for name, values in feature_matrix.items()
    }

    pillars: Dict[str, Dict[str, float]] = {
        "CR_MACRO_FISCAL": {
            "gdp_growth": 1.0,
            "inflation_cpi": -1.0,
            "debt_pct_gdp": -0.5,
            "ca_pct_gdp": 0.5,
        },
        "CR_EXTERNAL_FX": {
            "ca_pct_gdp": 0.7,
            "energy_import_dep": -0.6,
            "food_import_dep": -0.4,
            "shipping_activity_change": 0.3,
        },
        "CR_FIN_SYSTEM": {
            "debt_pct_gdp": -0.7,
            "inflation_cpi": -0.3,
            "gdp_growth": 0.3,
        },
        "CR_SOCIO_ECON": {
            "unemployment_rate": -0.7,
            "gdp_growth": 0.3,
            "event_stress_pulse": -0.3,
        },
        "CR_CLIMATE_ENV": {
            "co2_per_capita": -0.6,
            "energy_import_dep": -0.2,
            "food_import_dep": -0.2,
        },
    }

    pillar_weights: Dict[str, float] = {
        "CR_MACRO_FISCAL": 0.25,
        "CR_EXTERNAL_FX": 0.2,
        "CR_FIN_SYSTEM": 0.2,
        "CR_SOCIO_ECON": 0.2,
        "CR_CLIMATE_ENV": 0.15,
    }

    for row in rows:
        node = _get_or_create_country_node(session, row.country_id)
        pillar_scores: Dict[str, Optional[float]] = {}
        pillar_feature_contribs: Dict[str, List[Tuple[str, float]]] = {}

        for pillar_code, weights in pillars.items():
            contribs: List[Tuple[float, float]] = []
            feature_contribs: List[Tuple[str, float]] = []
            for feature_name, weight in weights.items():
                z_value = standardized[feature_name].get(row.country_id)
                if z_value is None:
                    continue
                contribs.append((weight, z_value))
                feature_contribs.append((feature_name, weight * z_value))
            score = _weighted_average(contribs)
            pillar_scores[pillar_code] = score
            pillar_feature_contribs[pillar_code] = feature_contribs
            _upsert_node_metric(session, node.id, pillar_code, year, score)

        available_weights = {
            code: w for code, w in pillar_weights.items() if pillar_scores.get(code) is not None
        }
        weight_sum = sum(available_weights.values())
        resilience_value: Optional[float] = None
        if available_weights and weight_sum > 0:
            resilience_value = (
                sum(pillar_scores[code] * weight for code, weight in available_weights.items())
                / weight_sum
            )

        resilience_metric = _upsert_node_metric(
            session, node.id, "CR_RESILIENCE", year, resilience_value
        )

        # Feature contributions to the overall resilience score
        session.query(NodeMetricContrib).filter(
            NodeMetricContrib.node_metric_id == resilience_metric.id
        ).delete()

        if resilience_value is not None and weight_sum > 0:
            next_contrib_id = (
                session.query(func.coalesce(func.max(NodeMetricContrib.id), 0)).scalar()
                or 0
            )
            for pillar_code, weights in pillars.items():
                pillar_score = pillar_scores.get(pillar_code)
                if pillar_score is None:
                    continue
                normalized_pillar_weight = pillar_weights[pillar_code] / weight_sum
                feature_weights = pillar_feature_contribs.get(pillar_code, [])
                abs_sum = sum(abs(w) for _, w in feature_weights) or 1.0
                for feature_name, raw_contrib in feature_weights:
                    contribution = normalized_pillar_weight * raw_contrib / abs_sum
                    next_contrib_id += 1
                    session.add(
                        NodeMetricContrib(
                            id=next_contrib_id,
                            node_metric_id=resilience_metric.id,
                            feature_name=feature_name,
                            contribution=contribution,
                        )
                    )

    session.commit()
