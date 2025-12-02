from __future__ import annotations

"""Computation of sovereign ESG pillar and aggregate scores."""

from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence

import pandas as pd
import yaml
from sqlalchemy.orm import Session

from app.db.models import Country, Node, NodeMetric, SovereignESGRaw
from app.metrics.catalogue import COUNTRY_METRIC_CODES
from app.metrics.utils import get_or_create_country_node, upsert_node_metric

CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "esg_indicators.yml"


class ESGConfigError(ValueError):
    """Raised when the ESG configuration file is invalid."""


def _load_config() -> List[Mapping[str, object]]:
    with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or []
    if not isinstance(data, list):
        raise ESGConfigError("ESG config must be a list of pillar definitions")
    return data


def _winsorise(series: pd.Series, lower: float = 0.01, upper: float = 0.99) -> pd.Series:
    lower_val = series.quantile(lower)
    upper_val = series.quantile(upper)
    return series.clip(lower=lower_val, upper=upper_val)


def _percentile_rank(series: pd.Series) -> pd.Series:
    return series.rank(pct=True).fillna(0.0) * 100.0


def _standardise_indicator(values: Dict[str, float], invert: bool = False) -> Dict[str, float]:
    if not values:
        return {}
    series = pd.Series(values)
    clipped = _winsorise(series)
    percentiles = _percentile_rank(clipped)
    if invert:
        percentiles = 100.0 - percentiles
    return percentiles.to_dict()


def _load_raw_values(session: Session, year: int) -> Dict[str, Dict[str, float]]:
    records = (
        session.query(SovereignESGRaw)
        .filter(SovereignESGRaw.year == year)
        .all()
    )
    out: Dict[str, Dict[str, float]] = defaultdict(dict)
    for rec in records:
        key = f"{rec.provider}:{rec.indicator_code}"
        out[rec.country_code][key] = float(rec.value)
    return out


def _load_internal_metrics(session: Session, year: int) -> Dict[str, Dict[str, float]]:
    metrics = (
        session.query(NodeMetric, Node)
        .join(Node, NodeMetric.node_id == Node.id)
        .filter(NodeMetric.as_of_year == year, Node.ref_type == "country")
        .all()
    )
    out: Dict[str, Dict[str, float]] = defaultdict(dict)
    for metric, node in metrics:
        out[str(node.ref_id)][metric.metric_code] = float(metric.value)
    return out


def _weighted_average(values: Sequence[float], weights: Sequence[float]) -> Optional[float]:
    if not values:
        return None
    total_weight = sum(weights)
    if total_weight == 0:
        return None
    weighted_sum = sum(v * w for v, w in zip(values, weights))
    return weighted_sum / total_weight


def _pillar_score(
    country_code: str,
    pillar_cfg: Mapping[str, object],
    standardized_external: Mapping[str, Dict[str, float]],
    standardized_internal: Mapping[str, Dict[str, float]],
) -> Optional[float]:
    indicators: Iterable[Mapping[str, object]] = pillar_cfg.get("indicators", [])
    scores: List[float] = []
    weights: List[float] = []
    for indicator in indicators:
        weight = float(indicator.get("weight", 1.0))
        if weight <= 0:
            continue
        value: Optional[float] = None
        if indicator.get("internal_metric"):
            metric_code = str(indicator["internal_metric"])
            value = standardized_internal.get(country_code, {}).get(metric_code)
        else:
            provider = str(indicator.get("provider"))
            code = str(indicator.get("indicator_code"))
            key = f"{provider}:{code}"
            value = standardized_external.get(country_code, {}).get(key)
        if value is None:
            continue
        scores.append(value)
        weights.append(weight)
    return _weighted_average(scores, weights)


def _clamp_0_100(value: float) -> float:
    return max(0.0, min(100.0, value))


def compute_sovereign_esg_for_year(session: Session, year: int) -> None:
    """Compute ESG pillar and aggregate scores for all countries in a year."""

    config = _load_config()
    countries = session.query(Country).all()
    if not countries:
        return

    external_invert: Dict[str, bool] = {}
    internal_invert: Dict[str, bool] = {}
    for pillar_cfg in config:
        for indicator in pillar_cfg.get("indicators", []):
            invert_flag = bool(indicator.get("invert", False))
            if indicator.get("internal_metric"):
                internal_invert[str(indicator["internal_metric"])] = invert_flag
            else:
                provider = str(indicator.get("provider"))
                code = str(indicator.get("indicator_code"))
                external_invert[f"{provider}:{code}"] = invert_flag

    raw_values = _load_raw_values(session, year)
    internal_metrics = _load_internal_metrics(session, year)

    # Standardise all indicators ahead of weighting
    standardized_external: Dict[str, Dict[str, float]] = defaultdict(dict)
    standardized_internal: Dict[str, Dict[str, float]] = defaultdict(dict)

    # Build dicts by indicator
    external_by_indicator: Dict[str, Dict[str, float]] = defaultdict(dict)
    for country, metrics in raw_values.items():
        for key, value in metrics.items():
            external_by_indicator[key][country] = value

    internal_by_indicator: Dict[str, Dict[str, float]] = defaultdict(dict)
    for country, metrics in internal_metrics.items():
        for key, value in metrics.items():
            internal_by_indicator[key][country] = value

    # Apply winsorisation and percentile ranking
    for key, values in external_by_indicator.items():
        standardized = _standardise_indicator(
            values, invert=external_invert.get(key, "RISK" in key or "CO2" in key)
        )
        for country, val in standardized.items():
            standardized_external[country][key] = val

    for key, values in internal_by_indicator.items():
        invert = internal_invert.get(
            key,
            key
            in {
                "CLIMATE_TOTAL_RISK",
                "RISK_ENERGY",
                "RISK_FOOD",
                "HH_STRESS",
                "HOUSING_STRESS",
                "CREDIT_EXCESS",
            },
        )
        standardized = _standardise_indicator(values, invert=invert)
        for country, val in standardized.items():
            standardized_internal[country][key] = val

    # Compute pillar scores
    pillar_map: Dict[str, Dict[str, float]] = defaultdict(dict)
    for pillar_cfg in config:
        pillar_code = str(pillar_cfg.get("pillar"))
        if pillar_code not in {"E", "S", "G"}:
            raise ESGConfigError(f"Unknown pillar code {pillar_code}")
        for country in raw_values.keys() | internal_metrics.keys():
            score = _pillar_score(country, pillar_cfg, standardized_external, standardized_internal)
            if score is None:
                continue
            metric_code = f"ESG_{pillar_code}_SOV"
            pillar_map[country][metric_code] = score

    # Write pillar and total scores
    for country in countries:
        node = get_or_create_country_node(session, country.id)
        for metric_code in ["ESG_E_SOV", "ESG_S_SOV", "ESG_G_SOV"]:
            if metric_code in COUNTRY_METRIC_CODES:
                value = pillar_map.get(country.id, {}).get(metric_code)
                if value is not None:
                    upsert_node_metric(session, node.id, metric_code, year, _clamp_0_100(value))

        pillars = [pillar_map.get(country.id, {}).get(code) for code in ("ESG_E_SOV", "ESG_S_SOV", "ESG_G_SOV")]
        valid_pillars = [p for p in pillars if p is not None]
        if valid_pillars:
            total = sum(valid_pillars) / len(valid_pillars)
            upsert_node_metric(session, node.id, "ESG_TOTAL_SOV", year, _clamp_0_100(total))

    session.commit()
