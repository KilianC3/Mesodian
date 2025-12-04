from __future__ import annotations

import datetime as dt
import math
from typing import Dict, Optional

from sqlalchemy import and_, func
from sqlalchemy.orm import Session

from app.db.models import (
    CountryYearFeatures,
    Indicator,
    NodeMetric,
    TimeSeriesValue,
)
from app.metrics.utils import get_or_create_country_node


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


def _z_to_unit(value: Optional[float]) -> float:
    if value is None:
        return 0.5
    return 0.5 * (math.tanh(value) + 1.0)


def _get_indicator_value(
    session: Session, country_id: str, canonical_code: str, year: int
) -> Optional[float]:
    indicator = (
        session.query(Indicator)
        .filter(Indicator.canonical_code == canonical_code)
        .one_or_none()
    )
    if indicator is None:
        return None

    start = dt.date(year, 1, 1)
    end = dt.date(year, 12, 31)
    value_row = (
        session.query(TimeSeriesValue)
        .filter(
            TimeSeriesValue.indicator_id == indicator.id,
            TimeSeriesValue.country_id == country_id,
            and_(
                TimeSeriesValue.date >= start,
                TimeSeriesValue.date <= end,
            ),
        )
        .order_by(TimeSeriesValue.date.desc())
        .first()
    )
    return float(value_row.value) if value_row else None


def compute_transition_risk_for_year(session: Session, year: int) -> None:
    rows = (
        session.query(CountryYearFeatures)
        .filter(CountryYearFeatures.year == year)
        .all()
    )
    if not rows:
        return

    # Emissions intensity and change over time
    intensity_values: Dict[str, Optional[float]] = {}
    change_values: Dict[str, Optional[float]] = {}
    innovation_values: Dict[str, Optional[float]] = {}
    for row in rows:
        intensity_values[row.country_id] = float(row.co2_per_capita) if row.co2_per_capita is not None else None
        prev = (
            session.query(CountryYearFeatures)
            .filter(
                CountryYearFeatures.country_id == row.country_id,
                CountryYearFeatures.year == year - 1,
            )
            .one_or_none()
        )
        if prev and prev.co2_per_capita is not None and row.co2_per_capita is not None:
            change_values[row.country_id] = float(row.co2_per_capita) - float(prev.co2_per_capita)
        else:
            change_values[row.country_id] = None

        innovation_values[row.country_id] = _get_indicator_value(
            session, row.country_id, "GREEN_PATENTS", year
        )

    intensity_z = _standardize(intensity_values)
    change_z = _standardize(change_values)
    innovation_z = _standardize(innovation_values)

    for row in rows:
        node = get_or_create_country_node(session, row.country_id)

        intensity_component = _z_to_unit(intensity_z.get(row.country_id))
        change_component = _z_to_unit(change_z.get(row.country_id))
        innovation_component = 1.0 - _z_to_unit(innovation_z.get(row.country_id))

        risk_unit = min(
            1.0,
            max(
                0.0,
                0.5 * intensity_component
                + 0.3 * change_component
                + 0.2 * innovation_component,
            ),
        )
        risk_score = risk_unit * 100.0

        metric = (
            session.query(NodeMetric)
            .filter(
                NodeMetric.node_id == node.id,
                NodeMetric.metric_code == "RISKOP_TRANSITION",
                NodeMetric.as_of_year == year,
            )
            .one_or_none()
        )
        if metric:
            metric.value = risk_score
        else:
            next_id = session.query(func.coalesce(func.max(NodeMetric.id), 0)).scalar() or 0
            metric = NodeMetric(
                id=int(next_id) + 1,
                node_id=node.id,
                metric_code="RISKOP_TRANSITION",
                as_of_year=year,
                value=risk_score,
            )
            session.add(metric)
            session.flush()

    session.commit()
