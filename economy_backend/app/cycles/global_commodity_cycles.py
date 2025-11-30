from __future__ import annotations

import logging
from typing import Iterable

import numpy as np
import pandas as pd
from sqlalchemy.orm import Session
from statsmodels.tsa.filters.hp_filter import hpfilter

from app.cycles.utils import standardise_series_to_z, z_to_regime
from app.db.models import GlobalCycleIndex, Indicator, TimeSeriesValue

logger = logging.getLogger(__name__)

COMMODITY_CODES = {
    "commodity_energy": "COM_ENERGY_REAL_INDEX",
    "commodity_metals": "COM_METALS_REAL_INDEX",
    "commodity_agri": "COM_AGRI_REAL_INDEX",
}


def _fetch_series(session: Session, canonical_code: str) -> pd.Series:
    records = (
        session.query(TimeSeriesValue.date, TimeSeriesValue.value)
        .join(Indicator, Indicator.id == TimeSeriesValue.indicator_id)
        .filter(Indicator.canonical_code == canonical_code)
        .order_by(TimeSeriesValue.date)
        .all()
    )
    if not records:
        return pd.Series(dtype=float)
    series = pd.Series({pd.to_datetime(rec.date): float(rec.value) for rec in records})
    series = series[~series.index.duplicated(keep="last")].sort_index()
    return series


def commodity_regime_from_z(z: float) -> str:
    bands = [
        (None, -1.5, "Bust"),
        (-1.5, -0.5, "Low"),
        (-0.5, 0.5, "Normal"),
        (0.5, 1.5, "High"),
        (1.5, None, "Boom"),
    ]
    return z_to_regime(z, bands)


def _compute_cycle_component(series: pd.Series, lamb: float) -> pd.Series:
    log_series = np.log(series.dropna())
    if log_series.empty:
        raise ValueError("Commodity price series is empty after dropping NaNs")
    cycle, trend = hpfilter(log_series, lamb=lamb)
    return cycle


def compute_commodity_cycles(session: Session, frequency: str = "monthly") -> pd.DataFrame:
    lambda_value = 129600 if frequency == "monthly" else 1600
    cycles: dict[str, pd.Series] = {}
    for cycle_type, code in COMMODITY_CODES.items():
        series = _fetch_series(session, code)
        if series.empty:
            raise ValueError(f"No data for commodity index {code}")
        cycle_component = _compute_cycle_component(series, lamb=lambda_value)
        z_scores = standardise_series_to_z(cycle_component).rename(f"{cycle_type}_z")
        cycles[cycle_type] = z_scores

    combined = pd.concat(cycles.values(), axis=1, join="inner").sort_index()
    combined["frequency"] = frequency
    return combined.reset_index().rename(columns={"index": "date"})


def write_commodity_cycles_to_db(
    session: Session, df: pd.DataFrame, frequency: str, method_version: str = "com_hp_v1"
) -> None:
    for _, row in df.iterrows():
        for cycle_type, col_name in (
            ("commodity_energy", "commodity_energy_z"),
            ("commodity_metals", "commodity_metals_z"),
            ("commodity_agri", "commodity_agri_z"),
        ):
            z_value = float(row[col_name])
            regime = commodity_regime_from_z(z_value)
            existing = (
                session.query(GlobalCycleIndex)
                .filter(
                    GlobalCycleIndex.date == row["date"],
                    GlobalCycleIndex.frequency == frequency,
                    GlobalCycleIndex.scope == "global",
                    GlobalCycleIndex.cycle_type == cycle_type,
                    GlobalCycleIndex.method_version == method_version,
                )
                .one_or_none()
            )

            if existing:
                existing.cycle_score = z_value
                existing.cycle_regime = regime
                existing.coverage_gdp_share = float(row.get("coverage_gdp_share", 1.0))
            else:
                session.add(
                    GlobalCycleIndex(
                        date=row["date"],
                        frequency=frequency,
                        scope="global",
                        cycle_type=cycle_type,
                        cycle_score=z_value,
                        cycle_regime=regime,
                        method_version=method_version,
                        coverage_gdp_share=float(row.get("coverage_gdp_share", 1.0)),
                    )
                )

