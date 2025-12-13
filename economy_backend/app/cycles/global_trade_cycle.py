"""
Compute global trade cycle indices using PCA over trade-related indicators.
Provides utilities for standardising series, extracting principal components,
and mapping factor scores into regimes stored in `global_cycle_index`.
"""

from __future__ import annotations

import logging
from typing import Iterable

import pandas as pd
from sklearn.decomposition import PCA
from sqlalchemy.orm import Session

from app.cycles.utils import prefer_timing_class, standardise_series_to_z, z_to_regime
from app.db.models import GlobalCycleIndex, Indicator, TimeSeriesValue

logger = logging.getLogger(__name__)

GLOBAL_TRADE_CODES = [
    "WORLD_TRADE_VOL",
    "WORLD_IP",
    "GLOBAL_MANUF_PMI",
    "GLOBAL_EXPORT_ORDERS_PMI",
]


def _fetch_global_series(session: Session, canonical_codes: Iterable[str]) -> pd.DataFrame:
    records = (
        session.query(
            TimeSeriesValue.date,
            Indicator.canonical_code,
            TimeSeriesValue.value,
            Indicator.timing_class,
        )
        .join(Indicator, Indicator.id == TimeSeriesValue.indicator_id)
        .filter(Indicator.canonical_code.in_(list(canonical_codes)))
        .all()
    )
    if not records:
        return pd.DataFrame()
    df = pd.DataFrame(
        [
            {
                "date": rec.date,
                "canonical_code": rec.canonical_code,
                "value": float(rec.value),
                "timing_class": getattr(rec, "timing_class", None),
            }
            for rec in records
        ]
    )
    df["date"] = pd.to_datetime(df["date"])
    df = prefer_timing_class(df)
    return df.drop(columns=["timing_class"], errors="ignore")


def gtmc_regime_from_z(z: float) -> str:
    bands = [
        (None, -1.5, "Global_trade_recession"),
        (-1.5, -0.5, "Weak_trade"),
        (-0.5, 0.5, "Normal_trade"),
        (0.5, 1.5, "Strong_trade"),
        (1.5, None, "Trade_boom"),
    ]
    return z_to_regime(z, bands)


def compute_gtmc_index(session: Session, frequency: str = "monthly") -> pd.DataFrame:
    df = _fetch_global_series(session, GLOBAL_TRADE_CODES)
    if df.empty:
        raise ValueError("No global trade/manufacturing data available")

    pivot = df.pivot_table(index="date", columns="canonical_code", values="value", aggfunc="last")
    pivot = pivot.sort_index().ffill()
    z_pivot = pivot.apply(standardise_series_to_z, axis=0)
    z_pivot = z_pivot.dropna(how="any")
    if z_pivot.empty or z_pivot.shape[1] < 1:
        raise ValueError("Insufficient aligned trade data for GTMC computation")

    pca = PCA(n_components=1)
    scores = pca.fit_transform(z_pivot)
    factor_series = pd.Series(scores.flatten(), index=z_pivot.index, name="gtmc_factor")
    gtmc_z = standardise_series_to_z(factor_series).rename("gtmc_z")

    result = pd.DataFrame({"date": gtmc_z.index, "gtmc_z": gtmc_z.values, "coverage_gdp_share": 1.0})
    result["frequency"] = frequency
    return result


def write_gtmc_to_db(
    session: Session, df: pd.DataFrame, frequency: str, method_version: str = "gtmc_pca_v1"
) -> None:
    for _, row in df.iterrows():
        existing = (
            session.query(GlobalCycleIndex)
            .filter(
                GlobalCycleIndex.date == row["date"],
                GlobalCycleIndex.frequency == frequency,
                GlobalCycleIndex.scope == "global",
                GlobalCycleIndex.cycle_type == "trade",
                GlobalCycleIndex.method_version == method_version,
            )
            .one_or_none()
        )

        regime = gtmc_regime_from_z(float(row["gtmc_z"]))
        if existing:
            existing.cycle_score = float(row["gtmc_z"])
            existing.cycle_regime = regime
            existing.coverage_gdp_share = float(row.get("coverage_gdp_share", 1.0))
        else:
            record = GlobalCycleIndex(
                date=row["date"],
                frequency=frequency,
                scope="global",
                cycle_type="trade",
                cycle_score=float(row["gtmc_z"]),
                cycle_regime=regime,
                method_version=method_version,
                coverage_gdp_share=float(row.get("coverage_gdp_share", 1.0)),
            )
            session.add(record)

