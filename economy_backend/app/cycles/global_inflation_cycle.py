from __future__ import annotations

import logging
from typing import Iterable

import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
from sqlalchemy.orm import Session

from app.cycles.utils import prefer_timing_class, standardise_series_to_z, z_to_regime
from app.db.models import GlobalCycleIndex, Indicator, TimeSeriesValue

logger = logging.getLogger(__name__)

INFLATION_CODES = {"CPI_HEADLINE_YOY", "CPI_CORE_YOY"}
GDP_NOMINAL_CODE = "GDP_CURRENT_USD"


def _fetch_panel(session: Session, canonical_codes: Iterable[str]) -> pd.DataFrame:
    records = (
        session.query(
            TimeSeriesValue.date,
            TimeSeriesValue.country_id,
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
                "country_id": rec.country_id,
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


def _pivot_panel(df: pd.DataFrame) -> pd.DataFrame:
    pivot = df.pivot_table(index="date", columns="country_id", values="value", aggfunc="last")
    pivot = pivot.sort_index()
    return pivot


def gic_regime_from_z(z: float) -> str:
    bands = [
        (None, -1.5, "Global_disinflation"),
        (-1.5, -0.5, "Below_target"),
        (-0.5, 0.5, "Near_target"),
        (0.5, 1.5, "Above_target"),
        (1.5, None, "High_inflation"),
    ]
    return z_to_regime(z, bands)


def _compute_coverage(session: Session, dates: Iterable[pd.Timestamp], countries: Iterable[str]) -> pd.Series:
    records = (
        session.query(TimeSeriesValue.date, TimeSeriesValue.country_id, TimeSeriesValue.value)
        .join(Indicator, Indicator.id == TimeSeriesValue.indicator_id)
        .filter(Indicator.canonical_code == GDP_NOMINAL_CODE)
        .all()
    )
    if not records:
        return pd.Series(np.nan, index=pd.Index(dates))

    gdp_df = pd.DataFrame(
        [
            {"date": rec.date, "country_id": rec.country_id, "value": float(rec.value)}
            for rec in records
        ]
    )
    gdp_df["date"] = pd.to_datetime(gdp_df["date"])
    pivot = gdp_df.pivot_table(index="date", columns="country_id", values="value", aggfunc="last")
    pivot = pivot.sort_index().reindex(index=pd.to_datetime(list(dates)))
    included = [c for c in countries if c in pivot.columns]
    coverage = []
    for date in pivot.index:
        total = pivot.loc[date].dropna().sum()
        numerator = pivot.loc[date, included].dropna().sum() if included else 0
        coverage.append(float(numerator / total) if total and total > 0 else np.nan)
    return pd.Series(coverage, index=pivot.index)


def compute_gic_index(session: Session, frequency: str = "quarterly") -> pd.DataFrame:
    panel_df = _fetch_panel(session, INFLATION_CODES)
    if panel_df.empty:
        raise ValueError("No inflation data available for GIC computation")

    pivot = _pivot_panel(panel_df)
    pivot = pivot.apply(standardise_series_to_z, axis=0)
    pivot = pivot.dropna(how="all", axis=1)
    pivot = pivot.dropna(how="any")
    if pivot.empty or pivot.shape[1] < 1:
        raise ValueError("Insufficient inflation panel data")

    pca = PCA(n_components=1)
    scores = pca.fit_transform(pivot)
    factor_series = pd.Series(scores.flatten(), index=pivot.index, name="gic_factor")
    gic_z = standardise_series_to_z(factor_series).rename("gic_z")

    coverage = _compute_coverage(session, gic_z.index, pivot.columns)
    result = pd.DataFrame({"date": gic_z.index, "gic_z": gic_z.values, "coverage_gdp_share": coverage})
    result["frequency"] = frequency
    return result


def write_gic_to_db(
    session: Session, df: pd.DataFrame, frequency: str, method_version: str = "gic_pca_v1"
) -> None:
    for _, row in df.iterrows():
        regime = gic_regime_from_z(float(row["gic_z"]))
        existing = (
            session.query(GlobalCycleIndex)
            .filter(
                GlobalCycleIndex.date == row["date"],
                GlobalCycleIndex.frequency == frequency,
                GlobalCycleIndex.scope == "global",
                GlobalCycleIndex.cycle_type == "inflation",
                GlobalCycleIndex.method_version == method_version,
            )
            .one_or_none()
        )

        if existing:
            existing.cycle_score = float(row["gic_z"])
            existing.cycle_regime = regime
            existing.coverage_gdp_share = float(row.get("coverage_gdp_share", 1.0))
        else:
            session.add(
                GlobalCycleIndex(
                    date=row["date"],
                    frequency=frequency,
                    scope="global",
                    cycle_type="inflation",
                    cycle_score=float(row["gic_z"]),
                    cycle_regime=regime,
                    method_version=method_version,
                    coverage_gdp_share=float(row.get("coverage_gdp_share", 1.0)),
                )
            )

