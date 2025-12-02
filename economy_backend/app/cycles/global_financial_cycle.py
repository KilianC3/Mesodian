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

FINANCIAL_CODES = {
    "POLICY_RATE_US",
    "GLOBAL_IG_SPREAD",
    "GLOBAL_HY_SPREAD",
    "EMBIG_SPREAD",
    "VIX",
    "GLOBAL_EQUITY_RETURN",
}

INVERTED_CODES = {"GLOBAL_EQUITY_RETURN"}


def _fetch_financial_series(session: Session, canonical_codes: Iterable[str]) -> pd.DataFrame:
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


def _prepare_financial_matrix(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    for code in INVERTED_CODES:
        mask = df["canonical_code"] == code
        df.loc[mask, "value"] = -df.loc[mask, "value"]
    pivot = df.pivot_table(index="date", columns="canonical_code", values="value", aggfunc="last")
    pivot = pivot.sort_index().ffill()
    return pivot


def gfc_regime_from_z(z: float) -> str:
    bands = [
        (1.5, None, "Very_tight"),
        (0.5, 1.5, "Tight"),
        (-0.5, 0.5, "Neutral"),
        (-1.5, -0.5, "Easy"),
        (None, -1.5, "Very_easy"),
    ]
    return z_to_regime(z, bands)


def compute_gfc_index(session: Session, frequency: str = "monthly") -> pd.DataFrame:
    df = _fetch_financial_series(session, FINANCIAL_CODES)
    if df.empty:
        raise ValueError("No financial series available for GFC computation")

    pivot = _prepare_financial_matrix(df)
    z_pivot = pivot.apply(standardise_series_to_z, axis=0)
    z_pivot = z_pivot.dropna(how="any")
    if z_pivot.empty or z_pivot.shape[1] < 1:
        raise ValueError("Insufficient aligned financial series for GFC")

    pca = PCA(n_components=1)
    scores = pca.fit_transform(z_pivot)
    factor_series = pd.Series(scores.flatten(), index=z_pivot.index, name="gfc_factor")

    vix_series = z_pivot.get("VIX")
    if vix_series is not None and not vix_series.empty:
        aligned_factor, aligned_vix = factor_series.align(vix_series, join="inner")
        if len(aligned_factor.dropna()) > 1 and len(aligned_vix.dropna()) > 1:
            corr = np.corrcoef(aligned_factor, aligned_vix)[0, 1]
            if corr < 0:
                factor_series = -factor_series

    gfc_z = standardise_series_to_z(factor_series).rename("gfc_z")
    result = pd.DataFrame({"date": gfc_z.index, "gfc_z": gfc_z.values, "coverage_gdp_share": 1.0})
    result["frequency"] = frequency
    return result


def write_gfc_to_db(
    session: Session, df: pd.DataFrame, frequency: str, method_version: str = "gfc_pca_v1"
) -> None:
    for _, row in df.iterrows():
        regime = gfc_regime_from_z(float(row["gfc_z"]))
        existing = (
            session.query(GlobalCycleIndex)
            .filter(
                GlobalCycleIndex.date == row["date"],
                GlobalCycleIndex.frequency == frequency,
                GlobalCycleIndex.scope == "global",
                GlobalCycleIndex.cycle_type == "financial_conditions",
                GlobalCycleIndex.method_version == method_version,
            )
            .one_or_none()
        )

        if existing:
            existing.cycle_score = float(row["gfc_z"])
            existing.cycle_regime = regime
            existing.coverage_gdp_share = float(row.get("coverage_gdp_share", 1.0))
        else:
            session.add(
                GlobalCycleIndex(
                    date=row["date"],
                    frequency=frequency,
                    scope="global",
                    cycle_type="financial_conditions",
                    cycle_score=float(row["gfc_z"]),
                    cycle_regime=regime,
                    method_version=method_version,
                    coverage_gdp_share=float(row.get("coverage_gdp_share", 1.0)),
                )
            )

