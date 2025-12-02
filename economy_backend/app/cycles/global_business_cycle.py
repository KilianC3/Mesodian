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

GDP_GROWTH_CODES = {"GDP_REAL_YOY", "GDP_REAL"}
GDP_NOMINAL_CODE = "GDP_CURRENT_USD"


def _fetch_indicator_panel(
    session: Session, canonical_codes: Iterable[str]
) -> pd.DataFrame:
    query = (
        session.query(
            TimeSeriesValue.date,
            TimeSeriesValue.country_id,
            Indicator.canonical_code,
            TimeSeriesValue.value,
            Indicator.timing_class,
        )
        .join(Indicator, Indicator.id == TimeSeriesValue.indicator_id)
        .filter(Indicator.canonical_code.in_(list(canonical_codes)))
    )
    records = query.all()
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


def _pivot_country_series(df: pd.DataFrame) -> pd.DataFrame:
    pivot = (
        df.pivot_table(
            index="date", columns="country_id", values="value", aggfunc="last"
        )
        .sort_index()
    )
    pivot = pivot.loc[~pivot.index.duplicated(keep="last")]
    return pivot


def gbc_regime_from_z(z: float) -> str:
    bands = [
        (None, -1.5, "Global_slump"),
        (-1.5, -0.5, "Below_trend"),
        (-0.5, 0.5, "Near_trend"),
        (0.5, 1.5, "Above_trend"),
        (1.5, None, "Global_boom"),
    ]
    return z_to_regime(z, bands)


def compute_gbc_index(session: Session, frequency: str = "annual") -> pd.DataFrame:
    """Compute the Global Business Cycle index using PCA on country GDP growth."""

    growth_df = _fetch_indicator_panel(session, GDP_GROWTH_CODES)
    if growth_df.empty:
        raise ValueError("No GDP growth data available to compute GBC")

    gdp_panel = _pivot_country_series(growth_df)
    z_panel = gdp_panel.apply(standardise_series_to_z, axis=0)
    z_panel = z_panel.dropna(how="all", axis=1)
    z_panel = z_panel.dropna(how="any")

    if z_panel.empty or z_panel.shape[1] < 1:
        raise ValueError("Insufficient panel data to compute GBC")

    pca = PCA(n_components=1)
    factor_scores = pca.fit_transform(z_panel)
    factor_series = pd.Series(factor_scores.flatten(), index=z_panel.index, name="gbc_factor")
    gbc_z = standardise_series_to_z(factor_series).rename("gbc_z")

    coverage = _compute_gdp_coverage(session, z_panel.index, z_panel.columns)
    result = pd.DataFrame({"date": gbc_z.index, "gbc_z": gbc_z.values, "coverage_gdp_share": coverage})
    result["frequency"] = frequency
    return result


def _compute_gdp_coverage(
    session: Session, dates: Iterable[pd.Timestamp], included_countries: Iterable[str]
) -> pd.Series:
    gdp_nominal_df = _fetch_indicator_panel(session, {GDP_NOMINAL_CODE})
    if gdp_nominal_df.empty:
        return pd.Series(np.nan, index=pd.Index(dates))

    gdp_pivot = _pivot_country_series(gdp_nominal_df)
    gdp_pivot = gdp_pivot.reindex(index=pd.to_datetime(list(dates)))
    included = [c for c in included_countries if c in gdp_pivot.columns]
    coverage_values = []
    for date in gdp_pivot.index:
        total = gdp_pivot.loc[date].dropna().sum()
        numerator = gdp_pivot.loc[date, included].dropna().sum() if included else 0
        if total and total > 0:
            coverage_values.append(float(numerator / total))
        else:
            coverage_values.append(np.nan)
    return pd.Series(coverage_values, index=gdp_pivot.index)


def write_gbc_to_db(
    session: Session, df: pd.DataFrame, frequency: str, method_version: str = "gbc_pca_v1"
) -> None:
    for _, row in df.iterrows():
        existing = (
            session.query(GlobalCycleIndex)
            .filter(
                GlobalCycleIndex.date == row["date"],
                GlobalCycleIndex.frequency == frequency,
                GlobalCycleIndex.scope == "global",
                GlobalCycleIndex.cycle_type == "business",
                GlobalCycleIndex.method_version == method_version,
            )
            .one_or_none()
        )

        regime = gbc_regime_from_z(float(row["gbc_z"]))
        coverage_share = row.get("coverage_gdp_share")

        if existing:
            existing.cycle_score = float(row["gbc_z"])
            existing.cycle_regime = regime
            existing.coverage_gdp_share = coverage_share
        else:
            record = GlobalCycleIndex(
                date=row["date"],
                frequency=frequency,
                scope="global",
                cycle_type="business",
                cycle_score=float(row["gbc_z"]),
                cycle_regime=regime,
                method_version=method_version,
                coverage_gdp_share=coverage_share,
            )
            session.add(record)

