"""
Utility functions shared by cycle extraction modules. Provides helpers for
standardising series, filtering indicators by timing class, and mapping scores
into discrete regimes used by global and regional cycle pipelines.
"""

from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd


def standardise_series_to_z(series: pd.Series) -> pd.Series:
    """Return z-scored series, returning zeros when variance is zero.

    Parameters
    ----------
    series:
        A pandas Series of numeric values.

    Returns
    -------
    pd.Series
        Z-scored values with NaNs preserved. If the population standard deviation
        is zero or NaN, non-null entries are set to zero so downstream PCA does
        not fail due to constant series.
    """

    centered = series - series.mean(skipna=True)
    std = series.std(skipna=True)
    if pd.isna(std) or std == 0:
        return centered.where(centered.isna(), 0.0)
    return centered / std


def z_to_regime(z_value: float, bands: Iterable[tuple[float | None, float | None, str]]) -> str:
    """Map a z-score to a categorical regime label using configured bands.

    Bands are evaluated in order. Bounds are interpreted as:
    - lower bound inclusive when provided
    - upper bound exclusive when provided
    If the z-score does not fall into any band, a ValueError is raised to make
    the classification deterministic and debuggable.
    """

    for lower, upper, label in bands:
        lower_ok = lower is None or z_value >= lower or np.isclose(z_value, lower)
        upper_ok = upper is None or z_value < upper or np.isclose(z_value, upper)
        if lower_ok and upper_ok:
            return label
    raise ValueError(f"z_value {z_value} did not match any regime band")


def prefer_timing_class(
    df: pd.DataFrame, *, preferred: str = "leading", timing_col: str = "timing_class"
) -> pd.DataFrame:
    """Return the subset of rows matching the preferred timing class when available.

    If no rows carry the preferred timing class the original frame is returned to
    avoid dropping all inputs. The helper is tolerant of missing timing metadata.
    """

    if timing_col not in df.columns:
        return df
    preferred_df = df[df[timing_col] == preferred]
    return preferred_df if not preferred_df.empty else df

