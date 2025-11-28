from __future__ import annotations

import numpy as np
import pandas as pd


def compute_yoy_growth(series: pd.Series) -> pd.Series:
    """Compute year-over-year percentage growth for a time series.

    The function attempts to infer the frequency from the index. Monthly data
    uses a 12-period difference, quarterly uses 4, and other frequencies fall
    back to a single period change. Results are expressed in percent.
    """

    if series is None:
        return pd.Series(dtype=float)

    s = series.sort_index().astype(float)
    if s.empty:
        return pd.Series(dtype=float, index=s.index)

    periods = 1
    if isinstance(s.index, (pd.DatetimeIndex, pd.PeriodIndex)):
        freq = s.index.inferred_freq
        if freq:
            if freq.startswith("M"):
                periods = 12
            elif freq.startswith("Q"):
                periods = 4
            elif freq.startswith(("A", "Y")):
                periods = 1
        elif len(s) > 1:
            # Fallback: if monthly-like spacing is detected, prefer 12 periods
            deltas = s.index.to_series().diff().dropna().dt.days if isinstance(s.index, pd.DatetimeIndex) else None
            if deltas is not None and (deltas.median() or 0) < 40:
                periods = 12
    yoy = s.pct_change(periods=periods) * 100.0
    return yoy


def compute_ratio(num: pd.Series, den: pd.Series) -> pd.Series:
    """Compute a ratio between two aligned series, avoiding division by zero."""

    num_aligned, den_aligned = num.align(den, join="outer")
    ratio = num_aligned.astype(float) / den_aligned.replace({0: np.nan}).astype(float)
    ratio[(den_aligned == 0) | den_aligned.isna()] = np.nan
    return ratio


def compute_rolling_vol(series: pd.Series, window: int) -> float:
    """Return the rolling standard deviation of percentage changes."""

    if series is None or series.empty or window <= 1:
        return float("nan")
    returns = series.sort_index().astype(float).pct_change().dropna()
    if returns.empty:
        return float("nan")
    vol = returns.rolling(window).std().dropna()
    return float(vol.iloc[-1]) if not vol.empty else float("nan")


def compute_trend_gap(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Compute a linear trend and the gap between the series and that trend."""

    s = series.sort_index().astype(float)
    if s.empty:
        empty = pd.Series(dtype=float, index=s.index)
        return empty, empty

    x = np.arange(len(s))
    coeffs = np.polyfit(x, s.values, 1)
    trend_values = coeffs[0] * x + coeffs[1]
    trend = pd.Series(trend_values, index=s.index)
    gap = s - trend
    return trend, gap


def normalise_0_100(series: pd.Series) -> pd.Series:
    """Normalise a series to the range 0-100 using min-max scaling."""

    s = series.astype(float)
    if s.empty:
        return pd.Series(dtype=float, index=s.index)
    min_val = s.min()
    max_val = s.max()
    if pd.isna(min_val) or pd.isna(max_val):
        return pd.Series(dtype=float, index=s.index)
    if max_val == min_val:
        return pd.Series(50.0, index=s.index)
    scaled = (s - min_val) / (max_val - min_val)
    return scaled * 100.0
