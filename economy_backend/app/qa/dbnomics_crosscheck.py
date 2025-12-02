"""Cross-check helpers comparing primary sources with DB.nomics mirrors."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional

import pandas as pd
from sqlalchemy.orm import Session


@dataclass
class DbnomicsSeriesConfig:
    provider_code: str
    dataset_code: str
    series_code: str
    frequency: Optional[str] = None


@dataclass
class CrosscheckResult:
    coverage_intersection: int
    mean_abs_diff: float
    max_abs_diff: float
    share_exceeding_tolerance: float


def _align_series(primary: pd.Series, secondary: pd.Series) -> pd.DataFrame:
    df = pd.DataFrame({"primary": primary, "dbnomics": secondary}).dropna()
    return df


def crosscheck_indicator(
    session: Session,
    indicator_code: str,
    primary_source: str,
    dbnomics_series_config: DbnomicsSeriesConfig,
    tolerance: float = 0.5,
    primary_fetcher: Optional[Callable[[Session, str], pd.DataFrame]] = None,
    dbnomics_fetcher: Optional[Callable[[DbnomicsSeriesConfig], pd.DataFrame]] = None,
) -> CrosscheckResult:
    """Compare a primary series against the DB.nomics mirror."""

    if primary_fetcher is None or dbnomics_fetcher is None:
        raise ValueError("Both primary_fetcher and dbnomics_fetcher must be provided")

    primary_df = primary_fetcher(session, indicator_code)
    dbnomics_df = dbnomics_fetcher(dbnomics_series_config)

    primary_series = primary_df.set_index("date")["value"] if not primary_df.empty else pd.Series(dtype=float)
    dbnomics_series = dbnomics_df.set_index("date")["value"] if not dbnomics_df.empty else pd.Series(dtype=float)

    aligned = _align_series(primary_series, dbnomics_series)
    if aligned.empty:
        return CrosscheckResult(0, 0.0, 0.0, 0.0)

    abs_diff = (aligned["primary"] - aligned["dbnomics"]).abs()
    coverage_intersection = len(aligned)
    mean_abs_diff = float(abs_diff.mean())
    max_abs_diff = float(abs_diff.max())
    share_exceeding_tolerance = float((abs_diff > tolerance).mean()) if tolerance > 0 else 0.0

    return CrosscheckResult(
        coverage_intersection=coverage_intersection,
        mean_abs_diff=mean_abs_diff,
        max_abs_diff=max_abs_diff,
        share_exceeding_tolerance=share_exceeding_tolerance,
    )
