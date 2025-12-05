from __future__ import annotations

"""
Data-quality scoring utilities for feature panels.

Functions here compute coverage and freshness scores for ``country_year_features``
and related panels. They are used by feature builders and metrics to flag
countries with sparse or stale inputs.
"""

import math
from datetime import date
from typing import Dict, Optional


def compute_data_coverage_score(feature_values: Dict[str, float | None]) -> float:
    """Return a 0-100 score based on how many feature values are present."""

    if not feature_values:
        return 0.0
    total = len(feature_values)
    present = 0
    for value in feature_values.values():
        if value is None:
            continue
        if isinstance(value, float) and math.isnan(value):
            continue
        present += 1
    return (present / total) * 100.0


def compute_data_freshness_score(latest_observation_date: Optional[date], now: date) -> float:
    """Return a 0-100 score that decays as data gets older.

    A linear decay is applied over a five-year window: observations from today
    score 100, and anything older than five years scores 0.
    """

    if latest_observation_date is None:
        return 0.0
    days_diff = (now - latest_observation_date).days
    if days_diff <= 0:
        return 100.0
    max_age_days = 365 * 5
    score = 100.0 * (1 - min(days_diff, max_age_days) / max_age_days)
    return max(score, 0.0)
