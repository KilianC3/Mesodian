"""Indicator timing classification helper.

This module applies a static mapping from indicator canonical codes to timing
classes (leading, coincident, lagging). The mapping can be extended as new
indicators are onboarded. Applying the classification is idempotent and can be
invoked as a maintenance task or as part of ingestion jobs.
"""

from __future__ import annotations

from typing import Dict

from sqlalchemy.orm import Session

from app.db.models import Indicator


ALLOWED_TIMING_CLASSES = {"leading", "coincident", "lagging"}

# Minimal representative mapping; extend as coverage grows.
TIMING_CLASS_MAP: Dict[str, str] = {
    "GDP_REAL_YOY": "coincident",
    "IP_INDEX": "coincident",
    "CPI_HEADLINE_YOY": "lagging",
    "CPI_CORE_YOY": "lagging",
    "GLOBAL_EXPORT_ORDERS_PMI": "leading",
    "GLOBAL_MANUF_PMI": "leading",
    "POLICY_RATE": "lagging",
    "YIELD_CURVE_10Y_2Y": "leading",
    "UNEMPLOYMENT_RATE": "lagging",
}


def apply_timing_classification(session: Session) -> None:
    """Update Indicator.timing_class based on the static TIMING_CLASS_MAP.

    Indicators not present in the mapping are left untouched to avoid
    overwriting any manually curated values.
    """

    indicators = (
        session.query(Indicator)
        .filter(Indicator.canonical_code.in_(list(TIMING_CLASS_MAP.keys())))
        .all()
    )
    for indicator in indicators:
        timing_class = TIMING_CLASS_MAP.get(indicator.canonical_code)
        if timing_class not in ALLOWED_TIMING_CLASSES:
            raise ValueError(
                f"Unsupported timing class '{timing_class}' for code {indicator.canonical_code}"
            )
        indicator.timing_class = timing_class
    session.flush()


if __name__ == "__main__":  # pragma: no cover - CLI helper
    from app.db.engine import SessionLocal

    with SessionLocal() as session:
        apply_timing_classification(session)
        session.commit()
        print("Applied indicator timing classification")

