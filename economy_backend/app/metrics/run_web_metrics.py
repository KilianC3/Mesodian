from __future__ import annotations

"""CLI orchestrator for web and edge metrics."""

import argparse
import logging

from sqlalchemy.orm import Session

from app.db.engine import SessionLocal
from app.metrics.web_metrics import compute_web_metrics_for_year
from app.metrics.web_relationship_metrics import compute_edge_relationship_metrics_for_year

logger = logging.getLogger(__name__)


def compute_all_web_and_edge_metrics(session: Session, year: int) -> None:
    compute_web_metrics_for_year(session, year)
    compute_edge_relationship_metrics_for_year(session, year)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute web and edge metrics")
    parser.add_argument("--year", type=int, required=True, help="Target year")
    return parser.parse_args()


if __name__ == "__main__":  # pragma: no cover - CLI entry
    args = _parse_args()
    session = SessionLocal()
    try:
        compute_all_web_and_edge_metrics(session, args.year)
        session.commit()
        print(f"Computed web and edge metrics for {args.year}")
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("Failed to compute web metrics: %s", exc)
        session.rollback()
        raise
    finally:
        session.close()
