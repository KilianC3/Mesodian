from __future__ import annotations

import logging

from sqlalchemy.orm import Session

from app.cycles.global_business_cycle import compute_gbc_index, write_gbc_to_db
from app.cycles.global_trade_cycle import compute_gtmc_index, write_gtmc_to_db
from app.cycles.global_commodity_cycles import (
    compute_commodity_cycles,
    write_commodity_cycles_to_db,
)
from app.cycles.global_inflation_cycle import compute_gic_index, write_gic_to_db
from app.cycles.global_financial_cycle import compute_gfc_index, write_gfc_to_db

logger = logging.getLogger(__name__)


def compute_all_global_cycles(session: Session) -> None:
    try:
        gbc_df = compute_gbc_index(session)
        write_gbc_to_db(session, gbc_df, frequency="annual")
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("Failed to compute GBC: %s", exc)

    try:
        gtmc_df = compute_gtmc_index(session)
        write_gtmc_to_db(session, gtmc_df, frequency="monthly")
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("Failed to compute GTMC: %s", exc)

    try:
        commodity_df = compute_commodity_cycles(session)
        write_commodity_cycles_to_db(session, commodity_df, frequency="monthly")
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("Failed to compute commodity cycles: %s", exc)

    try:
        gic_df = compute_gic_index(session)
        write_gic_to_db(session, gic_df, frequency="quarterly")
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("Failed to compute GIC: %s", exc)

    try:
        gfc_df = compute_gfc_index(session)
        write_gfc_to_db(session, gfc_df, frequency="monthly")
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("Failed to compute GFC: %s", exc)


if __name__ == "__main__":  # pragma: no cover - CLI entry
    from app.db.engine import SessionLocal

    session = SessionLocal()
    try:
        compute_all_global_cycles(session)
        session.commit()
    finally:
        session.close()

