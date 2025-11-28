from __future__ import annotations

import datetime as dt
import logging
from typing import Any, Dict, Iterable, List, Sequence, Type

from sqlalchemy import and_, insert
from sqlalchemy.orm import Session

from app.db.models import Indicator, RawBase, TimeSeriesValue

logger = logging.getLogger(__name__)


def resolve_indicator_id(session: Session, canonical_code: str) -> int:
    indicator = (
        session.query(Indicator).filter(Indicator.canonical_code == canonical_code).one_or_none()
    )
    if indicator is None:
        raise ValueError(f"Indicator with canonical_code={canonical_code} not found")
    return int(indicator.id)


def store_raw_payload(
    session: Session,
    model: Type[RawBase],
    *,
    params: Dict[str, Any],
    payload: Any,
) -> None:
    record = model(fetched_at=dt.datetime.utcnow(), params=params, payload=payload)
    session.add(record)


def bulk_upsert_timeseries(session: Session, rows: Sequence[Dict[str, Any]]) -> None:
    if not rows:
        return

    table = TimeSeriesValue.__table__
    dialect_name = session.bind.dialect.name if session.bind else ""
    if dialect_name == "postgresql":  # pragma: no cover - environment specific
        stmt = insert(table).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=[table.c.indicator_id, table.c.country_id, table.c.date],
            set_={
                "value": stmt.excluded.value,
                "source": stmt.excluded.source,
                "ingested_at": stmt.excluded.ingested_at,
            },
        )
        session.execute(stmt)
        return

    # Fallback for SQLite or other dialects
    for row in rows:
        existing = (
            session.query(TimeSeriesValue)
            .filter(
                and_(
                    TimeSeriesValue.indicator_id == row["indicator_id"],
                    TimeSeriesValue.country_id == row["country_id"],
                    TimeSeriesValue.date == row["date"],
                )
            )
            .one_or_none()
        )
        if existing:
            existing.value = row["value"]
            existing.source = row.get("source", existing.source)
            existing.ingested_at = row.get("ingested_at", existing.ingested_at)
        else:
            session.add(TimeSeriesValue(**row))


def ensure_date(value: Any) -> dt.date:
    if isinstance(value, dt.date):
        return value
    if isinstance(value, dt.datetime):
        return value.date()
    return dt.date.fromisoformat(str(value))

