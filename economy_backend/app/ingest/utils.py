from __future__ import annotations

import datetime as dt
import logging
from typing import Any, Dict, Iterable, List, Sequence, Type

from sqlalchemy import and_, func, insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from app.db.models import (
    AnalystRating,
    EquityFinancials,
    EquityFundamentals,
    Indicator,
    InsiderTrade,
    RawBase,
    StockNews,
    TimeSeriesValue,
    TradeFlow,
)

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
    record = model(fetched_at=dt.datetime.now(dt.timezone.utc), params=params, payload=payload)
    session.add(record)


def bulk_upsert_timeseries(session: Session, rows: Sequence[Dict[str, Any]]) -> None:
    if not rows:
        return

    table = TimeSeriesValue.__table__
    dialect_name = session.bind.dialect.name if session.bind else ""
    if dialect_name == "postgresql":  # pragma: no cover - environment specific
        stmt = pg_insert(table).values(rows)
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


def upsert_shipping_country_month(
    session: Session, country_id: str, year: int, month: int, *, activity: Any, transits: Any
) -> None:
    from app.db.models import ShippingCountryMonth

    existing = (
        session.query(ShippingCountryMonth)
        .filter(
            ShippingCountryMonth.country_id == country_id,
            ShippingCountryMonth.year == year,
            ShippingCountryMonth.month == month,
        )
        .one_or_none()
    )
    if existing:
        existing.activity_level = activity
        existing.transits = transits
    else:
        next_id = (
            session.query(func.coalesce(func.max(ShippingCountryMonth.id), 0)).scalar() or 0
        )
        session.add(
            ShippingCountryMonth(
                id=int(next_id) + 1,
                country_id=country_id,
                year=year,
                month=month,
                activity_level=activity,
                transits=transits,
            )
        )


def bulk_upsert_tradeflows(session: Session, rows: Sequence[Dict[str, Any]]) -> None:
    """Upsert trade flow rows keyed by reporter, partner, year, section, flow type."""

    if not rows:
        return

    # TODO: Add unique constraint to TradeFlow table to enable PostgreSQL upsert optimization
    # For now, use individual query-and-upsert approach for all dialects
    # NOTE: Using .first() instead of .one_or_none() to handle existing duplicates gracefully
    
    for row in rows:
        existing = (
            session.query(TradeFlow)
            .filter(
                and_(
                    TradeFlow.reporter_country_id == row["reporter_country_id"],
                    TradeFlow.partner_country_id == row["partner_country_id"],
                    TradeFlow.year == row["year"],
                    TradeFlow.hs_section == row.get("hs_section"),
                    TradeFlow.flow_type == row.get("flow_type"),
                )
            )
            .first()  # Use .first() to get first match even if duplicates exist
        )
        if existing:
            existing.value_usd = row.get("value_usd", existing.value_usd)
        else:
            session.add(TradeFlow(**row))


def bulk_upsert_equity_fundamentals(session: Session, rows: Sequence[Dict[str, Any]]) -> None:
    """Upsert equity fundamentals rows keyed by ticker and date."""
    if not rows:
        return

    table = EquityFundamentals.__table__
    dialect_name = session.bind.dialect.name if session.bind else ""
    if dialect_name == "postgresql":
        stmt = pg_insert(table).values(rows)
        # Only update columns that are present in the data (exclude id, ticker, date)
        update_cols = {}
        for col in table.c:
            if col.name not in ("id", "ticker", "date"):
                update_cols[col.name] = stmt.excluded[col.name]
        stmt = stmt.on_conflict_do_update(
            index_elements=[table.c.ticker, table.c.date],
            set_=update_cols,
        )
        session.execute(stmt)
        return

    for row in rows:
        existing = (
            session.query(EquityFundamentals)
            .filter(
                and_(
                    EquityFundamentals.ticker == row["ticker"],
                    EquityFundamentals.date == row["date"],
                )
            )
            .one_or_none()
        )
        if existing:
            for key, value in row.items():
                if key not in ("id", "ticker", "date"):
                    setattr(existing, key, value)
        else:
            session.add(EquityFundamentals(**row))


def bulk_upsert_stock_news(session: Session, rows: Sequence[Dict[str, Any]]) -> None:
    """Upsert stock news rows keyed by ticker, timestamp, and headline."""
    if not rows:
        return

    table = StockNews.__table__
    dialect_name = session.bind.dialect.name if session.bind else ""
    if dialect_name == "postgresql":
        stmt = pg_insert(table).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=[table.c.ticker, table.c.timestamp, table.c.headline],
            set_={col.name: stmt.excluded[col.name] for col in table.c if col.name not in ("id", "ticker", "timestamp", "headline")},
        )
        session.execute(stmt)
        return

    for row in rows:
        existing = (
            session.query(StockNews)
            .filter(
                and_(
                    StockNews.ticker == row["ticker"],
                    StockNews.timestamp == row["timestamp"],
                    StockNews.headline == row["headline"],
                )
            )
            .one_or_none()
        )
        if existing:
            for key, value in row.items():
                if key not in ("id", "ticker", "timestamp", "headline"):
                    setattr(existing, key, value)
        else:
            session.add(StockNews(**row))


def bulk_upsert_analyst_ratings(session: Session, rows: Sequence[Dict[str, Any]]) -> None:
    """Upsert analyst rating rows keyed by ticker, date, firm, and action."""
    if not rows:
        return

    table = AnalystRating.__table__
    dialect_name = session.bind.dialect.name if session.bind else ""
    if dialect_name == "postgresql":
        stmt = pg_insert(table).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=[table.c.ticker, table.c.date, table.c.firm, table.c.action],
            set_={col.name: stmt.excluded[col.name] for col in table.c if col.name not in ("id", "ticker", "date", "firm", "action")},
        )
        session.execute(stmt)
        return

    for row in rows:
        existing = (
            session.query(AnalystRating)
            .filter(
                and_(
                    AnalystRating.ticker == row["ticker"],
                    AnalystRating.date == row["date"],
                    AnalystRating.firm == row["firm"],
                    AnalystRating.action == row["action"],
                )
            )
            .one_or_none()
        )
        if existing:
            for key, value in row.items():
                if key not in ("id", "ticker", "date", "firm", "action"):
                    setattr(existing, key, value)
        else:
            session.add(AnalystRating(**row))


def bulk_upsert_insider_trades(session: Session, rows: Sequence[Dict[str, Any]]) -> None:
    """Upsert insider trade rows keyed by ticker, insider_name, date, and transaction_type."""
    if not rows:
        return

    table = InsiderTrade.__table__
    dialect_name = session.bind.dialect.name if session.bind else ""
    if dialect_name == "postgresql":
        stmt = pg_insert(table).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=[table.c.ticker, table.c.insider_name, table.c.date, table.c.transaction_type],
            set_={col.name: stmt.excluded[col.name] for col in table.c if col.name not in ("id", "ticker", "insider_name", "date", "transaction_type")},
        )
        session.execute(stmt)
        return

    for row in rows:
        existing = (
            session.query(InsiderTrade)
            .filter(
                and_(
                    InsiderTrade.ticker == row["ticker"],
                    InsiderTrade.insider_name == row["insider_name"],
                    InsiderTrade.date == row["date"],
                    InsiderTrade.transaction_type == row["transaction_type"],
                )
            )
            .one_or_none()
        )
        if existing:
            for key, value in row.items():
                if key not in ("id", "ticker", "insider_name", "date", "transaction_type"):
                    setattr(existing, key, value)
        else:
            session.add(InsiderTrade(**row))


def bulk_upsert_equity_financials(session: Session, rows: Sequence[Dict[str, Any]]) -> None:
    """Upsert equity financials rows keyed by ticker, statement_type, year, and line_item."""
    if not rows:
        return

    table = EquityFinancials.__table__
    dialect_name = session.bind.dialect.name if session.bind else ""
    if dialect_name == "postgresql":
        stmt = pg_insert(table).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=[table.c.ticker, table.c.statement_type, table.c.year, table.c.line_item],
            set_={"value": stmt.excluded.value},
        )
        session.execute(stmt)
        return

    for row in rows:
        existing = (
            session.query(EquityFinancials)
            .filter(
                and_(
                    EquityFinancials.ticker == row["ticker"],
                    EquityFinancials.statement_type == row["statement_type"],
                    EquityFinancials.year == row["year"],
                    EquityFinancials.line_item == row["line_item"],
                )
            )
            .one_or_none()
        )
        if existing:
            existing.value = row.get("value", existing.value)
        else:
            session.add(EquityFinancials(**row))


def parse_timeseries_rows(
    df: Any,
    *,
    indicator_id: int,
    country_id: str,
    source: str,
    time_column: str = "time",
    value_column: str = "value",
    location_fields: Iterable[str] = ("LOCATION", "REF_AREA", "REF_AREA_ID", "geo"),
) -> List[Dict[str, Any]]:
    """Parse a dataframe-like object into TimeSeriesValue rows."""

    rows: List[Dict[str, Any]] = []
    if df is None:
        return rows

    location_fields = list(location_fields)

    for _, row in df.iterrows():  # type: ignore[call-arg]
        location = None
        for field in location_fields:
            if field in row and row[field]:
                location = row[field]
                break
        if location and str(location).upper() != country_id.upper():
            continue

        time_value = row.get(time_column) if isinstance(row, dict) else getattr(row, time_column, None)
        value = row.get(value_column) if isinstance(row, dict) else getattr(row, value_column, None)
        if time_value is None or value is None:
            continue
        try:
            date = ensure_date(time_value)
            numeric_value = float(value)
        except Exception as exc:  # pragma: no cover - data dependent
            logger.warning("Skipping row %s: %s", row, exc)
            continue
        rows.append(
            {
                "indicator_id": indicator_id,
                "country_id": country_id,
                "date": date,
                "value": numeric_value,
                "source": source,
                "ingested_at": dt.datetime.now(dt.timezone.utc),
            }
        )
    return rows


def ensure_date(value: Any) -> dt.date:
    if isinstance(value, dt.date):
        return value
    if isinstance(value, dt.datetime):
        return value.date()
    text = str(value)
    if text.isdigit() and len(text) == 4:
        return dt.date(int(text), 12, 31)
    # Handle YYYY-Qn format (IMF quarterly data: 2018-Q1)
    if len(text) == 7 and text[4] == '-' and text[5] == 'Q' and text[:4].isdigit() and text[6].isdigit():
        year = int(text[:4])
        quarter = int(text[6])
        month = (quarter - 1) * 3 + 1  # Q1->1, Q2->4, Q3->7, Q4->10
        return dt.date(year, month, 1)
    # Handle YYYY-Mnn format (IMF monthly data: 2018-M01)
    if len(text) == 8 and text[4] == '-' and text[5] == 'M' and text[:4].isdigit() and text[6:].isdigit():
        year = int(text[:4])
        month = int(text[6:])
        return dt.date(year, month, 1)
    # Handle YYYY-MM format (monthly data)
    if len(text) == 7 and text[4] == '-' and text[:4].isdigit() and text[5:].isdigit():
        year = int(text[:4])
        month = int(text[5:])
        return dt.date(year, month, 1)
    # Handle ISO datetime strings (with or without timezone)
    if "T" in text:
        # Strip timezone designators but keep date component intact
        cleaned = text
        if "+" in cleaned:
            cleaned = cleaned.split("+")[0]
        if "Z" in cleaned:
            cleaned = cleaned.replace("Z", "")
        try:
            return dt.datetime.fromisoformat(cleaned).date()
        except ValueError:
            # Fall back to parsing only the date portion before 'T'
            try:
                return dt.date.fromisoformat(cleaned.split("T")[0])
            except ValueError:
                pass
    return dt.date.fromisoformat(text)



