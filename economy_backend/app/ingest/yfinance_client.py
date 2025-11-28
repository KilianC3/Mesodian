import datetime as dt
import logging
import time
from typing import Dict, Iterable, List, Optional

import pandas as pd
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.db.models import Asset, AssetPrice, RawYfinance
from app.ingest.utils import store_raw_payload
from app.pools.loader import get_all_tickers

logger = logging.getLogger(__name__)


def fetch_prices(tickers: List[str], start: dt.date, end: dt.date) -> Dict[str, pd.DataFrame]:
    try:
        import yfinance as yf  # type: ignore
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError("yfinance is required for price ingestion") from exc

    data = yf.download(
        tickers=tickers,
        start=start,
        end=end + dt.timedelta(days=1),
        group_by="ticker",
        auto_adjust=False,
        progress=False,
    )

    if isinstance(data.columns, pd.MultiIndex):
        result: Dict[str, pd.DataFrame] = {}
        for ticker in tickers:
            if ticker in data:
                result[ticker] = data[ticker].dropna(how="all")
        return result
    else:
        return {tickers[0]: data.dropna(how="all")}


def _upsert_asset(session: Session, symbol: str, *, asset_type: str = "EQUITY") -> Asset:
    asset = session.query(Asset).filter(Asset.symbol == symbol).one_or_none()
    if asset:
        return asset
    asset = Asset(symbol=symbol, name=symbol, asset_type=asset_type)
    session.add(asset)
    session.flush()
    return asset


def _upsert_prices(session: Session, asset: Asset, df: pd.DataFrame) -> None:
    for date, row in df.iterrows():
        existing = (
            session.query(AssetPrice)
            .filter(AssetPrice.asset_id == asset.id, AssetPrice.date == date.date())
            .one_or_none()
        )
        values = {
            "open": row.get("Open"),
            "high": row.get("High"),
            "low": row.get("Low"),
            "close": row.get("Close"),
            "adj_close": row.get("Adj Close"),
            "volume": row.get("Volume"),
        }
        if existing:
            for field, value in values.items():
                setattr(existing, field, value)
        else:
            next_id = session.query(func.coalesce(func.max(AssetPrice.id), 0)).scalar() or 0
            session.add(
                AssetPrice(
                    id=int(next_id) + 1,
                    asset_id=asset.id,
                    date=date.date(),
                    **values,
                )
            )


def ingest_full(
    session: Session,
    *,
    tickers: Optional[Iterable[str]] = None,
    lookback_days: int = 365,
    batch_size: int = 25,
    throttle_seconds: float = 0.1,
) -> None:
    all_tickers = list(tickers) if tickers else get_all_tickers()
    end_date = dt.date.today()
    start_date = end_date - dt.timedelta(days=lookback_days)

    for i in range(0, len(all_tickers), batch_size):
        batch = all_tickers[i : i + batch_size]
        try:
            price_data = fetch_prices(batch, start=start_date, end=end_date)
        except Exception:
            logger.exception("Failed to fetch yfinance batch %s", batch)
            continue

        serialized: Dict[str, Dict[str, List[object]]] = {}
        for ticker, df in price_data.items():
            reset_df = df.reset_index()
            if "index" in reset_df.columns:
                reset_df["index"] = reset_df["index"].astype(str)
            serialized[ticker] = reset_df.to_dict(orient="list")

        store_raw_payload(
            session,
            RawYfinance,
            params={"tickers": batch, "start": str(start_date), "end": str(end_date)},
            payload=serialized,
        )

        for ticker, df in price_data.items():
            asset = _upsert_asset(session, ticker)
            _upsert_prices(session, asset, df)

        session.commit()
        if throttle_seconds:
            time.sleep(throttle_seconds)

