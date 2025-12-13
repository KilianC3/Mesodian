import datetime as dt
import logging
import time
from typing import Dict, Iterable, List, Optional

import pandas as pd
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.db.models import Asset, AssetPrice, RawYfinance
from app.ingest.sample_mode import (
    SampleConfig,
    IngestionError,
)
from app.ingest.utils import store_raw_payload
from app.pools.loader import get_all_tickers

logger = logging.getLogger(__name__)


def fetch_prices(tickers: List[str], start: dt.date, end: dt.date) -> Dict[str, pd.DataFrame]:
    try:
        import yfinance as yf  # type: ignore
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError("yfinance is required for price ingestion") from exc

    try:
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
    except Exception as e:
        raise IngestionError("YFINANCE", "N/A", f"Fetch error: {e}")


def _upsert_asset(session: Session, symbol: str, *, asset_type: str = "EQUITY") -> Asset:
    asset = session.query(Asset).filter(Asset.symbol == symbol).one_or_none()
    if asset:
        return asset
    asset = Asset(symbol=symbol, name=symbol, asset_type=asset_type)
    session.add(asset)
    session.flush()
    return asset


def _upsert_prices(session: Session, asset: Asset, df: pd.DataFrame) -> None:
    import numpy as np
    
    def convert_value(val):
        """Convert numpy/pandas types to Python native types."""
        if val is None or pd.isna(val):
            return None
        if isinstance(val, (np.integer, np.floating)):
            return float(val)
        return val
    
    for date, row in df.iterrows():
        existing = (
            session.query(AssetPrice)
            .filter(AssetPrice.asset_id == asset.id, AssetPrice.date == date.date())
            .one_or_none()
        )
        values = {
            "open": convert_value(row.get("Open")),
            "high": convert_value(row.get("High")),
            "low": convert_value(row.get("Low")),
            "close": convert_value(row.get("Close")),
            "adj_close": convert_value(row.get("Adj Close")),
            "volume": convert_value(row.get("Volume")),
        }
        if existing:
            for field, value in values.items():
                setattr(existing, field, value)
        else:
            # Let database auto-increment handle ID assignment
            session.add(
                AssetPrice(
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
    sample_config: Optional[SampleConfig] = None,
) -> None:
    """Ingest YFINANCE data with optional sample mode."""
    sample_config = sample_config or SampleConfig()
    all_tickers = list(tickers) if tickers else get_all_tickers()
    
    # Limit tickers in sample mode
    if sample_config.enabled:
        all_tickers = all_tickers[:sample_config.max_records_per_ticker]
    
    end_date = dt.date.today()
    # In sample mode, only fetch 5 days of data
    if sample_config.enabled:
        start_date = end_date - dt.timedelta(days=sample_config.max_records_per_ticker)
    else:
        start_date = end_date - dt.timedelta(days=lookback_days)

    for i in range(0, len(all_tickers), batch_size):
        batch = all_tickers[i : i + batch_size]
        try:
            price_data = fetch_prices(batch, start=start_date, end=end_date)
        except IngestionError:
            raise
        except Exception as e:
            logger.error(f"YFINANCE: Failed to fetch batch {batch}: {e}")
            if sample_config.strict_validation:
                raise IngestionError("YFINANCE", "N/A", f"Batch fetch failed: {e}")
            continue

        serialized: Dict[str, Dict[str, List[object]]] = {}
        for ticker, df in price_data.items():
            reset_df = df.reset_index()
            # Convert all datetime/timestamp columns to strings for JSON serialization
            for col in reset_df.columns:
                if pd.api.types.is_datetime64_any_dtype(reset_df[col]):
                    reset_df[col] = reset_df[col].astype(str)
            serialized[ticker] = reset_df.to_dict(orient="list")

        store_raw_payload(
            session,
            RawYfinance,
            params={"tickers": batch, "start": str(start_date), "end": str(end_date)},
            payload=serialized,
        )

        for ticker, df in price_data.items():
            try:
                asset = _upsert_asset(session, ticker)
                _upsert_prices(session, asset, df)
            except Exception as e:
                logger.error(f"YFINANCE: Failed to upsert {ticker}: {e}")
                if sample_config.strict_validation:
                    raise IngestionError("YFINANCE", ticker, f"Upsert failed: {e}")

        session.commit()
        logger.info(f"YFINANCE: Ingested {len(price_data)} tickers")
        if throttle_seconds:
            time.sleep(throttle_seconds)

