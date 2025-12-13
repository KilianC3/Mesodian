import datetime as dt
import logging
from io import StringIO
from typing import Dict, Iterable, List, Optional

import pandas as pd
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.db.models import Asset, AssetPrice, RawStooq
from app.ingest.sample_mode import (
    SampleConfig,
    IngestionError,
)
from app.ingest.utils import store_raw_payload

logger = logging.getLogger(__name__)


STOOQ_CONFIG: Dict[str, Dict[str, str]] = {
    "^SPX": {"name": "S&P 500", "type": "INDEX", "min_date": "1957-01-02"},  # S&P 500 created in 1957
    "EURUSD": {"name": "EUR/USD", "type": "FX"},
}


def fetch_stooq_csv(symbol: str, *, sample_config: Optional[SampleConfig] = None) -> pd.DataFrame:
    """Fetch Stooq CSV data with optional date range limiting."""
    sample_config = sample_config or SampleConfig()
    try:
        import httpx
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("httpx required for stooq ingestion") from exc

    url = f"https://stooq.pl/q/d/l/?s={symbol.lower()}&i=d"
    
    # Add date range parameters for sample mode
    if sample_config.enabled:
        end_date = dt.date.today()
        start_date = end_date - dt.timedelta(days=sample_config.max_records_per_ticker * 2)
        url += f"&d1={start_date.strftime('%Y%m%d')}&d2={end_date.strftime('%Y%m%d')}"
    
    try:
        response = httpx.get(url, timeout=30.0)
        response.raise_for_status()
        df = pd.read_csv(StringIO(response.text))
        
        # Translate Polish column names to English
        column_mapping = {
            'Data': 'Date',
            'Otwarcie': 'Open',
            'Najwyzszy': 'High',
            'Najnizszy': 'Low',
            'Zamkniecie': 'Close',
            'Wolumen': 'Volume'
        }
        df.rename(columns=column_mapping, inplace=True)
        
        # Limit rows in sample mode
        if sample_config.enabled and len(df) > sample_config.max_records_per_ticker:
            df = df.tail(sample_config.max_records_per_ticker)
        
        return df
    except Exception as e:
        raise IngestionError("STOOQ", symbol, f"Fetch error: {e}")


def _upsert_asset(session: Session, symbol: str, cfg: Dict[str, str]) -> Asset:
    asset = session.query(Asset).filter(Asset.symbol == symbol).one_or_none()
    if asset:
        return asset
    asset = Asset(symbol=symbol, name=cfg.get("name", symbol), asset_type=cfg.get("type", "INDEX"))
    session.add(asset)
    session.flush()
    return asset


def _upsert_prices(session: Session, asset: Asset, df: pd.DataFrame, cfg: Dict[str, str]) -> None:
    # Get minimum date filter if configured
    min_date_str = cfg.get("min_date")
    min_date = dt.date.fromisoformat(min_date_str) if min_date_str else None
    
    for _, row in df.iterrows():
        date_val = row.get("Date")
        if date_val is None or pd.isna(date_val):
            logger.warning(f"STOOQ: Skipping row with null date for asset {asset.symbol}")
            continue
        
        try:
            date = dt.date.fromisoformat(str(date_val))
        except (ValueError, TypeError) as e:
            logger.warning(f"STOOQ: Invalid date format '{date_val}' for asset {asset.symbol}: {e}")
            continue
        
        # Skip dates before minimum if configured
        if min_date and date < min_date:
            continue
        
        existing = (
            session.query(AssetPrice)
            .filter(AssetPrice.asset_id == asset.id, AssetPrice.date == date)
            .one_or_none()
        )
        values = {
            "open": row.get("Open"),
            "high": row.get("High"),
            "low": row.get("Low"),
            "close": row.get("Close"),
            "volume": row.get("Volume"),
        }
        if existing:
            for field, value in values.items():
                setattr(existing, field, value)
        else:
            # Let database auto-increment handle ID assignment
            session.add(AssetPrice(asset_id=asset.id, date=date, **values))
    
    # Flush to ensure changes are persisted before processing next asset
    session.flush()


def ingest_full(
    session: Session,
    *,
    symbol_subset: Optional[Iterable[str]] = None,
    sample_config: Optional[SampleConfig] = None,
) -> None:
    """Ingest STOOQ data with optional sample mode."""
    sample_config = sample_config or SampleConfig()
    selected = set(symbol_subset) if symbol_subset else None

    for symbol, cfg in STOOQ_CONFIG.items():
        if selected and symbol not in selected:
            continue
        try:
            df = fetch_stooq_csv(symbol, sample_config=sample_config)
            if df.empty and sample_config.fail_on_empty:
                raise IngestionError("STOOQ", symbol, "Empty dataframe returned")
        except IngestionError:
            raise
        except Exception as e:
            logger.error(f"STOOQ: Failed to fetch CSV for {symbol}: {e}")
            if sample_config.strict_validation:
                raise IngestionError("STOOQ", symbol, f"Fetch failed: {e}")
            continue

        try:
            store_raw_payload(session, RawStooq, params={"symbol": symbol}, payload=df.to_dict())
            asset = _upsert_asset(session, symbol, cfg)
            _upsert_prices(session, asset, df, cfg)
            logger.info(f"STOOQ: Ingested {len(df)} records for {symbol}")
        except Exception as e:
            logger.error(f"STOOQ: Failed to store {symbol}: {e}")
            if sample_config.strict_validation:
                raise IngestionError("STOOQ", symbol, f"Store failed: {e}")

    session.commit()

