import datetime as dt
import logging
from io import StringIO
from typing import Dict, Iterable, List, Optional

import pandas as pd
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.db.models import Asset, AssetPrice, RawStooq
from app.ingest.utils import store_raw_payload

logger = logging.getLogger(__name__)


STOOQ_CONFIG: Dict[str, Dict[str, str]] = {
    "^SPX": {"name": "S&P 500", "type": "INDEX"},
    "EURUSD": {"name": "EUR/USD", "type": "FX"},
}


def fetch_stooq_csv(symbol: str) -> pd.DataFrame:
    try:
        import httpx
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("httpx required for stooq ingestion") from exc

    url = f"https://stooq.pl/q/d/l/?s={symbol.lower()}&i=d"
    response = httpx.get(url, timeout=30.0)
    response.raise_for_status()
    return pd.read_csv(StringIO(response.text))


def _upsert_asset(session: Session, symbol: str, cfg: Dict[str, str]) -> Asset:
    asset = session.query(Asset).filter(Asset.symbol == symbol).one_or_none()
    if asset:
        return asset
    asset = Asset(symbol=symbol, name=cfg.get("name", symbol), asset_type=cfg.get("type", "INDEX"))
    session.add(asset)
    session.flush()
    return asset


def _upsert_prices(session: Session, asset: Asset, df: pd.DataFrame) -> None:
    for _, row in df.iterrows():
        date = dt.date.fromisoformat(str(row.get("Date")))
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
            next_id = session.query(func.coalesce(func.max(AssetPrice.id), 0)).scalar() or 0
            session.add(AssetPrice(id=int(next_id) + 1, asset_id=asset.id, date=date, **values))


def ingest_full(
    session: Session,
    *,
    symbol_subset: Optional[Iterable[str]] = None,
) -> None:
    selected = set(symbol_subset) if symbol_subset else None

    for symbol, cfg in STOOQ_CONFIG.items():
        if selected and symbol not in selected:
            continue
        try:
            df = fetch_stooq_csv(symbol)
        except Exception:
            logger.exception("Failed to fetch Stooq CSV for %s", symbol)
            continue

        store_raw_payload(session, RawStooq, params={"symbol": symbol}, payload=df.to_dict())
        asset = _upsert_asset(session, symbol, cfg)
        _upsert_prices(session, asset, df)

    session.commit()

