import asyncio
import datetime as dt
import json
import logging
from typing import Any, AsyncIterator, Dict, Iterable, List, Optional

from sqlalchemy.orm import Session

from app.config import get_settings
from app.config.country_universe import COUNTRY_UNIVERSE
from app.db.models import RawAisstream
from app.ingest.utils import store_raw_payload, upsert_shipping_country_month

logger = logging.getLogger(__name__)


def map_position_to_country(message: Dict[str, Any]) -> str:
    if "country" in message:
        return str(message.get("country"))
    ship = message.get("ship" or "vessel", {})
    if isinstance(ship, dict) and ship.get("flagCountry"):
        return str(ship.get("flagCountry"))
    return "UNK"


async def stream_messages(api_key: str) -> AsyncIterator[Dict[str, Any]]:
    try:
        import websockets
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError("websockets required for AIS Stream ingestion") from exc

    uri = f"wss://stream.aisstream.io/v0/stream?api-key={api_key}"
    async with websockets.connect(uri, ping_interval=None) as websocket:  # pragma: no cover
        async for msg in websocket:
            try:
                yield json.loads(msg)
            except json.JSONDecodeError:
                logger.debug("Skipping non-JSON message from AIS stream")


def _aggregate_messages(messages: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    aggregates: Dict[str, Dict[str, Any]] = {}
    for msg in messages:
        ts = msg.get("timestamp") or msg.get("time")
        if ts is None:
            continue
        try:
            ts_dt = dt.datetime.fromisoformat(str(ts)).date()
        except ValueError:
            continue
        country = map_position_to_country(msg)
        key = (country, ts_dt.year, ts_dt.month)
        aggregates.setdefault(key, {"count": 0, "transits": set()})
        aggregates[key]["count"] += 1
        mmsi = msg.get("mmsi") or msg.get("MMSI")
        if mmsi:
            aggregates[key]["transits"].add(mmsi)
    return aggregates


def ingest_full(
    session: Session,
    *,
    duration_seconds: int = 5,
    message_limit: Optional[int] = None,
    message_source: Optional[AsyncIterator[Dict[str, Any]]] = None,
) -> None:
    settings = get_settings()
    collected: List[Dict[str, Any]] = []

    async def _run() -> None:
        nonlocal collected
        source = message_source or stream_messages(settings.aisstream_api_key)
        start = dt.datetime.utcnow()
        async for message in source:
            collected.append(message)
            if message_limit and len(collected) >= message_limit:
                break
            if (dt.datetime.utcnow() - start).total_seconds() >= duration_seconds:
                break

    try:
        asyncio.run(_run())
    except Exception:
        logger.exception("AIS stream ingestion encountered an error")

    store_raw_payload(session, RawAisstream, params={"count": len(collected)}, payload=collected)

    aggregates = _aggregate_messages(collected)
    for (country, year, month), stats in aggregates.items():
        if COUNTRY_UNIVERSE and country not in COUNTRY_UNIVERSE and country != "UNK":
            continue
        upsert_shipping_country_month(
            session,
            country,
            year,
            month,
            activity=stats["count"],
            transits=len(stats["transits"]),
        )

    session.commit()

