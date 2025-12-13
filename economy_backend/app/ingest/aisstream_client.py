import asyncio
import datetime as dt
import json
import logging
from typing import Any, AsyncIterator, Dict, Iterable, List, Optional

from sqlalchemy.orm import Session

from app.config import get_settings
from app.config.country_universe import COUNTRY_UNIVERSE
from app.db.models import RawAisstream
from app.ingest.sample_mode import (
    SampleConfig,
    validate_timeseries_data,
    IngestionError,
)
from app.ingest.utils import store_raw_payload, upsert_shipping_country_month

logger = logging.getLogger(__name__)


def map_position_to_country(message: Dict[str, Any]) -> str:
    if "country" in message:
        return str(message.get("country"))
    ship = message.get("ship" or "vessel", {})
    if isinstance(ship, dict) and ship.get("flagCountry"):
        return str(ship.get("flagCountry"))
    return "UNK"


async def stream_messages(api_key: str, max_retries: int = 3) -> AsyncIterator[Dict[str, Any]]:
    """Stream AIS messages with automatic reconnect on connection failure.
    
    Args:
        api_key: AISstream API key
        max_retries: Maximum number of reconnect attempts (default: 3)
        
    Yields:
        Dict messages from AIS stream
        
    Raises:
        RuntimeError: If websockets library not installed
        ConnectionError: If all reconnect attempts fail
    """
    try:
        import websockets
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError("websockets required for AIS Stream ingestion") from exc

    uri = f"wss://stream.aisstream.io/v0/stream?api-key={api_key}"
    retry_delay = 1.0  # Initial retry delay in seconds
    
    for attempt in range(max_retries):
        try:
            async with websockets.connect(
                uri, 
                ping_interval=20,  # Send ping every 20s to keep connection alive
                ping_timeout=10,   # Wait 10s for pong response
            ) as websocket:  # pragma: no cover
                logger.info(f"AISstream WebSocket connected (attempt {attempt + 1}/{max_retries})")
                async for msg in websocket:
                    try:
                        yield json.loads(msg)
                    except json.JSONDecodeError:
                        logger.debug("Skipping non-JSON message from AIS stream")
                # If we exit cleanly, we're done
                return
                
        except (websockets.exceptions.ConnectionClosed, websockets.exceptions.WebSocketException) as e:
            logger.warning(f"AISstream connection lost (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                logger.info(f"Reconnecting in {retry_delay:.1f}s...")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 30)  # Exponential backoff, max 30s
            else:
                raise ConnectionError(f"AISstream: Failed after {max_retries} attempts") from e


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
    sample_config: Optional[SampleConfig] = None,
) -> None:
    sample_config = sample_config or SampleConfig()
    settings = get_settings()
    collected: List[Dict[str, Any]] = []
    
    # Limit message_limit in sample mode
    if sample_config.enabled and message_limit is None:
        message_limit = 10

    async def _run() -> None:
        nonlocal collected
        source = message_source or stream_messages(settings.aisstream_api_key)
        start = dt.datetime.now(dt.timezone.utc)
        async for message in source:
            collected.append(message)
            if message_limit and len(collected) >= message_limit:
                break
            if (dt.datetime.now(dt.timezone.utc) - start).total_seconds() >= duration_seconds:
                break

    try:
        asyncio.run(_run())
    except Exception as e:
        logger.error("AIS stream ingestion encountered an error: %s", e)
        if sample_config.strict_validation:
            raise IngestionError("AISSTREAM", "GLOBAL", f"Stream error: {e}")

    store_raw_payload(session, RawAisstream, params={"count": len(collected)}, payload=collected)

    aggregates = _aggregate_messages(collected)
    
    # Build validation rows
    validation_rows: List[Dict[str, Any]] = []
    for (country, year, month), stats in aggregates.items():
        if COUNTRY_UNIVERSE and country not in COUNTRY_UNIVERSE and country != "UNK":
            continue
        
        # Create a pseudo timeseries row for validation
        validation_rows.append({
            "indicator_id": 0,  # Placeholder
            "country_id": country,
            "date": dt.date(year, month, 1),
            "value": float(stats["count"]),
            "source": "AISSTREAM",
            "ingested_at": dt.datetime.now(dt.timezone.utc),
        })
        
        try:
            upsert_shipping_country_month(
                session,
                country,
                year,
                month,
                activity=stats["count"],
                transits=len(stats["transits"]),
            )
        except Exception as e:
            logger.error("Failed to upsert shipping data for %s/%d/%d: %s", country, year, month, e)
            if sample_config.strict_validation:
                raise IngestionError("AISSTREAM", country, f"Upsert failed: {e}")
    
    # Validate aggregated results
    if sample_config.enabled and validation_rows:
        validation = validate_timeseries_data(
            validation_rows,
            expected_countries=list(set(r["country_id"] for r in validation_rows)),
            sample_config=sample_config,
        )
        logger.info("AISStream processed %d aggregated records", validation.record_count)
        if sample_config.strict_validation:
            validation.raise_if_invalid()

    session.commit()

