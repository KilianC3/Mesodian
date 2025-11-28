import datetime as dt
import logging
from typing import Dict, Iterable, List, Optional

from sqlalchemy.orm import Session

from app.db.models import RawRss
from app.ingest.utils import bulk_upsert_timeseries, ensure_date, resolve_indicator_id, store_raw_payload

logger = logging.getLogger(__name__)


CENTRAL_BANK_FEEDS: Dict[str, Dict[str, str]] = {
    "USA": {
        "name": "Federal Reserve",
        "url": "https://www.federalreserve.gov/feeds/press_all.xml",
    },
    "EUR": {
        "name": "European Central Bank",
        "url": "https://www.ecb.europa.eu/rss/press.html",
    },
}


def fetch_feed(url: str) -> List[Dict[str, str]]:
    try:
        import feedparser
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError("feedparser required for RSS ingestion") from exc

    parsed = feedparser.parse(url)
    entries: List[Dict[str, str]] = []
    for entry in parsed.entries:
        entries.append(
            {
                "title": entry.get("title", ""),
                "published": entry.get("published"),
                "link": entry.get("link"),
            }
        )
    return entries


def ingest_full(
    session: Session,
    *,
    country_subset: Optional[Iterable[str]] = None,
) -> None:
    selected = set(country_subset) if country_subset else None
    indicator_id = resolve_indicator_id(session, "POLICY_RATE_CHANGE_FLAG")
    all_rows: List[Dict[str, object]] = []

    for country_id, cfg in CENTRAL_BANK_FEEDS.items():
        if selected and country_id not in selected:
            continue
        try:
            entries = fetch_feed(cfg["url"])
        except Exception:
            logger.exception("Failed to fetch RSS feed for %s", country_id)
            continue

        store_raw_payload(session, RawRss, params={"country": country_id}, payload=entries)
        for entry in entries:
            published = entry.get("published")
            try:
                date = ensure_date(published) if published else dt.date.today()
            except Exception:
                continue
            all_rows.append(
                {
                    "indicator_id": indicator_id,
                    "country_id": country_id,
                    "date": date,
                    "value": 1.0,
                    "source": "CENTRAL_BANK_RSS",
                    "ingested_at": dt.datetime.utcnow(),
                }
            )

    bulk_upsert_timeseries(session, all_rows)
    session.commit()

