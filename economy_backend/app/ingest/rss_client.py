import datetime as dt
import logging
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from sqlalchemy.orm import Session

import yaml

from app.db.models import RawRss
from app.ingest.sample_mode import (
    SampleConfig,
    validate_timeseries_data,
    IngestionError,
)
from app.ingest.utils import bulk_upsert_timeseries, ensure_date, resolve_indicator_id, store_raw_payload

logger = logging.getLogger(__name__)

# Map ISO2 codes to ISO3 codes for country_id resolution
ISO2_TO_ISO3 = {
    "US": "USA", "EA": "DEU",  # EA (Euro Area) → DEU as representative
    "GB": "GBR", "CA": "CAN", "MX": "MEX", "BR": "BRA",
    "ZA": "ZAF", "AU": "AUS", "NZ": "NZL", "JP": "JPN",
    "KR": "KOR", "CN": "CHN", "IN": "IND", "ID": "IDN",
    "TR": "TUR", "CH": "CHE", "SE": "SWE", "NO": "NOR",
    "DK": "DNK", "PL": "POL", "CZ": "CZE", "HU": "HUN",
    "RU": "RUS", "AR": "ARG", "CL": "CHL", "CO": "COL",
    "PE": "PER", "TH": "THA", "MY": "MYS", "PH": "PHL",
    "SG": "SGP", "VN": "VNM", "EG": "EGY", "NG": "NGA",
    "KE": "KEN", "GH": "GHA", "ET": "ETH", "TZ": "TZA",
    "UG": "UGA", "ZM": "ZMB", "ZW": "ZWE", "BW": "BWA",
}


def _load_feed_catalog() -> Dict[str, Dict[str, str]]:
    catalog_path = Path(__file__).resolve().parents[2] / "config" / "catalogs" / "providers.yaml"
    if not catalog_path.exists():
        return {}
    try:
        with open(catalog_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except Exception:
        return {}
    feeds = data.get("RSS", {}).get("feeds", {}) if isinstance(data, dict) else {}
    normalized: Dict[str, Dict[str, str]] = {}
    for code, cfg in feeds.items():
        url = cfg.get("feed_url") if isinstance(cfg, dict) else None
        if not url:
            continue
        # Get country code and convert ISO2 to ISO3
        country_iso2 = cfg.get("country", "")
        country_iso3 = ISO2_TO_ISO3.get(country_iso2, country_iso2)
        normalized[code.upper()] = {
            "name": cfg.get("name", code),
            "url": url,
            "timezone": cfg.get("timezone"),
            "country_id": country_iso3,  # Store ISO3 code for database
        }
    return normalized


CENTRAL_BANK_FEEDS: Dict[str, Dict[str, str]] = _load_feed_catalog() or {
    "US": {
        "name": "Federal Reserve",
        "url": "https://www.federalreserve.gov/feeds/press_all.xml",
    },
}


def fetch_feed(url: str, *, sample_config: Optional[SampleConfig] = None) -> List[Dict[str, str]]:
    try:
        import feedparser
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError("feedparser required for RSS ingestion") from exc

    parsed = feedparser.parse(url)
    entries: List[Dict[str, str]] = []
    for entry in parsed.entries:
        # Handle RFC822/RSS date format
        published_date = None
        if hasattr(entry, 'published_parsed') and entry.published_parsed:
            try:
                import time
                published_date = dt.datetime(*entry.published_parsed[:6]).isoformat()
            except (ValueError, TypeError):
                pass
        if not published_date and entry.get("published"):
            published_date = entry.get("published")
        
        entries.append(
            {
                "title": entry.get("title", ""),
                "published": published_date,
                "link": entry.get("link"),
            }
        )
    
    # Limit entries in sample mode
    if sample_config and sample_config.enabled and sample_config.max_records_per_country:
        entries = entries[:sample_config.max_records_per_country]
    
    return entries


def ingest_full(
    session: Session,
    *,
    country_subset: Optional[Iterable[str]] = None,
    sample_config: Optional[SampleConfig] = None,
) -> None:
    sample_config = sample_config or SampleConfig()
    selected = set(country_subset) if country_subset else None
    
    # Limit in sample mode
    if sample_config.enabled and selected is None:
        selected = set(list(CENTRAL_BANK_FEEDS.keys())[:2])
    
    indicator_id = resolve_indicator_id(session, "POLICY_RATE_CHANGE_FLAG")
    all_rows: List[Dict[str, object]] = []

    for feed_code, cfg in CENTRAL_BANK_FEEDS.items():
        country_id = cfg.get("country_id", feed_code)  # Use ISO3 country_id from config
        if selected and feed_code not in selected:
            continue
        try:
            entries = fetch_feed(cfg["url"], sample_config=sample_config)
        except Exception as e:
            logger.error("Failed to fetch RSS feed for %s: %s", feed_code, e)
            if sample_config.strict_validation:
                raise IngestionError("CENTRAL_BANK_RSS", feed_code, f"Fetch failed: {e}")
            continue

        store_raw_payload(session, RawRss, params={"country": feed_code}, payload=entries)
        
        # Deduplicate by date to avoid cardinality violations
        seen_dates = set()
        for entry in entries:
            published = entry.get("published")
            try:
                date = ensure_date(published) if published else dt.date.today()
            except Exception as e:
                logger.error("Failed to parse RSS entry date for %s: %s", feed_code, e)
                if sample_config.strict_validation:
                    raise IngestionError("CENTRAL_BANK_RSS", feed_code, f"Parse error: {e}")
                continue
            
            # Skip if we already have this country+date combination
            key = (country_id, date)
            if key in seen_dates:
                continue
            seen_dates.add(key)
            
            all_rows.append(
                {
                    "indicator_id": indicator_id,
                    "country_id": country_id,  # Use ISO3 code here
                    "date": date,
                    "value": 1.0,
                    "source": "CENTRAL_BANK_RSS",
                    "ingested_at": dt.datetime.now(dt.timezone.utc),
                }
            )
    
    # Validate before bulk insert
    if sample_config.enabled and all_rows:
        validation = validate_timeseries_data(
            all_rows,
            expected_countries=list(set(r["country_id"] for r in all_rows)),
            sample_config=sample_config,
        )
        logger.info("RSS parsed %d total records", validation.record_count)
        if sample_config.strict_validation:
            validation.raise_if_invalid()

    bulk_upsert_timeseries(session, all_rows)
    session.commit()

