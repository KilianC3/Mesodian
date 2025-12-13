"""
ok.surf News API Client
Docs: https://ok.surf

Fetches economics and finance related news from Business, Technology, Science, World, and US sections.
Uses POST /news-section endpoint with keyword filtering for economic/finance relevance.
"""
import datetime as dt
import logging
from typing import Any, Dict, Iterable, List, Optional

import httpx
from sqlalchemy.orm import Session

from app.db.models import RawOksurf
from app.ingest.sample_mode import IngestionError, SampleConfig
from app.ingest.utils import store_raw_payload

logger = logging.getLogger(__name__)

OKSURF_BASE_URL = "https://ok.surf/api/v1"

# News sections for economics/finance news
NEWS_SECTIONS = ["Business", "Technology", "Science", "World", "US"]

# Keywords for filtering economics and finance related articles
ECON_KEYWORDS = [
    "economy", "economic", "inflation", "rate", "interest", "gdp", "growth",
    "recession", "market", "stock", "bond", "yields", "fed", "federal reserve",
    "ecb", "central bank", "unemployment", "jobs report", "cpi", "pce",
    "trade", "tariff", "debt", "budget", "fiscal", "finance", "financial",
    "monetary", "policy", "treasury", "dollar", "currency", "forex", "imf",
    "world bank", "investment", "earnings", "profit", "revenue", "export",
    "import", "manufacturing", "consumer", "retail", "housing", "commodity"
]


def is_econ_finance_article(title: str) -> bool:
    """
    Check if article title contains economics/finance keywords.
    
    Args:
        title: Article title to check
    
    Returns:
        True if title contains any economics/finance keyword
    """
    if not title:
        return False
    
    title_lower = title.lower()
    return any(keyword in title_lower for keyword in ECON_KEYWORDS)


def fetch_econ_news(
    *,
    sample_config: Optional[SampleConfig] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch economics and finance relevant news from OK Surf
    for sections Business, Technology, Science, World, US.
    
    Filters articles to only include those with economics/finance keywords in title.
    
    Args:
        sample_config: Optional sample mode configuration
    
    Returns:
        List of dicts with keys:
        - section
        - title
        - link
        - source
        - source_icon
        - og
        - raw (original article dict)
    
    Raises:
        httpx.HTTPError: If the API request fails
    """
    url = f"{OKSURF_BASE_URL}/news-section"
    
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
    }
    
    body = {
        "sections": NEWS_SECTIONS
    }
    
    logger.info(f"Fetching ok.surf news for sections: {NEWS_SECTIONS}")
    
    try:
        response = httpx.post(url, json=body, headers=headers, timeout=30.0)
        response.raise_for_status()
        data = response.json()
        
        # Flatten response into list of articles
        articles = []
        for section_name, items in data.items():
            if not isinstance(items, list):
                logger.warning(f"Section {section_name} has non-list items: {type(items)}")
                continue
            
            for item in items:
                # Add section if not present
                if "section" not in item:
                    item["section"] = section_name
                
                # Extract title for filtering
                title = item.get("title", "")
                
                # Filter for economics/finance relevance
                if not is_econ_finance_article(title):
                    continue
                
                # Normalize to clean record structure
                article = {
                    "section": item.get("section", section_name),
                    "title": title,
                    "link": item.get("link", ""),
                    "source": item.get("source", ""),
                    "source_icon": item.get("source_icon", ""),
                    "og": item.get("og", {}),
                    "raw": item  # Keep original for debugging
                }
                
                articles.append(article)
        
        logger.info(f"Fetched {len(articles)} economics/finance articles from OK Surf")
        return articles
        
    except httpx.HTTPError as e:
        logger.error(f"Failed to fetch ok.surf news: {e}")
        raise


def fetch_oksurf_news(
    section: str,
    *,
    sample_config: Optional[SampleConfig] = None
) -> List[Dict[str, Any]]:
    """
    DEPRECATED: Use fetch_econ_news() instead.
    Legacy function for backward compatibility.
    
    Fetch news from ok.surf for a specific section.
    
    Args:
        section: News section (World, US, Technology, Business, Science)
        sample_config: Sample configuration for testing
        
    Returns:
        List of news articles
        
    Raises:
        IngestionError: If API request fails
    """
    logger.warning("fetch_oksurf_news() is deprecated, use fetch_econ_news() instead")
    return fetch_econ_news(sample_config=sample_config)


def ingest_full(
    session: Session,
    *,
    sample_config: Optional[SampleConfig] = None,
) -> None:
    """
    Ingest economics and finance news from ok.surf.
    
    Fetches news from all configured sections (Business, Technology, Science, World, US)
    and filters for economics/finance relevance based on title keywords.
    
    Args:
        session: Database session
        sample_config: Optional sample mode configuration
    
    Raises:
        IngestionError: If fetch or storage fails
    """
    sample_config = sample_config or SampleConfig()
    
    logger.info("Starting OKSURF news ingestion")
    
    try:
        # Fetch all economics/finance articles from all sections
        articles = fetch_econ_news(sample_config=sample_config)
        
        if not articles:
            logger.warning("No economics/finance articles found")
            if sample_config.fail_on_empty:
                raise IngestionError("OKSURF", "all_sections", "No articles returned")
            return
        
        # Store raw payload
        params = {
            "sections": NEWS_SECTIONS,
            "count": len(articles),
            "fetched_at": dt.datetime.utcnow().isoformat()
        }
        
        payload = {
            "articles": articles,
            "meta": {
                "sections": NEWS_SECTIONS,
                "total_articles": len(articles),
                "filters": {
                    "keywords": ECON_KEYWORDS
                }
            }
        }
        
        store_raw_payload(
            session=session,
            model=RawOksurf,
            params=params,
            payload=payload
        )
        
        session.commit()
        logger.info(f"Successfully ingested {len(articles)} OKSURF articles")
        
    except httpx.HTTPError as e:
        raise IngestionError("OKSURF", "all_sections", f"HTTP error: {e}")
    except Exception as e:
        raise IngestionError("OKSURF", "all_sections", f"Ingestion error: {e}")
