"""
FinViz web scraper for equity fundamentals, earnings, analyst ratings, news, and insider trades.
Implements production-grade scraping with rate limiting, retry logic, and comprehensive parsing.

Data Sources:
- Company fundamentals: P/E, P/B, EPS, market cap, dividend yield, debt/equity, ROE, etc.
- Financial statements: Income statement, balance sheet, cash flow
- Analyst ratings: Upgrades/downgrades with firm, date, price target
- News: Headlines with timestamps and sources
- Insider trades: Insider name, relationship, transaction type, shares, value
- Market overview: Indices performance, sector rotation

Rate Limiting: 1.5-2.5s delays between requests (respectful crawling)
Anti-Detection: Rotating user agents, proper headers, referer
Retry Logic: Exponential backoff on 429/5xx errors
"""

import asyncio
import logging
import random
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from decimal import Decimal

import httpx
from bs4 import BeautifulSoup
from sqlalchemy.orm import Session

from app.ingest.sample_mode import SampleConfig, ValidationResult


logger = logging.getLogger(__name__)


# Rotating user agents for anti-detection
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.2; rv:121.0) Gecko/20100101 Firefox/121.0",
]


class FinVizScraperError(Exception):
    """Raised when FinViz scraping fails."""


class FinVizComprehensiveScraper:
    """Production-grade FinViz scraper with comprehensive data extraction."""

    BASE_URL = "https://finviz.com"
    RATE_LIMIT_DELAY = (1.5, 2.5)  # Random delay between requests (seconds)
    MAX_RETRIES = 3
    BACKOFF_BASE = 5.0  # Base delay for exponential backoff (seconds)

    def __init__(self, timeout_seconds: float = 30.0):
        self.timeout_seconds = timeout_seconds
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self) -> "FinVizComprehensiveScraper":
        self._client = httpx.AsyncClient(timeout=self.timeout_seconds)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._client:
            await self._client.aclose()
        self._client = None

    def _get_headers(self) -> Dict[str, str]:
        """Generate headers with rotating user agent."""
        return {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Referer": "https://finviz.com",
        }

    async def _rate_limited_request(self, url: str) -> str:
        """Make HTTP request with rate limiting and retry logic."""
        if not self._client:
            raise FinVizScraperError("Client not initialized. Use async context manager.")

        # Rate limiting: sleep before request
        delay = random.uniform(*self.RATE_LIMIT_DELAY)
        await asyncio.sleep(delay)

        attempt = 0
        last_exc: Optional[Exception] = None

        while attempt < self.MAX_RETRIES:
            attempt += 1
            try:
                headers = self._get_headers()
                logger.info(f"GET {url} (attempt {attempt})")
                response = await self._client.get(url, headers=headers, follow_redirects=True)

                if response.status_code == 200:
                    return response.text
                elif response.status_code == 429:
                    # Rate limited - exponential backoff
                    backoff_delay = self.BACKOFF_BASE * (2 ** (attempt - 1))
                    logger.warning(f"Rate limited (429), backing off for {backoff_delay}s")
                    await asyncio.sleep(backoff_delay)
                    continue
                elif 500 <= response.status_code < 600:
                    # Server error - retry with backoff
                    backoff_delay = self.BACKOFF_BASE * (2 ** (attempt - 1))
                    logger.warning(f"Server error ({response.status_code}), backing off for {backoff_delay}s")
                    await asyncio.sleep(backoff_delay)
                    continue
                else:
                    response.raise_for_status()

            except httpx.HTTPError as exc:
                last_exc = exc
                logger.warning(f"HTTP error on attempt {attempt} for {url}: {exc}")
                if attempt >= self.MAX_RETRIES:
                    raise FinVizScraperError(f"Request to {url} failed after {self.MAX_RETRIES} retries") from exc
                await asyncio.sleep(self.BACKOFF_BASE * (2 ** (attempt - 1)))

        if last_exc:
            raise FinVizScraperError(f"Request to {url} failed after {self.MAX_RETRIES} retries") from last_exc
        raise FinVizScraperError(f"Request to {url} failed")

    def _parse_number(self, value: str) -> Optional[float]:
        """Parse FinViz number format (handles K, M, B suffixes and percentages)."""
        if not value or value in {"-", "N/A", ""}:
            return None

        value = value.strip().replace(",", "")

        # Handle percentage
        if "%" in value:
            try:
                return float(value.replace("%", "")) / 100.0
            except ValueError:
                return None

        # Handle K, M, B suffixes
        multipliers = {"K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12}
        for suffix, multiplier in multipliers.items():
            if value.endswith(suffix):
                try:
                    return float(value[:-1]) * multiplier
                except ValueError:
                    return None

        # Plain number
        try:
            return float(value)
        except ValueError:
            return None

    def _parse_finviz_date(self, date_str: str) -> Optional[datetime]:
        """Parse FinViz date formats: 'Dec-05-24', 'Today', 'Yesterday'."""
        if not date_str or date_str == "-":
            return None

        date_str = date_str.strip()
        today = datetime.now().date()

        if date_str.lower() == "today":
            return datetime.combine(today, datetime.min.time())
        elif date_str.lower() == "yesterday":
            return datetime.combine(today - timedelta(days=1), datetime.min.time())

        # Try parsing 'Dec-05-24' format
        try:
            return datetime.strptime(date_str, "%b-%d-%y")
        except ValueError:
            pass

        # Try parsing 'Dec 05' format (current year)
        try:
            parsed = datetime.strptime(date_str, "%b %d")
            return parsed.replace(year=today.year)
        except ValueError:
            pass

        logger.warning(f"Could not parse date: {date_str}")
        return None

    def _parse_finviz_timestamp(self, timestamp_str: str) -> Optional[datetime]:
        """Parse FinViz timestamp: 'Today 09:30AM', 'Yesterday 02:15PM', 'Dec-05-24 11:00AM'."""
        if not timestamp_str or timestamp_str == "-":
            return None

        timestamp_str = timestamp_str.strip()
        today = datetime.now().date()

        # Split date and time parts
        parts = timestamp_str.split()
        if len(parts) == 2:
            date_part, time_part = parts

            # Parse date part
            if date_part.lower() == "today":
                date = today
            elif date_part.lower() == "yesterday":
                date = today - timedelta(days=1)
            else:
                try:
                    date = datetime.strptime(date_part, "%b-%d-%y").date()
                except ValueError:
                    logger.warning(f"Could not parse timestamp date: {date_part}")
                    return None

            # Parse time part
            try:
                time = datetime.strptime(time_part, "%I:%M%p").time()
                return datetime.combine(date, time)
            except ValueError:
                logger.warning(f"Could not parse timestamp time: {time_part}")
                return None

        logger.warning(f"Could not parse timestamp: {timestamp_str}")
        return None

    async def scrape_snapshot_fundamentals(self, ticker: str) -> Dict[str, Any]:
        """
        Scrape the main fundamentals table (11x11 grid) from FinViz quote page.
        
        Returns dict with keys: P/E, Forward P/E, PEG, P/S, P/B, P/C, P/FCF,
        Dividend %, ROA, ROE, ROI, Gross Margin, Operating Margin, Net Margin,
        Payout Ratio, Debt/Eq, Current Ratio, Quick Ratio, LT Debt/Eq, 
        EPS (ttm), EPS next Y, EPS next Q, EPS this Y, EPS next Y, EPS next 5Y,
        EPS past 5Y, Sales past 5Y, Sales Q/Q, EPS Q/Q, Market Cap, Income, Sales,
        Book/sh, Cash/sh, Employees, Optionable, Shortable, Recom, etc.
        """
        url = f"{self.BASE_URL}/quote.ashx?t={ticker}"
        html = await self._rate_limited_request(url)
        soup = BeautifulSoup(html, "lxml")

        fundamentals = {"ticker": ticker, "date": datetime.now().date()}

        # Find the snapshot table (class='snapshot-table2')
        snapshot_table = soup.find("table", class_="snapshot-table2")
        if not snapshot_table:
            logger.warning(f"No snapshot table found for {ticker}")
            return fundamentals

        # Parse all rows
        rows = snapshot_table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            # Each row has 12 cells: label1, value1, label2, value2, ... label6, value6
            for i in range(0, len(cells), 2):
                if i + 1 < len(cells):
                    label = cells[i].get_text(strip=True)
                    value = cells[i + 1].get_text(strip=True)
                    
                    # Store with parsed number
                    parsed_value = self._parse_number(value)
                    fundamentals[label] = parsed_value

        return fundamentals

    async def scrape_financial_statements(self, ticker: str) -> Dict[str, Any]:
        """
        Scrape financial statements (Income Statement, Balance Sheet, Cash Flow).
        
        Returns dict with keys: income_statement, balance_sheet, cash_flow (each containing yearly data).
        """
        url = f"{self.BASE_URL}/quote.ashx?t={ticker}&ty=c&ta=1"
        html = await self._rate_limited_request(url)
        soup = BeautifulSoup(html, "lxml")

        statements = {"ticker": ticker}

        # FinViz financial tables have class 'js-table-wrapper'
        tables = soup.find_all("table", class_="js-table-wrapper")
        
        # Parse each table (Income Statement, Balance Sheet, Cash Flow)
        statement_names = ["income_statement", "balance_sheet", "cash_flow"]
        for idx, table in enumerate(tables[:3]):
            if idx >= len(statement_names):
                break
            
            statement_name = statement_names[idx]
            statement_data = {}

            rows = table.find_all("tr")
            if not rows:
                continue

            # First row: headers (years)
            header_row = rows[0]
            years = [cell.get_text(strip=True) for cell in header_row.find_all("th")[1:]]  # Skip first cell (label)

            # Remaining rows: line items
            for row in rows[1:]:
                cells = row.find_all("td")
                if not cells:
                    continue

                line_item = cells[0].get_text(strip=True)
                values = [self._parse_number(cells[i].get_text(strip=True)) for i in range(1, len(cells))]

                statement_data[line_item] = dict(zip(years, values))

            statements[statement_name] = statement_data

        return statements

    async def scrape_analyst_ratings(self, ticker: str) -> List[Dict[str, Any]]:
        """
        Scrape analyst ratings/upgrades/downgrades.
        
        Returns list of dicts with keys: date, action, firm, from_rating, to_rating, price_target.
        """
        url = f"{self.BASE_URL}/quote.ashx?t={ticker}&ty=c&ta=1"
        html = await self._rate_limited_request(url)
        soup = BeautifulSoup(html, "lxml")

        ratings = []

        # Find analyst ratings table (usually has 'news-table' or similar class)
        ratings_table = soup.find("table", class_="fullview-ratings-outer")
        if not ratings_table:
            logger.warning(f"No analyst ratings table found for {ticker}")
            return ratings

        rows = ratings_table.find_all("tr")[1:]  # Skip header
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 5:
                continue

            rating_data = {
                "ticker": ticker,
                "date": self._parse_finviz_date(cells[0].get_text(strip=True)),
                "action": cells[1].get_text(strip=True),
                "firm": cells[2].get_text(strip=True),
                "from_rating": cells[3].get_text(strip=True) if len(cells) > 3 else None,
                "to_rating": cells[4].get_text(strip=True) if len(cells) > 4 else None,
                "price_target": self._parse_number(cells[5].get_text(strip=True)) if len(cells) > 5 else None,
            }
            ratings.append(rating_data)

        return ratings

    async def scrape_news(self, ticker: str) -> List[Dict[str, Any]]:
        """
        Scrape recent news headlines.
        
        Returns list of dicts with keys: timestamp, headline, source, url.
        """
        url = f"{self.BASE_URL}/quote.ashx?t={ticker}"
        html = await self._rate_limited_request(url)
        soup = BeautifulSoup(html, "lxml")

        news_items = []

        # Find news table (class='news-table')
        news_table = soup.find("table", class_="fullview-news-outer")
        if not news_table:
            logger.warning(f"No news table found for {ticker}")
            return news_items

        rows = news_table.find_all("tr")
        current_date = None

        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 2:
                continue

            # First cell: timestamp or date
            timestamp_text = cells[0].get_text(strip=True)
            
            # If only time is present, use current_date
            if len(timestamp_text.split()) == 1 and ":" in timestamp_text:
                if current_date:
                    timestamp_text = f"{current_date} {timestamp_text}"
            else:
                # Full timestamp or date
                parts = timestamp_text.split()
                if len(parts) >= 1:
                    current_date = parts[0]

            timestamp = self._parse_finviz_timestamp(timestamp_text)

            # Second cell: headline and source
            headline_cell = cells[1]
            link = headline_cell.find("a")
            if link:
                headline = link.get_text(strip=True)
                news_url = link.get("href", "")
                source = headline_cell.find("span", class_="news-link-right")
                source_text = source.get_text(strip=True) if source else "Unknown"

                news_items.append({
                    "ticker": ticker,
                    "timestamp": timestamp,
                    "headline": headline,
                    "source": source_text,
                    "url": news_url,
                })

        return news_items

    async def scrape_insider_trades(self, ticker: str) -> List[Dict[str, Any]]:
        """
        Scrape insider trading activity.
        
        Returns list of dicts with keys: date, insider_name, relationship, transaction_type,
        cost, shares, value, shares_total.
        """
        url = f"{self.BASE_URL}/quote.ashx?t={ticker}&ty=c&ta=1"
        html = await self._rate_limited_request(url)
        soup = BeautifulSoup(html, "lxml")

        insider_trades = []

        # Find insider trading table
        insider_table = soup.find("table", class_="body-table")
        if not insider_table:
            logger.warning(f"No insider trading table found for {ticker}")
            return insider_trades

        rows = insider_table.find_all("tr")[1:]  # Skip header
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 7:
                continue

            trade_data = {
                "ticker": ticker,
                "insider_name": cells[0].get_text(strip=True),
                "relationship": cells[1].get_text(strip=True),
                "date": self._parse_finviz_date(cells[2].get_text(strip=True)),
                "transaction_type": cells[3].get_text(strip=True),
                "cost": self._parse_number(cells[4].get_text(strip=True)),
                "shares": self._parse_number(cells[5].get_text(strip=True)),
                "value": self._parse_number(cells[6].get_text(strip=True)),
                "shares_total": self._parse_number(cells[7].get_text(strip=True)) if len(cells) > 7 else None,
            }
            insider_trades.append(trade_data)

        return insider_trades

    async def scrape_comprehensive(self, ticker: str) -> Dict[str, Any]:
        """
        Scrape ALL available data for a ticker (one-stop method).
        
        Returns dict with keys: fundamentals, financials, analyst_ratings, news, insider_trades.
        """
        logger.info(f"Starting comprehensive scrape for {ticker}")

        results = {
            "ticker": ticker,
            "fetched_at": datetime.now(),
        }

        try:
            results["fundamentals"] = await self.scrape_snapshot_fundamentals(ticker)
        except Exception as exc:
            logger.error(f"Failed to scrape fundamentals for {ticker}: {exc}")
            results["fundamentals"] = {}

        try:
            results["financials"] = await self.scrape_financial_statements(ticker)
        except Exception as exc:
            logger.error(f"Failed to scrape financials for {ticker}: {exc}")
            results["financials"] = {}

        try:
            results["analyst_ratings"] = await self.scrape_analyst_ratings(ticker)
        except Exception as exc:
            logger.error(f"Failed to scrape analyst ratings for {ticker}: {exc}")
            results["analyst_ratings"] = []

        try:
            results["news"] = await self.scrape_news(ticker)
        except Exception as exc:
            logger.error(f"Failed to scrape news for {ticker}: {exc}")
            results["news"] = []

        try:
            results["insider_trades"] = await self.scrape_insider_trades(ticker)
        except Exception as exc:
            logger.error(f"Failed to scrape insider trades for {ticker}: {exc}")
            results["insider_trades"] = []

        logger.info(f"Completed comprehensive scrape for {ticker}")
        return results


def _transform_fundamentals_to_db_format(fundamentals: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform FinViz fundamentals labels to database column names.
    
    FinViz uses labels like "P/E", "Forward P/E", "Market Cap", etc.
    Database uses snake_case column names: pe_ratio, forward_pe, market_cap, etc.
    """
    # Mapping from FinViz labels to database column names
    label_to_column = {
        "P/E": "pe_ratio",
        "Forward P/E": "forward_pe",
        "PEG": "peg_ratio",
        "P/S": "ps_ratio",
        "P/B": "pb_ratio",
        "P/C": "pc_ratio",
        "P/FCF": "pfcf_ratio",
        "Dividend %": "dividend_yield",
        "Payout Ratio": "payout_ratio",
        "EPS (ttm)": "eps_ttm",
        "EPS next Y": "eps_next_y",
        "EPS next Q": "eps_next_q",
        "EPS this Y": "eps_this_y",
        "EPS next 5Y": "eps_next_5y",
        "EPS past 5Y": "eps_past_5y",
        "Sales past 5Y": "sales_past_5y",
        "Sales Q/Q": "sales_qq",
        "EPS Q/Q": "eps_qq",
        "ROA": "roa",
        "ROE": "roe",
        "ROI": "roi",
        "Gross Margin": "gross_margin",
        "Oper. Margin": "operating_margin",
        "Profit Margin": "net_margin",
        "Debt/Eq": "debt_equity",
        "LT Debt/Eq": "lt_debt_equity",
        "Current Ratio": "current_ratio",
        "Quick Ratio": "quick_ratio",
        "Beta": "beta",
        "ATR": "atr",
        "Volatility": "volatility",
        "Insider Own": "insider_own",
        "Insider Trans": "insider_trans",
        "Inst Own": "inst_own",
        "Inst Trans": "inst_trans",
        "Short Float": "short_float",
        "Short Ratio": "short_ratio",
        "Target Price": "target_price",
        "Recom": "recommendation",
        "Market Cap": "market_cap",
    }
    
    # Create database-compatible dict
    db_record = {
        "ticker": fundamentals.get("ticker"),
        "date": fundamentals.get("date"),
    }
    
    # Map all fundamentals labels to column names
    for finviz_label, db_column in label_to_column.items():
        if finviz_label in fundamentals:
            db_record[db_column] = fundamentals[finviz_label]
    
    return db_record


async def ingest_full(
    session: Session,
    *,
    sample_config: Optional[SampleConfig] = None,
    tickers: Optional[List[str]] = None,
) -> ValidationResult:
    """
    Ingest FinViz data for specified tickers (or equity pools).
    
    Args:
        session: Database session
        sample_config: Sample mode configuration (limits tickers if enabled)
        tickers: List of tickers to scrape (defaults to all pools)
    
    Returns:
        ValidationResult with success/failure counts
    """
    sample_config = sample_config or SampleConfig()

    # Load ticker universe from pools if not provided
    if tickers is None:
        from app.pools.loader import get_all_tickers
        tickers = get_all_tickers()
        logger.info(f"Loaded ticker universe from pools: {len(tickers)} tickers")

    # Limit tickers in sample mode
    if sample_config.enabled:
        tickers = tickers[: sample_config.max_records_per_country]
        logger.info(f"Sample mode: limiting to {len(tickers)} tickers")

    successes = 0
    failures = 0
    all_fundamentals = []
    all_news = []
    all_ratings = []
    all_insider_trades = []

    async with FinVizComprehensiveScraper() as scraper:
        for ticker in tickers:
            try:
                data = await scraper.scrape_comprehensive(ticker)

                # Convert datetime objects to ISO strings for JSON serialization
                import json
                from datetime import date, datetime as dt
                def json_serial(obj):
                    if isinstance(obj, (dt, date)):
                        return obj.isoformat()
                    raise TypeError(f"Type {type(obj)} not serializable")
                
                # Serialize and deserialize to convert all datetime objects
                serializable_data = json.loads(json.dumps(data, default=json_serial))

                # Store raw payload in raw.raw_finviz
                from app.db.models import RawFinViz
                raw_record = RawFinViz(
                    fetched_at=dt.now(),
                    params={"ticker": ticker},
                    payload=serializable_data,
                )
                session.add(raw_record)

                # Collect normalized data for bulk upsert
                if data.get("fundamentals"):
                    # Transform fundamentals to database format
                    db_fundamentals = _transform_fundamentals_to_db_format(data["fundamentals"])
                    all_fundamentals.append(db_fundamentals)
                if data.get("news"):
                    all_news.extend(data["news"])
                if data.get("analyst_ratings"):
                    all_ratings.extend(data["analyst_ratings"])
                if data.get("insider_trades"):
                    all_insider_trades.extend(data["insider_trades"])

                successes += 1

            except Exception as exc:
                logger.error(f"Failed to ingest {ticker}: {exc}")
                failures += 1

    # Bulk upsert to warehouse tables
    from app.ingest.utils import (
        bulk_upsert_analyst_ratings,
        bulk_upsert_equity_fundamentals,
        bulk_upsert_insider_trades,
        bulk_upsert_stock_news,
    )

    bulk_upsert_equity_fundamentals(session, all_fundamentals)
    bulk_upsert_stock_news(session, all_news)
    bulk_upsert_analyst_ratings(session, all_ratings)
    bulk_upsert_insider_trades(session, all_insider_trades)

    session.commit()

    logger.info(f"FinViz ingestion complete: {successes} successes, {failures} failures")
    logger.info(f"Persisted: {len(all_fundamentals)} fundamentals, {len(all_news)} news, "
                f"{len(all_ratings)} ratings, {len(all_insider_trades)} insider trades")

    return ValidationResult(
        valid=failures == 0,
        errors=[f"{failures} tickers failed"] if failures > 0 else [],
        warnings=[],
        record_count=successes,
        countries=[],
    )
