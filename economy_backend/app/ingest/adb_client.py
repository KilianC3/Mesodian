"""
ADB (Asian Development Bank) KIDB SDMX v4 API client.

API Documentation: https://kidb.adb.org/api
- Base URL: https://kidb.adb.org/api/v4/sdmx
- Correct Format: GET /data/ADB,{DATAFLOW}/FREQ.INDICATORS.COUNTRIES
- Example: /data/ADB,PPL_POP/A.PO_POP.PHI+THA (annual, population indicator, Philippines+Thailand)
- SDMX Key Format: FREQUENCY.INDICATOR(S).ECONOMY_CODE(S)
  - Use dots (.) as separators between dimensions
  - Use plus (+) for multiple values within a dimension
  - Empty dimension means "all values" (e.g., A.. means all indicators)

Available Dataflows (from kidb.adb.org/api):
- PPL (People): Population, labor, poverty, social indicators
- EO (Economy & Output): National accounts, GDP, production indices
- MFP (Money/Finance/Prices): CPI, money supply, exchange rates, financial indicators
- GG (Government): Government finance, governance indicators
- GLB (Globalization): Trade, balance of payments, reserves, external debt, capital flows
- TC (Transport/Communication): Infrastructure and connectivity
- ENV (Environment): Land, pollution, water, climate

Rate Limiting: 20 requests per minute (CRITICAL - enforce in code)
"""

import datetime as dt
import logging
import time
from threading import Lock
from typing import Any, Dict, Iterable, List, Optional
from xml.etree import ElementTree as ET

import httpx
from sqlalchemy.orm import Session

from app.db.models import RawAdb
from app.ingest.sample_mode import SampleConfig
from app.ingest.utils import bulk_upsert_timeseries, ensure_date, resolve_indicator_id, store_raw_payload

logger = logging.getLogger(__name__)


ADB_BASE_URL = "https://kidb.adb.org/api/v4/sdmx"

# ADB uses ISO3-like codes (PHI, THA, IND, INO, etc.)
# Note: These are ADB-specific, not standard ISO3
ADB_COUNTRY_CODES = {
    "IND": "IND",  # India
    "IDN": "INO",  # Indonesia (ADB uses INO, not IDN)
    "PHL": "PHI",  # Philippines (ADB uses PHI, not PHL)
    "VNM": "VIE",  # Vietnam (ADB uses VIE, not VNM)
    "THA": "THA",  # Thailand
    "CHN": "PRC",  # China (ADB uses PRC - People's Republic of China)
    "JPN": "JPN",  # Japan
    "KOR": "KOR",  # South Korea
    "BGD": "BAN",  # Bangladesh (ADB uses BAN)
    "PAK": "PAK",  # Pakistan
    "MYS": "MAL",  # Malaysia (ADB uses MAL)
    "SGP": "SIN",  # Singapore (ADB uses SIN)
}

# Rate limiter for ADB API (20 requests per minute)
class ADBRateLimiter:
    def __init__(self):
        self.last_request_times: List[float] = []
        self.lock = Lock()
        self.requests_per_minute = 20
        self.window_seconds = 60
    
    def acquire(self):
        with self.lock:
            now = time.time()
            # Remove requests older than 60 seconds
            self.last_request_times = [t for t in self.last_request_times if now - t < self.window_seconds]
            
            # If at limit, wait until oldest request expires
            if len(self.last_request_times) >= self.requests_per_minute:
                sleep_time = self.window_seconds - (now - self.last_request_times[0]) + 1
                if sleep_time > 0:
                    logger.info(f"ADB rate limit: sleeping {sleep_time:.1f}s")
                    time.sleep(sleep_time)
                    now = time.time()
                    self.last_request_times = [t for t in self.last_request_times if now - t < self.window_seconds]
            
            self.last_request_times.append(now)

ADB_LIMITER = ADBRateLimiter()

# Comprehensive indicator mapping
# Each ADB indicator code maps to a Mesodian canonical indicator string
# Format: "ADB_{CATEGORY}_{INDICATOR}" for new indicators
ADB_INDICATOR_MAPPING: Dict[str, str] = {
    # Population (PPL_POP)
    "PO_POP": "ADB_POPULATION_TOTAL",
    "PO_POP_FE": "ADB_POPULATION_FEMALE",
    "PO_POP_MA": "ADB_POPULATION_MALE",
    
    # Labor & Employment (PPL_LE)
    "LU_PE_NUM": "ADB_UNEMPLOYMENT_PERSONS",
    "LU_UR": "ADB_UNEMPLOYMENT_RATE",
    "LF_LF_NUM": "ADB_LABOR_FORCE",
    
    # National Accounts - GDP (EO_NA_CURR_GDP_EXP)
    "NA_GDP_EXP_CURR_USD": "ADB_GDP_CURRENT_USD",
    "NA_GNI_CURR_USD": "ADB_GNI_CURRENT_USD",
    
    # Prices (MFP_PR)
    "PR_CPI": "ADB_CPI",
    "PR_INFL": "ADB_INFLATION_RATE",
    
    # Money & Finance (MFP_MF)
    "MF_M2": "ADB_MONEY_SUPPLY_M2",
    "MF_IR_LENDING": "ADB_INTEREST_RATE_LENDING",
    
    # Exchange Rates (MFP_XR)
    "XR_ER_USD": "ADB_EXCHANGE_RATE_USD",
    
    # Government Finance (GG_GF)
    "GF_REV_PC_GDP": "ADB_GOV_REVENUE_PCT_GDP",
    "GF_EXP_PC_GDP": "ADB_GOV_EXPENDITURE_PCT_GDP",
    
    # External Trade (GLB_ET)
    "ET_EX_USD": "ADB_EXPORTS_USD",
    "ET_IM_USD": "ADB_IMPORTS_USD",
    "ET_TB_USD": "ADB_TRADE_BALANCE_USD",
    
    # Balance of Payments (GLB_BP)
    "BP_CA_USD": "ADB_CURRENT_ACCOUNT_USD",
    "BP_FDI_USD": "ADB_FDI_INFLOWS_USD",
    
    # International Reserves (GLB_IR)
    "IR_TOT_USD": "ADB_RESERVES_TOTAL_USD",
    
    # External Debt (GLB_EI)
    "EI_DOD_USD": "ADB_EXTERNAL_DEBT_USD",
    "EI_DOD_PC_GNI": "ADB_EXTERNAL_DEBT_PCT_GNI",
}

# Dataflow configuration with specific indicators for diverse data
ADB_SERIES: Dict[str, Dict[str, Any]] = {
    # People - Population
    "PPL_POP": {
        "dataflow": "PPL_POP",
        "indicator_codes": ["PO_POP", "PO_POP_FE", "PO_POP_MA"],
        "frequency": "A",
        "countries": ["IND", "IDN", "PHL", "VNM", "THA", "CHN", "JPN"],
    },
    # People - Labor & Employment
    "PPL_LE": {
        "dataflow": "PPL_LE",
        "indicator_codes": ["LU_PE_NUM", "LU_UR", "LF_LF_NUM"],
        "frequency": "A",
        "countries": ["IND", "IDN", "PHL", "VNM", "THA", "CHN", "JPN"],
    },
    # Economy - National Accounts (GDP)
    "EO_NA_CURR_GDP_EXP": {
        "dataflow": "EO_NA_CURR_GDP_EXP",
        "indicator_codes": ["NA_GDP_EXP_CURR_USD", "NA_GNI_CURR_USD"],
        "frequency": "A",
        "countries": ["IND", "IDN", "PHL", "VNM", "THA", "CHN", "JPN"],
    },
    # Money/Finance/Prices - Prices
    "MFP_PR": {
        "dataflow": "MFP_PR",
        "indicator_codes": ["PR_CPI", "PR_INFL"],
        "frequency": "A",
        "countries": ["IND", "IDN", "PHL", "VNM", "THA", "CHN", "JPN"],
    },
    # Money/Finance/Prices - Money & Finance
    "MFP_MF": {
        "dataflow": "MFP_MF",
        "indicator_codes": ["MF_M2", "MF_IR_LENDING"],
        "frequency": "A",
        "countries": ["IND", "IDN", "PHL", "VNM", "THA", "CHN", "JPN"],
    },
    # Money/Finance/Prices - Exchange Rates
    "MFP_XR": {
        "dataflow": "MFP_XR",
        "indicator_codes": ["XR_ER_USD"],
        "frequency": "A",
        "countries": ["IND", "IDN", "PHL", "VNM", "THA", "CHN", "JPN"],
    },
    # Government Finance
    "GG_GF": {
        "dataflow": "GG_GF",
        "indicator_codes": ["GF_REV_PC_GDP", "GF_EXP_PC_GDP"],
        "frequency": "A",
        "countries": ["IND", "IDN", "PHL", "VNM", "THA", "CHN", "JPN"],
    },
    # Globalization - External Trade
    "GLB_ET": {
        "dataflow": "GLB_ET",
        "indicator_codes": ["ET_EX_USD", "ET_IM_USD", "ET_TB_USD"],
        "frequency": "A",
        "countries": ["IND", "IDN", "PHL", "VNM", "THA", "CHN", "JPN"],
    },
    # Globalization - Balance of Payments
    "GLB_BP": {
        "dataflow": "GLB_BP",
        "indicator_codes": ["BP_CA_USD", "BP_FDI_USD"],
        "frequency": "A",
        "countries": ["IND", "IDN", "PHL", "VNM", "THA", "CHN", "JPN"],
    },
    # Globalization - International Reserves
    "GLB_IR": {
        "dataflow": "GLB_IR",
        "indicator_codes": ["IR_TOT_USD"],
        "frequency": "A",
        "countries": ["IND", "IDN", "PHL", "VNM", "THA", "CHN", "JPN"],
    },
    # Globalization - External Debt
    "GLB_EI": {
        "dataflow": "GLB_EI",
        "indicator_codes": ["EI_DOD_USD", "EI_DOD_PC_GNI"],
        "frequency": "A",
        "countries": ["IND", "IDN", "PHL", "VNM", "THA", "CHN", "JPN"],
    },
}


def fetch_adb_sdmx_data(
    dataflow: str,
    frequency: str,
    indicators: List[str],
    countries: List[str],
    start_period: Optional[str] = "2000",
    end_period: Optional[str] = "2024",
    timeout_seconds: float = 30.0,
) -> bytes:
    """
    Fetch SDMX data from ADB KIDB API v4.
    
    CORRECT FORMAT: GET /data/ADB,{DATAFLOW}/FREQ.INDICATORS.COUNTRIES
    Example: /data/ADB,PPL_POP/A.PO_POP.PHI+THA
    
    Args:
        dataflow: Dataflow ID (e.g., "PPL_POP", "PPL_LE")
        frequency: Frequency code (A=annual, Q=quarterly, M=monthly)
        indicators: List of indicator codes (e.g., ["PO_POP", "PO_POP_FE"])
        countries: List of ADB country codes (e.g., ["IND", "PHI", "THA"])
        start_period: Starting year (default: 2000)
        end_period: Ending year (default: 2024)
    
    Returns:
        SDMX-XML response bytes
    
    Raises:
        httpx.HTTPError: If request fails
    """
    # Build SDMX key: FREQUENCY.INDICATORS.COUNTRIES
    indicator_part = "+".join(indicators) if indicators else ""
    country_part = "+".join(countries)
    
    # Format: FREQ.INDICATOR.COUNTRY
    if indicator_part:
        sdmx_key = f"{frequency}.{indicator_part}.{country_part}"
    else:
        # Empty indicators = all indicators = ".."
        sdmx_key = f"{frequency}..{country_part}"
    
    # CRITICAL: Format is ADB,{DATAFLOW} with comma, not slash!
    url = f"{ADB_BASE_URL}/data/ADB,{dataflow}/{sdmx_key}"
    
    params = {}
    if start_period:
        params["startPeriod"] = start_period
    if end_period:
        params["endPeriod"] = end_period
    
    logger.info(f"ADB SDMX GET {url} params={params}")
    
    # Apply rate limiting (20 requests per minute)
    ADB_LIMITER.acquire()
    
    with httpx.Client(timeout=timeout_seconds) as client:
        response = client.get(url, params=params)
        response.raise_for_status()
        return response.content


def parse_adb_sdmx_xml(
    xml_content: bytes,
    indicator_mapping: Dict[str, str],
    country_iso3: str,
    adb_country_code: str,
    source: str = "ADB"
) -> List[Dict[str, Any]]:
    """
    Parse SDMX-XML response from ADB and extract time series data.
    
    CRITICAL: Each Series tag has an INDICATOR attribute that must be mapped
    to the correct Mesodian indicator_id to avoid duplicate key violations.
    
    Args:
        xml_content: SDMX-XML bytes from ADB API
        indicator_mapping: Dict mapping ADB indicator codes to Mesodian canonical strings
        country_iso3: ISO3 country code (e.g., "IND")
        adb_country_code: ADB country code (e.g., "IND", "PHI", "INO")
        source: Source name (default: "ADB")
    
    Returns:
        List of time series records ready for database insertion
    """
    rows: List[Dict[str, Any]] = []
    
    try:
        root = ET.fromstring(xml_content)
        
        # ADB returns structure-specific SDMX with NO NAMESPACE on Series/Obs!
        # Series has attributes: FREQ, INDICATOR, ECONOMY_CODE
        # Obs has attributes: TIME_PERIOD, OBS_VALUE
        
        for series in root.iter("Series"):
            # Get attributes from series
            economy_code = series.get("ECONOMY_CODE")
            adb_indicator_code = series.get("INDICATOR")
            
            # Only process series for our target country
            if economy_code != adb_country_code:
                continue
            
            # Map ADB indicator code to Mesodian canonical indicator
            if not adb_indicator_code:
                logger.warning(f"Series missing INDICATOR attribute, skipping")
                continue
            
            canonical_indicator = indicator_mapping.get(adb_indicator_code)
            if not canonical_indicator:
                logger.warning(f"Unmapped ADB indicator: {adb_indicator_code}, skipping")
                continue
            
            # Extract observations
            for obs in series.iter("Obs"):
                time_period = obs.get("TIME_PERIOD")
                obs_value = obs.get("OBS_VALUE")
                
                if time_period and obs_value:
                    try:
                        value = float(obs_value)
                        # Time period format: YYYY (annual), YYYY-QQ (quarterly), YYYY-MM (monthly)
                        date = ensure_date(time_period)
                        
                        rows.append({
                            "canonical_indicator": canonical_indicator,
                            "country_iso3": country_iso3,
                            "date": date,
                            "value": value,
                            "source": source,
                        })
                    except (ValueError, TypeError) as e:
                        logger.debug(f"Skipping invalid observation: {time_period}={obs_value}, error={e}")
                        continue
        
        logger.info(f"Parsed {len(rows)} observations from ADB XML for country={country_iso3}")
        return rows
    
    except ET.ParseError as e:
        logger.error(f"Failed to parse ADB XML: {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error parsing ADB XML: {e}")
        return []


def ingest_adb_data(session: Session, start_year: int = 2000, end_year: int = 2024) -> Dict[str, int]:
    """
    Ingest data from ADB KIDB SDMX API for configured series.
    
    This function:
    1. Iterates through configured dataflows (population, employment, GDP, prices, finance, trade, etc.)
    2. For each dataflow, fetches data for multiple indicators across countries
    3. Maps each ADB indicator code to a unique Mesodian canonical indicator
    4. Resolves indicator_id for each canonical indicator
    5. Stores raw XML payloads
    6. Bulk upserts time series data
    
    Args:
        session: SQLAlchemy database session
        start_year: Starting year for data fetch (default: 2000)
        end_year: Ending year for data fetch (default: 2024)
    
    Returns:
        Dict with ingestion statistics:
          - 'series_fetched': Number of series configurations fetched
          - 'records_inserted': Total number of time series records inserted
          - 'indicators_processed': Number of unique indicators processed
    """
    stats = {
        "series_fetched": 0,
        "records_inserted": 0,
        "indicators_processed": 0,
    }
    
    processed_indicators = set()
    
    for series_name, series_config in ADB_SERIES.items():
        logger.info(f"Processing ADB series: {series_name}")
        
        dataflow = series_config["dataflow"]
        frequency = series_config["frequency"]
        indicator_codes = series_config["indicator_codes"]
        country_iso3_list = series_config["countries"]
        
        # Convert ISO3 country codes to ADB-specific codes
        adb_country_codes = []
        for iso3 in country_iso3_list:
            adb_code = ADB_COUNTRY_CODES.get(iso3)
            if adb_code:
                adb_country_codes.append(adb_code)
            else:
                logger.warning(f"No ADB country mapping for ISO3: {iso3}")
        
        if not adb_country_codes:
            logger.warning(f"No valid ADB countries for series {series_name}, skipping")
            continue
        
        # Fetch SDMX data for this dataflow
        try:
            xml_content = fetch_adb_sdmx_data(
                dataflow=dataflow,
                frequency=frequency,
                indicators=indicator_codes,
                countries=adb_country_codes,
                start_period=str(start_year),
                end_period=str(end_year),
            )
            
            stats["series_fetched"] += 1
            
            # Store raw payload (decode bytes to string for JSON serialization)
            store_raw_payload(
                session=session,
                model=RawAdb,
                params={
                    "dataflow": dataflow,
                    "frequency": frequency,
                    "indicators": indicator_codes,
                    "countries": adb_country_codes,
                },
                payload=xml_content.decode("utf-8"),
            )
            
            # Parse XML and prepare data for each country
            for iso3 in country_iso3_list:
                adb_country_code = ADB_COUNTRY_CODES.get(iso3)
                if not adb_country_code:
                    continue
                
                rows = parse_adb_sdmx_xml(
                    xml_content=xml_content,
                    indicator_mapping=ADB_INDICATOR_MAPPING,
                    country_iso3=iso3,
                    adb_country_code=adb_country_code,
                    source="ADB",
                )
                
                if not rows:
                    logger.info(f"No data parsed for {iso3} from dataflow {dataflow}")
                    continue
                
                # Group rows by canonical indicator
                indicator_groups: Dict[str, List[Dict]] = {}
                for row in rows:
                    canonical = row["canonical_indicator"]
                    if canonical not in indicator_groups:
                        indicator_groups[canonical] = []
                    indicator_groups[canonical].append(row)
                
                # Process each indicator group separately
                for canonical_indicator, indicator_rows in indicator_groups.items():
                    # Resolve indicator_id
                    indicator_id = resolve_indicator_id(session, canonical_indicator)
                    if indicator_id is None:
                        logger.warning(f"Could not resolve indicator: {canonical_indicator}, skipping")
                        continue
                    
                    # Add indicator_id to each row
                    for row in indicator_rows:
                        row["indicator_id"] = indicator_id
                        # Rename country_iso3 to country_id for DB schema
                        row["country_id"] = row.pop("country_iso3")
                        # Remove canonical_indicator as it's not in the DB schema
                        row.pop("canonical_indicator", None)
                        # Add ingested_at timestamp
                        row["ingested_at"] = dt.datetime.now(dt.timezone.utc)
                    
                    # Bulk upsert
                    bulk_upsert_timeseries(session, indicator_rows)
                    num_inserted = len(indicator_rows)
                    stats["records_inserted"] += num_inserted
                    processed_indicators.add(canonical_indicator)
                    
                    logger.info(
                        f"Inserted {num_inserted} records for {canonical_indicator} "
                        f"(country={iso3}, dataflow={dataflow})"
                    )
            
        except httpx.HTTPError as e:
            logger.error(f"HTTP error fetching ADB series {series_name}: {e}")
            continue
        except Exception as e:
            logger.error(f"Error processing ADB series {series_name}: {e}")
            continue
    
    stats["indicators_processed"] = len(processed_indicators)
    
    logger.info(
        f"ADB ingestion complete: {stats['series_fetched']} series fetched, "
        f"{stats['records_inserted']} records inserted, "
        f"{stats['indicators_processed']} unique indicators processed"
    )
    
    return stats


def ingest_full(session: Session, sample_config: Optional[SampleConfig] = None) -> None:
    """
    Standard ingestion interface for ADB data source.
    
    This wrapper provides the standard `ingest_full(session, sample_config)` interface
    expected by the jobs.py PROVIDERS list.
    
    Args:
        session: Database session
        sample_config: Optional sample mode configuration (not used for ADB)
    """
    # Run full ingestion with recent data (last 5 years)
    current_year = dt.datetime.now().year
    start_year = current_year - 5
    end_year = current_year
    
    stats = ingest_adb_data(session, start_year=start_year, end_year=end_year)
    
    logger.info(
        f"ADB ingest_full complete: {stats['records_inserted']} records, "
        f"{stats['indicators_processed']} indicators"
    )
