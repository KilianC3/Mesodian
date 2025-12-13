import datetime as dt
import gzip
import logging
import os
from io import BytesIO
from typing import Any, Dict, Iterable, Optional, List

import httpx
import pandas as pd
from sqlalchemy.orm import Session

from app.config.country_universe import COUNTRY_UNIVERSE
from app.db.models import RawUnctad
from app.ingest.sample_mode import (
    SampleConfig,
    validate_timeseries_data,
    limit_dataframe_by_country,
    IngestionError,
)
from app.ingest.utils import (
    bulk_upsert_timeseries,
    resolve_indicator_id,
    store_raw_payload,
)
from app.ingest.rate_limiter import UNCTAD_LIMITER

logger = logging.getLogger(__name__)


# UNCTAD API - OData-style endpoint
# API docs: https://unctadstat.unctad.org/datacentre/
# Base URL: https://unctadstat-user-api.unctad.org/
# 
# Authentication: ClientId and ClientSecret headers
# Format: {DATASET_ID}/cur/Facts?culture=en
# 
# Query Parameters (OData):
# - $select: Fields to return
# - $filter: Filter conditions (e.g., "Year eq 2021 and Economy/Code in ('840')")
# - $orderby: Sort order
# - $compute: Computed fields
# - $format: Output format (csv, json)
# - compress: gz for gzip compression
#
# Example datasets:
# - US.TradeGoods: Trade in goods
# - US.Seafarers: Seafarer supply
# - US.FDI: Foreign Direct Investment
UNCTAD_BASE = "https://unctadstat-user-api.unctad.org"

# Map datasets to our canonical indicators
# Full coverage: Trade, Economy, Maritime (40+ datasets)
UNCTAD_DATASETS: Dict[str, Dict[str, Any]] = {
    # === A. INTERNATIONAL TRADE ===
    "US.TradeMerchTotal": {
        "canonical_indicator": "UNCTAD_TRADE_MERCH_TOTAL",
        "countries": ["USA", "GBR", "DEU", "FRA", "CHN", "IND", "JPN", "ITA", "ESP", "NLD"],
        "description": "Merchandise total trade and share (exports, imports, balance)",
    },


    "US.CreativeServ_Indiv_Tot": {
        "canonical_indicator": "UNCTAD_CREATIVE_SERV_TOTAL",
        "countries": ["USA", "GBR", "DEU", "FRA", "CHN", "IND", "JPN", "ITA", "ESP", "NLD"],
        "description": "Creative services exports and imports by economy",
    },
    "US.CreativeServ_Group_E": {
        "canonical_indicator": "UNCTAD_CREATIVE_SERV_EXPORTS",
        "countries": ["USA", "GBR", "DEU", "FRA", "CHN", "IND", "JPN", "ITA", "ESP", "NLD"],
        "description": "Creative services exports of selected groups",
    },
    
    # === B. ECONOMY, INVESTMENT & FINANCE ===

    "US.GDPTotal": {
        "canonical_indicator": "UNCTAD_GDP_CURRENT",
        "countries": ["USA", "GBR", "DEU", "FRA", "CHN", "IND", "JPN", "ITA", "ESP", "NLD"],
        "description": "GDP at current prices",
    },





    "US.CommodityPrices": {
        "canonical_indicator": "UNCTAD_COMMODITY_PRICE_INDEX",
        "countries": ["WLD"],  # Global commodity prices
        "description": "Commodity price indices (energy, metals, agriculture, food)",
    },
    
    # === C. MARITIME & TRANSPORT ===
    "US.MerchantFleet": {
        "canonical_indicator": "UNCTAD_FLEET_FLAG",
        "countries": ["USA", "GBR", "DEU", "FRA", "CHN", "IND", "JPN", "ITA", "ESP", "NLD", "GRC", "NOR", "KOR", "SGP"],
        "description": "Merchant fleet by flag of registration (DWT, vessels, ship types)",
    },


    "US.SeaborneTrade": {
        "canonical_indicator": "UNCTAD_SEABORNE_TRADE",
        "countries": ["USA", "GBR", "DEU", "FRA", "CHN", "IND", "JPN", "ITA", "ESP", "NLD"],
        "description": "Seaborne trade loaded/unloaded by cargo type",
    },
    "US.PortCalls": {
        "canonical_indicator": "UNCTAD_PORT_CALLS",
        "countries": ["USA", "GBR", "DEU", "FRA", "CHN", "IND", "JPN", "ITA", "ESP", "NLD"],
        "description": "Port calls by port and ship type",
    },
    "US.PortCallsArrivals_S": {
        "canonical_indicator": "UNCTAD_PORT_ARRIVALS",
        "countries": ["USA", "GBR", "DEU", "FRA", "CHN", "IND", "JPN", "ITA", "ESP", "NLD"],
        "description": "Port arrivals and performance statistics",
    },



    "US.Seafarers": {
        "canonical_indicator": "UNCTAD_SEAFARER_SUPPLY",
        "countries": ["USA", "GBR", "DEU", "FRA", "CHN", "IND", "JPN", "ITA", "ESP", "NLD", "PHL", "IDN"],
        "description": "Seafarer supply (officers and ratings by nationality)",
    },

}


def fetch_unctad_data(
    dataset_id: str,
    *,
    filter_expr: Optional[str] = None,
    orderby: Optional[str] = None,
    top: Optional[int] = None,
    sample_config: Optional[SampleConfig] = None,
) -> pd.DataFrame:
    """
    Fetch data from UNCTAD OData API.
    
    NOTE: UNCTAD API returns measure-specific columns (M0100_Value, M5011_Value, etc.)
    instead of a generic 'Value' column. We fetch all columns and parse measures dynamically.
    
    Args:
        dataset_id: Dataset ID (e.g., "US.TradeMerchTotal", "US.GDPTotal")
        filter_expr: $filter parameter (OData filter expression, optional)
        orderby: $orderby parameter (optional)
        top: $top parameter (limit records, optional)
        sample_config: Sample mode configuration
    
    Returns:
        DataFrame with all columns including Economy_Label, Year, Flow_Label, M*_Value columns
    
    Example:
        fetch_unctad_data(
            "US.TradeMerchTotal",
            filter_expr="Year ge 2020 and Economy/Code eq '840'",
            orderby="Year asc"
        )
    """
    sample_config = sample_config or SampleConfig()
    
    # Get credentials from environment
    client_id = os.getenv("UNCTAD_CLIENT_ID")
    client_secret = os.getenv("UNCTAD_SECRET")
    
    if not client_id or not client_secret:
        raise IngestionError("UNCTAD", dataset_id, "Missing UNCTAD_CLIENT_ID or UNCTAD_SECRET in environment")
    
    # Build URL
    url = f"{UNCTAD_BASE}/{dataset_id}/cur/Facts"
    
    # Build form parameters (no $select to get all columns)
    params = {
        "$format": "csv",
        "compress": "gz",
    }
    
    if filter_expr:
        params["$filter"] = filter_expr
    
    if orderby:
        params["$orderby"] = orderby
    if top:
        params["$top"] = str(top)
    
    # Add culture parameter
    params["culture"] = "en"
    
    try:
        # Rate limiting: 100 requests/minute
        UNCTAD_LIMITER.acquire()
        # Use POST with form data (as in UNCTAD example)
        response = httpx.post(
            url,
            data=params,
            headers={
                "ClientId": client_id,
                "ClientSecret": client_secret,
            },
            timeout=60.0,
            follow_redirects=True,
        )
        response.raise_for_status()
        
        # Decompress gzip response
        if response.headers.get("Content-Encoding") == "gzip" or params.get("compress") == "gz":
            content = gzip.decompress(response.content)
        else:
            content = response.content
        
        # Parse CSV
        df = pd.read_csv(BytesIO(content), encoding="utf-8")
        
        if df.empty and sample_config.fail_on_empty:
            raise IngestionError("UNCTAD", dataset_id, "Empty response from API")
        
        return df
        
    except httpx.HTTPStatusError as e:
        raise IngestionError("UNCTAD", dataset_id, f"HTTP {e.response.status_code}: {e.response.text[:200]}")
    except Exception as e:
        raise IngestionError("UNCTAD", dataset_id, f"Fetch error: {e}")


def _parse_unctad_dataframe(
    df: pd.DataFrame,
    *,
    indicator_id: int,
    country_id: str,
    year_column: str = "Year",
    measure_code: str = "M0100",  # Default measure code
    flow_filter: Optional[str] = None,  # Optional: filter by Flow_Label (e.g., 'Imports', 'Exports')
    sample_config: Optional[SampleConfig] = None,
) -> List[Dict[str, Any]]:
    """
    Parse UNCTAD dataframe with dynamic measure column detection.
    
    UNCTAD returns measure-specific columns like M0100_Value, M5011_Value.
    We identify and parse the requested measure column.
    
    Args:
        df: DataFrame from UNCTAD API
        indicator_id: Indicator ID to associate with
        country_id: ISO3 country code
        year_column: Name of year column (default: "Year")
        measure_code: Measure code to extract (default: "M0100" for primary value)
        flow_filter: Optional filter for Flow_Label column (e.g., 'Imports', 'Exports')
        sample_config: Sample mode configuration
    """
    sample_config = sample_config or SampleConfig()
    rows: List[Dict[str, Any]] = []
    
    if df is None or df.empty:
        if sample_config.fail_on_empty:
            raise IngestionError("UNCTAD", country_id, "Empty dataframe returned")
        return rows
    
    # Identify the value column (measure-specific)
    value_column = f"{measure_code}_Value"
    
    if value_column not in df.columns:
        # Try to find any measure column
        measure_cols = [col for col in df.columns if col.endswith('_Value') and col.startswith('M')]
        if measure_cols:
            value_column = measure_cols[0]  # Use first available measure
            logger.info(f"UNCTAD: Using {value_column} instead of {measure_code}_Value")
        else:
            logger.warning(f"UNCTAD: No measure columns found in {df.columns.tolist()}")
            if sample_config.fail_on_empty:
                raise IngestionError("UNCTAD", country_id, f"No measure columns found")
            return rows
    
    # Apply flow filter if specified
    if flow_filter and 'Flow_Label' in df.columns:
        df = df[df['Flow_Label'] == flow_filter].copy()
        if df.empty:
            logger.warning(f"UNCTAD: No data after filtering for Flow_Label={flow_filter}")
            return rows
    
    # Limit to sample size
    if sample_config.enabled and not df.empty:
        df = df.tail(sample_config.max_records_per_country)
    
    for _, row in df.iterrows():
        year = row.get(year_column)
        value = row.get(value_column)
        
        if year is None or pd.isna(year) or value is None or pd.isna(value):
            continue  # Skip rows with missing data
        
        try:
            # Convert year to date (assume end of year)
            date = dt.date(int(year), 12, 31)
            
            # Handle value (may be string with commas)
            numeric_value = float(str(value).replace(",", ""))
            
        except Exception as exc:
            logger.debug(f"UNCTAD: Parse error for {country_id}: {exc}")
            continue
        
        rows.append(
            {
                "indicator_id": indicator_id,
                "country_id": country_id,
                "date": date,
                "value": numeric_value,
                "source": "UNCTAD",
                "ingested_at": dt.datetime.utcnow(),
            }
        )
    
    # Validate results
    if sample_config.enabled and rows:
        validation = validate_timeseries_data(
            rows,
            expected_countries=[country_id],
            sample_config=sample_config
        )
        if sample_config.strict_validation:
            validation.raise_if_invalid()
    
    return rows


# ISO3 to UNCTAD country code mapping (M49 codes)
# UNCTAD uses UN M49 numeric codes
ISO3_TO_M49 = {
    "USA": "840",
    "GBR": "826",
    "DEU": "276",
    "FRA": "250",
    "CHN": "156",
    "IND": "356",
    "JPN": "392",
    "ITA": "380",
    "ESP": "724",
    "NLD": "528",
}


def ingest_full(
    session: Session,
    *,
    indicator_subset: Optional[Iterable[str]] = None,
    country_subset: Optional[Iterable[str]] = None,
    sample_config: Optional[SampleConfig] = None,
) -> None:
    """Ingest data from UNCTAD API with optional sample mode."""
    sample_config = sample_config or SampleConfig()
    selected_indicators = set(indicator_subset) if indicator_subset else None
    selected_countries = set(country_subset) if country_subset else None

    for dataset_id, cfg in UNCTAD_DATASETS.items():
        if selected_indicators and dataset_id not in selected_indicators:
            continue
        
        try:
            indicator_id = resolve_indicator_id(session, cfg["canonical_indicator"])
        except Exception as e:
            logger.error(f"UNCTAD: Failed to resolve indicator {cfg['canonical_indicator']}: {e}")
            if sample_config.strict_validation:
                raise IngestionError("UNCTAD", "N/A", f"Indicator resolution failed: {e}")
            continue
        
        # Get countries to fetch
        countries = cfg.get("countries", ["USA", "GBR", "DEU", "FRA", "CHN"])
        if selected_countries:
            countries = [c for c in countries if c in selected_countries]
        if sample_config.enabled:
            countries = countries[:3]  # Limit to 3 countries in sample mode
        
        for country_id in countries:
            try:
                # Map ISO3 to M49 code
                m49_code = ISO3_TO_M49.get(country_id)
                if not m49_code:
                    logger.warning(f"UNCTAD: No M49 code mapping for {country_id}, skipping")
                    continue
                
                # Build filter for this country (get last 10 years of data)
                current_year = dt.datetime.now().year
                start_year = current_year - 10 if not sample_config.enabled else current_year - 2
                
                filter_expr = f"Year ge {start_year} and Economy/Code eq '{m49_code}'"
                
                # Fetch data (all columns, no select)
                df = fetch_unctad_data(
                    dataset_id,
                    filter_expr=filter_expr,
                    orderby="Year asc",
                    sample_config=sample_config,
                )
                
                # Store raw response (sample, convert NaN to None for JSON compatibility)
                sample_dict = df.head(5).to_dict() if not df.empty else {}
                # Replace NaN with None for JSON serialization
                import math
                def clean_nan(obj):
                    if isinstance(obj, dict):
                        return {k: clean_nan(v) for k, v in obj.items()}
                    elif isinstance(obj, list):
                        return [clean_nan(v) for v in obj]
                    elif isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
                        return None
                    return obj
                
                store_raw_payload(
                    session,
                    RawUnctad,
                    params={"dataset": dataset_id, "country": country_id},
                    payload={"sample_data": clean_nan(sample_dict)},
                )
                
                # Parse dataframe (M0100 is typically the primary value column)
                # For trade data with Flow_Label, we filter to 'Imports' to avoid duplicates
                rows = _parse_unctad_dataframe(
                    df,
                    indicator_id=indicator_id,
                    country_id=country_id,
                    year_column="Year",
                    measure_code="M0100",  # Primary measure
                    flow_filter="Imports",  # Filter to imports only (exports would be separate indicator)
                    sample_config=sample_config,
                )
                
                # Deduplicate rows by (indicator_id, country_id, date) - keep last value
                # This handles cases where multiple measure columns or flows exist
                seen_keys = {}
                deduped_rows = []
                for row in rows:
                    key = (row["indicator_id"], row["country_id"], row["date"])
                    seen_keys[key] = row  # Overwrites with latest value
                deduped_rows = list(seen_keys.values())
                
                if len(rows) != len(deduped_rows):
                    logger.warning(f"UNCTAD: Deduplicated {len(rows)} → {len(deduped_rows)} rows for {country_id}/{dataset_id}")
                
                bulk_upsert_timeseries(session, deduped_rows)
                logger.info(f"UNCTAD: Ingested {len(deduped_rows)} records for {country_id}/{dataset_id}")
                
            except IngestionError as ie:
                # Log but continue for known issues
                if "Empty response" in str(ie) or "No data" in str(ie):
                    logger.warning(f"UNCTAD: No data available for {country_id}/{dataset_id}")
                else:
                    logger.error(f"UNCTAD: {ie}")
                    if sample_config.strict_validation:
                        raise
            except Exception as e:
                logger.error(f"UNCTAD: Failed for {country_id}/{dataset_id}: {e}")
                if sample_config.strict_validation:
                    raise IngestionError("UNCTAD", country_id, f"Ingestion failed: {e}")
    
    session.commit()
