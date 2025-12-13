import datetime as dt
import logging
from typing import Any, Dict, Iterable, List, Optional, Union

import pandas as pd
from sqlalchemy.orm import Session

# ==================================================================================
# CRITICAL: FAOSTAT IS A PYTHON MODULE (pip install faostat)
# ==================================================================================
# USER DIRECTIVE (Session 19): "FOR THE LAST TIME FAOSTAT IS A PYTHON MODULE. 
# WRITE THIS DOWN SO I NEVER HAVE TO SAY THIS AGAIN."
#
# NEVER CALL https://faostatservices.fao.org/... DIRECTLY!
# ALWAYS USE: faostat.get_data_df(domain, pars=..., ...)
#
# Installation: pip install faostat
# PyPI: https://pypi.org/project/faostat/
# GitHub: https://github.com/Predicta-Analytics/faostat
#
# CORRECT USAGE:
#   import faostat
#   df = faostat.get_data_df(
#       "QCL",  # Domain (Crops and Livestock)
#       pars={
#           "area": ["2"],      # Country codes (FAO area codes)
#           "element": [2510],  # Element codes (integers)
#           "item": [44],       # Item codes (integers) 
#           "year": [2010, 2011, 2012]  # Years (integers)
#       },
#       show_flags=False,
#       null_values=False,
#       show_notes=False,
#       strval=False
#   )
#
# CORRECT ELEMENT CODES (QCL domain):
#   Production quantity: 2510 (NOT 5510)
#   Yield: 2413 (NOT 5419)
#
# EXAMPLE WORKING ITEM:
#   Barley: item_code = "44"
# ==================================================================================

import faostat

from app.config.country_universe import COUNTRY_UNIVERSE
from app.db.models import RawFaostat
from app.ingest.utils import (
    bulk_upsert_timeseries,
    ensure_date,
    resolve_indicator_id,
    store_raw_payload,
)
from app.ingest.sample_mode import SampleConfig, validate_timeseries_data, limit_dataframe_by_country, IngestionError

logger = logging.getLogger(__name__)


# ISO 3-letter codes → FAO area codes mapping
# FAOSTAT uses their own numeric country codes (NOT M49!)
# Source: faostat.get_par('QCL', 'area')
ISO_TO_FAO_AREA = {
    "USA": "231",   # United States of America
    "CAN": "33",    # Canada
    "MEX": "138",   # Mexico
    "DEU": "79",    # Germany
    "FRA": "68",    # France
    "ITA": "106",   # Italy
    "ESP": "203",   # Spain
    "NLD": "150",   # Netherlands (Kingdom of the)
    "GBR": "229",   # United Kingdom of Great Britain and Northern Ireland
    "IRL": "104",   # Ireland
    "CHE": "211",   # Switzerland
    "AUT": "11",    # Austria
    "BEL": "255",   # Belgium
    "DNK": "54",    # Denmark
    "FIN": "67",    # Finland
    "GRC": "84",    # Greece
    "NOR": "162",   # Norway
    "PRT": "174",   # Portugal
    "SWE": "210",   # Sweden
    "JPN": "110",   # Japan
    "KOR": "116",   # Republic of Korea
    "CHN": "351",   # China, mainland
    "IND": "100",   # India
    "BRA": "21",    # Brazil
    "ARG": "9",     # Argentina
    "CHL": "40",    # Chile
    "COL": "44",    # Colombia
    "PER": "170",   # Peru
    "AUS": "10",    # Australia
    "NZL": "156",   # New Zealand
    "ZAF": "202",   # South Africa
    "EGY": "59",    # Egypt
    "NGA": "159",   # Nigeria
    "KEN": "114",   # Kenya
    "THA": "216",   # Thailand
    "IDN": "101",   # Indonesia
    "MYS": "131",   # Malaysia
    "PHL": "171",   # Philippines
    "SGP": "200",   # Singapore
    "VNM": "237",   # Viet Nam
    "SAU": "194",   # Saudi Arabia
    "ARE": "225",   # United Arab Emirates
    "TUR": "223",   # Türkiye
    "POL": "173",   # Poland
    "CZE": "167",   # Czechia
    "HUN": "97",    # Hungary
    "ROU": "183",   # Romania
    "RUS": "185",   # Russian Federation
    "UKR": "230",   # Ukraine
    "ISR": "105",   # Israel
    "PAK": "165",   # Pakistan
    "BGD": "16",    # Bangladesh
    "LKA": "38",    # Sri Lanka
    "AFG": "2",     # Afghanistan
}


# FAOSTAT Configuration with CORRECTED element codes
FAOSTAT_CONFIG: Dict[str, Dict[str, Any]] = {
    "production": {
        "domain": "QCL",  # Crops and Livestock Products
        "element_code": "2510",  # Production quantity (CORRECTED from 5510)
        "item_codes": ["15"],  # Wheat only (barley has poor coverage)
        "canonical_indicator": "FAOSTAT_PRODUCTION",
    },
    "yield": {
        "domain": "QCL",
        "element_code": "2413",  # Yield (CORRECTED from 5419)
        "item_codes": ["15"],  # Wheat
        "canonical_indicator": "FAOSTAT_YIELD",
    },
}


def _split_comma_or_list(value: Union[str, List[str], List[int]]) -> List[str]:
    """
    Split comma-separated string or convert list to string list.
    
    Args:
        value: Either "44,221" or ["44", "221"] or [44, 221]
    
    Returns:
        List of strings: ["44", "221"]
    """
    if isinstance(value, str):
        return [s.strip() for s in value.split(",") if s.strip()]
    elif isinstance(value, list):
        return [str(v) for v in value]
    else:
        return [str(value)]


def fetch_faostat(
    domain: str,
    params: Dict[str, Any],
    *,
    sample_config: Optional[SampleConfig] = None
) -> pd.DataFrame:
    """
    Fetch from FAOSTAT using faostat.get_data_df() (CORRECT METHOD).
    
    NEVER call https://faostatservices.fao.org directly!
    ALWAYS use faostat.get_data_df() with proper pars mapping.
    
    Args:
        domain: FAOSTAT domain code (e.g., 'QCL' for crops/livestock)
        params: Internal parameters mapping:
            - area_code: FAO area code (e.g., "2" for Afghanistan)
            - element_code: Element code as string (e.g., "2510" for production)
            - item_code: Item code(s) as string "44" or "44,221" or list ["44", "221"]
            - year: Year(s) as string "2010,2011" or list [2010, 2011]
        sample_config: Optional sampling configuration
    
    Returns:
        DataFrame with FAOSTAT data
    """
    sample_config = sample_config or SampleConfig()
    
    try:
        # Build pars dict for faostat.get_data_df()
        pars: Dict[str, List[Union[str, int]]] = {}
        
        # Map area_code → pars["area"] (list of strings)
        if "area_code" in params and params["area_code"]:
            pars["area"] = [str(params["area_code"])]
        
        # Map element_code → pars["element"] (list of integers)
        if "element_code" in params and params["element_code"]:
            pars["element"] = [int(params["element_code"])]
        
        # Map item_code → pars["item"] (list of integers)
        if "item_code" in params and params["item_code"]:
            item_codes = _split_comma_or_list(params["item_code"])
            pars["item"] = [int(code) for code in item_codes]
        
        # Map year → pars["year"] (list of integers)
        if "year" in params and params["year"]:
            years = _split_comma_or_list(params["year"])
            pars["year"] = [int(y) for y in years]
        
        logger.info(f"FAOSTAT: Calling get_data_df(domain={domain}, pars={pars})")
        
        # CORRECT CALL: Use faostat.get_data_df() with pars
        df = faostat.get_data_df(
            domain,
            pars=pars,
            show_flags=False,
            null_values=False,
            show_notes=False,
            strval=False
        )
        
        # Limit in sample mode
        if sample_config.enabled and not df.empty:
            df = df.tail(sample_config.max_records_per_country * 10)
        
        logger.info(f"FAOSTAT: Received {len(df)} rows")
        return df
        
    except Exception as e:
        country = params.get("area_code", "UNKNOWN")
        error_msg = f"Fetch error using faostat.get_data_df(): {e}"
        logger.error(f"FAOSTAT: {error_msg}")
        raise IngestionError("FAOSTAT", str(country), error_msg)


def _faostat_payload_to_dataframe(raw: Any) -> pd.DataFrame:
    """
    Convert FAOSTAT response to DataFrame.
    
    With faostat.get_data_df(), this already returns a DataFrame,
    so this is a pass-through. Kept for backward compatibility.
    """
    if isinstance(raw, pd.DataFrame):
        return raw
    # Legacy: if still using dict response
    data = raw.get("data", []) if isinstance(raw, dict) else []
    return pd.DataFrame(data)


def _parse_faostat(
    df: pd.DataFrame,
    *,
    indicator_id: int,
    country_id: str,
    source: str,
    sample_config: Optional[SampleConfig] = None,
) -> List[Dict[str, Any]]:
    """
    Parse FAOSTAT dataframe with strict validation.
    
    Expected columns from faostat.get_data_df():
    - Area Code: Country code (may be string or int)
    - Year: Year as integer
    - Value: Numeric value
    """
    sample_config = sample_config or SampleConfig()
    rows: List[Dict[str, Any]] = []
    
    if df is None or df.empty:
        if sample_config.fail_on_empty:
            raise IngestionError("FAOSTAT", country_id, "Empty dataframe returned")
        return rows

    # Limit to sample size in test mode
    if sample_config.enabled and not df.empty:
        # faostat.get_data_df uses "Area Code" (without M49 suffix)
        if "Area Code" in df.columns:
            df = limit_dataframe_by_country(df, "Area Code", sample_config.max_records_per_country)

    for _, row in df.iterrows():
        # faostat.get_data_df() returns "Area Code" not "Area Code (M49)"
        area_code = str(row.get("Area Code", "")).strip()
        if not area_code:
            logger.warning(f"FAOSTAT: Missing Area Code in row")
            continue
            
        # FAOSTAT stores country codes as integers in Area Code
        # Our country_id is a string (e.g., "2")
        # Compare as strings after stripping whitespace
        if area_code != country_id:
            if sample_config.strict_validation:
                logger.warning(f"FAOSTAT: Row country {area_code} doesn't match requested {country_id}")
            continue
        
        value = row.get("Value")
        year = row.get("Year")
        
        if value is None or pd.isna(value):
            if sample_config.strict_validation:
                logger.warning(f"FAOSTAT: Missing value for {country_id} year {year}")
            continue
        if year is None or pd.isna(year):
            if sample_config.strict_validation:
                raise IngestionError("FAOSTAT", country_id, "Missing year in response")
            continue
        
        try:
            date = ensure_date(f"{int(year)}-12-31")
            numeric_value = float(value)
        except Exception as exc:
            logger.error(f"FAOSTAT: Parse error for {country_id}: {exc}")
            if sample_config.strict_validation:
                raise IngestionError("FAOSTAT", country_id, f"Parse error: {exc}")
            continue
        
        rows.append(
            {
                "indicator_id": indicator_id,
                "country_id": country_id,
                "date": date,
                "value": numeric_value,
                "source": source,
                "ingested_at": dt.datetime.now(dt.timezone.utc),
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


def ingest_full(
    session: Session,
    *,
    indicator_subset: Optional[Iterable[str]] = None,
    country_subset: Optional[Iterable[str]] = None,
    sample_config: Optional[SampleConfig] = None,
) -> None:
    """
    Ingest FAOSTAT data with optional sample mode.
    
    Args:
        session: Database session
        indicator_subset: Optional subset of indicators
        country_subset: Optional subset of countries (CRITICAL: prevents US-only bias)
        sample_config: Sample mode configuration for testing
    """
    sample_config = sample_config or SampleConfig()
    selected_indicators = set(indicator_subset) if indicator_subset else None
    selected_countries = set(country_subset) if country_subset else None

    # Limit countries in sample mode to avoid hitting all 54
    countries_to_fetch = list(COUNTRY_UNIVERSE)
    if selected_countries:
        countries_to_fetch = [c for c in countries_to_fetch if c in selected_countries]
    if sample_config.enabled:
        # Only test a diverse sample of countries
        countries_to_fetch = countries_to_fetch[:5]  # Limit to 5 countries in sample mode

    for name, cfg in FAOSTAT_CONFIG.items():
        if selected_indicators and name not in selected_indicators:
            continue
        
        try:
            indicator_id = resolve_indicator_id(session, cfg["canonical_indicator"])
        except Exception as e:
            logger.error(f"FAOSTAT: Failed to resolve indicator {cfg['canonical_indicator']}: {e}")
            if sample_config.strict_validation:
                raise IngestionError("FAOSTAT", "N/A", f"Indicator resolution failed: {e}")
            continue
        
        for country_id in countries_to_fetch:
            # Convert ISO code to FAO area code
            fao_area_code = ISO_TO_FAO_AREA.get(country_id)
            if not fao_area_code:
                logger.warning(f"FAOSTAT: No FAO area code mapping for {country_id}, skipping")
                continue
                
            try:
                # Build year range
                current_year = dt.datetime.now().year
                if sample_config.enabled:
                    # Sample mode: Last 2-5 years
                    year_start = current_year - sample_config.max_years
                    year_end = current_year - 1
                else:
                    # Full mode: Last 10 years
                    year_start = current_year - 10
                    year_end = current_year - 1
                
                # Generate year list
                years = list(range(year_start, year_end + 1))
                
                params = {
                    "area_code": fao_area_code,  # Use FAO numeric code
                    "item_code": ",".join(cfg["item_codes"]),
                    "element_code": cfg["element_code"],
                    "year": years,  # Pass as list of integers
                }
                
                raw = fetch_faostat(cfg["domain"], params, sample_config=sample_config)
                
                # Store DataFrame as JSON for raw payload storage
                if isinstance(raw, pd.DataFrame):
                    raw_payload = {"data": raw.to_dict(orient="records")}
                else:
                    raw_payload = raw
                
                store_raw_payload(
                    session,
                    RawFaostat,
                    params={"indicator": name, "country": country_id},  # Store with ISO code
                    payload=raw_payload,
                )
                df = _faostat_payload_to_dataframe(raw)
                rows = _parse_faostat(
                    df,
                    indicator_id=indicator_id,
                    country_id=fao_area_code,  # Parse using FAO code
                    source="FAOSTAT",
                    sample_config=sample_config,
                )
                
                # Convert FAO area codes back to ISO codes in rows
                for row in rows:
                    row["country_id"] = country_id  # Use ISO code for database
                    
                bulk_upsert_timeseries(session, rows)
                logger.info(f"FAOSTAT: Ingested {len(rows)} records for {country_id}/{name}")
            except IngestionError:
                raise
            except Exception as e:
                error_msg = f"Failed for {country_id}/{name}: {e}"
                logger.error(f"FAOSTAT: {error_msg}")
                if sample_config.strict_validation:
                    raise IngestionError("FAOSTAT", country_id, error_msg)

    session.commit()


def debug_faostat():
    """
    Debug function to prove FAOSTAT works with known good parameters.
    
    Tests with:
    - domain = "QCL" (Crops and Livestock)
    - area_code = "2" (Afghanistan)
    - element_code = "2510" (Production quantity)
    - item_code = "44" (Barley)
    - years = 2010-2019
    
    Prints:
    - DataFrame shape
    - Column names
    - First 10-25 rows sorted by Year
    """
    print("="*80)
    print("FAOSTAT DEBUG MODE - Known Good Request")
    print("="*80)
    
    # Known good parameters
    pars = {
        "area": ["2"],        # Afghanistan
        "element": [2510],    # Production quantity
        "item": [44],         # Barley
        "year": list(range(2010, 2020))  # 2010-2019
    }
    
    print(f"\nParameters:")
    print(f"  Domain: QCL (Crops and Livestock)")
    print(f"  Area: {pars['area']} (Afghanistan)")
    print(f"  Element: {pars['element']} (Production quantity)")
    print(f"  Item: {pars['item']} (Barley)")
    print(f"  Years: {pars['year']}")
    
    try:
        print(f"\nCalling faostat.get_data_df()...")
        df = faostat.get_data_df(
            "QCL",
            pars=pars,
            show_flags=False,
            null_values=False,
            show_notes=False,
            strval=False
        )
        
        print(f"\n✅ SUCCESS!")
        print(f"DataFrame shape: {df.shape} (rows={df.shape[0]}, columns={df.shape[1]})")
        print(f"\nColumns: {list(df.columns)}")
        
        # Sort by Year and show first rows
        if "Year" in df.columns:
            df_sorted = df.sort_values("Year")
            num_rows = min(25, len(df_sorted))
            print(f"\nFirst {num_rows} rows (sorted by Year):")
            print(df_sorted.head(num_rows).to_string(index=False))
        else:
            num_rows = min(25, len(df))
            print(f"\nFirst {num_rows} rows:")
            print(df.head(num_rows).to_string(index=False))
        
        return df
        
    except Exception as e:
        print(f"\n❌ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    # Run debug mode when executed directly
    debug_faostat()
