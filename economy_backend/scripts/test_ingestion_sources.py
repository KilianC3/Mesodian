#!/usr/bin/env python3
"""
Test and display data from all ingestion sources.

Usage:
    python scripts/test_ingestion_sources.py [source_name]
    
Examples:
    python scripts/test_ingestion_sources.py                # Test all sources
    python scripts/test_ingestion_sources.py ons           # Test only ONS
    python scripts/test_ingestion_sources.py wdi fred     # Test WDI and FRED
"""

import asyncio
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import logging
from typing import Any, Dict, List, Optional
from tabulate import tabulate
import pandas as pd

from app.ingest.sample_mode import SampleConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Suppress noisy loggers
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)


def print_header(title: str):
    """Print a formatted header."""
    print("\n" + "="*80)
    print(f"  {title}")
    print("="*80 + "\n")


def print_dataframe(df: pd.DataFrame, max_rows: int = 10):
    """Print a pandas DataFrame in a nice table format."""
    if df.empty:
        print("  [No data]")
        return
    
    # Limit rows
    display_df = df.head(max_rows)
    
    # Format for display
    print(tabulate(display_df, headers='keys', tablefmt='grid', showindex=True))
    
    if len(df) > max_rows:
        print(f"\n  ... and {len(df) - max_rows} more rows")
    print(f"\n  Total: {len(df)} rows, {len(df.columns)} columns")


def print_dict_list(data: List[Dict], max_rows: int = 10):
    """Print a list of dictionaries as a table."""
    if not data:
        print("  [No data]")
        return
    
    df = pd.DataFrame(data)
    print_dataframe(df, max_rows)


async def test_ons():
    """Test ONS (UK only)."""
    print_header("ONS - UK Office for National Statistics")
    
    from app.ingest.ons_client import fetch_series, parse_ons
    
    try:
        # Test CPIH (inflation)
        print("Fetching CPIH (UK Inflation)...")
        payload = await fetch_series("CPIH", sample_config=SampleConfig(enabled=True))
        
        print(f"\nPayload keys: {list(payload.keys())}")
        
        # Parse the data
        rows = parse_ons(
            payload, 
            indicator_id=999, 
            country_id="GBR", 
            source="ONS",
            sample_config=SampleConfig(enabled=True)
        )
        
        print(f"\n✓ Successfully fetched {len(rows)} records")
        print_dict_list(rows)
        
        # Test GDP
        print("\n" + "-"*80)
        print("Fetching GDP (UK Growth)...")
        payload = await fetch_series("GDP", sample_config=SampleConfig(enabled=True))
        rows = parse_ons(
            payload, 
            indicator_id=999, 
            country_id="GBR", 
            source="ONS",
            sample_config=SampleConfig(enabled=True)
        )
        print(f"\n✓ Successfully fetched {len(rows)} records")
        print_dict_list(rows)
        
        return True
    except Exception as e:
        print(f"\n✗ Error: {e}")
        logger.exception("ONS test failed")
        return False


async def test_comtrade():
    """Test UN COMTRADE (multiple countries)."""
    print_header("UN COMTRADE - Trade Data")
    
    from app.ingest.comtrade_client import fetch_raw_trade, parse_to_trade_flows
    
    test_cases = [
        ("USA", "CHN", 2022),
        ("GBR", "DEU", 2022),
        ("FRA", "USA", 2022),
    ]
    
    success = True
    for reporter, partner, year in test_cases:
        try:
            print(f"\nFetching {reporter} → {partner} trade ({year})...")
            payload = await fetch_raw_trade(
                reporter, partner, year, "TOTAL",
                sample_config=SampleConfig(enabled=True)
            )
            
            flows = parse_to_trade_flows(payload, sample_config=SampleConfig(enabled=True))
            print(f"✓ Successfully fetched {len(flows)} trade flows")
            print_dict_list(flows, max_rows=5)
        except Exception as e:
            print(f"✗ Error: {e}")
            success = False
    
    return success


def test_faostat():
    """Test FAOSTAT (multiple countries)."""
    print_header("FAOSTAT - Food & Agriculture")
    
    from app.ingest.faostat_client import fetch_faostat, _faostat_payload_to_dataframe, _parse_faostat
    
    # M49 codes: USA=840, CHN=156, IND=356, BRA=76
    test_countries = [
        ("840", "USA"),
        ("156", "CHN"),
        ("356", "IND"),
    ]
    
    success = True
    for m49_code, country_name in test_countries:
        try:
            print(f"\nFetching cereal production for {country_name} (M49: {m49_code})...")
            params = {
                "area_code": m49_code,
                "element_code": "5510",  # Production
                "item_code": "15",  # Cereals
                "year": "2020,2021,2022",
                "show_codes": "true"
            }
            
            raw = fetch_faostat("QCL", params, sample_config=SampleConfig(enabled=True))
            df = _faostat_payload_to_dataframe(raw)
            
            print(f"DataFrame shape: {df.shape}")
            
            rows = _parse_faostat(
                df,
                indicator_id=999,
                country_id=country_name,
                source="FAOSTAT",
                sample_config=SampleConfig(enabled=True)
            )
            
            print(f"✓ Successfully fetched {len(rows)} records")
            print_dict_list(rows, max_rows=5)
        except Exception as e:
            print(f"✗ Error: {e}")
            logger.exception(f"FAOSTAT test failed for {country_name}")
            success = False
    
    return success


def test_eurostat():
    """Test EUROSTAT (EU countries only)."""
    print_header("EUROSTAT - European Statistics")
    
    from app.ingest.eurostat_client import fetch_series, parse_eurostat
    
    test_countries = ["DEU", "FRA", "ITA", "ESP"]
    
    async def _test():
        success = True
        for country in test_countries:
            try:
                print(f"\nFetching HICP (inflation) for {country}...")
                payload = await fetch_series(
                    "teicp010", 
                    country,
                    sample_config=SampleConfig(enabled=True)
                )
                
                rows = parse_eurostat(
                    payload,
                    indicator_id=999,
                    country_id=country,
                    source="EUROSTAT",
                    sample_config=SampleConfig(enabled=True)
                )
                
                print(f"✓ Successfully fetched {len(rows)} records")
                print_dict_list(rows, max_rows=5)
            except Exception as e:
                print(f"✗ Error: {e}")
                success = False
        return success
    
    return asyncio.run(_test())


async def test_fred():
    """Test FRED (USA only)."""
    print_header("FRED - Federal Reserve Economic Data")
    
    from app.ingest.fred_client import fetch_series, parse_observations
    
    test_series = [
        ("CPIAUCSL", "CPI"),
        ("UNRATE", "Unemployment Rate"),
    ]
    
    success = True
    for series_id, name in test_series:
        try:
            print(f"\nFetching {name} ({series_id})...")
            payload = await fetch_series(
                series_id,
                sample_config=SampleConfig(enabled=True)
            )
            
            rows = parse_observations(
                payload,
                indicator_id=999,
                country_id="USA",
                source="FRED",
                sample_config=SampleConfig(enabled=True)
            )
            
            print(f"✓ Successfully fetched {len(rows)} records")
            print_dict_list(rows, max_rows=5)
        except Exception as e:
            print(f"✗ Error: {e}")
            success = False
    
    return success


async def test_wdi():
    """Test World Bank WDI (multiple countries)."""
    print_header("WDI - World Bank Development Indicators")
    
    from app.ingest.wdi_client import fetch_indicator, parse_wdi
    
    test_countries = ["USA", "GBR", "DEU", "CHN", "IND"]
    indicator = "NY.GDP.MKTP.KD"  # Real GDP
    
    success = True
    for country in test_countries:
        try:
            print(f"\nFetching Real GDP for {country}...")
            payload = await fetch_indicator(
                country.lower(),
                indicator,
                sample_config=SampleConfig(enabled=True)
            )
            
            rows = parse_wdi(
                payload,
                indicator_id=999,
                country_id=country,
                source="WDI",
                sample_config=SampleConfig(enabled=True)
            )
            
            print(f"✓ Successfully fetched {len(rows)} records")
            print_dict_list(rows, max_rows=5)
        except Exception as e:
            print(f"✗ Error for {country}: {e}")
            success = False
    
    return success


def test_imf():
    """Test IMF (multiple countries)."""
    print_header("IMF - International Monetary Fund")
    
    from app.ingest.imf_client import parse_imf_dataframe
    from app.ingest.base_client import fetch_sdmx_dataset
    
    test_countries = ["USA", "GBR", "DEU", "FRA", "JPN"]
    
    success = True
    for country in test_countries:
        try:
            print(f"\nFetching CPI Index for {country}...")
            
            # Build dataset path
            full_path = f"IFS/M.{country}.PCPI_IX.A"
            
            df = fetch_sdmx_dataset(
                "https://dataservices.imf.org/REST/SDMX_JSON.svc",
                full_path,
                sample_config=SampleConfig(enabled=True)
            )
            
            print(f"DataFrame shape: {df.shape}")
            
            rows = parse_imf_dataframe(
                df,
                indicator_id=999,
                country_id=country,
                source="IMF",
                sample_config=SampleConfig(enabled=True)
            )
            
            print(f"✓ Successfully fetched {len(rows)} records")
            print_dict_list(rows, max_rows=5)
        except Exception as e:
            print(f"✗ Error for {country}: {e}")
            success = False
    
    return success


def test_oecd():
    """Test OECD (OECD member countries)."""
    print_header("OECD - Organisation for Economic Co-operation and Development")
    
    from app.ingest.oecd_client import parse_oecd
    from app.ingest.base_client import fetch_sdmx_dataset
    
    test_countries = ["USA", "GBR", "DEU", "FRA", "JPN"]
    
    success = True
    for country in test_countries:
        try:
            print(f"\nFetching CPI data for {country}...")
            
            df = fetch_sdmx_dataset(
                "https://sdmx.oecd.org/public/rest",
                f"MEI/{country}.CPALTT01.M",
                sample_config=SampleConfig(enabled=True)
            )
            
            print(f"DataFrame shape: {df.shape}")
            
            rows = parse_oecd(
                df,
                indicator_id=999,
                country_id=country,
                source="OECD",
                sample_config=SampleConfig(enabled=True)
            )
            
            print(f"✓ Successfully fetched {len(rows)} records")
            print_dict_list(rows, max_rows=5)
        except Exception as e:
            print(f"✗ Error for {country}: {e}")
            success = False
    
    return success


async def test_eia():
    """Test EIA (USA energy data)."""
    print_header("EIA - Energy Information Administration")
    
    from app.ingest.eia_client import fetch_series, _parse_series_payload
    
    try:
        print("Fetching World Total Energy Consumption...")
        payload = await fetch_series(
            "INTL.44-1-WORL-QBTU.A",
            sample_config=SampleConfig(enabled=True)
        )
        
        print(f"Payload keys: {list(payload.keys())}")
        
        rows = _parse_series_payload(
            payload,
            indicator_id=999,
            country_id="WORLD",
            source="EIA",
            sample_config=SampleConfig(enabled=True)
        )
        
        print(f"✓ Successfully fetched {len(rows)} records")
        print_dict_list(rows)
        
        return True
    except Exception as e:
        print(f"✗ Error: {e}")
        logger.exception("EIA test failed")
        return False


def test_ember():
    """Test Ember Climate (global electricity data)."""
    print_header("Ember Climate - Electricity Data")
    
    from app.ingest.ember_client import fetch_csv
    
    try:
        print("Fetching Ember yearly electricity generation data...")
        url = "https://ember-climate.org/app/uploads/2022/07/yearly_full_release_long_format.csv"
        df = fetch_csv(url)
        
        print(f"\nDataFrame shape: {df.shape}")
        print(f"Columns: {df.columns.tolist()[:10]}")
        
        # Show sample for a few countries
        if 'Area' in df.columns:
            sample_countries = ['United States', 'United Kingdom', 'Germany', 'China', 'India']
            for country in sample_countries:
                country_data = df[df['Area'] == country]
                if not country_data.empty:
                    print(f"\n{country}: {len(country_data)} records")
                    print(country_data.head(3).to_string())
        
        print(f"\n✓ Successfully fetched data")
        return True
    except Exception as e:
        print(f"✗ Error: {e}")
        logger.exception("Ember test failed")
        return False


def test_gcp():
    """Test Global Carbon Project / OWID CO2."""
    print_header("Global Carbon Project / OWID - CO2 Data")
    
    from app.ingest.gcp_client import fetch_co2_dataset
    
    try:
        print("Fetching OWID CO2 dataset...")
        df = fetch_co2_dataset()
        
        print(f"\nDataFrame shape: {df.shape}")
        print(f"Columns: {df.columns.tolist()[:15]}")
        
        # Show sample for a few countries
        test_countries = ['USA', 'GBR', 'DEU', 'CHN', 'IND']
        for country_code in test_countries:
            country_data = df[df['iso_code'] == country_code]
            if not country_data.empty:
                print(f"\n{country_code}: {len(country_data)} records")
                print(f"  Year range: {country_data['year'].min()} - {country_data['year'].max()}")
                print(country_data[['year', 'co2', 'co2_per_capita']].tail(3).to_string())
        
        print(f"\n✓ Successfully fetched data")
        return True
    except Exception as e:
        print(f"✗ Error: {e}")
        logger.exception("GCP test failed")
        return False


async def test_patentsview():
    """Test PatentsView (USA patents)."""
    print_header("PatentsView - US Patent Data")
    
    from app.ingest.patentsview_client import fetch_patents, _parse_patents
    
    test_countries = ["USA", "GBR", "DEU", "CHN", "JPN"]
    
    success = True
    for country in test_countries:
        try:
            print(f"\nFetching patent counts for {country} (2022)...")
            payload = await fetch_patents(
                "computers",
                country,
                2022,
                sample_config=SampleConfig(enabled=True)
            )
            
            rows = _parse_patents(
                payload,
                indicator_id=999,
                country_id=country,
                year=2022,
                sample_config=SampleConfig(enabled=True)
            )
            
            print(f"✓ Successfully fetched {len(rows)} records")
            print_dict_list(rows)
        except Exception as e:
            print(f"✗ Error for {country}: {e}")
            success = False
    
    return success


async def test_openalex():
    """Test OpenAlex (academic publications)."""
    print_header("OpenAlex - Academic Publications")
    
    from app.ingest.openalex_client import fetch_openalex_works, _parse_openalex
    
    test_countries = ["USA", "GBR", "DEU", "CHN", "IND"]
    concept_id = "C154945302"  # Computer science concept
    
    success = True
    for country in test_countries:
        try:
            print(f"\nFetching publications for {country} (2022)...")
            payload = await fetch_openalex_works(
                concept_id,
                country,
                2022,
                sample_config=SampleConfig(enabled=True)
            )
            
            print(f"Total works: {payload.get('meta', {}).get('count', 0)}")
            
            rows = _parse_openalex(
                payload,
                indicator_id=999,
                country_id=country,
                year=2022,
                sample_config=SampleConfig(enabled=True)
            )
            
            print(f"✓ Successfully fetched {len(rows)} records")
            print_dict_list(rows)
        except Exception as e:
            print(f"✗ Error for {country}: {e}")
            success = False
    
    return success


def test_yfinance():
    """Test Yahoo Finance."""
    print_header("Yahoo Finance - Stock Prices")
    
    from app.ingest.yfinance_client import fetch_prices
    import datetime as dt
    
    try:
        print("Fetching prices for AAPL, MSFT...")
        end = dt.date.today()
        start = end - dt.timedelta(days=30)
        
        data = fetch_prices(["AAPL", "MSFT"], start, end)
        
        for ticker, df in data.items():
            print(f"\n{ticker}: {len(df)} records")
            print(df.head(5).to_string())
        
        print(f"\n✓ Successfully fetched data")
        return True
    except Exception as e:
        print(f"✗ Error: {e}")
        logger.exception("YFinance test failed")
        return False


def test_stooq():
    """Test Stooq."""
    print_header("Stooq - Financial Data")
    
    from app.ingest.stooq_client import fetch_stooq_csv
    
    symbols = ["^SPX", "EURUSD"]
    
    success = True
    for symbol in symbols:
        try:
            print(f"\nFetching {symbol}...")
            df = fetch_stooq_csv(symbol, sample_config=SampleConfig(enabled=True))
            
            print(f"DataFrame shape: {df.shape}")
            print(df.head(5).to_string())
            
            print(f"✓ Successfully fetched data")
        except Exception as e:
            print(f"✗ Error: {e}")
            success = False
    
    return success


def test_gdelt():
    """Test GDELT."""
    print_header("GDELT - Global Events")
    
    from app.ingest.gdelt_client import fetch_gdelt_events
    
    try:
        print("Fetching GDELT events for USA...")
        df = fetch_gdelt_events(
            params={"query": "country:USA"},
            sample_config=SampleConfig(enabled=True)
        )
        
        print(f"\nDataFrame shape: {df.shape}")
        print(f"Columns: {df.columns.tolist()[:10]}")
        
        if not df.empty:
            print("\nSample records:")
            print(df.head(5).to_string())
        
        print(f"\n✓ Successfully fetched data")
        return True
    except Exception as e:
        print(f"✗ Error: {e}")
        logger.exception("GDELT test failed")
        return False


# Test suite configuration
TESTS = {
    'ons': test_ons,
    'comtrade': test_comtrade,
    'faostat': test_faostat,
    'eurostat': test_eurostat,
    'fred': test_fred,
    'wdi': test_wdi,
    'imf': test_imf,
    'oecd': test_oecd,
    'eia': test_eia,
    'ember': test_ember,
    'gcp': test_gcp,
    'patentsview': test_patentsview,
    'openalex': test_openalex,
    'yfinance': test_yfinance,
    'stooq': test_stooq,
    'gdelt': test_gdelt,
}


async def run_tests(selected_tests: Optional[List[str]] = None):
    """Run selected tests or all tests."""
    
    if selected_tests:
        tests_to_run = {k: v for k, v in TESTS.items() if k in selected_tests}
        if not tests_to_run:
            print(f"Error: Unknown tests: {selected_tests}")
            print(f"Available tests: {', '.join(TESTS.keys())}")
            return
    else:
        tests_to_run = TESTS
    
    print("\n" + "="*80)
    print(f"  Testing {len(tests_to_run)} ingestion source(s)")
    print("="*80)
    
    results = {}
    
    for name, test_func in tests_to_run.items():
        try:
            if asyncio.iscoroutinefunction(test_func):
                result = await test_func()
            else:
                result = test_func()
            results[name] = result
        except Exception as e:
            print(f"\n✗ {name.upper()} test crashed: {e}")
            logger.exception(f"{name} test crashed")
            results[name] = False
    
    # Print summary
    print_header("TEST SUMMARY")
    
    success_count = sum(1 for r in results.values() if r)
    total_count = len(results)
    
    for name, success in results.items():
        status = "✓ PASS" if success else "✗ FAIL"
        print(f"  {name.upper():20s} {status}")
    
    print(f"\n  Total: {success_count}/{total_count} passed")
    
    return success_count == total_count


def main():
    """Main entry point."""
    import sys
    
    selected = sys.argv[1:] if len(sys.argv) > 1 else None
    
    if selected and selected[0] in ['--help', '-h']:
        print(__doc__)
        print("\nAvailable sources:")
        for name in sorted(TESTS.keys()):
            print(f"  - {name}")
        return
    
    try:
        success = asyncio.run(run_tests(selected))
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)


if __name__ == '__main__':
    main()
