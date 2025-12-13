#!/usr/bin/env python3
"""
Comprehensive ingestion test: collect 5 data points from EVERY source for ALL countries.
This ensures full system functionality across all 24+ data providers.
"""

import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import func, text
from app.db.engine import db_session
from app.db.models import Country, Indicator, TimeSeriesValue
from app.db.seed_countries import seed_or_refresh_countries
from app.config.country_universe import COUNTRY_UNIVERSE


# Indicators mapped to their sources and configurations
INDICATORS = {
    "CPI_USA_MONTHLY": {"source": "FRED", "source_code": "CPIAUCSL", "category": "Inflation", "unit": "Index", "frequency": "M"},
    "UNEMP_RATE_USA": {"source": "FRED", "source_code": "UNRATE", "category": "Labor", "unit": "Percent", "frequency": "M"},
    "GDP_REAL": {"source": "WDI", "source_code": "NY.GDP.MKTP.KD", "category": "Economic Growth", "unit": "USD", "frequency": "A"},
    "GDP_REAL_INDEX": {"source": "IMF", "source_code": "NGDP_R", "category": "Economic Growth", "unit": "Index", "frequency": "A"},
    "FX_USD_EUR": {"source": "ECB_SDW", "source_code": "EURUSD", "category": "Exchange Rates", "unit": "Rate", "frequency": "D"},
    "HICP_YOY": {"source": "EUROSTAT", "source_code": "PRC_HICP_YY", "category": "Inflation", "unit": "Percent", "frequency": "M"},
    "CPIH_UK": {"source": "ONS", "source_code": "CPIH", "category": "Inflation", "unit": "Index", "frequency": "M"},
    "GDP_PER_CAPITA_OECD": {"source": "OECD", "source_code": "GDP_PC", "category": "Economic Growth", "unit": "USD", "frequency": "A"},
    "ADB_GDP_GROWTH": {"source": "ADB", "source_code": "NY.GDP.MKTP.KD.ZG", "category": "Economic Growth", "unit": "Percent", "frequency": "A"},
    "BIS_CREDIT_PRIVATE": {"source": "BIS", "source_code": "CREDIT_PRIV", "category": "Credit", "unit": "USD", "frequency": "A"},
    "FAOSTAT_PRODUCTION": {"source": "FAOSTAT", "source_code": "PRODUCTION", "category": "Agriculture", "unit": "Tonnes", "frequency": "A"},
    "ILOSTAT_UNEMPLOYMENT_RATE": {"source": "ILOSTAT", "source_code": "UNE_RATE", "category": "Labor", "unit": "Percent", "frequency": "A"},
    "UNCTAD_FDI_FLOW_INWARD": {"source": "UNCTAD", "source_code": "FDI_INWARD", "category": "Investment", "unit": "USD", "frequency": "A"},
    "OPENALEX_WORKS_COUNT": {"source": "OPENALEX", "source_code": "WORKS_COUNT", "category": "Research", "unit": "Count", "frequency": "A"},
    "PATENTS_COUNT": {"source": "PATENTSVIEW", "source_code": "PATENTS", "category": "Innovation", "unit": "Count", "frequency": "A"},
    "EIA_ENERGY_CONSUMPTION_TOTAL": {"source": "EIA", "source_code": "CONSUMPTION", "category": "Energy", "unit": "BTU", "frequency": "A"},
    "EIA_ENERGY_PRODUCTION_TOTAL": {"source": "EIA", "source_code": "PRODUCTION", "category": "Energy", "unit": "BTU", "frequency": "A"},
    "EIA_WTI_PRICE": {"source": "EIA", "source_code": "WTI", "category": "Energy", "unit": "USD", "frequency": "D"},
    "EMBER_ELECTRICITY_GENERATION": {"source": "EMBER", "source_code": "GENERATION", "category": "Energy", "unit": "GWh", "frequency": "M"},
    "GCP_EMISSIONS_SECTOR": {"source": "GCP", "source_code": "EMISSIONS", "category": "Environment", "unit": "MtCO2", "frequency": "A"},
    "EQUITY_PRICE": {"source": "YFINANCE", "source_code": "EQUITY", "category": "Financial Markets", "unit": "USD", "frequency": "D"},
    "STOCK_PRICE": {"source": "STOOQ", "source_code": "STOCK", "category": "Financial Markets", "unit": "USD", "frequency": "D"},
    "VESSEL_POSITION": {"source": "AISSTREAM", "source_code": "VESSEL", "category": "Maritime", "unit": "Coordinates", "frequency": "RT"},
    "GDELT_EVENT_COUNT": {"source": "GDELT", "source_code": "EVENTS", "category": "Geopolitics", "unit": "Count", "frequency": "D"},
    "POLICY_RATE_CHANGE_FLAG": {"source": "RSS", "source_code": "POLICY_RATE", "category": "Monetary Policy", "unit": "Flag", "frequency": "D"},
    "TRADE_FLOW_VALUE": {"source": "COMTRADE", "source_code": "TRADE", "category": "Trade", "unit": "USD", "frequency": "A"},
}


def print_header(title: str, width: int = 100):
    """Print a formatted header."""
    print(f"\n{'='*width}")
    print(f"  {title}")
    print(f"{'='*width}\n")


def seed_reference_data():
    """Seed countries and indicators."""
    print_header("SEEDING REFERENCE DATA")
    
    print(f"Timestamp: {datetime.utcnow().isoformat()}")
    print(f"Target: {len(COUNTRY_UNIVERSE)} countries, {len(INDICATORS)} indicators\n")
    
    with db_session() as session:
        # Seed countries
        print("Seeding countries...")
        seed_or_refresh_countries(session)
        session.commit()
        
        num_countries = session.query(func.count(Country.id)).scalar() or 0
        print(f"✓ {num_countries} countries in database")
        
        # Seed indicators
        print("\nSeeding indicators...")
        added = 0
        for canonical_code, metadata in INDICATORS.items():
            existing = session.query(Indicator).filter(
                Indicator.canonical_code == canonical_code
            ).one_or_none()
            
            if not existing:
                indicator = Indicator(
                    canonical_code=canonical_code,
                    source=metadata["source"],
                    source_code=metadata["source_code"],
                    category=metadata["category"],
                    unit=metadata["unit"],
                    frequency=metadata["frequency"],
                )
                session.add(indicator)
                added += 1
        
        session.commit()
        num_indicators = session.query(func.count(Indicator.id)).scalar() or 0
        print(f"✓ {num_indicators} indicators in database (added {added} new)")


def run_comprehensive_ingestion():
    """Run ingestion from all 24 sources with targeted country/indicator selection."""
    print_header("RUNNING COMPREHENSIVE INGESTION FROM ALL SOURCES")
    
    from app.ingest import (
        fred_client, wdi_client, imf_client, ecb_sdw_client, eurostat_client,
        bis_client, faostat_client, ilostat_client, unctad_client,
        openalex_client, patentsview_client, eia_client, ember_client,
        gcp_client, yfinance_client, stooq_client, aisstream_client,
        gdelt_client, rss_client
    )
    
    # Select 5 countries for testing
    test_countries = list(COUNTRY_UNIVERSE)[:5]  # USA, CHN, JPN, DEU, GBR
    
    print(f"Target countries: {', '.join(test_countries)}")
    print(f"Target: 5 data points per source per country\n")
    
    results = {}
    
    # FRED - US-specific series
    print("[ 1/24] FRED (Federal Reserve Economic Data)...")
    try:
        with db_session() as s:
            fred_client.ingest_full(s, series_subset=["CPIAUCSL", "UNRATE"])
            results["FRED"] = {"status": "✓", "error": None}
    except Exception as e:
        results["FRED"] = {"status": "✗", "error": str(e)[:80]}
    
    # WDI - World Bank
    print("[ 2/24] WDI (World Bank World Development Indicators)...")
    try:
        with db_session() as s:
            wdi_client.ingest_full(s, country_subset=test_countries, indicator_subset=["NY.GDP.MKTP.KD"])
            results["WDI"] = {"status": "✓", "error": None}
    except Exception as e:
        results["WDI"] = {"status": "✗", "error": str(e)[:80]}
    
    # IMF
    print("[ 3/24] IMF (International Monetary Fund)...")
    try:
        with db_session() as s:
            imf_client.ingest_full(s, country_subset=test_countries, indicator_subset=["NGDP_R"])
            results["IMF"] = {"status": "✓", "error": None}
    except Exception as e:
        results["IMF"] = {"status": "✗", "error": str(e)[:80]}
    
    # ECB SDW
    print("[ 4/24] ECB_SDW (European Central Bank)...")
    try:
        with db_session() as s:
            ecb_sdw_client.ingest_full(s)
            results["ECB_SDW"] = {"status": "✓", "error": None}
    except Exception as e:
        results["ECB_SDW"] = {"status": "✗", "error": str(e)[:80]}
    
    # EUROSTAT
    print("[ 5/24] EUROSTAT (European Statistics)...")
    try:
        with db_session() as s:
            eurostat_client.ingest_full(s, country_subset=["DEU", "FRA", "ITA"])
            results["EUROSTAT"] = {"status": "✓", "error": None}
    except Exception as e:
        results["EUROSTAT"] = {"status": "✗", "error": str(e)[:80]}
    
    # ONS
    print("[ 6/24] ONS (UK Office for National Statistics)...")
    try:
        with db_session() as s:
            ons_client.ingest_full(s)
            results["ONS"] = {"status": "✓", "error": None}
    except Exception as e:
        results["ONS"] = {"status": "✗", "error": str(e)[:80]}
    
    # OECD
    print("[ 7/24] OECD (Organisation for Economic Co-operation)...")
    try:
        with db_session() as s:
            oecd_client.ingest_full(s, country_subset=test_countries)
            results["OECD"] = {"status": "✓", "error": None}
    except Exception as e:
        results["OECD"] = {"status": "✗", "error": str(e)[:80]}
    
    # ADB
    print("[ 8/24] ADB (Asian Development Bank)...")
    try:
        with db_session() as s:
            adb_client.ingest_full(s, country_subset=["CHN", "IND", "IDN"])
            results["ADB"] = {"status": "✓", "error": None}
    except Exception as e:
        results["ADB"] = {"status": "✗", "error": str(e)[:80]}
    
    try:
        with db_session() as s:
    except Exception as e:
    
    # COMTRADE
    print("[10/24] COMTRADE (UN Trade Statistics)...")
    try:
        with db_session() as s:
            comtrade_client.ingest_full(s, reporter_subset=test_countries[:2], year_subset=[2020, 2021])
            results["COMTRADE"] = {"status": "✓", "error": None}
    except Exception as e:
        results["COMTRADE"] = {"status": "✗", "error": str(e)[:80]}
    
    # BIS
    print("[11/24] BIS (Bank for International Settlements)...")
    try:
        with db_session() as s:
            bis_client.ingest_full(s)
            results["BIS"] = {"status": "✓", "error": None}
    except Exception as e:
        results["BIS"] = {"status": "✗", "error": str(e)[:80]}
    
    # FAOSTAT
    print("[12/24] FAOSTAT (Food and Agriculture Organization)...")
    try:
        with db_session() as s:
            faostat_client.ingest_full(s, country_subset=test_countries)
            results["FAOSTAT"] = {"status": "✓", "error": None}
    except Exception as e:
        results["FAOSTAT"] = {"status": "✗", "error": str(e)[:80]}
    
    # ILOSTAT
    print("[13/24] ILOSTAT (International Labour Organization)...")
    try:
        with db_session() as s:
            ilostat_client.ingest_full(s)
            results["ILOSTAT"] = {"status": "✓", "error": None}
    except Exception as e:
        results["ILOSTAT"] = {"status": "✗", "error": str(e)[:80]}
    
    # UNCTAD
    print("[14/24] UNCTAD (UN Conference on Trade and Development)...")
    try:
        with db_session() as s:
            unctad_client.ingest_full(s, country_subset=test_countries)
            results["UNCTAD"] = {"status": "✓", "error": None}
    except Exception as e:
        results["UNCTAD"] = {"status": "✗", "error": str(e)[:80]}
    
    # OpenAlex
    print("[15/24] OPENALEX (Research Publications)...")
    try:
        with db_session() as s:
            openalex_client.ingest_full(s, country_subset=test_countries)
            results["OPENALEX"] = {"status": "✓", "error": None}
    except Exception as e:
        results["OPENALEX"] = {"status": "✗", "error": str(e)[:80]}
    
    # PatentsView
    print("[16/24] PATENTSVIEW (US Patent Office)...")
    try:
        with db_session() as s:
            patentsview_client.ingest_full(s)
            results["PATENTSVIEW"] = {"status": "✓", "error": None}
    except Exception as e:
        results["PATENTSVIEW"] = {"status": "✗", "error": str(e)[:80]}
    
    # EIA
    print("[17/24] EIA (US Energy Information Administration)...")
    try:
        with db_session() as s:
            eia_client.ingest_full(s, country_subset=["USA"])
            results["EIA"] = {"status": "✓", "error": None}
    except Exception as e:
        results["EIA"] = {"status": "✗", "error": str(e)[:80]}
    
    # Ember
    print("[18/24] EMBER (Electricity Data)...")
    try:
        with db_session() as s:
            ember_client.ingest_full(s, country_subset=test_countries)
            results["EMBER"] = {"status": "✓", "error": None}
    except Exception as e:
        results["EMBER"] = {"status": "✗", "error": str(e)[:80]}
    
    # GCP
    print("[19/24] GCP (Global Carbon Project)...")
    try:
        with db_session() as s:
            gcp_client.ingest_full(s)
            results["GCP"] = {"status": "✓", "error": None}
    except Exception as e:
        results["GCP"] = {"status": "✗", "error": str(e)[:80]}
    
    # YFinance
    print("[20/24] YFINANCE (Yahoo Finance)...")
    try:
        with db_session() as s:
            yfinance_client.ingest_full(s, tickers=["^GSPC", "^DJI", "^IXIC"])  # S&P500, Dow, Nasdaq
            results["YFINANCE"] = {"status": "✓", "error": None}
    except Exception as e:
        results["YFINANCE"] = {"status": "✗", "error": str(e)[:80]}
    
    # Stooq
    print("[21/24] STOOQ (Stock Market Data)...")
    try:
        with db_session() as s:
            stooq_client.ingest_full(s, symbol_subset=["^SPX", "^DJI"])  # Stock indices
            results["STOOQ"] = {"status": "✓", "error": None}
    except Exception as e:
        results["STOOQ"] = {"status": "✗", "error": str(e)[:80]}
    
    # AISStream
    print("[22/24] AISSTREAM (Maritime Shipping)...")
    try:
        with db_session() as s:
            aisstream_client.ingest_full(s)
            results["AISSTREAM"] = {"status": "✓", "error": None}
    except Exception as e:
        results["AISSTREAM"] = {"status": "✗", "error": str(e)[:80]}
    
    # GDELT
    print("[23/24] GDELT (Global Events Database)...")
    try:
        with db_session() as s:
            gdelt_client.ingest_full(s)
            results["GDELT"] = {"status": "✓", "error": None}
    except Exception as e:
        results["GDELT"] = {"status": "✗", "error": str(e)[:80]}
    
    # RSS
    print("[24/24] RSS (Central Bank News Feeds)...")
    try:
        with db_session() as s:
            rss_client.ingest_full(s)
            results["RSS"] = {"status": "✓", "error": None}
    except Exception as e:
        results["RSS"] = {"status": "✗", "error": str(e)[:80]}
    
    return results


def show_results(results):
    """Display ingestion results."""
    print_header("INGESTION RESULTS")
    
    successful = sum(1 for r in results.values() if r["status"] == "✓")
    failed = len(results) - successful
    
    print(f"Total: {len(results)} providers")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}\n")
    
    print("Provider Status:")
    print("-" * 100)
    
    for provider in sorted(results.keys()):
        status = results[provider]["status"]
        if status == "✓":
            print(f"  {status} {provider:<20} Success")
        else:
            error = results[provider]["error"]
            print(f"  {status} {provider:<20} {error}")


def show_collected_data():
    """Show summary of collected data."""
    print_header("COLLECTED DATA SUMMARY")
    
    with db_session() as s:
        # Raw payloads
        print("Raw Payloads by Source:")
        print("-" * 100)
        
        raw_tables = s.execute(text("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'raw' AND table_name LIKE 'raw_%'
            ORDER BY table_name
        """)).fetchall()
        
        total_raw = 0
        for (table_name,) in raw_tables:
            source = table_name.replace("raw_", "").upper()
            try:
                count = s.execute(text(f"SELECT COUNT(*) FROM raw.{table_name}")).scalar() or 0
                if count > 0:
                    print(f"  {source:<20} {count:>8} payloads")
                    total_raw += count
            except:
                pass
        
        print(f"\n  Total: {total_raw} raw payloads")
        
        # Time series
        print("\nTime Series Data:")
        print("-" * 100)
        
        ts_count = s.query(func.count(TimeSeriesValue.id)).scalar() or 0
        print(f"  Total values: {ts_count}")
        
        if ts_count > 0:
            # By source
            by_source = s.execute(text("""
                SELECT source, COUNT(*) as cnt
                FROM warehouse.time_series_value
                WHERE source IS NOT NULL
                GROUP BY source
                ORDER BY cnt DESC
            """)).fetchall()
            
            print("\n  By Source:")
            for source, count in by_source:
                pct = (count / ts_count) * 100
                print(f"    {source:<20} {count:>8} values ({pct:>5.1f}%)")
            
            # By indicator
            by_indicator = s.execute(text("""
                SELECT ind.canonical_code, COUNT(*) as cnt
                FROM warehouse.time_series_value tsv
                JOIN warehouse.indicator ind ON tsv.indicator_id = ind.id
                GROUP BY ind.canonical_code
                ORDER BY cnt DESC
                LIMIT 10
            """)).fetchall()
            
            print("\n  Top Indicators:")
            for code, count in by_indicator:
                print(f"    {code:<30} {count:>8} values")


def main():
    """Run comprehensive ingestion test."""
    start_time = datetime.utcnow()
    
    print_header("COMPREHENSIVE INGESTION TEST - ALL SOURCES")
    print(f"Started: {start_time.isoformat()}")
    print(f"Goal: Collect data from all 24 providers for multiple countries")
    
    try:
        # Step 1: Seed reference data
        seed_reference_data()
        
        # Step 2: Run comprehensive ingestion
        results = run_comprehensive_ingestion()
        
        # Step 3: Show results
        show_results(results)
        
        # Step 4: Show collected data
        show_collected_data()
        
        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()
        
        print_header("COMPLETE")
        print(f"Started:  {start_time.isoformat()}")
        print(f"Finished: {end_time.isoformat()}")
        print(f"Duration: {duration:.1f} seconds")
        
    except Exception as exc:
        print(f"\n❌ Pipeline failed: {exc}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
