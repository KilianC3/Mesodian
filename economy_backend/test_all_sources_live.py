#!/usr/bin/env python3
"""
Test all sources with LIVE API calls (sample mode) to determine actual working status.
This script tests each source individually and reports which ones work vs which ones fail.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from dotenv import load_dotenv
load_dotenv()  # Load environment variables from .env file

from app.db.engine import db_session
from app.ingest.sample_mode import SampleConfig
import logging

# Suppress noisy loggers
logging.basicConfig(level=logging.ERROR)
logging.getLogger('httpx').setLevel(logging.ERROR)
logging.getLogger('urllib3').setLevel(logging.ERROR)

# Default sample config with lenient validation
DEFAULT_SAMPLE_CONFIG = SampleConfig(enabled=True, strict_validation=False)

def test_source(name: str, test_func):
    """Test a single source and report status."""
    try:
        print(f"\nTesting {name}...", end=" ", flush=True)
        test_func()
        print("✅ SUCCESS")
        return True, None
    except Exception as e:
        error_msg = str(e)[:100]
        print(f"❌ FAILED: {error_msg}")
        return False, error_msg


# Test functions for each source
def test_fred():
    from app.ingest.fred_client import ingest_full
    with db_session() as session:
        ingest_full(session, sample_config=DEFAULT_SAMPLE_CONFIG)
        session.commit()

def test_wdi():
    from app.ingest.wdi_client import ingest_full
    with db_session() as session:
        ingest_full(session, sample_config=DEFAULT_SAMPLE_CONFIG)
        session.commit()

def test_imf():
    from app.ingest.imf_client import ingest_full
    with db_session() as session:
        ingest_full(session, sample_config=DEFAULT_SAMPLE_CONFIG)
        session.commit()

def test_openalex():
    from app.ingest.openalex_client import ingest_full
    with db_session() as session:
        ingest_full(session, sample_config=DEFAULT_SAMPLE_CONFIG)
        session.commit()

def test_yfinance():
    from app.ingest.yfinance_client import ingest_full
    with db_session() as session:
        ingest_full(session, sample_config=DEFAULT_SAMPLE_CONFIG)
        session.commit()

def test_stooq():
    from app.ingest.stooq_client import ingest_full
    with db_session() as session:
        ingest_full(session, sample_config=DEFAULT_SAMPLE_CONFIG)
        session.commit()

def test_oecd():
    from app.ingest.oecd_client import ingest_full
    with db_session() as session:
        ingest_full(session, sample_config=DEFAULT_SAMPLE_CONFIG)
        session.commit()

def test_comtrade():
    from app.ingest.comtrade_client import ingest_full
    with db_session() as session:
        ingest_full(session, sample_config=DEFAULT_SAMPLE_CONFIG)
        session.commit()

def test_rss():
    from app.ingest.rss_client import ingest_full
    with db_session() as session:
        ingest_full(session, sample_config=DEFAULT_SAMPLE_CONFIG)
        session.commit()

def test_eurostat():
    from app.ingest.eurostat_client import ingest_full
    with db_session() as session:
        ingest_full(session, sample_config=DEFAULT_SAMPLE_CONFIG)
        session.commit()

def test_eia():
    from app.ingest.eia_client import ingest_full
    with db_session() as session:
        ingest_full(session, sample_config=DEFAULT_SAMPLE_CONFIG)
        session.commit()

def test_ember():
    from app.ingest.ember_client import ingest_full
    with db_session() as session:
        ingest_full(session, sample_config=DEFAULT_SAMPLE_CONFIG)
        session.commit()

def test_bis():
    from app.ingest.bis_client import ingest_full
    with db_session() as session:
        ingest_full(session, sample_config=DEFAULT_SAMPLE_CONFIG)
        session.commit()

def test_gcp():
    from app.ingest.gcp_client import ingest_full
    with db_session() as session:
        ingest_full(session, sample_config=DEFAULT_SAMPLE_CONFIG)
        session.commit()

def test_patentsview():
    from app.ingest.patentsview_client import ingest_full
    with db_session() as session:
        ingest_full(session, sample_config=DEFAULT_SAMPLE_CONFIG)
        session.commit()

def test_unctad():
    from app.ingest.unctad_client import ingest_full
    with db_session() as session:
        ingest_full(session, sample_config=DEFAULT_SAMPLE_CONFIG)
        session.commit()

def test_ons():
    from app.ingest.ons_client import ingest_full
    with db_session() as session:
        ingest_full(session, sample_config=DEFAULT_SAMPLE_CONFIG)
        session.commit()

def test_faostat():
    from app.ingest.faostat_client import ingest_full
    with db_session() as session:
        ingest_full(session, sample_config=DEFAULT_SAMPLE_CONFIG)
        session.commit()

def test_adb():
    from app.ingest.adb_client import ingest_full
    with db_session() as session:
        ingest_full(session, sample_config=DEFAULT_SAMPLE_CONFIG)
        session.commit()

def test_ecb():
    from app.ingest.ecb_sdw_client import ingest_full
    with db_session() as session:
        ingest_full(session, sample_config=DEFAULT_SAMPLE_CONFIG)
        session.commit()

def test_ilostat():
    from app.ingest.ilostat_client import ingest_full
    with db_session() as session:
        ingest_full(session, sample_config=DEFAULT_SAMPLE_CONFIG)
        session.commit()

def test_wto():
    from app.ingest.wto_client import ingest_full
    with db_session() as session:
        ingest_full(session, sample_config=DEFAULT_SAMPLE_CONFIG)
        session.commit()


def main():
    print("="*80)
    print("COMPREHENSIVE LIVE API TEST - ALL SOURCES")
    print("Testing with sample mode (max 1 entity per source)")
    print("="*80)
    
    sources = [
        ("FRED", test_fred),
        ("WDI", test_wdi),
        ("IMF", test_imf),
        ("OPENALEX", test_openalex),
        ("YFINANCE", test_yfinance),
        ("STOOQ", test_stooq),
        ("OECD", test_oecd),
        ("COMTRADE", test_comtrade),
        ("RSS", test_rss),
        ("EUROSTAT", test_eurostat),
        ("EIA", test_eia),
        ("EMBER", test_ember),
        ("BIS", test_bis),
        ("GCP", test_gcp),
        ("PATENTSVIEW", test_patentsview),
        ("UNCTAD", test_unctad),
        ("WTO", test_wto),
        ("ONS", test_ons),
        ("FAOSTAT", test_faostat),
        ("ADB", test_adb),
        ("ECB_SDW", test_ecb),
        ("ILOSTAT", test_ilostat),
    ]
    
    results = {}
    working = []
    failing = []
    
    for name, func in sources:
        success, error = test_source(name, func)
        results[name] = {"success": success, "error": error}
        if success:
            working.append(name)
        else:
            failing.append((name, error))
    
    print("\n" + "="*80)
    print("FINAL RESULTS")
    print("="*80)
    print(f"\n✅ WORKING: {len(working)}/{len(sources)} ({100*len(working)/len(sources):.1f}%)")
    for name in working:
        print(f"   • {name}")
    
    print(f"\n❌ FAILING: {len(failing)}/{len(sources)} ({100*len(failing)/len(sources):.1f}%)")
    for name, error in failing:
        print(f"   • {name}: {error}")
    
    print("\n" + "="*80)


if __name__ == "__main__":
    main()
