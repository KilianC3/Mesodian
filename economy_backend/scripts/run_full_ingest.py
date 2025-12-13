#!/usr/bin/env python3
"""
Full data ingestion test - runs all ingestion providers and shows what data was collected.
This will populate the database with real data from all 24+ sources.

Usage:
    python scripts/run_full_ingest.py              # Run all ingestion providers
    python scripts/run_full_ingest.py --provider fred   # Run specific provider
    python scripts/run_full_ingest.py --health-check    # Quick health check all
"""

import argparse
import sys
import time
from pathlib import Path
from datetime import datetime
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import func, text
from app.db.engine import db_session
from app.db.models import (
    Country,
    Indicator,
    TimeSeriesValue,
)
from app.ingest import jobs


def count_raw_data(session, source: str) -> int:
    """Count raw payloads for a source."""
    try:
        count = session.execute(
            text(f"SELECT COUNT(*) FROM raw.raw_{source.lower()}")
        ).scalar() or 0
        return count
    except:
        return 0


def print_header(title: str):
    """Print a formatted header."""
    print(f"\n{'='*80}")
    print(f"  {title}")
    print(f"{'='*80}\n")


def run_full_ingestion():
    """Run full ingestion from all providers and display results."""
    print_header("STARTING FULL DATA INGESTION")
    print(f"Started at: {datetime.now().isoformat()}\n")

    with db_session() as session:
        # Seed countries first
        from app.db.seed_countries import seed_or_refresh_countries
        print("Step 1: Seeding country universe...")
        seed_or_refresh_countries(session)
        num_countries = session.query(func.count(Country.id)).scalar() or 0
        print(f"  ✓ {num_countries} countries loaded\n")

        # Ingest from all providers
        print("Step 2: Ingesting data from all providers...\n")
        
        provider_stats = []
        for provider_name, module in jobs.PROVIDERS:
            print(f"  [{provider_name}]", end=" ", flush=True)
            start = time.time()
            try:
                module.ingest_full(session)
                elapsed = time.time() - start
                count = count_raw_data(session, provider_name)
                status = "✓" if count > 0 else "○"
                print(f"{status} ({count} payloads, {elapsed:.2f}s)")
                provider_stats.append((provider_name, True, count, elapsed))
            except Exception as e:
                elapsed = time.time() - start
                session.rollback()
                print(f"✗ Error: {str(e)[:40]}")
                provider_stats.append((provider_name, False, 0, elapsed))

        print("\nStep 3: Ingestion complete! Collecting statistics...\n")


def print_ingestion_summary():
    """Show summary of all ingested data."""
    with db_session() as session:
        print_header("INGESTION RESULTS")

        # Raw data by source
        print("Raw Ingested Data by Source:")
        print("-" * 80)
        
        raw_tables = session.execute(
            text("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'raw' AND table_name LIKE 'raw_%'
            """)
        ).fetchall()

        total_raw = 0
        for (table_name,) in sorted(raw_tables):
            source = table_name.replace("raw_", "").upper()
            count = session.execute(
                text(f"SELECT COUNT(*) FROM raw.{table_name}")
            ).scalar() or 0
            if count > 0:
                total_raw += count
                print(f"  {source:<20} {count:>8,} payloads")

        print(f"\n  {'TOTAL':<20} {total_raw:>8,} raw records\n")

        # Warehouse data
        print("\nWarehouse (Normalized Data):")
        print("-" * 80)
        
        num_countries = session.query(func.count(Country.id)).scalar() or 0
        num_indicators = session.query(func.count(Indicator.id)).scalar() or 0
        num_timeseries = session.query(func.count(TimeSeriesValue.id)).scalar() or 0

        print(f"  Countries:          {num_countries:>8,}")
        print(f"  Indicators:         {num_indicators:>8,}")
        print(f"  Time Series Values: {num_timeseries:>8,}")

        if num_timeseries > 0:
            date_range = session.query(
                func.min(TimeSeriesValue.date),
                func.max(TimeSeriesValue.date)
            ).first()
            if date_range[0]:
                print(f"\n  Date Range:         {date_range[0]} to {date_range[1]}")

            # Top indicators by observations
            print(f"\n  Top 10 Indicators by Coverage:")
            top_indicators = session.query(
                Indicator.canonical_code,
                func.count(TimeSeriesValue.id).label('count')
            ).join(
                TimeSeriesValue, Indicator.id == TimeSeriesValue.indicator_id
            ).group_by(
                Indicator.id
            ).order_by(
                func.count(TimeSeriesValue.id).desc()
            ).limit(10).all()

            for ind_code, count in top_indicators:
                code = ind_code or "unknown"
                print(f"    {code:<30} {count:>8,} observations")

        # Indicator sources
        print(f"\n  Indicators by Source:")
        source_counts = session.query(
            Indicator.source,
            func.count(Indicator.id)
        ).group_by(
            Indicator.source
        ).order_by(
            func.count(Indicator.id).desc()
        ).all()

        for source, count in source_counts[:15]:
            print(f"    {source:<25} {count:>8,} indicators")


def run_health_check():
    """Quick health check on all providers."""
    print_header("RUNNING HEALTH CHECKS")
    
    with db_session() as session:
        results = jobs.ingest_all_health_check(session)
        
        print("Provider Status:")
        print("-" * 80)
        
        successful = 0
        failed = 0
        for provider in sorted(results.keys()):
            status = results[provider]
            if status["ok"]:
                print(f"  ✓ {provider:<20} OK")
                successful += 1
            else:
                error = status["error"][:50] if status["error"] else "Unknown error"
                print(f"  ✗ {provider:<20} FAILED: {error}")
                failed += 1
        
        print(f"\nResults: {successful} OK, {failed} Failed")


def main():
    parser = argparse.ArgumentParser(
        description="Run full data ingestion from all providers and view results."
    )
    parser.add_argument(
        "--health-check",
        action="store_true",
        help="Run quick health check only"
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Show summary without re-ingesting"
    )
    
    args = parser.parse_args()

    try:
        if args.health_check:
            run_health_check()
        elif args.summary_only:
            print_ingestion_summary()
        else:
            run_full_ingestion()
            print_ingestion_summary()
            
            print_header("WHAT'S NEXT")
            print("""
View detailed data with:
  python scripts/inspect_data.py timeseries --limit 50
  python scripts/inspect_data.py indicators
  python scripts/dashboard.py

Compute metrics and features:
  python scripts/compute_all_metrics.py
  
View everything:
  python scripts/dashboard.py
""")
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
