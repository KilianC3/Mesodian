#!/usr/bin/env python3
"""
Compute all metrics and analytics after data ingestion.
This runs feature assembly, metric computation, cycle detection, and graph operations.

Usage:
    python scripts/compute_all_metrics.py              # Run all computations
    python scripts/compute_all_metrics.py --features   # Features only
    python scripts/compute_all_metrics.py --metrics    # Metrics only
"""

import argparse
import sys
import time
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import func, text
from app.db.engine import db_session
from app.db.models import (
    CountryYearFeatures,
    NodeMetric,
    EdgeMetric,
    WebMetric,
    GlobalCycleIndex,
)


def print_header(title: str):
    """Print a formatted header."""
    print(f"\n{'='*80}")
    print(f"  {title}")
    print(f"{'='*80}\n")


def compute_features():
    """Compute country-year features."""
    print_header("COMPUTING COUNTRY-YEAR FEATURES")
    
    try:
        from app.features import build_country_year_features
        
        with db_session() as session:
            print("Building country-year features...")
            start = time.time()
            
            build_country_year_features.build_all_country_year_features(session)
            
            elapsed = time.time() - start
            count = session.query(func.count(CountryYearFeatures.country_id)).scalar() or 0
            print(f"✓ Complete in {elapsed:.2f}s")
            print(f"  {count:,} country-year feature records created\n")
            
            # Show sample
            features = session.query(CountryYearFeatures).limit(5).all()
            if features:
                print("Sample features:")
                for feat in features:
                    print(f"  {feat.country_id} {feat.year}: GDP={feat.gdp_real}, Growth={feat.gdp_growth}%")
                    
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()


def compute_metrics():
    """Compute country metrics."""
    print_header("COMPUTING COUNTRY METRICS")
    
    try:
        from app.metrics.run_all import compute_all_country_metrics
        
        with db_session() as session:
            print("Computing all country metrics...")
            start = time.time()
            
            compute_all_country_metrics(session)
            
            elapsed = time.time() - start
            count = session.query(func.count(NodeMetric.id)).scalar() or 0
            print(f"✓ Complete in {elapsed:.2f}s")
            print(f"  {count:,} node metric records created\n")
            
            # Show sample metrics
            metrics = session.query(
                NodeMetric.metric_code,
                func.count(NodeMetric.id).label('count')
            ).group_by(NodeMetric.metric_code).all()
            
            if metrics:
                print("Metrics computed:")
                for code, cnt in sorted(metrics, key=lambda x: -x[1])[:10]:
                    print(f"  {code:<30} {cnt:>8,} values")
                    
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()


def compute_cycles():
    """Compute global and regional cycles."""
    print_header("COMPUTING ECONOMIC CYCLES")
    
    try:
        from app.cycles.run_global_cycles import run_global_cycles
        from app.cycles.run_regional_cycles import run_regional_cycles
        
        with db_session() as session:
            print("Computing global cycles...")
            start = time.time()
            run_global_cycles(session)
            elapsed = time.time() - start
            print(f"  ✓ Global cycles in {elapsed:.2f}s")
            
            print("\nComputing regional cycles...")
            start = time.time()
            run_regional_cycles(session)
            elapsed = time.time() - start
            print(f"  ✓ Regional cycles in {elapsed:.2f}s")
            
            count = session.query(func.count(GlobalCycleIndex.id)).scalar() or 0
            print(f"\n  {count:,} cycle index records created\n")
            
            # Show sample
            cycles = session.query(
                GlobalCycleIndex.cycle_type,
                GlobalCycleIndex.frequency,
                func.count(GlobalCycleIndex.id)
            ).group_by(
                GlobalCycleIndex.cycle_type,
                GlobalCycleIndex.frequency
            ).all()
            
            if cycles:
                print("Cycles by type:")
                for cycle_type, freq, cnt in cycles:
                    print(f"  {cycle_type:<25} {freq:<10} {cnt:>8,} records")
                    
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()


def compute_graph_metrics():
    """Compute graph metrics (centrality, roles, etc)."""
    print_header("COMPUTING GRAPH METRICS")
    
    try:
        from app.metrics.run_web_metrics import compute_all_web_and_edge_metrics
        
        with db_session() as session:
            print("Computing web and edge metrics...")
            start = time.time()
            
            compute_all_web_and_edge_metrics(session)
            
            elapsed = time.time() - start
            edge_count = session.query(func.count(EdgeMetric.id)).scalar() or 0
            web_count = session.query(func.count(WebMetric.id)).scalar() or 0
            print(f"✓ Complete in {elapsed:.2f}s")
            print(f"  {web_count:,} web metrics")
            print(f"  {edge_count:,} edge metrics\n")
            
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()


def show_summary():
    """Show summary of all computed analytics."""
    print_header("ANALYTICS SUMMARY")
    
    with db_session() as session:
        data = {
            "Features": session.query(func.count(CountryYearFeatures.country_id)).scalar() or 0,
            "Node Metrics": session.query(func.count(NodeMetric.id)).scalar() or 0,
            "Edge Metrics": session.query(func.count(EdgeMetric.id)).scalar() or 0,
            "Web Metrics": session.query(func.count(WebMetric.id)).scalar() or 0,
            "Cycle Indices": session.query(func.count(GlobalCycleIndex.id)).scalar() or 0,
        }
        
        print("Analytics Data in Database:")
        print("-" * 80)
        for key, count in data.items():
            status = "✓" if count > 0 else "○"
            print(f"  {status} {key:<25} {count:>12,}")
        
        print("\n" + "-" * 80)
        total = sum(data.values())
        print(f"  {'TOTAL':<25} {total:>12,} records")


def main():
    parser = argparse.ArgumentParser(
        description="Compute all metrics and analytics after data ingestion."
    )
    parser.add_argument("--features", action="store_true", help="Compute features only")
    parser.add_argument("--metrics", action="store_true", help="Compute metrics only")
    parser.add_argument("--cycles", action="store_true", help="Compute cycles only")
    parser.add_argument("--graph", action="store_true", help="Compute graph metrics only")
    parser.add_argument("--summary", action="store_true", help="Show summary only")
    
    args = parser.parse_args()

    print_header("ANALYTICS COMPUTATION")
    print(f"Started at: {datetime.now().isoformat()}\n")

    try:
        # Determine what to run
        run_features = not any([args.features, args.metrics, args.cycles, args.graph, args.summary])
        run_metrics = run_features or args.metrics
        run_cycles = run_features or args.cycles
        run_graph = run_features or args.graph
        
        if args.features or run_features:
            compute_features()
        if args.metrics or run_metrics:
            compute_metrics()
        if args.cycles or run_cycles:
            compute_cycles()
        if args.graph or run_graph:
            compute_graph_metrics()
        
        show_summary()
        
        print_header("DONE")
        print("""
Next steps:
  View all data: python scripts/dashboard.py
  Inspect metrics: python scripts/inspect_data.py metrics
  Start API: docker-compose run --rm -p 8000:8000 backend uvicorn app.main:app --reload
""")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
