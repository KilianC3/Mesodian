#!/usr/bin/env python3
"""
Comprehensive data and metrics visualization for Mesodian backend.
Shows all collected data from each source and computed metrics.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import func, text
from app.db.engine import db_session
from app.db.models import (
    Country, Indicator, TimeSeriesValue, RawFred,
    CountryYearFeatures, GlobalCycleIndex, Node, Edge, NodeMetric
)


def print_header(title: str, width: int = 100):
    """Print a formatted header."""
    print(f"\n{'='*width}")
    print(f"  {title}")
    print(f"{'='*width}\n")


def section(title: str, width: int = 100):
    """Print a section header."""
    print(f"\n{'-'*width}")
    print(f"  {title}")
    print(f"{'-'*width}\n")


def show_reference_data():
    """Show all reference data (countries and indicators)."""
    print_header("REFERENCE DATA")
    
    with db_session() as s:
        # Countries
        countries = s.query(Country).all()
        print(f"COUNTRIES: {len(countries)}")
        print("  Country Code | Name | Region | Income Group")
        print("  " + "-" * 80)
        for c in sorted(countries[:20], key=lambda x: x.id):
            print(f"  {c.id:12} | {c.name:20} | {c.region:30} | {c.income_group}")
        if len(countries) > 20:
            print(f"  ... and {len(countries) - 20} more")
        
        # Indicators
        section("INDICATORS")
        indicators = s.query(Indicator).all()
        print(f"TOTAL: {len(indicators)} indicators\n")
        print("  Canonical Code | Source | Source Code | Category | Unit | Frequency")
        print("  " + "-" * 100)
        for ind in sorted(indicators, key=lambda x: x.canonical_code or ""):
            print(f"  {str(ind.canonical_code):14} | {ind.source:10} | {ind.source_code:20} | {ind.category or 'N/A':15} | {ind.unit or 'N/A':15} | {ind.frequency or 'N/A'}")


def show_collected_data():
    """Show all collected time series data."""
    print_header("COLLECTED TIME SERIES DATA")
    
    with db_session() as s:
        # Overall statistics
        total_ts = s.query(func.count(TimeSeriesValue.id)).scalar() or 0
        print(f"TOTAL TIME SERIES VALUES: {total_ts}\n")
        
        if total_ts == 0:
            print("No time series data collected yet.\n")
            return
        
        # By indicator
        section("Top Indicators by Data Points")
        by_indicator = s.execute(text("""
            SELECT ind.canonical_code, COUNT(*) as cnt
            FROM warehouse.time_series_value tsv
            JOIN warehouse.indicator ind ON tsv.indicator_id = ind.id
            GROUP BY ind.canonical_code
            ORDER BY cnt DESC
            LIMIT 20
        """)).fetchall()
        
        print("  Indicator | Count | %")
        print("  " + "-" * 50)
        for canon_code, cnt in by_indicator:
            pct = (cnt / total_ts) * 100
            print(f"  {str(canon_code):25} | {cnt:6} | {pct:5.1f}%")
        
        # By country
        section("Top Countries by Data Points")
        by_country = s.execute(text("""
            SELECT c.id, c.name, COUNT(*) as cnt
            FROM warehouse.time_series_value tsv
            LEFT JOIN warehouse.country c ON tsv.country_id = c.id
            GROUP BY c.id, c.name
            ORDER BY cnt DESC
            LIMIT 20
        """)).fetchall()
        
        print("  Country | Name | Count | %")
        print("  " + "-" * 70)
        for country_id, name, cnt in by_country:
            pct = (cnt / total_ts) * 100
            country_display = country_id or "GLOBAL"
            name_display = name or "N/A"
            print(f"  {country_display:7} | {name_display:20} | {cnt:6} | {pct:5.1f}%")
        
        # By source
        section("Raw Data by Source")
        raw_counts = s.execute(text("""
            SELECT source, COUNT(*) as cnt FROM warehouse.time_series_value
            WHERE source IS NOT NULL
            GROUP BY source
            ORDER BY cnt DESC
        """)).fetchall()
        
        print("  Source | Count | %")
        print("  " + "-" * 50)
        for source, cnt in raw_counts:
            pct = (cnt / total_ts) * 100
            print(f"  {source:20} | {cnt:6} | {pct:5.1f}%")
        
        # Date range
        section("Data Date Range")
        date_info = s.execute(text("""
            SELECT MIN(date) as earliest, MAX(date) as latest
            FROM warehouse.time_series_value
        """)).fetchone()
        
        if date_info[0]:
            print(f"  Earliest: {date_info[0]}")
            print(f"  Latest:   {date_info[1]}")


def show_raw_data():
    """Show raw payloads from each source."""
    print_header("RAW DATA PAYLOADS")
    
    with db_session() as s:
        # Summary
        section("Raw Payload Summary")
        raw_counts = s.execute(text("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'raw' AND table_name LIKE 'raw_%'
            ORDER BY table_name
        """)).fetchall()
        
        total_payloads = 0
        print("  Source | Payload Count")
        print("  " + "-" * 40)
        for (table_name,) in raw_counts:
            try:
                count = s.execute(text(f"SELECT COUNT(*) FROM raw.{table_name}")).scalar() or 0
                if count > 0:
                    source = table_name.replace("raw_", "").upper()
                    print(f"  {source:20} | {count:6}")
                    total_payloads += count
            except:
                pass
        
        print("  " + "-" * 40)
        print(f"  {'TOTAL':20} | {total_payloads:6}")


def show_features_and_metrics():
    """Show computed features and metrics."""
    print_header("FEATURES & METRICS")
    
    with db_session() as s:
        # Country-Year Features
        section("Country-Year Features")
        features_count = s.execute(text("""
            SELECT COUNT(*) FROM warehouse.country_year_features
        """)).scalar() or 0
        print(f"Total: {features_count}\n")
        
        if features_count > 0:
            print("  Country | Year | Feature Count")
            print("  " + "-" * 50)
            by_country_year = s.execute(text("""
                SELECT country_id, year, COUNT(*) as cnt
                FROM warehouse.country_year_features
                GROUP BY country_id, year
                ORDER BY country_id, year DESC
                LIMIT 10
            """)).fetchall()
            
            for country_id, year, cnt in by_country_year:
                print(f"  {country_id:7} | {year:4} | {cnt}")
        
        # Global Cycles
        section("Global Cycle Indices")
        cycles_count = s.query(func.count(GlobalCycleIndex.id)).scalar() or 0
        print(f"Total: {cycles_count}\n")
        
        if cycles_count > 0:
            cycles = s.query(GlobalCycleIndex).order_by(GlobalCycleIndex.date.desc()).limit(5).all()
            for cycle in cycles:
                print(f"  {cycle.date}: {cycle.cycle_name} = {cycle.index_value}")


def show_graph_metrics():
    """Show graph structure and metrics."""
    print_header("GRAPH STRUCTURE & METRICS")
    
    with db_session() as s:
        # Nodes
        section("Nodes")
        nodes_count = s.query(func.count(Node.id)).scalar() or 0
        print(f"Total nodes: {nodes_count}\n")
        
        if nodes_count > 0:
            by_type = s.execute(text("""
                SELECT node_type, COUNT(*) as cnt
                FROM graph.node
                GROUP BY node_type
                ORDER BY cnt DESC
            """)).fetchall()
            
            print("  Type | Count")
            print("  " + "-" * 40)
            for node_type, cnt in by_type:
                print(f"  {str(node_type):20} | {cnt}")
        
        # Edges
        section("Edges")
        edges_count = s.query(func.count(Edge.id)).scalar() or 0
        print(f"Total edges: {edges_count}\n")
        
        if edges_count > 0:
            by_edge_type = s.execute(text("""
                SELECT edge_type, COUNT(*) as cnt
                FROM graph.edge
                GROUP BY edge_type
                ORDER BY cnt DESC
            """)).fetchall()
            
            print("  Type | Count")
            print("  " + "-" * 40)
            for edge_type, cnt in by_edge_type:
                print(f"  {str(edge_type):20} | {cnt}")
        
        # Node Metrics
        section("Node Metrics")
        metrics_count = s.query(func.count(NodeMetric.id)).scalar() or 0
        print(f"Total metrics: {metrics_count}\n")
        
        if metrics_count > 0:
            by_metric = s.execute(text("""
                SELECT metric_name, COUNT(*) as cnt
                FROM graph.node_metric
                GROUP BY metric_name
                ORDER BY cnt DESC
            """)).fetchall()
            
            print("  Metric | Count")
            print("  " + "-" * 40)
            for metric_name, cnt in by_metric:
                print(f"  {metric_name:30} | {cnt}")


def show_summary():
    """Show overall summary statistics."""
    print_header("SUMMARY")
    
    with db_session() as s:
        stats = {
            "Countries": s.query(func.count(Country.id)).scalar() or 0,
            "Indicators": s.query(func.count(Indicator.id)).scalar() or 0,
            "Time Series Values": s.query(func.count(TimeSeriesValue.id)).scalar() or 0,
            "Country-Year Features": s.execute(text("SELECT COUNT(*) FROM warehouse.country_year_features")).scalar() or 0,
            "Global Cycles": s.query(func.count(GlobalCycleIndex.id)).scalar() or 0,
            "Graph Nodes": s.query(func.count(Node.id)).scalar() or 0,
            "Graph Edges": s.query(func.count(Edge.id)).scalar() or 0,
            "Node Metrics": s.query(func.count(NodeMetric.id)).scalar() or 0,
        }
        
        print("ENTITY COUNTS")
        print("-" * 50)
        for entity, count in stats.items():
            status = "✓" if count > 0 else "✗"
            print(f"  {status} {entity:30} {count:>8}")


def main():
    """Run all visualizations."""
    try:
        show_summary()
        show_reference_data()
        show_collected_data()
        show_raw_data()
        show_features_and_metrics()
        show_graph_metrics()
        
        print_header("END OF REPORT")
        print("""
NEXT STEPS:
  1. Review data collection above - ensure we're collecting from right sources
  2. Run metrics computation: python scripts/compute_all_metrics.py
  3. View updated metrics: python scripts/show_all_data.py
  4. Start API: docker-compose run --rm -p 8000:8000 backend uvicorn app.main:app --reload
  5. Query database: psql postgresql://economy:economy@localhost:5433/economy_dev
        """)
    except Exception as exc:
        print(f"\n❌ Failed: {exc}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
