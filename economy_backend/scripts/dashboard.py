#!/usr/bin/env python3
"""
Quick dashboard showing data ingestion and metrics status.
Run with: python scripts/dashboard.py
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import func, text
from app.db.engine import db_session
from app.db.models import (
    Country,
    Indicator,
    TimeSeriesValue,
    CountryYearFeatures,
    TradeFlow,
    Node,
    Edge,
    NodeMetric,
    GlobalCycleIndex,
)


def dashboard():
    """Print a dashboard summary of all data in the system."""
    
    print("\n" + "=" * 80)
    print("MESODIAN ECONOMY BACKEND - DATA DASHBOARD")
    print("=" * 80)
    print(f"Generated: {datetime.now().isoformat()}\n")

    with db_session() as session:
        # Basic counts
        num_countries = session.query(func.count(Country.id)).scalar() or 0
        num_indicators = session.query(func.count(Indicator.id)).scalar() or 0
        num_timeseries = session.query(func.count(TimeSeriesValue.id)).scalar() or 0
        num_features = session.query(func.count(CountryYearFeatures.country_id)).scalar() or 0
        num_trades = session.query(func.count(TradeFlow.id)).scalar() or 0
        
        print("REFERENCE DATA")
        print("-" * 80)
        print(f"  Countries:        {num_countries:>8,}")
        print(f"  Indicators:       {num_indicators:>8,}")
        print()

        print("TIME SERIES DATA")
        print("-" * 80)
        print(f"  Total Observations: {num_timeseries:>8,}")
        
        # Date range
        date_range = session.query(
            func.min(TimeSeriesValue.date),
            func.max(TimeSeriesValue.date)
        ).first()
        if date_range[0]:
            print(f"  Date Range:       {date_range[0]} to {date_range[1]}")
        
        # Recent data
        recent = session.query(func.max(TimeSeriesValue.date)).scalar()
        if recent:
            days_old = (datetime.now().date() - recent).days
            recency = f"{days_old} days ago" if days_old >= 0 else "today"
            print(f"  Most Recent:      {recent} ({recency})")
        
        # Top sources
        print(f"\n  Top Indicator Sources:")
        sources = session.query(Indicator.source, func.count(Indicator.id)).group_by(Indicator.source).order_by(func.count(Indicator.id).desc()).limit(10).all()
        for source, count in sources:
            print(f"    {source:<20} {count:>6,} indicators")
        print()

        print("FEATURES & METRICS")
        print("-" * 80)
        print(f"  Country-Year Features: {num_features:>8,}")
        
        # Feature date range
        feat_range = session.query(
            func.min(CountryYearFeatures.year),
            func.max(CountryYearFeatures.year)
        ).first()
        if feat_range[0]:
            print(f"  Year Range:          {int(feat_range[0])} - {int(feat_range[1])}")
        
        # Metrics
        num_node_metrics = session.query(func.count(NodeMetric.id)).scalar() or 0
        num_cycles = session.query(func.count(GlobalCycleIndex.id)).scalar() or 0
        print(f"  Node Metrics:        {num_node_metrics:>8,}")
        print(f"  Global Cycles:       {num_cycles:>8,}")
        
        # Metric types
        if num_node_metrics > 0:
            print(f"\n  Node Metric Types:")
            metric_types = session.query(NodeMetric.metric_code, func.count(NodeMetric.id)).group_by(NodeMetric.metric_code).order_by(func.count(NodeMetric.id).desc()).limit(10).all()
            for mtype, count in metric_types:
                print(f"    {mtype:<25} {count:>8,} values")
        print()

        print("GRAPH STRUCTURE")
        print("-" * 80)
        num_nodes = session.query(func.count(Node.id)).scalar() or 0
        num_edges = session.query(func.count(Edge.id)).scalar() or 0
        print(f"  Nodes:                {num_nodes:>8,}")
        print(f"  Edges:                {num_edges:>8,}")
        
        if num_nodes > 0:
            print(f"\n  Node Types:")
            node_types = session.query(Node.node_type, func.count(Node.id)).group_by(Node.node_type).order_by(func.count(Node.id).desc()).all()
            for ntype, count in node_types:
                print(f"    {str(ntype):<25} {count:>8,}")
        
        if num_edges > 0:
            print(f"\n  Edge Types:")
            edge_types = session.query(Edge.edge_type, func.count(Edge.id)).group_by(Edge.edge_type).order_by(func.count(Edge.id).desc()).all()
            for etype, count in edge_types:
                print(f"    {str(etype):<25} {count:>8,}")
        print()

        print("TRADE DATA")
        print("-" * 80)
        print(f"  Trade Flows:          {num_trades:>8,}")
        
        if num_trades > 0:
            trade_range = session.query(
                func.min(TradeFlow.year),
                func.max(TradeFlow.year)
            ).first()
            if trade_range[0]:
                print(f"  Year Range:           {int(trade_range[0])} - {int(trade_range[1])}")
            
            total_value = session.query(func.sum(TradeFlow.value_usd)).scalar() or 0
            print(f"  Total Value (USD):    ${total_value/1e12:>8.2f}T")
        print()

        print("RAW DATA STORAGE")
        print("-" * 80)
        
        # Get sizes of raw tables
        try:
            raw_tables = session.execute(
                text("""
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_schema = 'raw'
                """)
            ).fetchall()
            
            if raw_tables:
                print(f"  Ingestion Sources:")
                for (table_name,) in raw_tables:
                    count = session.execute(
                        text(f"SELECT COUNT(*) FROM raw.{table_name}")
                    ).scalar() or 0
                    if count > 0:
                        source = table_name.replace("raw_", "").upper()
                        print(f"    {source:<20} {count:>8,} payloads")
        except:
            print("  (Unable to query raw data tables)")
        
    print("=" * 80)
    print("\nFor detailed inspection, use: python scripts/inspect_data.py --help\n")


if __name__ == "__main__":
    try:
        dashboard()
    except Exception as e:
        print(f"\nError: {e}")
        print("\nEnsure DATABASE_URL is set and backend is running.")
        sys.exit(1)
