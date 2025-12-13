#!/usr/bin/env python3
"""
Data inspection utilities for Mesodian economy backend.
View ingested data, features, metrics, and graph structures across all sources.

Usage:
    python scripts/inspect_data.py --help
    python scripts/inspect_data.py countries
    python scripts/inspect_data.py timeseries --indicator GDP_REAL --country USA
    python scripts/inspect_data.py features --country USA --year 2023
    python scripts/inspect_data.py metrics --metric CR_RESILIENCE --year 2023
    python scripts/inspect_data.py graph --web TRADE
    python scripts/inspect_data.py raw --source fred --limit 10
"""

import argparse
import sys
from pathlib import Path
from typing import Optional

# Add parent to path so we can import app
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import func, select, text
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
    EdgeMetric,
    WebMetric,
)


def list_countries() -> None:
    """List all countries with their regions and income groups."""
    with db_session() as session:
        countries = session.query(Country).order_by(Country.id).all()
        if not countries:
            print("No countries found in database.")
            return

        print(f"\n{'ID':<5} {'Name':<30} {'Region':<25} {'Income Group':<20}")
        print("-" * 80)
        for c in countries:
            print(f"{c.id:<5} {c.name:<30} {c.region or 'N/A':<25} {c.income_group or 'N/A':<20}")
        print(f"\nTotal: {len(countries)} countries")


def list_indicators() -> None:
    """List all indicators with their sources and metadata."""
    with db_session() as session:
        indicators = session.query(Indicator).order_by(Indicator.source, Indicator.source_code).all()
        if not indicators:
            print("No indicators found in database.")
            return

        print(f"\n{'ID':<5} {'Source':<15} {'Code':<25} {'Canonical':<20} {'Frequency':<10} {'Unit':<15}")
        print("-" * 90)
        for ind in indicators[:50]:  # Show first 50
            canonical = ind.canonical_code or "—"
            unit = ind.unit or "—"
            print(f"{ind.id:<5} {ind.source:<15} {ind.source_code:<25} {canonical:<20} {ind.frequency or '—':<10} {unit:<15}")
        if len(indicators) > 50:
            print(f"\n... and {len(indicators) - 50} more indicators")
        print(f"\nTotal: {len(indicators)} indicators")


def timeseries_data(indicator: Optional[str] = None, country: Optional[str] = None, limit: int = 100) -> None:
    """Query time series data, optionally filtered by indicator and country."""
    with db_session() as session:
        query = session.query(TimeSeriesValue, Indicator, Country).join(
            Indicator, TimeSeriesValue.indicator_id == Indicator.id
        ).outerjoin(Country, TimeSeriesValue.country_id == Country.id)

        if indicator:
            query = query.filter(Indicator.canonical_code == indicator)
        if country:
            query = query.filter(TimeSeriesValue.country_id == country)

        results = query.order_by(TimeSeriesValue.date.desc()).limit(limit).all()

        if not results:
            print(f"No time series data found" + 
                  (f" for indicator {indicator}" if indicator else "") +
                  (f" and country {country}" if country else ""))
            return

        print(f"\n{'Date':<12} {'Country':<10} {'Indicator':<30} {'Value':<15} {'Source':<10}")
        print("-" * 80)
        for ts, ind, ctry in results:
            ctry_name = ctry.id if ctry else "Global"
            ind_code = ind.canonical_code or ind.source_code
            print(f"{str(ts.date):<12} {ctry_name:<10} {ind_code:<30} {ts.value:<15.4f} {ts.source or '—':<10}")
        print(f"\nShowing {len(results)} records")


def country_features(country: str, year: Optional[int] = None) -> None:
    """Show country-year features for a specific country."""
    with db_session() as session:
        query = session.query(CountryYearFeatures).filter(CountryYearFeatures.country_id == country)
        if year:
            query = query.filter(CountryYearFeatures.year == year)

        features = query.order_by(CountryYearFeatures.year.desc()).all()

        if not features:
            print(f"No features found for country {country}" + (f" year {year}" if year else ""))
            return

        print(f"\nCountry: {country}")
        print("=" * 100)

        for feat in features:
            print(f"\nYear: {feat.year}")
            print("-" * 50)
            attrs = {
                "GDP (Real)": feat.gdp_real,
                "GDP Growth (%)": feat.gdp_growth,
                "Inflation (CPI %)": feat.inflation_cpi,
                "Current Account (% GDP)": feat.ca_pct_gdp,
                "Debt (% GDP)": feat.debt_pct_gdp,
                "Unemployment (%)": feat.unemployment_rate,
                "CO2 per Capita": feat.co2_per_capita,
                "Energy Import Dep.": feat.energy_import_dep,
                "Food Import Dep.": feat.food_import_dep,
                "Shipping Activity": feat.shipping_activity_level,
                "Shipping Change (%)": feat.shipping_activity_change,
                "Event Stress Pulse": feat.event_stress_pulse,
                "Data Coverage Score": feat.data_coverage_score,
                "Data Freshness Score": feat.data_freshness_score,
            }
            for key, val in attrs.items():
                if val is not None:
                    if isinstance(val, float):
                        print(f"  {key:<30} {val:>12.4f}")
                    else:
                        print(f"  {key:<30} {val:>12}")


def country_metrics(country: Optional[str] = None, metric_code: Optional[str] = None, year: Optional[int] = None) -> None:
    """List computed metrics for countries and nodes."""
    with db_session() as session:
        query = session.query(NodeMetric, Node).join(Node, NodeMetric.node_id == Node.id)

        if country:
            query = query.filter(Node.country_code == country)
        if metric_code:
            query = query.filter(NodeMetric.metric_code == metric_code)
        if year:
            query = query.filter(NodeMetric.as_of_year == year)

        results = query.order_by(NodeMetric.as_of_year.desc(), NodeMetric.metric_code).limit(200).all()

        if not results:
            print("No metrics found" + 
                  (f" for country {country}" if country else "") +
                  (f" and metric {metric_code}" if metric_code else ""))
            return

        print(f"\n{'Year':<6} {'Country':<10} {'Metric':<30} {'Value':<15} {'Node':<20}")
        print("-" * 85)
        for metric, node in results:
            node_name = f"{node.name}" if node.name else node.ref_id or "—"
            print(f"{metric.as_of_year:<6} {node.country_code or '—':<10} {metric.metric_code:<30} {metric.value:<15.4f} {node_name:<20}")
        print(f"\nShowing {len(results)} metrics")


def trade_flows(reporter: Optional[str] = None, partner: Optional[str] = None, year: Optional[int] = None, limit: int = 50) -> None:
    """Show trade flows between countries."""
    with db_session() as session:
        query = session.query(TradeFlow).order_by(TradeFlow.year.desc(), TradeFlow.value_usd.desc())

        if reporter:
            query = query.filter(TradeFlow.reporter_country_id == reporter)
        if partner:
            query = query.filter(TradeFlow.partner_country_id == partner)
        if year:
            query = query.filter(TradeFlow.year == year)

        results = query.limit(limit).all()

        if not results:
            print("No trade flows found" + 
                  (f" from {reporter}" if reporter else "") +
                  (f" to {partner}" if partner else ""))
            return

        print(f"\n{'Year':<6} {'Reporter':<10} {'Partner':<10} {'Type':<10} {'Section':<10} {'Value (USD B)':<15}")
        print("-" * 75)
        for flow in results:
            value_b = flow.value_usd / 1e9 if flow.value_usd else 0
            print(f"{flow.year:<6} {flow.reporter_country_id:<10} {flow.partner_country_id:<10} {flow.flow_type or '—':<10} {flow.hs_section or '—':<10} {value_b:<15.2f}")
        print(f"\nShowing {len(results)} trade flows")


def graph_overview() -> None:
    """Show overview of graph structure."""
    with db_session() as session:
        num_nodes = session.query(func.count(Node.id)).scalar() or 0
        num_edges = session.query(func.count(Edge.id)).scalar() or 0
        num_node_metrics = session.query(func.count(NodeMetric.id)).scalar() or 0
        num_edge_metrics = session.query(func.count(EdgeMetric.id)).scalar() or 0
        num_web_metrics = session.query(func.count(WebMetric.id)).scalar() or 0

        print("\n" + "=" * 60)
        print("GRAPH STRUCTURE OVERVIEW")
        print("=" * 60)
        print(f"Nodes:              {num_nodes:>10,}")
        print(f"Edges:              {num_edges:>10,}")
        print(f"Node Metrics:       {num_node_metrics:>10,}")
        print(f"Edge Metrics:       {num_edge_metrics:>10,}")
        print(f"Web Metrics:        {num_web_metrics:>10,}")
        print("=" * 60)

        # Sample nodes by type
        print("\nNodes by Type:")
        with db_session() as s:
            types = s.query(Node.node_type, func.count(Node.id)).group_by(Node.node_type).all()
            for node_type, count in types:
                print(f"  {str(node_type):<20} {count:>6,}")

        # Sample edges by type
        print("\nEdges by Type:")
        with db_session() as s:
            edge_types = s.query(Edge.edge_type, func.count(Edge.id)).group_by(Edge.edge_type).all()
            for edge_type, count in edge_types:
                print(f"  {str(edge_type):<20} {count:>6,}")


def raw_data(source: str, limit: int = 20) -> None:
    """View raw ingested payload data."""
    table_name = f"raw_{source}"
    with db_session() as session:
        try:
            result = session.execute(
                text(f"""
                    SELECT id, fetched_at, params, payload 
                    FROM raw.{table_name} 
                    ORDER BY fetched_at DESC 
                    LIMIT :limit
                """),
                {"limit": limit}
            )
            rows = result.fetchall()
            
            if not rows:
                print(f"No data found in raw.{table_name}")
                return

            print(f"\nRaw data from {source}:")
            print("=" * 100)
            for i, row in enumerate(rows, 1):
                print(f"\n{i}. ID: {row[0]}, Fetched: {row[1]}")
                print(f"   Params: {row[2]}")
                print(f"   Payload keys: {list((row[3] or {}).keys())}")
                
        except Exception as e:
            print(f"Error querying raw.{table_name}: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Inspect Mesodian database: view ingested data, features, and metrics."
    )
    subparsers = parser.add_subparsers(dest="command", help="Data to inspect")

    # Countries
    subparsers.add_parser("countries", help="List all countries")

    # Indicators
    subparsers.add_parser("indicators", help="List all indicators")

    # Time series
    ts_parser = subparsers.add_parser("timeseries", help="View time series data")
    ts_parser.add_argument("--indicator", help="Filter by indicator code (e.g., GDP_REAL)")
    ts_parser.add_argument("--country", help="Filter by country code (e.g., USA)")
    ts_parser.add_argument("--limit", type=int, default=100, help="Max rows to show")

    # Features
    feat_parser = subparsers.add_parser("features", help="View country-year features")
    feat_parser.add_argument("--country", required=True, help="Country code")
    feat_parser.add_argument("--year", type=int, help="Filter by year")

    # Metrics
    met_parser = subparsers.add_parser("metrics", help="View computed metrics")
    met_parser.add_argument("--country", help="Filter by country code")
    met_parser.add_argument("--metric", help="Filter by metric code")
    met_parser.add_argument("--year", type=int, help="Filter by year")

    # Trade flows
    trade_parser = subparsers.add_parser("trade", help="View trade flows")
    trade_parser.add_argument("--from", dest="reporter", help="Reporter country")
    trade_parser.add_argument("--to", dest="partner", help="Partner country")
    trade_parser.add_argument("--year", type=int, help="Filter by year")
    trade_parser.add_argument("--limit", type=int, default=50)

    # Graph
    subparsers.add_parser("graph", help="Graph structure overview")

    # Raw data
    raw_parser = subparsers.add_parser("raw", help="View raw ingested data")
    raw_parser.add_argument("--source", required=True, help="Source name (e.g., fred, comtrade, eia)")
    raw_parser.add_argument("--limit", type=int, default=20)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    if args.command == "countries":
        list_countries()
    elif args.command == "indicators":
        list_indicators()
    elif args.command == "timeseries":
        timeseries_data(args.indicator, args.country, args.limit)
    elif args.command == "features":
        country_features(args.country, args.year)
    elif args.command == "metrics":
        country_metrics(args.country, args.metric, args.year)
    elif args.command == "trade":
        trade_flows(args.reporter, args.partner, args.year, args.limit)
    elif args.command == "graph":
        graph_overview()
    elif args.command == "raw":
        raw_data(args.source, args.limit)


if __name__ == "__main__":
    main()
