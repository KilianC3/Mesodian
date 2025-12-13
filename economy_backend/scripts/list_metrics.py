#!/usr/bin/env python3
"""
List all metric_code values currently stored in the database.

Usage:
    export DATABASE_URL=postgresql://...
    python scripts/list_metrics.py
"""

import os
import sys
from collections import defaultdict
from typing import Dict, List, Set

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine


def get_database_url() -> str:
    """Get database URL from environment variables."""
    url = os.environ.get("DATABASE_URL") or os.environ.get("POSTGRES_URL")
    if not url:
        raise RuntimeError(
            "Database URL not found. Please set DATABASE_URL or POSTGRES_URL environment variable.\n"
            "Example: export DATABASE_URL=postgresql://user:pass@localhost:5432/dbname"
        )
    return url


def find_metric_codes(engine: Engine) -> Dict[str, List[str]]:
    """
    Find all distinct metric_code values across all tables.
    
    Returns:
        Dictionary mapping "schema.table" to list of metric codes.
    """
    inspector = inspect(engine)
    
    # Get all schemas, excluding system schemas
    all_schemas = inspector.get_schema_names()
    schemas = [s for s in all_schemas if s not in ("information_schema", "pg_catalog")]
    
    metric_codes_by_table: Dict[str, List[str]] = {}
    
    for schema in schemas:
        tables = inspector.get_table_names(schema=schema)
        
        for table in tables:
            # Get column names for this table
            columns = inspector.get_columns(table, schema=schema)
            column_names = [col["name"] for col in columns]
            
            # Check if this table has a metric_code column
            if "metric_code" in column_names:
                # Query distinct metric codes
                query = text(f'SELECT DISTINCT metric_code FROM "{schema}"."{table}" ORDER BY metric_code')
                
                with engine.connect() as conn:
                    result = conn.execute(query)
                    codes = [row[0] for row in result if row[0] is not None]
                    
                    if codes:
                        full_table_name = f"{schema}.{table}"
                        metric_codes_by_table[full_table_name] = codes
    
    return metric_codes_by_table


def list_metrics() -> None:
    """List all metric codes in the database and generate a Python dict stub."""
    url = get_database_url()
    engine = create_engine(url, pool_pre_ping=True)
    
    try:
        print("=" * 80)
        print("METRIC CODES REPORT")
        print("=" * 80)
        print()
        
        metric_codes_by_table = find_metric_codes(engine)
        
        if not metric_codes_by_table:
            print("No tables with 'metric_code' column found.")
            return
        
        # Section 1: Metric codes by table
        print("METRIC CODES BY TABLE")
        print("-" * 80)
        for table, codes in sorted(metric_codes_by_table.items()):
            print(f"\n{table}:")
            for code in codes:
                print(f"  - {code}")
        
        # Section 2: Consolidated list
        all_codes: Set[str] = set()
        for codes in metric_codes_by_table.values():
            all_codes.update(codes)
        
        print("\n")
        print("=" * 80)
        print("CONSOLIDATED METRIC CODE LIST")
        print("=" * 80)
        print()
        for code in sorted(all_codes):
            print(f"  {code}")
        
        # Section 3: Python dict stub
        print("\n")
        print("=" * 80)
        print("METRIC_DESCRIPTIONS DICT STUB")
        print("=" * 80)
        print()
        print("# Copy this dict to app/metrics/metric_descriptions.py")
        print()
        print("METRIC_DESCRIPTIONS = {")
        for code in sorted(all_codes):
            print(f'    "{code}": "TODO: one line description",')
        print("}")
        print()
        
        print("=" * 80)
        print(f"Found {len(all_codes)} unique metric codes across {len(metric_codes_by_table)} tables")
        print("=" * 80)
        
    finally:
        engine.dispose()


def main() -> None:
    """Main entry point."""
    try:
        list_metrics()
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
