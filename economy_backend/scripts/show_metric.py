#!/usr/bin/env python3
"""
Display data for a specific metric code across all tables.

Usage:
    export DATABASE_URL=postgresql://...
    python scripts/show_metric.py GBC
    python scripts/show_metric.py COM_ENERGY
"""

import os
import sys
from typing import List, Tuple

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


def find_tables_with_metric_code(engine: Engine) -> List[Tuple[str, str, List[str]]]:
    """
    Find all tables that have a metric_code column.
    
    Returns:
        List of (schema, table, column_names) tuples.
    """
    inspector = inspect(engine)
    
    # Get all schemas, excluding system schemas
    all_schemas = inspector.get_schema_names()
    schemas = [s for s in all_schemas if s not in ("information_schema", "pg_catalog")]
    
    tables_with_metric_code = []
    
    for schema in schemas:
        tables = inspector.get_table_names(schema=schema)
        
        for table in tables:
            # Get column names for this table
            columns = inspector.get_columns(table, schema=schema)
            column_names = [col["name"] for col in columns]
            
            # Check if this table has a metric_code column
            if "metric_code" in column_names:
                tables_with_metric_code.append((schema, table, column_names))
    
    return tables_with_metric_code


def determine_order_by(column_names: List[str]) -> str:
    """Determine the best ORDER BY clause based on available columns."""
    time_columns = ["year", "date", "datetime", "timestamp", "period", "time"]
    
    for col in time_columns:
        if col in column_names:
            return col
    
    # Default to first column if no time dimension found
    return column_names[0] if column_names else "*"


def show_metric(metric_code: str, limit: int = 10) -> None:
    """Show all rows for a specific metric code across all tables."""
    url = get_database_url()
    engine = create_engine(url, pool_pre_ping=True)
    
    try:
        print("=" * 80)
        print(f"METRIC CODE: {metric_code}")
        print("=" * 80)
        print()
        
        tables = find_tables_with_metric_code(engine)
        
        if not tables:
            print("No tables with 'metric_code' column found.")
            return
        
        found_any = False
        
        for schema, table, column_names in tables:
            # Determine order by clause
            order_by = determine_order_by(column_names)
            
            # Query rows for this metric code
            query = text(f'''
                SELECT * FROM "{schema}"."{table}"
                WHERE metric_code = :code
                ORDER BY "{order_by}"
                LIMIT :limit
            ''')
            
            with engine.connect() as conn:
                result = conn.execute(query, {"code": metric_code, "limit": limit})
                rows = result.fetchall()
                
                if rows:
                    found_any = True
                    print(f"\nTable: {schema}.{table}")
                    print("-" * 80)
                    print(f"Found {len(rows)} rows (showing up to {limit})")
                    print()
                    
                    # Try to use pandas for nicer formatting
                    try:
                        import pandas as pd
                        df = pd.DataFrame(rows, columns=result.keys())
                        print(df.to_string(index=False))
                    except ImportError:
                        # Fall back to printing dicts
                        print(f"Columns: {', '.join(result.keys())}")
                        for i, row in enumerate(rows, 1):
                            print(f"\nRow {i}:")
                            for key, value in row._mapping.items():
                                print(f"  {key}: {value}")
                    print()
        
        if not found_any:
            print(f"No rows found for metric_code='{metric_code}'")
            print()
            print("Try running 'python scripts/list_metrics.py' to see all available metric codes.")
        
        print("=" * 80)
        
    finally:
        engine.dispose()


def main() -> None:
    """Main entry point."""
    if len(sys.argv) != 2:
        print("Usage: python scripts/show_metric.py <METRIC_CODE>", file=sys.stderr)
        print("Example: python scripts/show_metric.py GBC", file=sys.stderr)
        sys.exit(1)
    
    metric_code = sys.argv[1]
    
    try:
        show_metric(metric_code)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
