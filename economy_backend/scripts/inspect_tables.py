#!/usr/bin/env python3
"""
Quick database introspection for Mesodian.

Usage:
    export DATABASE_URL=postgresql://...
    python scripts/inspect_tables.py
"""

import os
import sys
from typing import Optional

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


def print_sample_rows(engine: Engine, schema: str, table: str, limit: int = 5) -> None:
    """Print sample rows from a table."""
    query = text(f'SELECT * FROM "{schema}"."{table}" LIMIT :limit')
    
    with engine.connect() as conn:
        result = conn.execute(query, {"limit": limit})
        rows = result.fetchall()
        
        if not rows:
            return
        
        # Try to use pandas for nicer formatting
        try:
            import pandas as pd
            df = pd.DataFrame(rows, columns=result.keys())
            print(df.to_string(index=False))
        except ImportError:
            # Fall back to printing dicts
            print(f"  Columns: {', '.join(result.keys())}")
            for i, row in enumerate(rows, 1):
                print(f"  Row {i}: {dict(row._mapping)}")


def inspect_database() -> None:
    """Inspect all tables in the database and print schema information."""
    url = get_database_url()
    engine = create_engine(url, pool_pre_ping=True)
    
    try:
        inspector = inspect(engine)
        
        # Get all schemas, excluding system schemas
        all_schemas = inspector.get_schema_names()
        schemas = [s for s in all_schemas if s not in ("information_schema", "pg_catalog")]
        
        if not schemas:
            print("No user schemas found in database.")
            return
        
        print("=" * 80)
        print("DATABASE INSPECTION REPORT")
        print("=" * 80)
        print()
        
        for schema in sorted(schemas):
            tables = inspector.get_table_names(schema=schema)
            
            if not tables:
                continue
            
            print(f"\nSchema: {schema}")
            print("-" * 80)
            
            for table in sorted(tables):
                print(f"\nTable: {schema}.{table}")
                
                # Get row count
                with engine.connect() as conn:
                    count_query = text(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
                    count = conn.execute(count_query).scalar()
                    print(f"  Rows: {count:,}")
                
                # Print sample rows if table has data
                if count > 0:
                    print(f"  Sample rows (up to 5):")
                    print_sample_rows(engine, schema, table, limit=5)
                    print()
        
        print("=" * 80)
        print("INSPECTION COMPLETE")
        print("=" * 80)
        
    finally:
        engine.dispose()


def main() -> None:
    """Main entry point."""
    try:
        inspect_database()
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
