#!/usr/bin/env python3
"""Test each data source individually to see which ones actually work."""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from sqlalchemy import func
from app.db.engine import db_session
from app.db.models import TimeSeriesValue

print("=" * 80)
print("TESTING ACTUAL DATA IN DATABASE")
print("=" * 80)

with db_session() as session:
    # Get counts by source
    sources = session.query(
        TimeSeriesValue.source,
        func.count(TimeSeriesValue.id).label('count'),
        func.min(TimeSeriesValue.date).label('min_date'),
        func.max(TimeSeriesValue.date).label('max_date'),
        func.count(func.distinct(TimeSeriesValue.country_id)).label('countries')
    ).group_by(TimeSeriesValue.source).order_by(func.count(TimeSeriesValue.id).desc()).all()
    
    total_values = sum(s.count for s in sources)
    
    print(f"\n📊 TOTAL DATA POINTS: {total_values:,}\n")
    print(f"{'SOURCE':<20} {'VALUES':>8} {'%':>6} {'COUNTRIES':>10} {'DATE RANGE':<30}")
    print("-" * 80)
    
    for source in sources:
        pct = (source.count / total_values) * 100
        date_range = f"{source.min_date} to {source.max_date}"
        print(f"{source.source:<20} {source.count:>8,} {pct:>5.1f}% {source.countries:>10} {date_range:<30}")
    
    # Sample actual values
    print("\n" + "=" * 80)
    print("SAMPLE ACTUAL DATA POINTS")
    print("=" * 80)
    
    for source in sources[:3]:  # Top 3 sources
        print(f"\n{source.source}:")
        samples = session.query(TimeSeriesValue).filter(
            TimeSeriesValue.source == source.source
        ).limit(3).all()
        
        for ts in samples:
            print(f"  {ts.date} | {ts.country_id} | Value: {ts.value}")
