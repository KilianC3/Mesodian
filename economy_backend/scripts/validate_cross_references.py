#!/usr/bin/env python3
"""
Cross-Reference Validation Script

ONLY compares indicators that are THE EXACT SAME THING from different sources.
Same units, same methodology - directly comparable.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import argparse
from datetime import datetime
from typing import Dict, List

import pandas as pd
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.db.engine import get_db
from app.db.models import TimeSeriesValue, Indicator


# ONLY include indicators that are THE EXACT SAME measurement from different sources
CROSS_REFERENCE_MAPPING = {
    "Oil_WTI": {
        "description": "WTI Crude Oil Price (USD/barrel)",
        "indicators": [
            "OIL_WTI_FRED",     # FRED - WTI
            "EIA_WTI_PRICE",    # EIA - WTI
        ],
    },
    "Oil_Brent": {
        "description": "Brent Crude Oil Price (USD/barrel)",
        "indicators": [
            "OIL_BRENT_FRED",   # FRED - Brent
            "BRENT_OIL_SPOT_PRICE",  # EIA - Brent
        ],
    },
    "USD_EUR_FX": {
        "description": "USD/EUR Exchange Rate",
        "indicators": [
            "FX_USD_EUR_FRED",  # FRED
            "FX_USD_EUR",       # ECB_SDW
        ],
    },
    "UK_CPI_Index": {
        "description": "UK Consumer Price Index",
        "indicators": [
            "CPI_GBR_FRED",     # FRED - UK CPI
            "CPIH_UK",          # ONS - UK CPI
        ],
    },
}


def get_indicator_data(session: Session, canonical_code: str) -> pd.DataFrame:
    """Fetch all time series data for a given indicator."""
    indicator = session.query(Indicator).filter(
        Indicator.canonical_code == canonical_code
    ).first()
    
    if not indicator:
        return pd.DataFrame()
    
    data = session.query(
        TimeSeriesValue.country_id,
        TimeSeriesValue.date,
        TimeSeriesValue.value,
        TimeSeriesValue.source
    ).filter(
        TimeSeriesValue.indicator_id == indicator.id
    ).all()
    
    if not data:
        return pd.DataFrame()
    
    df = pd.DataFrame(data, columns=["country_id", "date", "value", "source"])
    df["indicator"] = canonical_code
    return df


def validate_cross_references(session: Session, threshold: float = 0.05, min_common_dates: int = 3) -> Dict:
    """Validate cross-referenced indicators."""
    results = {
        "timestamp": datetime.now().isoformat(),
        "threshold": threshold,
        "min_common_dates": min_common_dates,
        "categories": {},
        "summary": {
            "total_categories": 0,
            "total_comparisons": 0,
            "passed_comparisons": 0,
            "failed_comparisons": 0,
        }
    }
    
    for category, config in CROSS_REFERENCE_MAPPING.items():
        print(f"\n{'='*80}")
        print(f"Category: {category} - {config['description']}")
        print(f"{'='*80}")
        
        category_results = {
            "description": config["description"],
            "indicators": config["indicators"],
            "comparisons": [],
            "summary": {
                "indicators_with_data": 0,
                "comparisons_made": 0,
                "passed": 0,
                "failed": 0,
            }
        }
        
        # Fetch data for all indicators in this category
        indicator_data = {}
        for indicator_code in config["indicators"]:
            df = get_indicator_data(session, indicator_code)
            if not df.empty:
                indicator_data[indicator_code] = df
                category_results["summary"]["indicators_with_data"] += 1
                print(f"  ✓ {indicator_code}: {len(df)} records")
            else:
                print(f"  ✗ {indicator_code}: No data")
        
        # Compare each pair of indicators
        indicator_codes = list(indicator_data.keys())
        for i in range(len(indicator_codes)):
            for j in range(i + 1, len(indicator_codes)):
                ind1_code = indicator_codes[i]
                ind2_code = indicator_codes[j]
                
                df1 = indicator_data[ind1_code]
                df2 = indicator_data[ind2_code]
                
                # Find common countries
                common_countries = set(df1["country_id"]) & set(df2["country_id"])
                
                if not common_countries:
                    continue
                
                for country in common_countries:
                    df1_country = df1[df1["country_id"] == country].copy()
                    df2_country = df2[df2["country_id"] == country].copy()
                    
                    # Merge on date
                    merged = pd.merge(
                        df1_country,
                        df2_country,
                        on=["date", "country_id"],
                        suffixes=("_1", "_2")
                    )
                    
                    if len(merged) < min_common_dates:
                        continue
                    
                    # Calculate divergence
                    merged["abs_diff"] = abs(merged["value_1"] - merged["value_2"])
                    merged["pct_diff"] = abs((merged["value_1"] - merged["value_2"]) / merged["value_1"]) * 100
                    
                    avg_divergence = merged["pct_diff"].mean()
                    max_divergence = merged["pct_diff"].max()
                    correlation = merged["value_1"].corr(merged["value_2"])
                    
                    comparison = {
                        "indicator1": ind1_code,
                        "indicator2": ind2_code,
                        "country": country,
                        "common_dates": len(merged),
                        "avg_divergence_pct": round(avg_divergence, 2),
                        "max_divergence_pct": round(max_divergence, 2),
                        "correlation": round(correlation, 4) if pd.notna(correlation) else None,
                        "passed": avg_divergence <= (threshold * 100),
                    }
                    
                    category_results["comparisons"].append(comparison)
                    category_results["summary"]["comparisons_made"] += 1
                    results["summary"]["total_comparisons"] += 1
                    
                    if comparison["passed"]:
                        category_results["summary"]["passed"] += 1
                        results["summary"]["passed_comparisons"] += 1
                        status = "✓ PASS"
                    else:
                        category_results["summary"]["failed"] += 1
                        results["summary"]["failed_comparisons"] += 1
                        status = "✗ FAIL"
                    
                    print(f"\n  {status} {country}: {ind1_code} vs {ind2_code}")
                    print(f"    Common dates: {len(merged)}")
                    print(f"    Avg divergence: {avg_divergence:.2f}%")
                    print(f"    Max divergence: {max_divergence:.2f}%")
                    print(f"    Correlation: {correlation:.4f}" if pd.notna(correlation) else "    Correlation: N/A")
        
        results["categories"][category] = category_results
        results["summary"]["total_categories"] += 1
        
        # Print category summary
        print(f"\n  Category Summary:")
        print(f"    Indicators with data: {category_results['summary']['indicators_with_data']}/{len(config['indicators'])}")
        print(f"    Comparisons made: {category_results['summary']['comparisons_made']}")
        print(f"    Passed: {category_results['summary']['passed']}")
        print(f"    Failed: {category_results['summary']['failed']}")
    
    return results


def print_summary_report(results: Dict):
    """Print a summary report of validation results."""
    print(f"\n\n{'='*80}")
    print("CROSS-REFERENCE VALIDATION SUMMARY")
    print(f"{'='*80}")
    print(f"Timestamp: {results['timestamp']}")
    print(f"Divergence Threshold: {results['threshold']*100}%")
    print(f"\nTotal Categories: {results['summary']['total_categories']}")
    print(f"Total Comparisons: {results['summary']['total_comparisons']}")
    if results['summary']['total_comparisons'] > 0:
        pass_rate = results['summary']['passed_comparisons']/results['summary']['total_comparisons']*100
        print(f"  Passed: {results['summary']['passed_comparisons']} ({pass_rate:.1f}%)")
        print(f"  Failed: {results['summary']['failed_comparisons']} ({100-pass_rate:.1f}%)")
    
    print(f"\n{'='*80}")
    print("CATEGORY BREAKDOWN")
    print(f"{'='*80}")
    for category, cat_results in results["categories"].items():
        print(f"\n{category}:")
        print(f"  Indicators with data: {cat_results['summary']['indicators_with_data']}/{len(cat_results['indicators'])}")
        print(f"  Comparisons: {cat_results['summary']['comparisons_made']}")
        print(f"  Passed: {cat_results['summary']['passed']}")
        print(f"  Failed: {cat_results['summary']['failed']}")


def main():
    parser = argparse.ArgumentParser(description="Validate cross-referenced indicators")
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.05,
        help="Maximum allowed divergence as decimal (default: 0.05 = 5%%)"
    )
    parser.add_argument(
        "--min-dates",
        type=int,
        default=3,
        help="Minimum number of common dates required (default: 3)"
    )
    
    args = parser.parse_args()
    
    print("="*80)
    print("CROSS-REFERENCE VALIDATION SCRIPT")
    print("="*80)
    print(f"Divergence Threshold: {args.threshold*100}%")
    print(f"Minimum Common Dates: {args.min_dates}")
    print("\nNOTE: Only comparing EXACT SAME indicators from different sources")
    print("      (same units, same methodology)")
    
    session = next(get_db())
    try:
        results = validate_cross_references(
            session,
            threshold=args.threshold,
            min_common_dates=args.min_dates
        )
        print_summary_report(results)
        
        # Return exit code based on failures
        if results["summary"]["failed_comparisons"] > 0:
            print(f"\n⚠️  {results['summary']['failed_comparisons']} comparisons failed validation")
            return 1
        else:
            print(f"\n✅ All comparisons passed validation")
            return 0
    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(main())
