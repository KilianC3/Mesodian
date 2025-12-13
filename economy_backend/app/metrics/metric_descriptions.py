"""
Metric descriptions for all computed metrics in the Mesodian system.

This module provides a single dictionary mapping metric_code values to
short one-line descriptions of how each metric is calculated. These codes
correspond to the values stored in the 'metric_code' column across various
database tables.

Usage:
    from app.metrics.metric_descriptions import METRIC_DESCRIPTIONS
    
    description = METRIC_DESCRIPTIONS.get("GBC", "Unknown metric")
    print(f"GBC: {description}")

To populate this dictionary:
1. Run: python scripts/list_metrics.py
2. Copy the generated METRIC_DESCRIPTIONS dict stub
3. Fill in the TODO placeholders with actual descriptions
"""

# Dictionary mapping metric_code -> one-line description
# Keys: metric_code values from metrics tables
# Values: Short (one sentence) description of how the metric is calculated
METRIC_DESCRIPTIONS: dict[str, str] = {
    # Fill with real codes once discovered via scripts/list_metrics.py
    # Example entries:
    # "GBC": "Global business cycle index from world GDP and PMIs",
    # "COM_ENERGY": "Energy commodity price index from WTI, Brent, and natural gas",
    # "HOUSEHOLD_STRESS": "Household financial stress from debt service ratio and unemployment",
}


def get_metric_description(metric_code: str) -> str:
    """
    Get the description for a metric code.
    
    Args:
        metric_code: The metric code to look up
        
    Returns:
        Description string, or a default message if not found
    """
    return METRIC_DESCRIPTIONS.get(metric_code, f"No description available for {metric_code}")


def list_all_metrics() -> list[str]:
    """
    Get a sorted list of all known metric codes.
    
    Returns:
        Sorted list of metric code strings
    """
    return sorted(METRIC_DESCRIPTIONS.keys())
