"""
Sample mode infrastructure for ingestion testing.

Provides:
- 5-record limits per country/ticker for testing
- Strict validation with no silent failures
- Explicit error surfacing
"""

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class SampleConfig:
    """Configuration for sample/test mode ingestion."""
    enabled: bool = False
    max_records_per_country: int = 5
    max_records_per_ticker: int = 5
    max_years: int = 2
    strict_validation: bool = True
    fail_on_empty: bool = True


@dataclass
class ValidationResult:
    """Result of data validation."""
    valid: bool
    errors: List[str]
    warnings: List[str]
    record_count: int
    countries: List[str]
    
    def raise_if_invalid(self):
        """Raise exception if validation failed."""
        if not self.valid:
            raise ValueError(f"Validation failed: {'; '.join(self.errors)}")


def validate_timeseries_data(
    rows: List[Dict[str, Any]],
    *,
    required_fields: Optional[List[str]] = None,
    expected_countries: Optional[List[str]] = None,
    sample_config: Optional[SampleConfig] = None
) -> ValidationResult:
    """
    Validate time series data with strict checks.
    
    Args:
        rows: List of time series records
        required_fields: Fields that must be present
        expected_countries: Countries that should have data
        sample_config: Sample mode configuration
        
    Returns:
        ValidationResult with validation status
    """
    errors = []
    warnings = []
    
    required_fields = required_fields or ["indicator_id", "country_id", "date", "value"]
    sample_config = sample_config or SampleConfig()
    
    # Check empty data
    if not rows:
        if sample_config.fail_on_empty:
            errors.append("No data returned (empty result)")
        else:
            warnings.append("Empty result returned")
        return ValidationResult(
            valid=not errors,
            errors=errors,
            warnings=warnings,
            record_count=0,
            countries=[]
        )
    
    # Check record limit in sample mode
    if sample_config.enabled and len(rows) > sample_config.max_records_per_country * 10:
        errors.append(f"Too many records ({len(rows)}), sample mode should limit to ~{sample_config.max_records_per_country} per country")
    
    # Validate each record
    countries_seen = set()
    for i, row in enumerate(rows):
        # Check required fields
        missing = [f for f in required_fields if f not in row or row[f] is None]
        if missing:
            errors.append(f"Record {i}: Missing required fields: {missing}")
            continue
        
        # Check country code format
        country = row.get("country_id")
        if country:
            countries_seen.add(str(country))
            # Allow special world/aggregate codes or standard ISO codes (2-3 chars)
            country_str = str(country)
            if country_str not in ["WLD", "GLOBAL", "WORLD", "ALL"] and len(country_str) not in [2, 3]:
                errors.append(f"Record {i}: Invalid country code format: {country}")
        
        # Check date field
        date_val = row.get("date")
        if date_val is None:
            errors.append(f"Record {i}: Date is None")
        
        # Check numeric value
        value = row.get("value")
        if value is not None:
            try:
                float(value)
            except (TypeError, ValueError):
                errors.append(f"Record {i}: Value is not numeric: {value}")
    
    # Check expected countries
    if expected_countries:
        missing_countries = set(expected_countries) - countries_seen
        if missing_countries:
            warnings.append(f"Expected countries not found in data: {missing_countries}")
    
    return ValidationResult(
        valid=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        record_count=len(rows),
        countries=list(countries_seen)
    )


def validate_trade_flows(
    flows: List[Dict[str, Any]],
    *,
    expected_reporters: Optional[List[str]] = None,
    sample_config: Optional[SampleConfig] = None
) -> ValidationResult:
    """Validate trade flow data."""
    errors = []
    warnings = []
    sample_config = sample_config or SampleConfig()
    
    if not flows:
        if sample_config.fail_on_empty:
            errors.append("No trade flows returned")
        return ValidationResult(valid=not errors, errors=errors, warnings=warnings, record_count=0, countries=[])
    
    required_fields = ["reporter_country_id", "partner_country_id", "year", "value_usd"]
    reporters_seen = set()
    
    for i, flow in enumerate(flows):
        missing = [f for f in required_fields if f not in flow or flow[f] is None]
        if missing:
            errors.append(f"Flow {i}: Missing fields: {missing}")
            continue
        
        reporters_seen.add(str(flow["reporter_country_id"]))
        
        # Validate year
        try:
            year = int(flow["year"])
            if year < 1900 or year > 2030:
                errors.append(f"Flow {i}: Invalid year: {year}")
        except (TypeError, ValueError):
            errors.append(f"Flow {i}: Year not numeric: {flow['year']}")
    
    if expected_reporters:
        missing = set(expected_reporters) - reporters_seen
        if missing:
            warnings.append(f"Expected reporters not found: {missing}")
    
    return ValidationResult(
        valid=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        record_count=len(flows),
        countries=list(reporters_seen)
    )


def limit_dataframe_by_country(
    df: pd.DataFrame,
    country_column: str,
    max_per_country: int = 5
) -> pd.DataFrame:
    """Limit dataframe to max N rows per country."""
    if df.empty:
        return df
    
    return df.groupby(country_column, group_keys=False).head(max_per_country)


def limit_records_by_country(
    records: List[Dict[str, Any]],
    country_key: str = "country_id",
    max_per_country: int = 5
) -> List[Dict[str, Any]]:
    """Limit records list to max N per country."""
    by_country: Dict[str, List[Dict[str, Any]]] = {}
    
    for record in records:
        country = record.get(country_key)
        if country not in by_country:
            by_country[country] = []
        if len(by_country[country]) < max_per_country:
            by_country[country].append(record)
    
    result = []
    for country_records in by_country.values():
        result.extend(country_records)
    
    return result


class IngestionError(Exception):
    """Raised when ingestion fails for a specific source/country."""
    
    def __init__(self, source: str, country: str, reason: str):
        self.source = source
        self.country = country
        self.reason = reason
        super().__init__(f"Ingestion failed for {source}/{country}: {reason}")
