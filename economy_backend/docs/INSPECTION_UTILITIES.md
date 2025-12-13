# Database Inspection Utilities - Quick Reference

## Overview

Four new utilities for inspecting database contents and metric codes:

1. **`scripts/inspect_tables.py`** - Full database schema and data inspection
2. **`scripts/list_metrics.py`** - List all metric codes and generate dict stub
3. **`scripts/show_metric.py`** - View data for a specific metric code
4. **`app/metrics/metric_descriptions.py`** - Module for metric descriptions (to be populated)

## Setup

Set your database URL:

```bash
export DATABASE_URL="postgresql://user:pass@localhost:5432/dbname"
# or
export POSTGRES_URL="postgresql://user:pass@localhost:5432/dbname"
```

## Usage

### 1. Inspect All Tables

```bash
python scripts/inspect_tables.py
```

**Output:**
```
================================================================================
DATABASE INSPECTION REPORT
================================================================================

Schema: public
--------------------------------------------------------------------------------

Table: public.country
  Rows: 54
  Sample rows (up to 5):
     id  code       name          region  income_group
      1   USA  United States  North America  High income
      2   GBR  United Kingdom      Europe  High income
      ...

Table: warehouse.time_series_value
  Rows: 125,847
  Sample rows (up to 5):
     id  indicator_id  country_id       date      value
      1            42           1  2020-01-01   21427.7
      2            42           1  2020-02-01   21542.3
      ...
```

### 2. List All Metric Codes

```bash
python scripts/list_metrics.py
```

**Output:**
```
================================================================================
METRIC CODES REPORT
================================================================================

METRIC CODES BY TABLE
--------------------------------------------------------------------------------

graph.node_metric:
  - CR_RESILIENCE
  - ENERGY_DEP
  - FOOD_DEP
  - GBC
  - HOUSEHOLD_STRESS
  ...

warehouse.country_year_metric:
  - IMPORT_CRITICALITY
  - POLICY_STANCE
  ...

================================================================================
CONSOLIDATED METRIC CODE LIST
================================================================================

  CR_RESILIENCE
  ENERGY_DEP
  FOOD_DEP
  GBC
  HOUSEHOLD_STRESS
  IMPORT_CRITICALITY
  ...

================================================================================
METRIC_DESCRIPTIONS DICT STUB
================================================================================

# Copy this dict to app/metrics/metric_descriptions.py

METRIC_DESCRIPTIONS = {
    "CR_RESILIENCE": "TODO: one line description",
    "ENERGY_DEP": "TODO: one line description",
    "FOOD_DEP": "TODO: one line description",
    "GBC": "TODO: one line description",
    ...
}
```

### 3. View Specific Metric

```bash
python scripts/show_metric.py GBC
```

**Output:**
```
================================================================================
METRIC CODE: GBC
================================================================================

Table: graph.node_metric
--------------------------------------------------------------------------------
Found 10 rows (showing up to 10)

   node_id metric_code  as_of_year     value  confidence
         1         GBC        2023      0.65        0.85
         1         GBC        2022      0.58        0.82
         1         GBC        2021      0.72        0.88
         ...

Table: warehouse.country_year_metric
--------------------------------------------------------------------------------
Found 5 rows (showing up to 10)

   country_code metric_code  year     value
            USA         GBC  2023      0.65
            CHN         GBC  2023      0.58
            ...
```

### 4. Populate Metric Descriptions

**Step 1:** Run list_metrics.py and copy the dict stub

```bash
python scripts/list_metrics.py > /tmp/metrics.txt
# Copy the METRIC_DESCRIPTIONS dict from the output
```

**Step 2:** Paste into `app/metrics/metric_descriptions.py`

**Step 3:** Replace each "TODO" with actual description:

```python
METRIC_DESCRIPTIONS: dict[str, str] = {
    "GBC": "Global business cycle index from world GDP and PMIs",
    "CR_RESILIENCE": "Country resilience score combining economic and structural factors",
    "ENERGY_DEP": "Energy import dependence as share of total consumption",
    "FOOD_DEP": "Food import dependence as share of total food supply",
    "HOUSEHOLD_STRESS": "Household financial stress from debt service ratio and unemployment",
    ...
}
```

**Step 4:** Use in your code:

```python
from app.metrics.metric_descriptions import METRIC_DESCRIPTIONS, get_metric_description

# Get description for a metric
desc = get_metric_description("GBC")
print(desc)  # "Global business cycle index from world GDP and PMIs"

# List all known metrics
from app.metrics.metric_descriptions import list_all_metrics
all_metrics = list_all_metrics()
print(f"Total metrics: {len(all_metrics)}")
```

## Features

- ✅ **Read-only**: No modifications to existing application code
- ✅ **Environment-aware**: Uses DATABASE_URL or POSTGRES_URL
- ✅ **Clear errors**: Helpful messages if database URL not set
- ✅ **Pandas-optional**: Works with or without pandas installed
- ✅ **System-aware**: Excludes information_schema and pg_catalog
- ✅ **Type-safe**: Includes type hints throughout
- ✅ **Executable**: All scripts are chmod +x

## Integration with Existing Tools

These utilities complement the existing inspection scripts:

- **`scripts/dashboard.py`** - High-level overview (still useful for aggregates)
- **`scripts/inspect_data.py`** - Structured queries by data type (still useful for filtering)
- **NEW `scripts/inspect_tables.py`** - Raw table-level inspection (great for discovering structure)
- **NEW `scripts/list_metrics.py`** - Metric code discovery (fills the gap)
- **NEW `scripts/show_metric.py`** - Quick metric lookup (faster than writing SQL)

## Common Workflows

### Discover What Data Exists

```bash
# 1. See everything
python scripts/inspect_tables.py

# 2. Get high-level stats
python scripts/dashboard.py

# 3. Query specific data type
python scripts/inspect_data.py timeseries --country USA
```

### Work with Metrics

```bash
# 1. Discover all metrics
python scripts/list_metrics.py

# 2. Populate descriptions
# Copy dict stub to app/metrics/metric_descriptions.py

# 3. Look up specific metric
python scripts/show_metric.py HOUSEHOLD_STRESS

# 4. Use in code
python -c "from app.metrics.metric_descriptions import get_metric_description; print(get_metric_description('GBC'))"
```

### Debug Data Issues

```bash
# See raw table structure
python scripts/inspect_tables.py | grep "your_table_name"

# Check specific metric values
python scripts/show_metric.py YOUR_METRIC_CODE

# Filter by country/indicator
python scripts/inspect_data.py timeseries --indicator GDP_REAL --country CHN
```

## Notes

- All scripts use SQLAlchemy's inspector for metadata discovery
- Pandas is used for pretty printing when available, but not required
- Scripts are designed to be copy-paste friendly for documentation
- The metric_descriptions module is intended to be version-controlled with your descriptions
