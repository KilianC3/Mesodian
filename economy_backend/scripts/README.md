# Mesodian Backend Scripts

Production-ready scripts for data ingestion, metrics computation, and system inspection.

## Core Scripts

### Data Ingestion

**Run full data ingestion:**
```bash
python scripts/run_full_ingest.py
```
Ingests data from all 24+ sources into the database using production ingestion clients.

**Run comprehensive ingestion:**
```bash
python scripts/run_full_comprehensive_ingest.py
```
Extended ingestion with additional validation and reporting.

**Test ingestion sources:**
```bash
python scripts/test_ingestion_sources.py
python scripts/test_all_sources.py
```
Verify individual ingestion sources are working correctly.

### Metrics & Features

**Compute all metrics:**
```bash
python scripts/compute_all_metrics.py
```
Calculate country metrics, ESG scores, resilience indicators, etc.

**Seed reference data:**
```bash
python scripts/seed_indicators.py
```
Initialize countries, indicators, and other reference data.

### Database Inspection

**Inspect all tables:**
```bash
python scripts/inspect_tables.py
```
Shows all schemas, tables, row counts, and sample data.

**Inspect specific data:**
```bash
python scripts/inspect_data.py
```
View collected data with filtering options.

**List metric codes:**
```bash
python scripts/list_metrics.py
```
Display all available metric codes across tables.

**Show specific metric:**
```bash
python scripts/show_metric.py
```
View values for a specific metric across countries/years.

**View specific metric data:**
```bash
python scripts/show_metric.py GBC
python scripts/show_metric.py HOUSEHOLD_STRESS
```

Shows:
- All rows for the specified metric code
- Data from all tables containing that metric
- Up to 10 rows per table with intelligent sorting

### 2. View Data Dashboard
Shows overall statistics on ingestion, metrics, and graph structure:

```bash
python scripts/dashboard.py
```

Output includes:
- Total countries, indicators, and time series observations
- Data date ranges and freshness
- Ingestion sources and their payloads
- Feature coverage and years
- Metric counts by type
- Graph node/edge counts and types
- Trade flow volumes

### 3. Inspect Detailed Data

The `inspect_data.py` script provides detailed queries across all data layers:

```bash
python scripts/inspect_data.py --help
```

## Available Commands

### Reference Data

**List all countries:**
```bash
python scripts/inspect_data.py countries
```
Shows: ID, name, region, income group

**List all indicators:**
```bash
python scripts/inspect_data.py indicators
```
Shows: ID, source, code, frequency, unit

### Time Series Data

**View raw time series observations:**
```bash
python scripts/inspect_data.py timeseries --limit 200
```

**Filter by indicator:**
```bash
python scripts/inspect_data.py timeseries --indicator GDP_REAL
```

**Filter by country:**
```bash
python scripts/inspect_data.py timeseries --country USA
```

**Combine filters:**
```bash
python scripts/inspect_data.py timeseries --indicator GDP_REAL --country USA --limit 50
```

### Country Features

**View all features for a country:**
```bash
python scripts/inspect_data.py features --country USA
```

**Filter by year:**
```bash
python scripts/inspect_data.py features --country USA --year 2023
```

Features shown:
- GDP (real and growth)
- Inflation, unemployment
- Current account, debt ratios
- CO2 per capita
- Energy and food import dependence
- Shipping activity
- Event stress pulse
- Data quality scores

### Computed Metrics

**View all node metrics:**
```bash
python scripts/inspect_data.py metrics
```

**Filter by country:**
```bash
python scripts/inspect_data.py metrics --country USA
```

**Filter by metric code:**
```bash
python scripts/inspect_data.py metrics --metric CR_RESILIENCE
```

**Filter by year:**
```bash
python scripts/inspect_data.py metrics --year 2023
```

Common metrics:
- `CR_RESILIENCE`: Country resilience score
- `NET_SYS_IMPORTANCE`: Network systemic importance
- `ENERGY_DEP`: Energy dependence
- `FOOD_DEP`: Food import dependence
- `IMPORT_CRITICALITY`: Import criticality index

### Trade Data

**View trade flows:**
```bash
python scripts/inspect_data.py trade --limit 100
```

**Filter by reporter (exporting country):**
```bash
python scripts/inspect_data.py trade --from USA
```

**Filter by partner (importing country):**
```bash
python scripts/inspect_data.py trade --to USA
```

**Filter by year:**
```bash
python scripts/inspect_data.py trade --year 2023
```

**Bilateral flows:**
```bash
python scripts/inspect_data.py trade --from USA --to MEX --year 2023
```

### Graph Structure

**View graph overview:**
```bash
python scripts/inspect_data.py graph
```

Shows:
- Node and edge counts
- Metric storage sizes
- Breakdown by node type (COUNTRY, SECTOR_GLOBAL, etc.)
- Breakdown by edge type (FLOW, INFLUENCE, MEMBERSHIP, etc.)

### Raw Ingestion Data

**View raw payloads from a source:**
```bash
python scripts/inspect_data.py raw --source fred --limit 10
```

Available sources: `fred`, `comtrade`, `eia`, `ember`, `eurostat`, `imf`, `oecd`, `wdi`, `bis`, `ilostat`, `faostat`, `adb`, `afdb`, `dbnomics`, `ons`, `ecb_sdw`, `yfinance`, `stooq`, `gdelt`, `rss`, `aisstream`, `patents_view`, `open_alex`

Shows:
- Ingestion ID and timestamp
- Query parameters
- Payload structure

## SQL Examples

For advanced queries, you can directly access the database. Set `DATABASE_URL` and use psql:

```bash
export DATABASE_URL="postgresql+psycopg2://economy:economy@localhost:5433/economy_dev"
psql postgresql://economy:economy@localhost:5433/economy_dev
```

### Find latest data for each source

```sql
SELECT source, COUNT(*) as count, MAX(fetched_at) as latest
FROM raw.*
GROUP BY source
ORDER BY latest DESC;
```

### Time series statistics by country

```sql
SELECT 
  country_id,
  COUNT(*) as observations,
  MIN(date) as earliest,
  MAX(date) as latest
FROM warehouse.time_series_value
GROUP BY country_id
ORDER BY observations DESC;
```

### Top metrics by country

```sql
SELECT 
  n.country_code,
  nm.metric_code,
  nm.as_of_year,
  nm.value,
  n.name
FROM graph.node_metric nm
JOIN graph.node n ON nm.node_id = n.id
WHERE n.country_code IS NOT NULL
ORDER BY nm.as_of_year DESC, nm.value DESC
LIMIT 50;
```

### Trade volumes by year

```sql
SELECT 
  year,
  SUM(value_usd) as total_value,
  COUNT(*) as num_flows
FROM warehouse.trade_flow
GROUP BY year
ORDER BY year DESC;
```

### Global cycle trends

```sql
SELECT 
  cycle_type,
  date,
  frequency,
  value
FROM warehouse.global_cycle_index
WHERE frequency = 'monthly'
ORDER BY cycle_type, date DESC
LIMIT 100;
```

## Environment Setup

Ensure you have the required environment variables set before running these scripts:

```bash
export DATABASE_URL="postgresql+psycopg2://economy:economy@localhost:5433/economy_dev"
export FRED_API_KEY="your_key"
export EIA_API_KEY="your_key"
export COMTRADE_API_KEY="your_key"
export AISSTREAM_API_KEY="your_key"
```

Or they can be set in `.env` in the `economy_backend/` directory.

## Running Inside Docker

If running from inside the container:

```bash
docker-compose run --rm backend python scripts/dashboard.py
docker-compose run --rm backend python scripts/inspect_data.py timeseries --limit 50
```
