# Data Visualization Tools

## Interactive Excel-Style Dashboard

View all ingestion data in an interactive web dashboard with Excel-like tables.

### Starting the Dashboard

```bash
cd /workspaces/Mesodian/economy_backend
python scripts/view_ingestion_data.py --port 8050
```

Then open your browser to: **http://localhost:8050**

### Features

✅ **Excel-Style Tables**
- Sortable columns (click headers)
- Filterable data (use filter boxes)
- Multi-column sorting (shift+click)
- Row selection

✅ **Multiple Data Sources**
- ONS (UK Statistics)
- WDI (World Bank Development Indicators)
- FRED (Federal Reserve Economic Data)
- GCP (Global Carbon Project - CO2 data)
- EUROSTAT (European Statistics)
- Ember (Electricity Data)

✅ **Export Functionality**
- Export to CSV (button in UI)
- Export to Excel (built-in table export)

✅ **Real-Time Updates**
- Click "Refresh Data" to reload
- Switch between sources instantly

### Screenshot of Dashboard

The dashboard shows:
1. **Header**: Source selector dropdown
2. **Refresh Button**: Reload data on demand
3. **Data Info Panel**: Shows record count, columns, last update time
4. **Export Button**: Download data as CSV
5. **Data Table**: Excel-style table with:
   - Sortable columns
   - Filterable rows
   - Multi-select capabilities
   - Pagination (50 rows per page)

### Data Displayed

#### WDI (World Bank) - Default View
- **Countries**: USA, GBR, DEU, FRA, CHN, IND, BRA, JPN
- **Indicator**: Real GDP (NY.GDP.MKTP.KD)
- **Columns**: indicator_id, country_id, date, value, source, ingested_at, indicator

#### FRED (Federal Reserve)
- **Series**: CPI, Unemployment Rate, GDP
- **Country**: USA only
- **Columns**: indicator_id, country_id, date, value, source, ingested_at, series, series_name

#### GCP (Global Carbon Project)
- **Countries**: Top 10 emitters
- **Years**: 2015-2024
- **Columns**: country, iso_code, year, co2, co2_per_capita, population, gdp

#### ONS (UK Statistics)
- **Series**: CPIH (Inflation)
- **Country**: GBR only
- **Columns**: indicator_id, country_id, date, value, source, ingested_at, series

#### EUROSTAT
- **Countries**: DEU, FRA, ITA, ESP, NLD, BEL
- **Dataset**: HICP (Inflation)
- **Columns**: indicator_id, country_id, date, value, source, ingested_at, dataset

#### Ember (Electricity)
- **Countries**: US, UK, Germany, France, China, India
- **Years**: 2018+
- **Columns**: Area, Year, Category, Value, Generation (TWh), etc.

---

## Command-Line Testing Tool

For quick testing without the web interface:

```bash
# Test all sources
python scripts/test_ingestion_sources.py

# Test specific sources
python scripts/test_ingestion_sources.py wdi fred ons

# Show available sources
python scripts/test_ingestion_sources.py --help
```

This displays data in terminal tables (useful for CI/CD or scripting).

---

## Usage Examples

### View World Bank GDP Data

1. Start dashboard: `python scripts/view_ingestion_data.py`
2. Open http://localhost:8050
3. Select "WDI (World Bank)" from dropdown
4. See GDP data for 8 countries (2020-2024)
5. Click column headers to sort
6. Use filter boxes to search (e.g., type "USA" in country_id filter)
7. Click "Export to CSV" to download

### Compare US Economic Indicators

1. Select "FRED (Federal Reserve)" from dropdown
2. View CPI, Unemployment, and GDP together
3. Sort by date to see time series
4. Filter by series_name to isolate indicators

### Analyze Global CO2 Emissions

1. Select "GCP (Global Carbon Project)"
2. View 10 countries with complete CO2 data
3. Compare co2 vs co2_per_capita columns
4. Filter by year to see specific periods

---

## Stopping the Dashboard

Press `Ctrl+C` in the terminal where it's running, or:

```bash
pkill -f "view_ingestion_data.py"
```

---

## Troubleshooting

### Port Already in Use

```bash
# Use a different port
python scripts/view_ingestion_data.py --port 8051
```

### Data Not Loading

- Check terminal for error messages
- Verify API keys are set (FRED_API_KEY, EIA_API_KEY)
- Some sources may have rate limits

### Slow Loading

- WDI and GCP fetch data for multiple countries (takes 5-10 seconds)
- FRED and ONS are fastest (USA/UK only)
- Ember downloads large CSV file (may take 15-20 seconds)

---

## Advanced Usage

### Custom Port

```bash
python scripts/view_ingestion_data.py --port 9000
```

### Debug Mode

```bash
python scripts/view_ingestion_data.py --debug
```

### Background Mode

```bash
python scripts/view_ingestion_data.py > /tmp/dashboard.log 2>&1 &
```

Check logs:
```bash
tail -f /tmp/dashboard.log
```

---

## Next Steps

To add more data sources to the dashboard:

1. Edit `scripts/view_ingestion_data.py`
2. Add new fetch function (e.g., `fetch_oecd_data()`)
3. Add to dropdown options
4. Add to update_table callback

Example:
```python
{'label': 'OECD', 'value': 'oecd'},

# In update_table:
elif source == 'oecd':
    df = asyncio.run(fetch_oecd_data())
```
