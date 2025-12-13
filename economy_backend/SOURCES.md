# DATA SOURCES - PRODUCTION DOCUMENTATION
**Last Updated:** 2025-12-12 (Session 21-22 - Data Expansion & Validation)
**Version:** 2.1

This document provides comprehensive technical documentation for all data sources in the Mesodian economy backend.

---

## 🎯 DATA EXPANSION STATUS (Target: 80-100 indicators per country)

### Current System Metrics
- **Total TimeSeriesValue Records:** 176,227 (↑ 17.4x from 10,101 baseline)
- **Total Indicators Configured:** 196 across 25 sources
- **Indicators With Data:** 59 (30% active)
- **Total Countries:** 56
- **Working Sources:** 18/25 (72%)

### Phase Completion Status
- ✅ **Phase 1:** FRED International Expansion - COMPLETE (168,127 records)
- ✅ **Phase 2:** WDI Configuration - COMPLETE (31 indicators ready)
- ✅ **Phase 3:** IMF Configuration - COMPLETE (38 indicators configured)
- ✅ **Phase 7:** Cross-Reference Validation - COMPLETE (script created, tested)
- 🔄 **Phase 4:** IMF SDMX Research - IN PROGRESS
- ⏳ **Phase 5:** IMF Full Ingestion - PLANNED
- ⏳ **Phase 6:** WDI Full Ingestion - PLANNED
- ⏳ **Phase 8:** Regional Sources Expansion - PLANNED

### Expansion Progress by Source

| Source | Configured | With Data | Records | Coverage | Status |
|--------|------------|-----------|---------|----------|--------|
| **FRED** | 25 | 23 | 168,127 | 92% | ✅ COMPLETE - G20 FX, international CPI, rates, commodities |
| **WDI** | 31 | 3 | 595 | 10% | 🔄 CONFIGURED - National accounts, demographics, trade, finance, infrastructure |
| **IMF** | 38 | 1 | 31 | 3% | 🔄 CONFIGURED - CPI validated, BOP/debt/trade pending SDMX fix |
| **ADB** | 26 | 1 | 29 | 4% | 📋 BASELINE - Asia development indicators |
| **UNCTAD** | 24 | 6 | 22 | 25% | 📋 BASELINE - FDI, trade, development |
| **WTO** | 14 | 3 | 29 | 21% | 📋 BASELINE - Trade statistics |
| **EIA** | 5 | 3 | 5,059 | 60% | ✅ WORKING - Energy prices, oil data |
| **EUROSTAT** | 2 | 2 | 1,116 | 100% | ✅ WORKING - EU GDP, HICP (expansion planned) |
| **EMBER** | 6 | 5 | 125 | 83% | ✅ WORKING - Energy & electricity data |
| **BIS** | 5 | 1 | 135 | 20% | 📋 BASELINE - Policy rates (expansion planned) |
| **GCP** | 3 | 2 | 460 | 67% | ✅ WORKING - CO2 emissions |
| **ECB_SDW** | 2 | 2 | 10 | 100% | ✅ WORKING - EUR exchange rates |
| **ONS** | 2 | 2 | 10 | 100% | ✅ WORKING - UK CPI, GDP |
| **ILOSTAT** | 1 | 1 | 122 | 100% | ✅ WORKING - Unemployment (expansion planned) |
| **OPENALEX** | 1 | 1 | 324 | 100% | ✅ WORKING - Academic publications |
| **FAOSTAT** | 1 | 1 | 5 | 100% | ✅ WORKING - Agricultural data (expansion planned) |
| **RSS** | 1 | 1 | 27 | 100% | ✅ WORKING - Central bank news |
| **OECD** | 2 | 1 | 1 | 50% | 📋 BASELINE - OECD economic data (expansion planned) |
| **Other** | 13 | 0 | 0 | 0% | 📋 CONFIGURED - Awaiting implementation |
| **TOTAL** | **196** | **59** | **176,227** | **30%** | 🎯 **Target: 80-100 per country** |

### Cross-Reference Validation Results

**Validation Script:** `scripts/validate_cross_references.py`  
**Last Run:** 2025-12-12  
**Methodology:** ONLY compares EXACT SAME indicators from different sources (same units, same methodology)

Currently validated indicator pairs:

| Category | Indicator | Sources | Status | Divergence | Correlation |
|----------|-----------|---------|--------|------------|-------------|
| **Oil - Brent** | Brent Crude Oil Price (USD/barrel) | FRED, EIA | ✅ PASS | 0.00% avg | 1.0000 |
| **Oil - WTI** | WTI Crude Oil Price (USD/barrel) | FRED, EIA | ⏸️ No overlap | - | - |
| **FX** | USD/EUR Exchange Rate | FRED, ECB_SDW | ✅ PASS | 0.52% avg | 0.7346 |
| **CPI** | UK Consumer Price Index | FRED, ONS | ✅ PASS | 0.00% avg | 1.0000 |

**Summary:**
- **Total Categories:** 4 indicator groups
- **Total Comparisons:** 3 country-date pairs
- **Pass Rate:** 100% (3/3 passed, threshold: 5% divergence)

**Key Findings:**
- ✅ **Brent Oil:** Perfect match between FRED and EIA sources (10 common dates)
- ✅ **USD/EUR:** FRED vs ECB within 0.52% average divergence (5 common dates)
- ✅ **UK CPI:** Perfect match between FRED and ONS (5 common dates)
- ⏸️ **WTI Oil:** Both sources have data, but no overlapping dates yet

**Note:** Cross-reference validation is designed to detect data quality issues by comparing identical indicators from authoritative sources. It does NOT compare:
- Different methodologies (e.g., CPI index vs inflation rate)
- Different units (e.g., total emissions vs per capita)
- Related but distinct indicators (e.g., WTI vs Brent oil prices)

These are considered separate data series and are NOT cross-validated.

### Recommended Next Steps

1. **Immediate:** Run WDI full ingestion (async or background job due to API slowness)
2. **High Priority:** Expand IMF (20+ indicators, SDMX client working)
3. **Medium Priority:** Expand regional sources (EUROSTAT, OECD, BIS)
4. **Follow-up:** Implement cross-reference validation scripts

---

## 📊 RATE LIMITS SUMMARY

| Source | Rate Limit | Implementation | Notes |
|--------|------------|----------------|-------|
| FRED | 100 req/day (no key) | None needed | Very generous limit |
| WDI | No official limit | Exponential backoff | Recommend 10 req/min |
| ADB | **20 req/minute** | **✅ IMPLEMENTED** | Sliding window limiter |
| IMF | No official limit | Built into sdmx1 | Automatic retry |
| OPENALEX | 100K req/day (polite pool) | Custom headers | mailto in User-Agent |
| YFINANCE | Unofficial, ~2K/hour | Delay between calls | 1-2 sec delay |
| OECD | 1000 req/hour | Built into sdmx | Automatic |
| COMTRADE | 100 req/hour (with key) | **NEEDS RATE LIMITER** | 250 req/hour premium |
| EUROSTAT | No published limit | Built into sdmx | Automatic |
| EIA | 1000 req/hour (with key) | **NEEDS RATE LIMITER** | Track usage |
| EMBER | 1000 req/day | **NEEDS RATE LIMITER** | Daily limit |
| BIS | No official limit | Built into sdmx | Automatic |
| GCP | No limit (static files) | None needed | CSV downloads |
| UNCTAD | 100 req/minute | **NEEDS RATE LIMITER** | OData API |
| WTO | 1000 req/hour | **NEEDS RATE LIMITER** | Subscription key |
| ONS | No published limit | **NEEDS RATE LIMITER** | Beta API, be polite |
| FAOSTAT | No limit (bulk download) | None needed | Uses pip package |
| ECB_SDW | No official limit | Built into sdmx | Automatic |
| ILOSTAT | 100 req/minute | **NEEDS RATE LIMITER** | SDMX REST |
| GDELT | No official limit | None needed | Keyword-based queries |
| OKSURF | No official limit | None needed | POST /news-section |

---

## 1. FRED (Federal Reserve Economic Data)

### Overview
- **Provider:** Federal Reserve Bank of St. Louis
- **Type:** REST API (JSON)
- **Auth:** API key (optional, increases limits)
- **Status:** ✅ WORKING (MASSIVELY EXPANDED)
- **Records:** 168,127 (↑ from 1,878 - 89x increase)
- **Coverage:** 15 countries/regions (↑ from 1)

### Expansion Details (Session 21 - 2025-12-12)
**Previously:** 2 series (USA CPI, USA Unemployment) covering only USA
**Now:** 23 series covering 15 countries/regions with international data

**New Series Added:**
- **G20 Exchange Rates (12 series):** DEXCHUS (CNY), DEXJPUS (JPY), DEXUSEU (EUR), DEXUSUK (GBP), DEXCAUS (CAD), DEXMXUS (MXN), DEXBZUS (BRL), DEXINUS (INR), DEXKOUS (KRW), DEXUSAL (AUD), DEXSZUS (CHF), DEXSDUS (SEK)
- **International CPI (5 series):** CHNCPIALLMINMEI (China), JPNCPIALLMINMEI (Japan), DEUCPIALLMINMEI (Germany), GBRCPIALLMINMEI (UK), CANCPIALLMINMEI (Canada)
- **International Interest Rates (2 series):** INTGSTJPM193N (Japan), INTGSTGBM193N (UK)
- **Commodity Prices (2 series):** DCOILWTICO (WTI Oil), DCOILBRENTEU (Brent Oil)

**Coverage Breakdown:**
- WLD (World/Global): 19,837 records (commodities)
- JPN (Japan): 15,311 records
- GBR (United Kingdom): 15,249 records
- CAN (Canada): 15,115 records
- CHE (Switzerland): 13,774 records
- SWE (Sweden): 13,773 records
- AUS (Australia): 13,767 records
- IND (India): 13,266 records
- CHN (China): 11,602 records
- KOR (South Korea): 11,160 records
- MEX (Mexico): 8,042 records
- BRA (Brazil): 7,757 records
- EMU (Euro Area): 6,753 records
- USA (United States): 1,878 records
- DEU (Germany): 843 records

### API Details
```
Base URL: https://api.stlouisfed.org/fred
Endpoints:
  - /series/observations (time series data)
  - /series (metadata)
Format: JSON
Rate Limit: 100 requests/day (no key), unlimited (with key)
Documentation: https://fred.stlouisfed.org/docs/api/fred/
```

### Configuration
```python
FRED_API_KEY = env.get("FRED_API_KEY")  # Optional
FRED_BASE_URL = "https://api.stlouisfed.org/fred"
SERIES = {
    "CPIAUCSL": "CPI_USA",      # CPI All Urban Consumers
    "UNRATE": "UNEMPLOYMENT_USA" # Unemployment Rate
}
```

### Rate Limiting
- **Current:** None needed (generous limits)
- **Recommendation:** Add if scaling to many series

### Data Format
```json
{
  "observations": [
    {
      "realtime_start": "2025-01-01",
      "realtime_end": "2025-01-01",
      "date": "2025-01-01",
      "value": "123.456"
    }
  ]
}
```

---

## 2. WDI (World Development Indicators)

### Overview
- **Provider:** World Bank
- **Type:** REST API (JSON/XML)
- **Auth:** None required
- **Status:** ✅ WORKING
- **Records:** 500

### API Details
```
Base URL: https://api.worldbank.org/v2
Endpoints:
  - /country/{country}/indicator/{indicator}
Format: JSON (add ?format=json)
Rate Limit: No official limit (recommend 10 req/min)
Documentation: https://datahelpdesk.worldbank.org/knowledgebase/articles/889392
```

### Configuration
```python
WDI_BASE_URL = "https://api.worldbank.org/v2"
INDICATORS = {
    "NY.GDP.MKTP.CD": "GDP_CURRENT_USD",
    "FP.CPI.TOTL": "CPI",
    "SL.UEM.TOTL.ZS": "UNEMPLOYMENT_PCT"
}
```

### Rate Limiting
- **Current:** Exponential backoff on errors
- **Recommendation:** Implement 10 req/min throttle for politeness

### Data Format
```json
[
  {"page": 1, "pages": 5, "per_page": 50, "total": 240},
  [
    {
      "indicator": {"id": "NY.GDP.MKTP.CD", "value": "GDP"},
      "country": {"id": "US", "value": "United States"},
      "value": "25000000000",
      "date": "2024"
    }
  ]
]
```

---

## 3. ADB (Asian Development Bank KIDB)

### Overview
- **Provider:** Asian Development Bank
- **Type:** SDMX v4 REST API
- **Auth:** None required
- **Status:** ✅ WORKING
- **Records:** 18+ (limited test run)

### API Details
```
Base URL: https://kidb.adb.org/api/v4/sdmx
API Documentation: https://kidb.adb.org/api
Endpoints:
  - /data/ADB,{DATAFLOW}/{KEY} (time series data)
Format: SDMX-XML (structure-specific, unnamespaced)
Rate Limit: 20 requests/minute (CRITICAL - enforced)
SDMX Key Format: FREQUENCY.INDICATOR(S).ECONOMY_CODE(S)
Example: /data/ADB,PPL_POP/A.PO_POP.PHI+THA
```

### Configuration
```python
ADB_BASE_URL = "https://kidb.adb.org/api/v4/sdmx"

# Available Dataflows (from API documentation):
DATAFLOWS = {
    "PPL": "People (population, labor, poverty, social)",
    "PPL_POP": "People - Population",
    "PPL_LE": "People - Labor & Employment",
    "PPL_POV": "People - Poverty Indicators",
    "PPL_SI": "People - Social Indicators",
    
    "EO": "Economy & Output",
    "EO_NA": "Economy - National Accounts",
    "EO_NA_CURR_GDP_EXP": "GDP by expenditure at current prices",
    "EO_NA_CONST_GDP_EXP": "GDP by expenditure at constant prices",
    
    "MFP": "Money, Finance, and Prices",
    "MFP_PR": "Prices (CPI, inflation)",
    "MFP_MF": "Money & Finance (M2, interest rates)",
    "MFP_XR": "Exchange Rates",
    
    "GG": "Government and Governance",
    "GG_GF": "Government Finance (revenue, expenditure)",
    
    "GLB": "Globalization",
    "GLB_ET": "External Trade (exports, imports)",
    "GLB_BP": "Balance of Payments",
    "GLB_IR": "International Reserves",
    "GLB_EI": "External Indebtedness",
    "GLB_CF": "Capital Flows",
    "GLB_TM": "Tourism",
    
    "TC": "Transport and Communication",
    "ENV": "Environment and Climate Change",
}

# ADB Country Codes (NON-STANDARD ISO3!)
ADB_COUNTRIES = {
    "IND": "IND",  # India
    "IDN": "INO",  # Indonesia (ADB: INO)
    "PHL": "PHI",  # Philippines (ADB: PHI)
    "VNM": "VIE",  # Vietnam (ADB: VIE)
    "THA": "THA",  # Thailand
    "CHN": "PRC",  # China (ADB: PRC)
    "BGD": "BAN",  # Bangladesh (ADB: BAN)
    "MYS": "MAL",  # Malaysia (ADB: MAL)
    "SGP": "SIN",  # Singapore (ADB: SIN)
    "JPN": "JPN",  # Japan
    "KOR": "KOR",  # South Korea
    "PAK": "PAK",  # Pakistan
}
```

### Indicator Mapping
```python
# Each ADB indicator code must be mapped to a unique Mesodian canonical code
# This prevents duplicate key violations when multiple series exist in one dataflow
ADB_INDICATORS = {
    # Population
    "PO_POP": "ADB_POPULATION_TOTAL",
    "PO_POP_FE": "ADB_POPULATION_FEMALE",
    "PO_POP_MA": "ADB_POPULATION_MALE",
    
    # Labor & Employment
    "LU_PE_NUM": "ADB_UNEMPLOYMENT_PERSONS",
    "LU_UR": "ADB_UNEMPLOYMENT_RATE",
    "LF_LF_NUM": "ADB_LABOR_FORCE",
    
    # National Accounts
    "NA_GDP_EXP_CURR_USD": "ADB_GDP_CURRENT_USD",
    "NA_GNI_CURR_USD": "ADB_GNI_CURRENT_USD",
    
    # Prices
    "PR_CPI": "ADB_CPI",
    "PR_INFL": "ADB_INFLATION_RATE",
    
    # Money & Finance
    "MF_M2": "ADB_MONEY_SUPPLY_M2",
    "MF_IR_LENDING": "ADB_INTEREST_RATE_LENDING",
    
    # Exchange Rates
    "XR_ER_USD": "ADB_EXCHANGE_RATE_USD",
    
    # Government Finance
    "GF_REV_PC_GDP": "ADB_GOV_REVENUE_PCT_GDP",
    "GF_EXP_PC_GDP": "ADB_GOV_EXPENDITURE_PCT_GDP",
    
    # Trade
    "ET_EX_USD": "ADB_EXPORTS_USD",
    "ET_IM_USD": "ADB_IMPORTS_USD",
    "ET_TB_USD": "ADB_TRADE_BALANCE_USD",
    
    # Balance of Payments
    "BP_CA_USD": "ADB_CURRENT_ACCOUNT_USD",
    "BP_FDI_USD": "ADB_FDI_INFLOWS_USD",
    
    # Reserves & Debt
    "IR_TOT_USD": "ADB_RESERVES_TOTAL_USD",
    "EI_DOD_USD": "ADB_EXTERNAL_DEBT_USD",
    "EI_DOD_PC_GNI": "ADB_EXTERNAL_DEBT_PCT_GNI",
}
```

### Rate Limiting ✅ **IMPLEMENTED**
- **Current:** ✅ Sliding window rate limiter (20 requests/minute)
- **Implementation:** `ADBRateLimiter` class with thread-safe token bucket
- **Enforcement:** Automatic sleep when limit reached
- **Priority:** CRITICAL - API strictly enforces limit

### SDMX Key Format
```
Format: FREQUENCY.INDICATOR(S).ECONOMY_CODE(S)
- Dots (.) separate dimensions
- Plus (+) for multiple values within a dimension
- Empty dimension means "all values"

Examples:
  A.PO_POP.PHI+THA          # Annual, population, Philippines+Thailand
  A.LU_UR+LU_PE_NUM.IND      # Annual, unemployment rate + persons, India
  A..PHI                     # Annual, ALL indicators, Philippines
  Q.PR_CPI.PRC+IND+INO      # Quarterly, CPI, China+India+Indonesia
```

### XML Parsing Notes
```xml
<!-- ADB returns structure-specific SDMX with NO NAMESPACE on Series/Obs! -->
<StructureSpecificData>
  <Series FREQ="A" INDICATOR="LU_PE_NUM" ECONOMY_CODE="INO">
    <Obs TIME_PERIOD="2020" OBS_VALUE="9767.754"/>
    <Obs TIME_PERIOD="2021" OBS_VALUE="9102.052"/>
  </Series>
</StructureSpecificData>

<!-- Key attributes -->
Series: FREQ, INDICATOR, ECONOMY_CODE
Obs: TIME_PERIOD, OBS_VALUE

<!-- CRITICAL: -->
1. No XML namespace on Series/Obs tags (use .iter("Series") not .findall())
2. INDICATOR attribute is per-series (not per-dataflow)
3. Multiple indicators in one response must be mapped separately
4. Time periods: YYYY (annual), YYYY-QQ (quarterly), YYYY-MM (monthly)
```

### Known Issues & Solutions
- **Issue:** Some dataflows return 403 Forbidden (e.g., GLB_IR, GLB_EI)
  - **Solution:** API access restrictions, not a client error
  - **Action:** Skip and continue with other dataflows

- **Issue:** Duplicate key violations when all series map to same indicator_id
  - **Solution:** ✅ FIXED - Comprehensive indicator mapping implemented
  - **Action:** Each ADB indicator code mapped to unique Mesodian canonical code

- **Issue:** ADB country codes differ from ISO3 (PHI≠PHL, INO≠IDN, VIE≠VNM, etc.)
  - **Solution:** ✅ FIXED - `ADB_COUNTRY_CODES` dictionary for translation
  - **Action:** Always convert ISO3 to ADB codes before API calls

### Verification
```bash
# Test ADB ingestion
python -c "
from app.db.engine import SessionLocal
from app.ingest.adb_client import ingest_adb_data

session = SessionLocal()
stats = ingest_adb_data(session, start_year=2020, end_year=2022)
session.commit()
print(f'Series fetched: {stats[\"series_fetched\"]}')
print(f'Records inserted: {stats[\"records_inserted\"]}')
print(f'Indicators processed: {stats[\"indicators_processed\"]}')
session.close()
"

# Check database
python -c "
from app.db.engine import SessionLocal
from app.db.models import TimeSeriesValue, Indicator

session = SessionLocal()
count = session.query(TimeSeriesValue).join(Indicator).filter(Indicator.source == 'ADB').count()
print(f'ADB records: {count}')
session.close()
"
```

---

## 4. IMF (International Monetary Fund)

### Overview
- **Provider:** International Monetary Fund
- **Type:** SDMX REST API
- **Auth:** None required
- **Status:** ✅ WORKING
- **Records:** 23

### API Details
```
Base URL: https://www.imf.org/external/datamapper/api/v1
Library: sdmx1 (pip package)
Format: SDMX-JSON
Rate Limit: No official limit (sdmx1 handles retries)
Documentation: https://www.imf.org/external/datamapper/api/help
```

### Configuration
```python
IMF_API_URL = "https://www.imf.org/external/datamapper/api/v1"
DATAFLOWS = ["CPI"]  # Consumer Price Index
```

### Rate Limiting
- **Current:** Built into sdmx1 library
- **Implementation:** Automatic retry with exponential backoff

---

## 5. OPENALEX (Research Database)

### Overview
- **Provider:** OpenAlex / OurResearch
- **Type:** REST API (JSON)
- **Auth:** Polite pool (email in User-Agent)
- **Status:** ✅ WORKING
- **Records:** 324

### API Details
```
Base URL: https://api.openalex.org
Endpoints:
  - /works (research papers)
Rate Limit: 100,000 req/day (polite pool), 10 req/sec (max)
Documentation: https://docs.openalex.org/
```

### Configuration
```python
OPENALEX_BASE_URL = "https://api.openalex.org"
POLITE_POOL = True
EMAIL = "your.email@example.com"  # Required for polite pool
```

### Rate Limiting
- **Current:** Custom headers with mailto
- **Implementation:** Add "mailto:email" in User-Agent
- **Polite Pool Benefits:** 10x higher rate limits

### Headers Required
```python
headers = {
    "User-Agent": "Mesodian Economy Backend (mailto:your.email@example.com)"
}
```

---

## 5. COMTRADE (UN Commodity Trade Statistics)

### Overview
- **Provider:** United Nations Statistics Division
- **Type:** REST API (JSON)
- **Auth:** API subscription key required
- **Status:** ✅ WORKING (rate limited in tests)
- **Records:** Variable

### API Details
```
Base URL: https://comtradeapi.un.org/public/v1
Endpoints:
  - /get/{frequency}/{typeCode}/{freqCode}
Format: JSON
Rate Limit: 100 requests/hour (free), 250 requests/hour (premium)
Documentation: https://comtradedeveloper.un.org/
```

### Configuration
```python
COMTRADE_API_KEY = env.get("COMTRADE_API_KEY")  # REQUIRED
COMTRADE_BASE_URL = "https://comtradeapi.un.org/public/v1"
```

### Rate Limiting ⚠️ **CRITICAL**
- **Current:** ❌ None implemented
- **Required:** Token bucket with 100 requests/hour
- **Implementation Priority:** HIGH
- **Failure Mode:** HTTP 429 (Too Many Requests)

### Recommended Implementation
```python
from threading import Lock
from time import time, sleep

class ComtradeRateLimiter:
    def __init__(self, requests_per_hour=100):
        self.rate = requests_per_hour
        self.tokens = requests_per_hour
        self.last_update = time()
        self.lock = Lock()
    
    def acquire(self):
        with self.lock:
            now = time()
            elapsed = now - self.last_update
            self.tokens = min(self.rate, self.tokens + elapsed * self.rate / 3600)
            self.last_update = now
            
            if self.tokens < 1:
                sleep_time = (1 - self.tokens) * 3600 / self.rate
                sleep(sleep_time)
                self.tokens = 1
            
            self.tokens -= 1
```

---

## 6. EIA (U.S. Energy Information Administration)

### Overview
- **Provider:** U.S. Department of Energy
- **Type:** REST API (JSON)
- **Auth:** API key required
- **Status:** ✅ WORKING
- **Records:** 5,059

### API Details
```
Base URL: https://api.eia.gov/v2
Endpoints:
  - /petroleum/pri/spt/data
  - /natural-gas/pri/sum/data
Format: JSON
Rate Limit: 1,000 requests/hour (with API key)
Documentation: https://www.eia.gov/opendata/
```

### Configuration
```python
EIA_API_KEY = env.get("EIA_API_KEY")  # REQUIRED
EIA_BASE_URL = "https://api.eia.gov/v2"
SERIES = {
    "PET.RWTC.D": "WTI_OIL_PRICE",
    "PET.RBRTE.D": "BRENT_OIL_PRICE"
}
```

### Rate Limiting ⚠️
- **Current:** ❌ None implemented
- **Required:** 1,000 requests/hour limit
- **Implementation Priority:** MEDIUM
- **Tracking:** Use sliding window counter

---

## 7. EMBER (Electricity Generation Database)

### Overview
- **Provider:** Ember Climate
- **Type:** REST API (JSON)
- **Auth:** API key required
- **Status:** ✅ WORKING
- **Records:** 125

### API Details
```
Base URL: https://api.ember-energy.org/v1
Endpoints:
  - /electricity-generation/yearly
Format: JSON
Rate Limit: 1,000 requests/day
Documentation: https://api.ember-energy.org/v1/docs
```

### Configuration
```python
EMBER_API_KEY = env.get("EMBER_API_KEY")  # REQUIRED
EMBER_BASE_URL = "https://api.ember-energy.org/v1"
```

### Rate Limiting ⚠️
- **Current:** ❌ None implemented
- **Required:** 1,000 requests/day (daily limit)
- **Implementation Priority:** MEDIUM
- **Reset:** Midnight UTC

---

## 8. ONS (Office for National Statistics UK)

### Overview
- **Provider:** UK Office for National Statistics
- **Type:** Beta API v1 (CSV downloads)
- **Auth:** None required
- **Status:** ✅ WORKING
- **Records:** 5

### API Details
```
Base URL: https://api.beta.ons.gov.uk/v1
Endpoints:
  - /datasets (list all datasets)
  - /datasets/{id} (dataset metadata)
  - /datasets/{id}/editions/{edition}/versions/{version} (version data)
Downloads: CSV files via download links
Rate Limit: No published limit (be polite)
Documentation: https://developer.ons.gov.uk/
```

### Configuration
```python
ONS_BASE_URL = "https://api.beta.ons.gov.uk/v1"
DATASETS = {
    "cpih01": {
        "indicator": "CPIH_UK",
        "aggregate_filter": "CP00",  # Overall CPIH index
        "frequency": "M"
    },
    "gdp-to-four-decimal-places": {
        "indicator": "GDP_MONTHLY_UK",
        "aggregate_filter": "ABMI",  # GDP index
        "frequency": "M"
    }
}
```

### Rate Limiting ⚠️
- **Current:** ❌ None implemented
- **Recommended:** 10 requests/minute for politeness
- **Implementation Priority:** LOW (beta API, be respectful)

### Dataset Discovery
```bash
# List all datasets
curl "https://api.beta.ons.gov.uk/v1/datasets?limit=100"

# Search by ID or type
curl "https://api.beta.ons.gov.uk/v1/datasets?id=gdp-to-four-decimal-places"

# Parameters:
#   - limit: max items (default 20)
#   - offset: starting index
#   - sort_order: ASC or DESC
#   - type: dataset type filter
#   - id: specific dataset ID
```

### CSV Format
```csv
v4_0,mmm-yy,Time,uk-only,Geography,aggregate_code,Aggregate
139.5,Oct-25,Oct-25,K02000001,United Kingdom,CP00,CPIH All items
```

---

## 9. UNCTAD (United Nations Conference on Trade and Development)

### Overview
- **Provider:** United Nations
- **Type:** OData API (Gzip CSV)
- **Auth:** Client ID + Secret
- **Status:** ✅ WORKING
- **Records:** 22

### API Details
```
Base URL: https://unctadstat-user-api.unctad.org
Format: Gzip-compressed CSV (OData $format=csv)
Rate Limit: 100 requests/minute (estimated)
Max Response Size: 62,500 rows per request
Documentation: Internal UNCTAD documentation
```

### Configuration
```python
UNCTAD_CLIENT_ID = env.get("UNCTAD_CLIENT_ID")  # REQUIRED
UNCTAD_SECRET = env.get("UNCTAD_SECRET")  # REQUIRED
UNCTAD_BASE_URL = "https://unctadstat-user-api.unctad.org"
```

### Rate Limiting ⚠️
- **Current:** ❌ None implemented
- **Estimated:** 100 requests/minute
- **Implementation Priority:** MEDIUM

### Valid Datasets (2025-12-11)
- ✅ `US.TradeMerchTotal` - Total merchandise trade
- ❌ `US.TradeGoods` - Returns HTTP 404
- ❌ `US.FDI` - Returns HTTP 404
- ❌ `US.GDPGrowth` - Returns HTTP 404

**Action Required:** Remove invalid datasets from configuration

---

## 10. WTO (World Trade Organization)

### Overview
- **Provider:** World Trade Organization
- **Type:** Time Series API (JSON)
- **Auth:** Subscription key (Ocp-Apim-Subscription-Key)
- **Status:** ✅ WORKING
- **Records:** 29

### API Details
```
Base URL: https://api.wto.org/timeseries/v1
Endpoints:
  - /indicators (list indicators)
  - /data (get time series)
Format: JSON
Rate Limit: 1,000 requests/hour (estimated)
Documentation: https://apiportal.wto.org/
```

### Configuration
```python
WTO_API_KEY = env.get("WTO_API_KEY")  # REQUIRED
WTO_BASE_URL = "https://api.wto.org/timeseries/v1"
```

### Rate Limiting ⚠️
- **Current:** ❌ None implemented
- **Estimated:** 1,000 requests/hour
- **Implementation Priority:** MEDIUM

### Valid Indicators (2025-12-11)
- ✅ `TP_A_0010` - Simple average MFN applied tariff
- ❌ `ITS_MTV_AM` - Returns HTTP 400 "No indicator found"
- ❌ `ITS_IVM_AM` - Returns HTTP 400
- ❌ `ITS_IVI_AM` - Returns HTTP 400
- ❌ `TCS_AM` - Returns HTTP 400
- ❌ `DDS_AM` - Returns HTTP 400

**Action Required:** Remove invalid indicators from configuration

---

## 11. FAOSTAT (Food and Agriculture Organization)

### ⚠️ CRITICAL: FAOSTAT IS A PYTHON MODULE (NOT A REST API)
**USER DIRECTIVE (Session 19):** "FOR THE LAST TIME FAOSTAT IS A PYTHON MODULE. WRITE THIS DOWN SO I NEVER HAVE TO SAY THIS AGAIN."

```bash
# Installation required
pip install faostat  # v1.1.2
```

```python
# Usage
import faostat

# List available datasets (69 total)
datasets = faostat.list_datasets()

# Fetch data by dataset code
data = faostat.get_data('QCL')  # Crops and livestock products
```

### Overview
- **Provider:** UN Food and Agriculture Organization
- **Type:** Python module (pip package)
- **Auth:** None required
- **Status:** ✅ Module works | ⚠️ Upstream API HTTP 500 (FAO servers down)
- **Records:** 0 (upstream issue, not our code)
- **PyPI:** https://pypi.org/project/faostat/
- **GitHub:** https://github.com/Predicta-Analytics/faostat
- **Upstream API:** https://faostatservices.fao.org/api/v1/en/data/{dataset}

### Current Issue (2025-01-19)
The faostat Python module works correctly, but the upstream FAO API returns HTTP 500:
```bash
$ curl -I https://faostatservices.fao.org/api/v1/en/data/QCL
HTTP/2 500
```
This is FAO's server issue, not our code. Monitor https://www.fao.org/faostat/ for service restoration.

### API Details (When Upstream Functional)
```
Package: faostat (pip install faostat)
Method: faostat.get_data(domain)
Domains: QCL (crops/livestock), QV (producer prices), QA (area/production), etc.
Caching: Automatic local caching via Python module
Rate Limit: None (bulk downloads)
Documentation: https://pypi.org/project/faostat/
GitHub: https://github.com/Predicta-Analytics/faostat
```

### CRITICAL SETUP REQUIREMENT
```bash
pip install faostat
```

### Configuration and Usage
```python
# Installation (ALREADY DONE - v1.1.2 installed)
# pip install faostat

import faostat

# List all available datasets
datasets = faostat.list_datasets()
print(datasets[:3])  # Display first three datasets

# Configure domains for specific indicators
DOMAINS = {
    "QCL": {  # Crops and Livestock Products
        "element_code": "5510",  # Production
        "item_codes": ["15", "27"]  # Wheat, Bovine meat
    },
    "QV": {  # Producer Prices
        "element_code": "5532",  # Producer price (USD/tonne)
        "item_codes": ["15", "27"]
    }
}

# Fetch data for a domain
data = faostat.get_data(domain="QCL")
# Filter by area_code, element_code, item_code
filtered = data[
    (data['area_code'] == 840) &  # USA
    (data['element_code'] == 5510) &  # Production
    (data['item_code'] == 15)  # Wheat
]
```

### Current Status
- **Package:** Installed (faostat==1.1.2) ✅
- **Client Code:** Rewritten to use pip package (app/ingest/faostat_client.py) ✅
- **Upstream Issue:** FAO API returns HTTP 500 (server-side error) ❌
- **Action:** Monitor FAO service status at https://www.fao.org/faostat/

### Rate Limiting
- **Current:** None needed (bulk downloads)
- **Caching:** Package handles automatic caching locally

### Known Issues
- **Upstream HTTP 500:** FAO API server returning internal server errors
- **Not our bug:** Issue is on FAO's infrastructure side
- **Workaround:** None available - must wait for FAO to fix their servers
- **Alternative:** Consider using WDI agricultural indicators if critical

---

## 12. EUROSTAT

### Overview
- **Provider:** European Commission
- **Type:** SDMX REST API
- **Auth:** None required
- **Status:** ✅ WORKING
- **Records:** 1,116

### API Details
```
Base URL: https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1
Library: pandasdmx (pip package)
Format: SDMX-ML
Rate Limit: No published limit (SDMX handles it)
Documentation: https://wikis.ec.europa.eu/display/EUROSTATHELP/API
```

### Rate Limiting
- **Current:** Built into pandasdmx library
- **Implementation:** Automatic retry and throttling

---

## 13. ADB (Asian Development Bank KIDB)

### Overview
- **Provider:** Asian Development Bank
- **Type:** SDMX v4 REST API
- **Auth:** None required
- **Status:** ❌ BLOCKED - Query format validation fails
- **Records:** 0

### INVESTIGATION SUMMARY (2025-12-11)

**API Confirmed Working:**
- Base URL: https://kidb.adb.org/api/v4/sdmx ✅
- Structure endpoints: 200 OK ✅
- Data endpoints: 404/422 errors ❌

**Root Cause:**
The API returns validation errors for all tested query formats:
- 422: "Incorrect format for dataflow ID"
- 422: "Incorrect format for dimensions"
- 404: Endpoint not found

**Tests Conducted:**
1. ✅ Dataflow discovery: `GET /structure/dataflow/all/all/+` (200 OK - 62 dataflows found)
2. ✅ Indicator list: `GET /dataflow/indicators/PPL_LE` (200 OK - returns JSON)
3. ❌ Data query: All format variations return 404/422

**Query Formats Tested (All Failed):**
```
/data/ADB/PPL_LE/A.LU_PE_NUM.356
/data/PPL_LE/A.LU_PE_NUM.356
/data/ADB:PPL_LE/A.LU_PE_NUM.356
/data/ADB%2FPPL_LE/A.LU_PE_NUM.356
/data/PPL_LE/A..356
/data/PPL_LE?freq=A&economy=356
```

### API Details
```
Base URL: https://kidb.adb.org/api/v4/sdmx
Structure Endpoints (WORKING):
  - /structure/dataflow/all/all/+ (list all dataflows)
  - /dataflow/indicators/{dataflow_id} (list indicators for dataflow)
  
Data Endpoints (NOT WORKING):
  - /data/{dataflow_id}/{sdmx_key} (422 validation errors)
  
SDMX Key Format (Per Documentation):
  FREQUENCY.INDICATOR(S).ECONOMY_CODE(S)
  - Frequency: A (annual), Q (quarterly), M (monthly)
  - Dimensions separated by period (.)
  - Multiple values separated by plus (+)
  
Example M49 Economy Codes:
  - 356 = India
  - 360 = Indonesia
  - 608 = Philippines
  - 704 = Vietnam
  - 764 = Thailand

Format Parameters:
  - startPeriod: Starting year (e.g., 2020)
  - endPeriod: Ending year (e.g., 2024)
  - format: sdmx-structure-xml (default), sdmx-json, sdmx-csv
  
Rate Limit: No official limit documented
Documentation: https://kidb.adb.org/
```

### Discovered Dataflows (Economic Indicators)
```
EO_NA_CONST_GOD: Growth of Demand (% annual change)
EO_NA_CONST_GOO: Growth of Output (% annual change)
EO_NA_CURR_GDP_SOD: Structure of Demand (% of GDP)
EO_NA_CURR_GDP_SOO: Structure of Output (% of GDP)
SDG_08: Goal 8 - Decent work and economic growth
PPL_LE: People and Poverty - Labor & Employment
```

### Sample Indicators (PPL_LE Dataflow)
```json
[
  {"code": "LLF_PE_NUM", "name": "Labor Force"},
  {"code": "LUD_PE_NUM_PS", "name": "Underemployed"},
  {"code": "LU_PE_NUM", "name": "Unemployed"},
  {"code": "LUW_PE_NUM_PS", "name": "Unpaid work"}
]
```

### Configuration (Not Yet Functional)
```python
ADB_BASE_URL = "https://kidb.adb.org/api/v4/sdmx"

# M49 3-digit numeric economy codes
ADB_COUNTRY_CODES = {
    "IND": "356",
    "IDN": "360",
    "PHL": "608",
    "VNM": "704",
    "THA": "764",
    "CHN": "156",
    "JPN": "392",
    "KOR": "410",
    "BGD": "050",
    "PAK": "586",
}

# Attempted configuration (validation fails)
ADB_SERIES = {
    "PPL_LE": {
        "canonical_indicator": "ADB_UNEMPLOYMENT",
        "dataflow": "ADB/PPL_LE",  # Agency/Dataflow format
        "indicator_code": "LU_PE_NUM",  # Unemployed
        "frequency": "A",
        "countries": ["IND", "IDN", "PHL"],
    },
}
```

### Alternative Access Methods Tested
- ❌ pandasdmx library: ADB not in supported sources list
- ❌ Python ADB package: No official package exists (pip search disabled)
- ❌ Query builder UI: Returns 404 at /api/query-builder
- ❌ API v1/v2/v3: Only v4 exists (all others return 404)

### ACTION REQUIRED

**CRITICAL:** Contact ADB support to obtain:
1. **Working example query** for data endpoint with actual response
2. **Correct dataflow ID format** (agency prefix? separator?)
3. **Dimension value constraints** (are there required dimensions?)
4. **API authentication** (is public access restricted?)

**Support Contacts:**
- Email: support@adb.org (general)
- API Documentation: https://kidb.adb.org/api
- Developer Forum: Check if exists

**Alternative Strategy:**
If API remains inaccessible, consider:
1. Use ADB bulk CSV downloads from Data Explorer UI
2. Use World Bank WDI for Asian development indicators
3. Exclude ADB from MVP until API access is resolved

### Rate Limiting
- **Current:** None needed (no successful data queries)
- **Recommendation:** Add 10 req/min when/if API access is resolved

### Files Modified
- **app/ingest/adb_client.py:** Rewritten for SDMX v4 (needs completion)

---

## 14. FinViz (Financial Visualizations Webscraper)

### Overview
- **Provider:** FinViz.com
- **Type:** Web scraping (BeautifulSoup + httpx)
- **Auth:** None required (public website)
- **Status:** ⚠️ NOT INTEGRATED (code exists but not in production)
- **Records:** 0 (not actively used)

### Scraping Details
```
Base URL: https://finviz.com
Target Pages:
  - /quote.ashx?t={TICKER} (fundamentals, ratios, financials)
  - /news.ashx?t={TICKER} (news headlines)
  - /insider.ashx?t={TICKER} (insider trades)
Rate Limit: 1.5-2.5 seconds between requests (respectful crawling)
Anti-Detection: Rotating user agents, proper headers, referer
Documentation: None (public website scraping)
```

### Configuration
```python
FINVIZ_BASE_URL = "https://finviz.com"
RATE_LIMIT_DELAY = (1.5, 2.5)  # Random delay in seconds
MAX_RETRIES = 3
BACKOFF_BASE = 5.0  # Exponential backoff for 429/5xx errors

# Rotating user agents for anti-detection
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
]
```

### Data Extracted
- **Fundamentals:** P/E, P/B, EPS, market cap, dividend yield, debt/equity, ROE, etc.
- **Financial Statements:** Income statement, balance sheet, cash flow
- **Analyst Ratings:** Upgrades/downgrades with firm, date, price target
- **News:** Headlines with timestamps and sources
- **Insider Trades:** Insider name, relationship, transaction type, shares, value
- **Market Overview:** Indices performance, sector rotation

### Rate Limiting
- **Current:** Built-in (1.5-2.5s delay between requests)
- **Retry Logic:** Exponential backoff on 429/5xx errors
- **Anti-Detection:** Rotating user agents, proper headers

### Known Issues
- **Not integrated:** Code exists in `app/ingest/finviz_client.py` but not in `jobs.py` PROVIDERS list
- **Web scraping risks:** Site structure changes can break parser
- **Rate limiting critical:** Aggressive scraping will get IP blocked
- **Action Required:**
  1. Decide if FinViz data is needed for MVP
  2. If yes, add to PROVIDERS list and create indicators
  3. If no, remove code to reduce maintenance burden
  4. Consider legal/ToS compliance before production use

### Usage (if integrated)
```python
from app.ingest.finviz_client import FinVizComprehensiveScraper

async with FinVizComprehensiveScraper() as scraper:
    data = await scraper.scrape_comprehensive("AAPL")
    # Returns: fundamentals, news, insider trades, analyst ratings
```

---

## 🚨 PRIORITY ACTIONS

### HIGH PRIORITY
1. ❌ **ADB API Research** - Need correct SDMX v4 query format with working examples
2. ✅ **ONS GDP Filter** - Fixed duplicate data by filtering for A--T (overall GDP)
3. ✅ **COMTRADE Duplicate Fix** - Changed .one_or_none() to .first() to handle existing duplicates

### MEDIUM PRIORITY
4. ❌ **EIA Rate Limiter** - Implement 1,000 req/hour limit
5. ❌ **EMBER Rate Limiter** - Implement 1,000 req/day limit
6. ❌ **UNCTAD Rate Limiter** - Implement 100 req/min limit
7. ❌ **WTO Rate Limiter** - Implement 1,000 req/hour limit

### LOW PRIORITY
8. ❌ **ILOSTAT Rate Limiter** - Implement 100 req/min limit

---

## 📝 RATE LIMITER IMPLEMENTATION GUIDE

### Token Bucket Algorithm (Recommended)
```python
from threading import Lock
from time import time, sleep

class RateLimiter:
    """Thread-safe token bucket rate limiter."""
    
    def __init__(self, requests_per_period: int, period_seconds: int = 3600):
        """
        Args:
            requests_per_period: Number of requests allowed per period
            period_seconds: Period duration in seconds (default: 1 hour)
        """
        self.rate = requests_per_period
        self.period = period_seconds
        self.tokens = requests_per_period
        self.last_update = time()
        self.lock = Lock()
    
    def acquire(self, tokens: int = 1) -> None:
        """Acquire tokens, blocking if necessary."""
        with self.lock:
            now = time()
            elapsed = now - self.last_update
            
            # Refill tokens based on elapsed time
            self.tokens = min(
                self.rate,
                self.tokens + elapsed * self.rate / self.period
            )
            self.last_update = now
            
            # Wait if insufficient tokens
            if self.tokens < tokens:
                sleep_time = (tokens - self.tokens) * self.period / self.rate
                sleep(sleep_time)
                self.tokens = tokens
            
            self.tokens -= tokens
```

### Usage Example
```python
# Initialize rate limiter
comtrade_limiter = RateLimiter(requests_per_period=100, period_seconds=3600)

# Use before API call
def fetch_comtrade_data(params):
    comtrade_limiter.acquire()  # Block until token available
    response = httpx.get(COMTRADE_URL, params=params)
    return response.json()
```

---

---

## 24. OKSURF (OK Surf News API)

### Overview
- **Provider:** ok.surf
- **Type:** REST API (JSON)
- **Auth:** None required
- **Status:** ✅ WORKING (2025-12-12)
- **Records:** 16 articles (filtered)

### API Details
```
Base URL: https://ok.surf/api/v1
Endpoint: POST /news-section
Format: JSON
Rate Limit: No official limit
Documentation: https://ok.surf
```

### Configuration
```python
OKSURF_BASE_URL = "https://ok.surf/api/v1"
NEWS_SECTIONS = ["Business", "Technology", "Science", "World", "US"]

# Economics/Finance Keywords for Filtering
ECON_KEYWORDS = [
    "economy", "economic", "inflation", "rate", "interest", "gdp", "growth",
    "recession", "market", "stock", "bond", "yields", "fed", "federal reserve",
    "ecb", "central bank", "unemployment", "jobs report", "cpi", "pce",
    "trade", "tariff", "debt", "budget", "fiscal", "finance", "financial",
    "monetary", "policy", "treasury", "dollar", "currency", "forex", "imf",
    "world bank", "investment", "earnings", "profit", "revenue", "export",
    "import", "manufacturing", "consumer", "retail", "housing", "commodity"
]
```

### Rate Limiting
- **Current:** None needed
- **Monitoring:** Track response times and implement if needed

### Data Format
```json
{
  "sections": ["Business", "Technology", "Science", "World", "US"]
}
```

**Response (flattened and filtered):**
```json
[
  {
    "section": "Business",
    "title": "Federal Reserve cuts interest rates by 0.25%",
    "link": "https://example.com/article",
    "source": "Yahoo Finance",
    "source_icon": "https://example.com/icon.png",
    "og": {},
    "raw": {}
  }
]
```

### Implementation Details

#### Keyword Filtering
- Fetches articles from 5 sections (Business, Technology, Science, World, US)
- Filters titles using 40+ economics/finance keywords
- Only stores articles relevant to economic/financial topics
- Reduces noise from non-economic news (sports, entertainment, etc.)

#### Function: `fetch_econ_news()`
```python
def fetch_econ_news(*, sample_config: Optional[SampleConfig] = None) -> List[Dict[str, Any]]:
    """Fetch economics and finance relevant news from OK Surf."""
    url = f"{OKSURF_BASE_URL}/news-section"
    headers = {"accept": "application/json", "Content-Type": "application/json"}
    body = {"sections": NEWS_SECTIONS}
    
    response = httpx.post(url, json=body, headers=headers, timeout=30.0)
    data = response.json()
    
    # Flatten and filter
    articles = []
    for section_name, items in data.items():
        for item in items:
            title = item.get("title", "")
            if is_econ_finance_article(title):  # Keyword filtering
                articles.append({...})
    
    return articles
```

#### Data Structure
```python
class RawOksurf(RawBase):
    __tablename__ = "raw_oksurf"
    # Inherits: id, fetched_at, params (JSONB), payload (JSONB)
```

**Storage:**
- Table: `raw.raw_oksurf`
- Columns: `id`, `fetched_at`, `params`, `payload`
- Index: `idx_raw_oksurf_fetched_at`

### Testing
```bash
# Test ingestion
python -c "from app.ingest.oksurf_client import ingest_full; from app.db.engine import get_db; session = next(get_db()); ingest_full(session)"

# Check results
psql -U economy -d economy_dev -c "SELECT COUNT(*) FROM raw.raw_oksurf;"
psql -U economy -d economy_dev -c "SELECT payload->'meta'->'total_articles' FROM raw.raw_oksurf LIMIT 1;"
```

### Known Issues
- None currently

### Troubleshooting

#### No articles returned
```python
# Check if API is accessible
import httpx
response = httpx.post("https://ok.surf/api/v1/news-section", 
                      json={"sections": ["Business"]}, 
                      timeout=30.0)
print(response.status_code)
```

#### Keyword filtering too strict
- Adjust `ECON_KEYWORDS` list in `oksurf_client.py`
- Add new economic/finance terms as needed

#### Want all articles (no filtering)
- Use `fetch_oksurf_news()` legacy function (deprecated)
- Or modify `is_econ_finance_article()` to return True

### Migration
```python
# Alembic migration: 0012_add_raw_oksurf.py
def upgrade() -> None:
    op.create_table(
        "raw_oksurf",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("params", JSONB, nullable=False),
        sa.Column("payload", JSONB, nullable=False),
        schema="raw",
    )
    op.create_index("idx_raw_oksurf_fetched_at", "raw_oksurf", ["fetched_at"])
```

### Session 20 Notes (2025-12-12)
- Initial implementation with GET `/news/{section}` failed (302 redirects)
- Switched to POST `/news-section` endpoint per official spec
- Implemented keyword-based filtering for economics/finance relevance
- Successfully tested with 16 filtered articles from 5 sections
- Created database table via manual SQL (Alembic migration added)

---

**Document Version:** 1.1  
**Last Updated:** 2025-12-12  
**Next Review:** 2026-01-12
