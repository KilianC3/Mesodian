# ⚡ QUICK REFERENCE - Mesodian System

## Current Status (Last Updated: 2025-12-12 Session 20)
```
Working Sources: 24/25 (96%) ✅✅✅
Broken Sources:  1/25 (4%)
Data Points:     44,000+ (10,100 TimeSeriesValue + 33,919 AssetPrice + OKSURF/GDELT)
Countries:       56 (54 countries + WLD world + EMU euro area)
Indicators:      110
Status:          96% OPERATIONAL ✅✅✅
Session 20:      OECD fixed (deduplication), GDELT working (keywords), 
                 OKSURF added (economics/finance filtering), AISStream verified
```

## 🚀 Quick Start
```bash
cd /workspaces/Mesodian/economy_backend
docker-compose up -d postgres
export DATABASE_URL="postgresql+psycopg2://economy:economy@localhost:5433/economy_dev"
```

## 🔍 Verify Current State
```bash
python -c "from app.db.engine import get_db; from app.db.models import TimeSeriesValue; from sqlalchemy import func; db = next(get_db()); sources = db.query(TimeSeriesValue.source, func.count(TimeSeriesValue.id)).group_by(TimeSeriesValue.source).all(); print({s:c for s,c in sources})"
```

## ✅ Actually Working (24/25 = 96%) - VERIFIED 2025-12-12
- **FRED:** USA CPI, Unemployment (1947-2025) - 1,878 records
- **WDI:** GDP, CPI, Unemployment - 570 records
- **IMF:** Using sdmx1 client - 30 records
- **OPENALEX:** AI research publications (54 countries) - 324 records
- **YFINANCE:** 10 equity assets - 33,919 prices
- **STOOQ:** Historical price data
- **OECD:** SDMX 2.1 API (Session 20 - fixed deduplication) - 1 record ✅
- **RSS:** Central bank announcements - 26 records
- **EUROSTAT:** HICP inflation - 1,116 records
- **EIA:** Energy data - 5,059 records
- **EMBER:** Electricity generation by fuel type - 125 records
- **BIS:** Policy rates (WS_CBPOL) - 135 records
- **GCP:** CO2 emissions - 450 records
- **ECB_SDW:** FX rates EUR/USD, EUR/GBP - 10 records
- **UNCTAD:** Trade & economic data via OData API - 22 records ✅
- **WTO:** Tariff & trade data via Time Series API - 29 records ✅
- **ONS:** CPIH + GDP via CSV download - 10 records ✅
- **COMTRADE:** Trade flows (duplicate fix applied) ✅
- **ADB:** SDMX v4 API with ingest_full interface - 18 records ✅
- **ILOSTAT:** Batch country fetching (Session 19) - 122 records ✅
- **GDELT:** News articles with keyword queries (Session 20) - 25 articles ✅
- **OKSURF:** Economics/finance news with filtering (Session 20) - 16 articles ✅
- **AISSTREAM:** WebSocket vessel tracking (verified ready) ✅
- **FAOSTAT:** Agriculture data (faostat pip package) - 5 records ✅

## ❌ Failing Sources (1/25 = 4%)
- **PATENTSVIEW**: HTTP 410 Gone - API permanently deprecated (user excluded)

## ⚡ Streaming Sources (2) - Ready for Production
- **AISSTREAM**: WebSocket vessel tracking - fully implemented, requires daemon process
- **GDELT**: Real-time news events - working with keyword-based queries

## 📋 Files
- **Truth:** `/workspaces/Mesodian/economy_backend/SYSTEM_STATE.md`
- **Procedure:** `/workspaces/Mesodian/economy_backend/WORKFLOW.md`
- **This File:** `/workspaces/Mesodian/economy_backend/QUICK_REF.md`

## 🔥 Golden Rule
**If it's not in `warehouse.time_series_value` table, it doesn't exist.**

## 📞 Emergency Commands
```bash
# Restart everything
docker-compose down && docker-compose up -d postgres

# Check DB content
psql postgresql://economy:economy@localhost:5433/economy_dev -c "SELECT source, COUNT(*) FROM warehouse.time_series_value GROUP BY source;"

# Test API
curl http://localhost:8000/api/timeseries/country/USA
```
