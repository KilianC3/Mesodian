# SYSTEM WORKFLOW - MANDATORY PROCEDURE FOR ALL SESSIONS

## 🔴 CRITICAL RULE: READ THIS FIRST EVERY SESSION

**Location of Truth:** `/workspaces/Mesodian/economy_backend/SYSTEM_STATE.md`

**Current Status (2025-12-11 Session 19):** 19/22 sources working (86.4%) ✅✅

---

## ⚡ SESSION 19 KEY ACHIEVEMENTS (2025-12-11)

### Production-Grade Codebase Audit
- **Code Cleanup:** Removed redundant files and cleaned up imports
- **Rate Limiters:** Added missing ILOSTAT rate limiter (100 req/min)
- **ADB Interface:** Added ingest_full() wrapper for standard compatibility
- **Test Fixes:** Fixed ember mock in test_ingest_energy_markets_events.py
- **Verification:** All 24 providers load correctly, 19/22 sources working (86.4%)
- **Documentation:** Updated all core docs to reflect current reality

---

## ⚡ SESSION 18 KEY ACHIEVEMENTS (2025-12-11)

### Production-Level Source Verification
- **ONS GDP Fix:** Added aggregate filter for A--T (overall GDP) to prevent duplicates
- **COMTRADE Fix:** Changed .one_or_none() to .first() to handle existing duplicate rows
- **ADB Research:** SDMX v4 API accessible but query format needs examples
- **FinViz Discovery:** Webscraper code exists but not integrated (needs decision)
- **Test Pass Rate:** 90.9% (up from 81.8%)

---

## ⚡ SESSION 16 KEY ACHIEVEMENTS (2025-12-11)

### Production-Grade Error Elimination
- **ONS:** Fixed with CSV download approach for Beta API v1 (5 records)
- **FAOSTAT:** Installed pip package, code ready (upstream server issues)
- **Test Pass Rate:** 86.4% (up from 77.3%)

### ONS Fix Details
- Rewrote client to download CSV files instead of JSON observations
- Parse CSV and filter by aggregate code (CP00 = overall CPIH)
- Handle MMM-YY date format (e.g., "Oct-25")
- Disabled raw_ons payload storage (table doesn't exist yet)

### FAOSTAT Fix Details
- Installed official `faostat` pip package (v1.1.2)
- Rewrote client to use `faostat.get_data(domain)`
- Package downloads bulk datasets and caches locally
- Code ready but upstream returns HTTP 500 (not our bug)

---

## ⚡ IMMEDIATE SESSION START PROCEDURE

1. **Start Infrastructure**
   ```bash
   cd /workspaces/Mesodian/economy_backend
   docker-compose up -d postgres
   sleep 3  # Wait for postgres to start
   ```

2. **Verify Database State**
   ```bash
   export DATABASE_URL="postgresql+psycopg2://economy:economy@localhost:5433/economy_dev"
   python -c "
from app.db.engine import get_db
from app.db.models import TimeSeriesValue
from sqlalchemy import func
db = next(get_db())
sources = db.query(TimeSeriesValue.source, func.count(TimeSeriesValue.id)).group_by(TimeSeriesValue.source).all()
print('SOURCES WITH DATA:', dict(sources))
"
   ```

3. **Read SYSTEM_STATE.md**
   - Review what's been fixed
   - See what's actually working
   - Check priority action items

---

## 🔧 FIXING A DATA SOURCE - MANDATORY STEPS

### DO NOT skip these steps:

1. **Understand the failure**
   ```bash
   # Check last ingestion error
   python scripts/run_full_comprehensive_ingest.py 2>&1 | grep -A 3 "SOURCE_NAME"
   ```

2. **Test API directly**
   ```bash
   # Example for OECD
   curl -v "https://actual-endpoint.com/path"
   ```

3. **Fix the code**
   - Update indicator definitions if missing
   - Fix API endpoint/query
   - Add provider limits if needed

4. **Run SINGLE source ingestion**
   ```python
   # Test just this one source
   from app.ingest.source_client import ingest_full
   from app.db.engine import get_db
   session = next(get_db())
   ingest_full(session, sample_config=SampleConfig(enabled=True))
   session.commit()
   ```

5. **VERIFY data in database**
   ```bash
   python -c "
from app.db.engine import get_db
from app.db.models import TimeSeriesValue
from sqlalchemy import func
db = next(get_db())
count = db.query(func.count(TimeSeriesValue.id)).filter(TimeSeriesValue.source == 'SOURCE_NAME').scalar()
print(f'DATA POINTS: {count}')
if count == 0:
    print('❌ NO DATA - FIX FAILED')
else:
    print('✅ DATA EXISTS')
"
   ```

6. **Update SYSTEM_STATE.md**
   - Move source from ❌ FAILING to ✅ WORKING
   - Add data point count
   - Document the fix in "FIXES COMPLETED" section

---

## 🚫 FORBIDDEN ACTIONS

### NEVER do these things:

1. ❌ Claim a source "works" without database verification
2. ❌ Trust ingestion script "Success" messages without checking data
3. ❌ Say "the system is fully operational" when 75% of sources fail
4. ❌ Forget to update SYSTEM_STATE.md after fixes
5. ❌ Run full ingestion without testing individual sources first
6. ❌ Skip checking if data values are zeros/nulls
7. ❌ Create new tracking documents instead of updating existing ones

---

## ✅ VERIFICATION CHECKLIST FOR "WORKING" STATUS

Before marking ANY source as working:

- [ ] Ingestion script completed without exceptions
- [ ] SELECT COUNT(*) FROM warehouse.time_series_value WHERE source='X' > 0
- [ ] SELECT AVG(value) shows non-zero average (where applicable)
- [ ] SELECT COUNT(DISTINCT date) shows multiple time periods
- [ ] SELECT COUNT(DISTINCT country_id) shows expected coverage
- [ ] API endpoint /api/timeseries/country/{iso3} returns this source's data
- [ ] SYSTEM_STATE.md updated with actual counts

**ALL 7 must be checked ✓**

---

## 📊 ITERATION WORKFLOW

### For each source to fix:

1. **Pick ONE source** from Priority Action Items in SYSTEM_STATE.md
2. **Research** the current API (docs, test with curl)
3. **Make minimal fix** to one file
4. **Test immediately** with sample data
5. **Verify in database** (run query)
6. **If failed:** revert, try different approach
7. **If succeeded:** Update SYSTEM_STATE.md, commit
8. **Move to next** source

### DO NOT:
- Try to fix multiple sources at once
- Make changes without testing
- Claim victory without verification
- Skip documentation

---

## 📝 DOCUMENTATION RULES

### When to update SYSTEM_STATE.md:

- ✅ After ANY code change that affects ingestion
- ✅ After verifying a source works or fails
- ✅ After discovering new information about APIs
- ✅ At end of session with current status

### What to update:

1. **Data Source Status table**
   - Move from FAILING to WORKING (or vice versa)
   - Update counts, dates, coverage

2. **Fixes Completed section**
   - Add entry with: Problem, Root Cause, Fix, Result
   - Include file paths and line numbers

3. **Priority Action Items**
   - Check off completed items
   - Reorder based on new information

4. **Database Verification Queries section**
   - Update "Last Results" with new counts
   - Update timestamp

---

## 🎯 MEASURING PROGRESS

### Metrics that matter:

1. **Source Success Rate:** X/24 working (currently 10/24 = 42%) ✅ **IMPROVING**
2. **Data Point Count:** Total values in database (currently 37,606) ✅ **EXCELLENT**
3. **Country Coverage:** Countries with data (54 seeded, ~8 with time series)
4. **Category Coverage:** GDP ✅, Inflation ✅, Energy ✅, Policy Rates ✅, Trade ❌, Patents ❌
5. **API Functionality:** Endpoints returning data ✅ **BIS now working with custom parser**

### Targets for "System Validated":

- At least 50% of sources working (12/24)
- At least 10,000 data points
- All 54 countries have some data
- All major categories covered

**Current: 17% sources, 2.6K points - NOT VALIDATED**

---

## 🔄 SESSION END PROCEDURE

Before ending ANY work session:

1. **Update SYSTEM_STATE.md** with:
   - Current database counts
   - What was fixed
   - What's still broken
   - Next priority items

2. **Run verification query** and paste results

3. **Commit changes** with clear message:
   ```bash
   git add SYSTEM_STATE.md
   git commit -m "Session update: Fixed X, Y still failing, Z data points"
   ```

4. **DON'T leave claims** like "system fully working" unless metrics hit targets

---

## 📂 FILE REFERENCE QUICK ACCESS

```bash
# Main state document
cat /workspaces/Mesodian/economy_backend/SYSTEM_STATE.md

# Check what's in database RIGHT NOW
cd /workspaces/Mesodian/economy_backend
export DATABASE_URL="postgresql+psycopg2://economy:economy@localhost:5433/economy_dev"
python test_sources_individually.py

# Run full ingestion (see all failures)
python scripts/run_full_comprehensive_ingest.py 2>&1 | tee last_ingestion.log

# Test single source
python -c "from app.ingest.fred_client import ingest_full; from app.db.engine import get_db; session = next(get_db()); ingest_full(session); session.commit()"
```

---

## 🧠 MENTAL MODEL

Think of this system as:

```
[External APIs] 
      ↓ (ingestion scripts)
[Raw Tables] 
      ↓ (parsing)
[warehouse.time_series_value] ← THE TRUTH
      ↓ (API)
[FastAPI Endpoints]
```

**THE TRUTH = what's in warehouse.time_series_value table**
Everything else is just a path to get data there or read it from there.

If it's not in that table, it doesn't exist. Period.

---

## 📚 CASE STUDY: BIS Fix (Session 6 - 2025-12-09)

**Perfect example of proper debugging workflow:**

### Problem Classification
- Initial status: "API v1 returns 404, v2 not implemented"
- Classified as: ❌ FAILING - API PARTIAL

### Investigation (Step 1: Test API Directly)
```bash
# Test v1 endpoint with curl
curl -I "https://stats.bis.org/api/v1/data/WS_CBPOL?c=US&firstNObservations=5"
# Result: HTTP 200 OK with 6.5MB XML response
# Conclusion: API WORKS! Client code is broken, not API
```

### Root Cause Analysis (Multiple Issues Found)
1. **Wrong URL construction:** Generic SDMX helper adds `/data/`, but BIS already has it
2. **Parser incompatibility:** PandasDMX cannot parse structure-specific SDMX without DSD
3. **Country code mismatch:** BIS uses ISO-2 (US), database uses ISO-3 (USA)

### Fix Implementation
1. Bypassed generic helper, constructed URL manually
2. Implemented custom XML parser using lxml (avoiding pandasdmx)
3. Added ISO-2 ↔ ISO-3 country code mapping dictionaries
4. Tested with single country (USA) first

### Verification (Step 5: VERIFY data)
```python
# After fix, tested ingestion
session.query(TimeSeriesValue).filter_by(source='BIS', country_id='USA').count()
# Result: 5 policy rate observations from 1954
# Values: 0.25%, 1.44%, 1.44%, 1.13%, 1.38% (reasonable for 1954 Fed rates)
```

### Documentation Update
- Updated SYSTEM_STATE.md: BIS moved from ❌ to ✅
- Updated QUICK_REF.md: Added BIS to working sources
- Added Session 6 section documenting the fix

**Key Takeaway:** Always test API directly with curl BEFORE assuming it's broken!

---

**REMEMBER: This workflow is mandatory. Deviating causes confusion and wasted effort.**
