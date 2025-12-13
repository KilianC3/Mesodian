#!/bin/bash
# SESSION STARTER - Run this at the beginning of every work session
# This gives you (and any AI assistant) immediate context

set -e

echo "════════════════════════════════════════════════════════════════"
echo "  MESODIAN SYSTEM - SESSION STARTER"
echo "════════════════════════════════════════════════════════════════"
echo ""

# 1. Start infrastructure
echo "1. Starting PostgreSQL..."
cd /workspaces/Mesodian/economy_backend
docker-compose up -d postgres
sleep 3

# 2. Set environment
export DATABASE_URL="postgresql+psycopg2://economy:economy@localhost:5433/economy_dev"

# 3. Check database state
echo ""
echo "2. Checking database state..."
python -c "
from app.db.engine import get_db
from app.db.models import TimeSeriesValue, Country, Indicator
from sqlalchemy import func

db = next(get_db())

countries = db.query(func.count(Country.id)).scalar()
indicators = db.query(func.count(Indicator.id)).scalar()
total_data = db.query(func.count(TimeSeriesValue.id)).scalar()

sources = db.query(
    TimeSeriesValue.source,
    func.count(TimeSeriesValue.id)
).group_by(TimeSeriesValue.source).all()

print(f'Countries:   {countries}')
print(f'Indicators:  {indicators}')
print(f'Data Points: {total_data:,}')
print(f'Sources:     {len(sources)}/24 working')
print('')
print('Working sources:')
for source, count in sources:
    print(f'  - {source}: {count:,} values')
" 2>/dev/null || echo "❌ Database connection failed"

# 4. Display current priorities
echo ""
echo "3. Current priorities from SYSTEM_STATE.md:"
echo "════════════════════════════════════════════════════════════════"
cat << 'EOF'

IMMEDIATE PRIORITIES:
1. Fix Missing Indicators (4 sources blocked):
   - Add GDP_GROWTH_UK → fixes ONS
   - Add CO2_TOTAL → fixes GCP  
   - Add BIS_CREDIT_PRIVATE_PCT_GDP → fixes BIS
   - Add missing EIA indicators

2. Investigate "Success" Claims:
   - EUROSTAT, IMF, STOOQ, YFINANCE
   - Were reported working but no data in DB

3. Fix OPENALEX: All 324 values = 0.0

HIGH PRIORITY:
4. Fix OECD (404 error)
5. Fix COMTRADE (404 error - we have API key)
6. Fix EMBER (403 error)

EOF

echo ""
echo "════════════════════════════════════════════════════════════════"
echo "  READY TO WORK"
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "📖 Key files:"
echo "   - SYSTEM_STATE.md (source of truth)"
echo "   - WORKFLOW.md (procedures)"
echo "   - QUICK_REF.md (commands)"
echo ""
echo "💡 When prompting AI assistant, say:"
echo "   'Read SYSTEM_STATE.md then work on [priority item]'"
echo ""
