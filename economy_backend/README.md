# Economy Backend

Economy Backend is a FastAPI-based data and analytics service that ingests macroeconomic, trade, finance, energy, climate, and ESG datasets, stores them in a warehouse/graph database, and exposes reusable pipelines for metrics and risk scoring.

## Architecture at a glance
- **Runtime:** Python 3.11+, FastAPI, SQLAlchemy.
- **Data stores:** Provider-specific `raw` landing tables, a normalized `warehouse`, and a `graph` schema for multi-layer webs.
- **Interfaces:** REST APIs for reference data, time series, features, metrics, webs, and dashboards via FastAPI routers.【F:app/main.py†L1-L38】
- **Batch + online:** Async ingestion clients fetch data into the warehouse; graph builders and metric pipelines read from the warehouse and persist outputs for API consumption.

```mermaid
flowchart LR
  subgraph Sources
    A[FRED/WDI/IMF/OECD/DB.nomics]
    B[Comtrade/Shipping/Markets]
    C[ESG Providers]
  end
  A & B & C --> R[Raw schema (provider payloads)]
  R --> W[Warehouse (metadata, series, trade, features, cycles)]
  W --> G[Graph webs (nodes/edges/metrics)]
  W --> M[Country/ESG metrics]
  G --> M
  M --> API[FastAPI read endpoints]
  G --> API
```

## Data domains and providers
Ingestion clients under `app/ingest/` cover macro indicators, trade flows, commodities and energy balances, financial markets, ESG signals, events/news, patents, and shipping activity. Each client writes exact payloads and parameters into the `raw` schema so downstream loaders remain deterministic and replayable.【F:app/db/models.py†L29-L137】 HTTP clients centralize retry/backoff behaviour for production-grade resilience to provider limits and transient failures.【F:app/ingest/base_client.py†L8-L138】

## Storage layers
### Raw schema
Landing tables record provider payloads, query params, and fetch timestamps for every API call to preserve provenance and enable auditing/replays.【F:app/db/models.py†L29-L137】

### Warehouse schema
- **Metadata registries:** Countries, indicators, and assets support consistent joins across series and trades.【F:app/db/models.py†L139-L173】
- **Time series & markets:** Indicator values and asset OHLCV keyed by country/asset/date.【F:app/db/models.py†L175-L232】
- **Trade & shipping:** Bilateral trade flows and AIS-derived activity for concentration and disruption signals.【F:app/db/models.py†L233-L259】
- **Features & cycles:** `country_year_features` aggregates macro, trade, climate, shipping, and data-quality signals; `global_cycle_index` stores PCA-based cycle scores with method versions and coverage metadata.【F:app/db/models.py†L261-L283】【F:app/db/models.py†L191-L212】
- **ESG staging:** `sovereign_esg_raw` holds pillar inputs before aggregation to composite scores.【F:app/db/models.py†L549-L563】

### Graph schema
Graphs model production, trade, finance, policy, climate, and risk webs:
- **Nodes:** Typed by role (country, country-sector, infrastructure, institution, indicator, event, web) with category, value-chain position, scale, structural role, and optional geo/sector tags.【F:app/db/models.py†L285-L372】
- **Edges:** Flow, influence, membership, or constraint relationships with direction, weights, and impact metadata across layers and systems.【F:app/db/models.py†L374-L470】
- **Metrics:** `node_metric`, `web_metric`, and `edge_metric` store quantitative scores (centrality, dependence, propagation, ESG, risk) with uniqueness constraints to avoid duplicates.【F:app/db/models.py†L472-L546】
- **Deterministic tagging:** Helpers assign default themes, scales, and roles during build time so graph construction remains reproducible across runs.【F:app/graph/schema_helpers.py†L1-L195】

## Metrics, cycles, and scores
- **Global and regional cycles:** PCA-based factors and regime mapping for trade, business, commodities, inflation, and financial conditions are written into `global_cycle_index`, including GDP coverage and method versioning for reproducibility.【F:app/cycles/global_trade_cycle.py†L1-L119】【F:app/db/models.py†L191-L212】
- **Country risk/resilience/climate:** Metrics compute energy/food/import dependence, concentration, shipping stress, and household/credit stress using warehouse features and trade flows.【F:app/metrics/risk_energy.py†L8-L87】
- **Graph/web metrics:** Centrality and propagation metrics enrich nodes and webs for structural-role analysis (see `app/graph/algorithms.py` and `app/metrics/web_metrics.py`).
- **ESG scores:** Sovereign ESG pillars and composite `ESG_TOTAL_SOV` scores blend external providers with internal risk metrics after winsorisation and percentile ranking; raw inputs remain auditable in `sovereign_esg_raw`.【F:app/metrics/sovereign_esg.py†L1-L200】【F:app/db/models.py†L549-L563】

## Production operations
- **Configuration:** Environment variables are loaded via Pydantic settings, including database URL, optional Redis URL, and provider API keys (FRED, EIA, Comtrade, AIS). Validation guards ensure deployment `ENV` is one of `dev`/`staging`/`prod`.【F:app/config/__init__.py†L1-L34】
- **Database migrations:** Apply Alembic migrations (see `alembic/`) to create/update warehouse and graph schemas before running ingestion or API services.
- **Service surfaces:** FastAPI routers expose health, reference, time series, features, metrics, webs, and dashboards for online reads; CORS defaults permit localhost development during integration.【F:app/main.py†L1-L38】
- **Resilience:** Async HTTP ingestion clients use capped retries, exponential backoff, and status-aware retry rules to handle rate limits and transient failures, keeping ingestion stable in production fetch loops.【F:app/ingest/base_client.py†L8-L138】

## Running the backend locally
1. Create and activate a virtual environment.
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```
2. Install dependencies.
   ```bash
   pip install -r requirements.txt
   ```
3. Export required environment variables (see `app/config/__init__.py`), including `POSTGRES_URL` and provider keys. SQLite defaults are used in tests if you want to avoid setting up Postgres.
4. Run the server.
   ```bash
   uvicorn app.main:app --reload
   ```

## Testing
See [README_tests.md](README_tests.md) for detailed guidance on running the test suite locally.
