# Economy Backend

Backend-only analytics stack for economic webs, risk, and resilience scoring. The service ingests macroeconomic, trade, finance, energy, climate, ESG, and event datasets; normalizes them into warehouse tables and graph schemas; and exposes reusable feature/metric pipelines and FastAPI endpoints for downstream tools.

## High-level architecture
- **Ingestion layer:** Provider clients fetch series from FRED, World Bank/WDI, IMF, OECD, BIS, ADB/AFDB, DB.nomics feeds, Eurostat/ECB SDW, EIA/Ember, FAOSTAT/UNCTAD/Comtrade, AIS/shipping, markets (Stooq, Yahoo Finance), patents (PatentsView), and research sources (OpenAlex, RSS/GDELT). Each client stores raw payloads in the `raw` schema and upserts normalized series.
- **Feature layer:** `country_year_features` aggregates macro, trade, climate, shipping, event-stress, and data-quality indicators to form the main Country × Year panel used by metrics. Helper utilities compute data coverage/freshness and shipping activity features.
- **Graph layer:** Web builders rely on the `graph.node` and `graph.edge` schema plus tagging helpers to create multi-layer production, trade, finance, policy, and climate/ESG webs. Metrics write to `graph.node_metric`, `graph.edge_metric`, and `graph.web_metric`.
- **Metrics and cycles:** Global/regional cycles (business, trade, commodities, inflation, financial conditions) are stored in `warehouse.global_cycle_index`. Country-level metrics cover risk/resilience, dependence, climate/ESG, and stress signals. Graph/web metrics capture centrality, propagation, and concentration. Orchestrators in `app/metrics/run_all.py` and `app/metrics/run_web_metrics.py` coordinate batch writes.
- **API and orchestration:** `app/main.py` wires FastAPI routers for reference data, time series, features, metrics, webs, dashboards, and health checks. CLI entry points within ingestion and metrics modules support offline rebuilds.

## Data sources
- **APIs:** FRED, WDI, IMF, OECD, BIS, EIA, Ember, Comtrade, AISStream, Stooq, Yahoo Finance, ECB SDW, OpenAlex, GDELT, RSS feeds, PatentsView.
- **DB.nomics/SDMX:** ADB/AFDB national statistics, central bank feeds, thematic datasets, and Eurostat/ONS series fetched via SDMX helpers.
- **Bulk/statistical files:** FAOSTAT agriculture, UNCTAD trade balances, and other CSV-based releases where APIs are not available.

## Database schemas
- **raw.*** landing tables capture provider payloads, parameters, and timestamps for deterministic replays.
- **warehouse.*** tables hold reference entities (countries, indicators, assets), normalized time series, trade/shipping flows, `country_year_features`, and `global_cycle_index` scores. ESG staging lives in `warehouse.sovereign_esg_raw`.
- **graph.*** tables define typed `node` and `edge` records plus metric tables (`node_metric`, `edge_metric`, `web_metric`) used by downstream analytics.

## Metrics overview
- **Global/regional cycles:** PCA-derived factors for business, trade, commodity, inflation, and financial conditions with coverage metadata.
- **Country metrics:** Risk energy/food/import dependence, inflation pressure, household/housing stress, credit excess, resilience composites, and climate/transition risk scores.
- **ESG metrics:** Sovereign ESG pillar inputs and composite `ESG_TOTAL_SOV` scores built from external providers and internal risk factors.
- **Graph/web metrics:** Centrality, propagation, relationship dependence, and web concentration metrics persisted to graph metric tables.

## Migrations and tests
- **Migrations:** Apply Alembic migrations from the `alembic/` directory (configure `DATABASE_URL` in the environment) before running ingestion or metrics jobs.
- **Tests:** Pytest is used for unit and smoke coverage. See [README_tests.md](README_tests.md) for the intended layout and upcoming test orchestration details.

## Getting started with Docker and Postgres
- Start the database: `docker-compose up -d postgres`.
- Apply migrations: `docker-compose run --rm backend alembic upgrade head`.
- Database connectivity across the app, Alembic, and compose is controlled via `DATABASE_URL`; the compose file sets `postgresql+psycopg2://economy:economy@postgres:5432/economy_dev` by default.
- ENUM migrations that alter types (for example adding new edge types) are executed in Alembic autocommit blocks to satisfy PostgreSQL requirements; follow the same pattern for future ENUM changes.

## Running locally
1. Create a virtual environment and install dependencies:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
2. Configure environment variables (see `app/config/__init__.py`) including `DATABASE_URL` plus provider API keys (FRED, EIA, COMTRADE, AISSTREAM).
3. Start the API server:
   ```bash
   uvicorn app.main:app --reload
   ```

The repository is backend-only; no UI frameworks or frontend assets are included.
