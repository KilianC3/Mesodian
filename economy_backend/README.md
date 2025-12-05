# Economy Backend

Backend services for the economic webs and risk analytics platform. The codebase ingests external economic data, normalises it into warehouse and graph schemas, computes metrics, and serves read-only APIs over FastAPI.

## High-level architecture

- **Ingestion layer:** Async HTTP clients under `app/ingest/` fetch macro, trade, energy, finance, ESG, patent, shipping, and news data from providers such as FRED, IMF, World Bank, OECD, Comtrade, Eurostat, DB.nomics, and market feeds. Payloads and query params are recorded in the `raw` schema for reproducibility.【F:app/ingest/base_client.py†L1-L142】【F:app/db/models.py†L31-L134】
- **Feature layer:** Builders in `app/features/` assemble `CountryYearFeatures` panels that join macro indicators, trade balances, climate proxies, and data-quality signals to support downstream analytics.【F:app/features/build_country_year_features.py†L1-L196】
- **Graph layer:** Graph utilities and schema helpers in `app/graph/` create typed nodes and edges for production, trade, finance, policy, and resilience webs, storing them in `graph.*` tables alongside node/edge/web metrics.【F:app/graph/schema_helpers.py†L1-L195】【F:app/db/models.py†L285-L546】
- **Metrics and cycles:** Modules in `app/metrics/` compute country risk/resilience, ESG scores, and web/edge centrality, while `app/cycles/` extracts global and regional cycles for trade, business, inflation, and finance.【F:app/metrics/run_all.py†L1-L156】【F:app/cycles/global_trade_cycle.py†L1-L119】
- **API layer:** `app/main.py` wires FastAPI routers for reference data, time series, features, metrics, webs, dashboards, and health endpoints, exposing read-only JSON responses for consumers.【F:app/main.py†L1-L38】

## Data sources

- **Macro and finance:** FRED, IMF, World Bank (WDI), OECD, BIS, national statistics (DB.nomics), ECB SDW.
- **Trade and shipping:** UN Comtrade, FAOSTAT, UNCTAD, AISStream, shipping features.
- **Energy and commodities:** EIA, Ember, market price feeds (stooq, yfinance).
- **ESG and research:** External ESG providers, patents (PatentsView), research networks (OpenAlex), events/news (GDELT, RSS).
- **Regional and pools metadata:** Static pools and regional groupings under `app/config/` and `app/pools/` support deterministic joins.

## Database schemas

- **raw.*** – Provider payloads plus request metadata for replayable ingestion.【F:app/db/models.py†L31-L134】
- **warehouse.*** – Normalised country/indicator registries, time series, trade flows, country-year features, cycles, and ESG staging tables.【F:app/db/models.py†L139-L563】
- **graph.*** – Nodes, edges, node metrics, web metrics, and edge metrics describing multi-layer webs for production, trade, finance, and resilience analysis.【F:app/db/models.py†L285-L546】

## Metrics overview

- **Global/regional cycles:** PCA-based indices and regime classifications for trade, business, inflation, commodities, and finance written via `app/cycles/*` modules.【F:app/cycles/run_global_cycles.py†L1-L97】
- **Country scores:** Energy and food dependence, import criticality, household stress, credit excess, and resilience metrics drawn from country-year features and trade flows.【F:app/metrics/risk_energy.py†L1-L86】【F:app/metrics/risk_food.py†L1-L91】
- **Web and edge metrics:** Centrality, propagation, and relationship metrics derived from graph layers for downstream dashboards and analysis.【F:app/metrics/web_metrics.py†L1-L110】【F:app/graph/algorithms.py†L1-L110】
- **ESG:** Sovereign ESG pillar calculations and composite totals leveraging external provider data and internal risk metrics.【F:app/metrics/sovereign_esg.py†L1-L200】

## Running migrations and tests

- **Migrations:** Apply Alembic migrations under `alembic/` using the configured `DATABASE_URL` to materialise warehouse and graph schemas before running ingestion or metrics jobs.【F:alembic/env.py†L1-L93】
- **Tests:** Pytest exercises ingestion utilities, graph helpers, and metric computations. See [README_tests.md](README_tests.md) for layout and execution guidance.

## Getting started with Docker and Postgres

1. Build and start the database service:
   ```bash
   docker-compose up -d postgres
   ```
2. Apply migrations inside the backend container so Postgres has the warehouse and graph schemas:
   ```bash
   docker-compose run --rm backend alembic upgrade head
   ```
3. Override the backend command for ad-hoc tasks such as running tests with:
   ```bash
   docker-compose run --rm backend pytest
   ```

All services read `DATABASE_URL` from the environment. The same value must be supplied to docker-compose, Alembic (`alembic/env.py`), and the runtime configuration (`app/config/__init__.py`) to keep migrations, the ORM engine, and tests aligned.【F:docker-compose.yml†L1-L34】【F:app/config/__init__.py†L1-L64】【F:alembic/env.py†L1-L48】

## Operating the backend locally

1. Create and activate a virtual environment:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Configure environment variables (database URL and provider API keys) as described in `app/config/__init__.py`.
4. Run the FastAPI service:
   ```bash
   uvicorn app.main:app --reload
   ```

The repository focuses solely on backend ingestion, analytics, and APIs; no frontend components are included.
