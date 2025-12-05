# Taskmaster Guide

This guide summarizes the backend architecture and sets expectations for future agents (Taskmaster, Codex-style tools, and maintainers) so changes stay integrated, documented, and testable. It is meta-documentation only and does not alter runtime behavior.

## 1. Architecture snapshot
- **Ingestion layer** (`app/ingest/`, `app/extern/`): Provider clients (FRED, IMF, OECD, World Bank/WDI, DB.nomics/SDMX, ECB SDW, EIA/Ember, FAOSTAT/UNCTAD/Comtrade, AIS/shipping, markets, patents, research feeds) fetch external data and normalize payloads into `raw.*` landing tables and curated `warehouse.*` series. Cross-check helpers live under `app/qa/`.
- **Feature layer** (`app/features/`): Country × Year builders (for example `build_country_year_features.py`, `shipping_features.py`, `data_quality.py`) assemble macro, trade, climate, shipping, and coverage indicators into `warehouse.country_year_features` and related feature tables.
- **Graph layer** (`app/db/models.py`, `app/graph/`): Node and edge schema definitions live with SQLAlchemy models. Graph utilities build multi-layer production/trade/finance/policy webs, manage projections, and provide algorithms/centrality helpers for populating `graph.node`, `graph.edge`, `graph.node_metric`, `graph.edge_metric`, and `graph.web_metric`.
- **Metrics and cycles layer** (`app/metrics/`, `app/cycles/`):
  - Cycle computation modules derive global/regional factors stored in `warehouse.global_cycle_index` and helpers orchestrate refreshes (`run_global_cycles.py`, `run_regional_cycles.py`).
  - Country metrics (risk/resilience, dependence, inflation pressure, credit excess, housing/household stress, transition/physical climate risk, data quality) aggregate features into metric tables.
  - Graph metrics (`graph_centrality.py`, `web_metrics.py`, `web_relationship_metrics.py`) and ESG scoring (`sovereign_esg.py`) persist to graph metric tables.
  - Orchestrators (`run_all.py`, `run_web_metrics.py`) coordinate bulk runs across metrics and webs.
- **API and orchestration layer** (`app/main.py`, `app/api/`): FastAPI routers expose reference data, time series, features, metrics, webs, dashboards, and health endpoints. CLI entry points inside ingest/metrics modules support offline rebuilds.
- **Tests and tooling** (`tests/`, `app/tests/`, `config/`): Tests are organized by domain (ingest, features, graph, metrics, smoke) with pytest markers for unit vs. integration. Infrastructure files (`Dockerfile`, `docker-compose.yml`, `alembic/`) support Postgres-backed execution; Alembic migrations rely on `DATABASE_URL` with autocommit blocks for ENUM changes.

Data flows: external sources → `raw.*` staging → normalized `warehouse.*` series → `warehouse.country_year_features` and `warehouse.global_cycle_index` → metrics and ESG scores → graph webs and metric tables → API exposure.

## 2. Module inventory
- **app/config/**: Centralized settings (regions, country universes, environment-driven configuration). `app/db/engine.py` consumes `DATABASE_URL` from here.
- **app/db/**: SQLAlchemy engine/session helpers, schema models for warehouse and graph tables, and seeding utilities for reference data.
- **app/ingest/**: One client per provider (e.g., `fred_client.py`, `imf_client.py`, `dbnomics_*`, `comtrade_client.py`, `energy clients`), plus base utilities and ingestion jobs; normalize data into raw and warehouse tables. `app/extern/` holds auxiliary reference loaders (e.g., income classifications).
- **app/features/**: Feature builders that transform normalized series into country-year panels, shipping-derived indicators, transforms, and data-quality summaries.
- **app/graph/**: Schema helpers, projection utilities, and graph algorithms (centrality, role assignment) used to build and analyze webs.
- **app/metrics/**: Country metrics, composite scores, climate and ESG routines, graph/web metrics, and orchestration scripts (`run_all.py`, `run_web_metrics.py`).
- **app/cycles/**: Global and regional cycle computation modules and runners that populate cycle indices.
- **app/api/**: FastAPI route modules for reference data, time series, features, metrics, webs, dashboards, and health checks.
- **app/pools/**: Portfolio pool definitions and loader utilities (`loader.py`, YAML configs).
- **app/qa/**: Quality-assurance helpers (e.g., DB.nomics cross-checks) supporting ingestion validation.
- **tests/**: Domain-organized pytest suite (`ingest/`, `features/`, `graph/`, `metrics/`, `smoke/`, optional `api/`) plus shared fixtures in `tests/conftest.py`. Integration tests rely on Postgres via `DATABASE_URL` and Alembic migrations.
- **config/**: Environment/sample configuration artifacts leveraged by pipelines or tests.
- **alembic/**: Migration environment and versioned scripts (ENUM alterations wrapped in autocommit blocks). `alembic/env.py` reads `DATABASE_URL` for engine creation.
- **docker-compose.yml / Dockerfile**: Container setup for backend plus Postgres, enabling migrations and pytest inside the composed stack.

## 3. Coding rules for agents
1. **No useless or orphan modules**: New modules must be invoked by orchestrators, APIs, or pipelines, referenced in README/TASKMASTER_GUIDE when substantive, and covered by at least one test. Avoid dead code islands.
2. **Integration first**: Wire new functionality into existing ingestion/feature/metrics/graph flows. If writing to the database, add coherent Alembic migrations (use autocommit for ENUM alterations) and include integration tests where applicable.
3. **Configuration discipline**: Use central config for `DATABASE_URL` and provider settings; avoid scattered hard-coded endpoints or secrets. Prefer environment variables and shared helpers.
4. **Testing expectations**: Non-trivial logic requires tests. Ingestion code should mock external HTTP responses; metrics should assert expected ranges/signs; graph algorithms should use small synthetic graphs; end-to-end changes should extend smoke/integration coverage.
5. **Documentation required**: Public functions/classes need docstrings. New modules require a module-level docstring following the established pattern. Update README and this guide when introducing new concepts (metrics, sources, webs).
6. **Minimal scope creep**: Keep changes scoped to the relevant layer. If a feature spans layers, document the plan here first and ensure each touched layer has corresponding tests and docs.

## 4. Workflow for future changes
1. **Scan**: Review the repo to locate the correct layer (ingest, features, graph, metrics, tests, infra) and existing orchestrators that should invoke the change.
2. **Plan**: Add a brief plan under the Change log section before coding, noting modules to modify and new code/tests to add.
3. **Implement**: Make coherent code and test updates; avoid unused functions or unreferenced modules. Align database writes with migrations and schema helpers.
4. **Verify**: Run targeted tests (unit/integration) and, when schema-affecting, execute Alembic upgrades against Postgres. Ensure `DATABASE_URL` is set consistently.
5. **Document**: Update README, README_tests.md, and this guide when adding new metrics, sources, webs, or architectural components.

## 5. Change log
- 2025-02-02 – Agent – Added Taskmaster guide summarizing architecture, module responsibilities, coding rules, and workflow expectations for future backend changes.
- 2025-02-14 – Agent – Corrected ingestion base_client module docstring syntax to restore valid imports and reconfirmed ENUM migrations rely on autocommit blocks.
- 2025-02-18 – Agent – Standardized database URL handling to accept DATABASE_URL or POSTGRES_URL via Pydantic env list and maintained explicit validation messaging.
