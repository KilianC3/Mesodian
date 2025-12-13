# TASKMASTER_GUIDE

## 1. Architecture snapshot
- **Ingestion layer (`app/ingest/`)**: Provider clients and job orchestrators convert external macro, trade, finance, energy, ESG, research, and events feeds into normalized raw and warehouse tables. Data flows from async/base clients → provider-specific loaders → `warehouse.raw_series` and related staging tables tracked in `app/db/models.py`.
- **Feature layer (`app/features/`)**: Country-year feature builders such as `build_country_year_features.py` join curated indicators, trade balances, climate proxies, and data-quality flags into `warehouse.country_year_features`, providing the canonical panel for downstream analytics.
- **Graph layer (`app/graph/`)**: Schema helpers, algorithms, and builders create typed nodes and edges for production, trade, finance, and policy webs backed by `graph.node`, `graph.edge`, and metric tables. Graph utilities enrich webs with tags, layers, and structural roles used by metrics.
- **Metrics and cycles layer (`app/metrics/`, `app/cycles/`)**: Computes global/regional cycles, country risk/resilience, ESG scores, and web/edge metrics. Pipelines read country-year features and graph layers, then write to metric tables for consumption by APIs and dashboards.
- **API and orchestration layer (`app/main.py`, `app/metrics/run_all.py`, `app/cycles/run_global_cycles.py`)**: FastAPI entrypoint exposes read-only endpoints, while orchestration scripts coordinate ingestion, feature building, metrics, and cycles over migrated schemas.
- **Testing and infra (`tests/`, `alembic/`, `docker-compose.yml`)**: Tests are split into unit, integration, and smoke coverage; Alembic manages schema migrations; Docker Compose provides Postgres plus backend container alignment via `DATABASE_URL`.

## 2. Module inventory
- **app/config/**: Central configuration (`__init__.py`) loads environment variables such as `DATABASE_URL`, provider credentials, and feature toggles; static region and pool metadata lives under `country_universe.py` and `pools/`.
- **app/db/**: Database engine helpers (`engine.py`) and ORM models (`models.py`) define raw, warehouse, and graph schemas; integrates with Alembic migrations under `alembic/versions/`.
- **app/ingest/**: Base HTTP client abstractions plus provider-specific loaders for macro, trade, finance, energy, ESG, patents, research, and event streams; writes requests/payloads to raw tables and normalized series to warehouse tables.
- **app/features/**: Builders that assemble country-year feature panels and related helpers for indicator selection and data quality scoring.
- **app/graph/**: Schema helpers and algorithms to construct webs, compute centrality, tag structural roles, and manage node/edge/web metrics.
- **app/metrics/**: Country metrics (energy, food, credit, household stress), ESG scoring, cycle-aware risk/resilience calculations, and web/edge metric calculators; orchestrated via `run_all.py`.
- **app/cycles/**: Cycle detection utilities and runners for global/regional trade, business, inflation, commodity, and finance indices.
- **app/api/**: FastAPI routers exposing read-only endpoints for reference data, time series, features, metrics, webs, and health checks wired in `app/main.py`.
- **tests/**: Domain folders (`ingest`, `features`, `graph`, `metrics`, `api`, `smoke`) with pytest markers for unit vs integration; shared fixtures in `tests/conftest.py` migrate schemas and provide DB sessions.
- **infra root**: Dockerfile and `docker-compose.yml` run backend plus Postgres; `alembic.ini` and `alembic/env.py` consume `DATABASE_URL`.

## 3. Coding rules for agents
1. **Avoid orphan modules**: New modules must be imported by orchestration or APIs, covered by at least one test, and referenced in docs when they introduce new concepts. Remove or refactor dead code instead of adding islands.
2. **Integration first**: Connect new features into existing pipelines (ingest → warehouse → features → metrics/graph). Ensure migrations accompany schema changes and tests exercise the integrated flow.
3. **Centralized configuration**: Read database and provider settings through `app/config/__init__.py`; do not hardcode URLs or secrets. Use environment variables wired through Docker, Alembic, and tests.
4. **Testing expectations**: Cover non-trivial logic with unit tests; mock external HTTP in ingestion tests; validate metrics with range/sign assertions; vet graph algorithms on synthetic webs; mark DB-dependent flows as `integration` or `smoke`.
5. **Documentation standards**: Every public module, class, and function needs a clear docstring that matches actual behaviour. Update README.md, README_tests.md, and this guide when introducing new metrics, sources, webs, or workflows.
6. **Controlled scope**: Prefer focused changes within a single layer. When cross-cutting updates are required, document the intent and affected modules here before coding.

## 4. Workflow for future changes
1. **Assess**: Scan the repo to locate the correct layer (ingest, features, graph, metrics, cycles, API, tests) for the planned change.
2. **Plan**: Add a short note to the Change log below describing planned modifications, target modules, and new tests before implementing.
3. **Implement**: Apply code and tests cohesively; avoid unused helpers or unreferenced modules. Align migrations with ORM changes.
4. **Verify**: Run relevant unit/integration suites; if DB schema changes, execute Alembic upgrade against a test database (e.g., docker-compose Postgres) before merging.
5. **Document**: Update top-level docs and this guide when adding new concepts, data sources, metrics, or graph layers so future agents understand integration points.

## 5. Change log
- 2025-03-17 – Agent – Added TASKMASTER_GUIDE.md capturing architecture snapshot, module inventory, coding rules, workflow, and change log for future agents.
- 2025-03-18 – Agent – Reviewed schema alignment and ensured the trade exposure enum migration executes in autocommit mode to satisfy PostgreSQL requirements; reinforced guidance on keeping migrations consistent with ORM models.
- 2025-03-19 – Agent – Realigned API integration tests to reuse Alembic-migrated database fixtures instead of SQLite scaffolding, ensuring endpoint coverage reflects the Postgres schema.
- 2025-03-20 – Agent – Hardened API integration test imports by enforcing `DATABASE_URL` presence and injecting the repository root onto `sys.path` within shared pytest fixtures to avoid module resolution conflicts.
- 2025-03-21 – Agent – Resolved lingering merge concerns in the API integration test by cleaning dependency override teardown to prevent cross-test contamination.
