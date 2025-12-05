# Test Suite – economy_backend

Backend test layout and future execution plan. This skeleton describes how the suite will be organized while infrastructure and fixtures are finalized.

## Layout (intended)
- `tests/ingest/` – Provider client tests with mocked HTTP/SDMX responses and warehouse upsert validation.
- `tests/features/` – Country × Year feature builders, data quality scoring, and transforms.
- `tests/graph/` – Graph schema helpers, projection algorithms, and centrality calculations.
- `tests/metrics/` – Country risk/resilience/climate metrics, ESG scores, and cycle computations.
- `tests/api/` – FastAPI routers for reference data, time series, features, metrics, webs, and dashboards.
- `tests/smoke/` – Lightweight end-to-end flows that run against SQLite or ephemeral Postgres instances.

## Notes
- Full Docker/Postgres/test wiring will be finalized in subsequent tasks as ingestion and metric pipelines are hardened.
- This document will be updated once the infrastructure and test cleanup tasks land; treat it as a guide for where new tests should live.
