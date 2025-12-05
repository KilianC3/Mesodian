# Test Suite – economy_backend

This document outlines how the backend test suite is organised and how it will evolve as infrastructure tasks complete.

## 1. Target layout

Tests live under `tests/` and are grouped by backend domain to mirror the codebase:

- **tests/ingest/** – provider clients, ingestion jobs, and raw/warehouse landing logic.
- **tests/features/** – country-year feature builders, transforms, and data-quality checks.
- **tests/graph/** – graph schema helpers, projection utilities, and edge/node metrics.
- **tests/metrics/** – country, web, and edge metrics plus ESG computations.
- **tests/cycles/** – global and regional cycle extraction and regime mapping.
- **tests/api/** – FastAPI endpoints for reference data, time series, features, metrics, and webs.
- **tests/smoke/** – lightweight sanity checks that exercise the main pipelines end-to-end.

## 2. Expectations

- Integration with Docker, Postgres, and live/mocked data toggles will be added in later tasks.
- Existing files in the root `tests/` directory will be migrated into the above layout while preserving coverage.

## 3. Running tests (current state)

Until the infra updates land, activate your virtualenv and run:

```bash
pytest
```

Environment variables such as `DATABASE_URL` and provider API keys are read by the settings layer; defaults in tests avoid contacting real services. This document will expand with Docker-compose and marker guidance once the test infrastructure is refactored.
