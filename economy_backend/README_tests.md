# Test Suite – economy_backend

This backend uses pytest with explicit markers to separate fast unit coverage from database-backed integration flows. The suite is organised by domain to mirror the codebase and to keep ingestion, feature building, graph projections, and metrics aligned.

## Layout

- `tests/ingest/` – provider clients, ingestion jobs, and warehouse landing logic.
- `tests/features/` – country-year feature builders and related transforms.
- `tests/graph/` – graph schema helpers, projections, centrality, and web/edge metrics.
- `tests/metrics/` – country metrics, cycles, ESG calculations, and timing helpers.
- `tests/api/` – FastAPI endpoints for reference data, series, features, metrics, and webs.
- `tests/smoke/` – end-to-end synthetic pipeline exercising migrations, features, metrics, webs, and cycles.

## Running the tests

All commands assume Docker is available and that `docker-compose.yml` starts both the backend and Postgres services.

```bash
# Start Postgres
docker-compose up -d postgres

# Apply migrations
docker-compose run --rm backend alembic upgrade head

# Full suite
docker-compose run --rm backend pytest -q

# Unit tests only
docker-compose run --rm backend pytest -m "unit" -q

# Integration tests only
docker-compose run --rm backend pytest -m "integration" -q

# Smoke test only
docker-compose run --rm backend pytest tests/smoke/test_smoke_end_to_end.py -q
```

The test environment relies on `DATABASE_URL` to point at the Postgres service (set in `docker-compose.yml`). External API calls are mocked or bypassed by fixtures; integration and smoke tests use synthetic data inserted directly into the database after Alembic migrations are applied.
