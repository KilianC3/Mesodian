# Test Suite – economy_backend

This backend-only repository now organizes tests by domain and separates fast unit checks from database-backed integration and smoke coverage.

## Layout
- `tests/ingest/` – Provider client behavior and ingestion helpers with mocked HTTP responses and local SQLite fixtures.
- `tests/features/` – Country × year feature builders, data quality scoring, and feature transforms.
- `tests/graph/` – Graph schema helpers, projection algorithms, and centrality calculations.
- `tests/metrics/` – Country risk/resilience/climate/ESG metrics plus cycle computations.
- `tests/api/` – FastAPI routers and basic contract checks executed against ephemeral SQLite sessions.
- `tests/smoke/` – End-to-end synthetic pipeline that targets the Postgres container and Alembic-managed schema.

## Markers and commands
- Unit tests avoid external services and persistent databases. Run them with:
  - `docker-compose run --rm backend pytest -m "unit" -q`
- Integration tests depend on Postgres and Alembic migrations. Run them with:
  - `docker-compose run --rm backend pytest -m "integration" -q`
- Full suite (unit + integration):
  - `docker-compose run --rm backend pytest -q`
- Smoke pipeline only:
  - `docker-compose run --rm backend pytest tests/smoke/test_smoke_end_to_end.py -q`

## Additional notes
- Tests assume `DATABASE_URL` points to the Postgres container (`postgresql+psycopg2://economy:economy@postgres:5432/economy_dev`).
- External API calls are mocked; the smoke test seeds synthetic data instead of hitting real services.
- Alembic migrations (with ENUM autocommit blocks) are executed automatically by integration fixtures to keep schema alignment consistent with the application.
