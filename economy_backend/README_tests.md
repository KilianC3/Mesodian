# Test Suite – economy_backend

This document explains how to run and understand the backend tests for the **economy_backend** project.

The goal of the tests is to give you a fast, local way to verify that the main backend components work end-to-end on synthetic or mocked data – without needing cloud services or real API keys.

---

## 1. Test layout

All tests live under the `tests/` directory and are grouped by backend domain:

- **Ingest**
  - `test_ingest_macro_core.py` – core macro/trade/finance ingestion into raw/warehouse tables with mocked HTTP providers.
  - `test_ingest_trade_finance_agri.py` – bilateral trade, financial flows, and agriculture series.
  - `test_ingest_energy_markets_events.py` – energy/commodity balances plus event/news series.
  - `test_ingest_dbnomics.py` – DB.nomics helper utilities and provider cross-checks.
  - `test_ingest_jobs.py` – ingestion job wiring and scheduling helpers.
  - `test_esg_ingestion.py` – ESG-related landing tables.

- **Features & metadata**
  - `test_features_country_year.py` – builds a minimal `CountryYearFeatures` panel for a sample of countries and years.
  - `test_country_regions_income.py` – validates country/region metadata and joins.
  - `test_catalogue_and_timing.py` – indicator catalogue wiring and timing-class assignments.
  - `test_indicator_timing.py` – timing utility functions for metric computations.

- **Graph**
  - `test_graph.py` – verifies the `Node`, `Edge`, `NodeMetric`, `WebMetric`, and `EdgeMetric` models and constraints.
  - `test_graph_centrality.py` – runs centrality algorithms and checks that `structural_role` is assigned correctly.
  - `test_schema_helpers.py` – deterministic node/edge tagging helpers.

- **Metrics & cycles**
  - `test_metrics.py` – country-level risk/resilience/climate metrics.
  - `test_edge_metrics.py` and `test_web_metrics.py` – web-level and edge-level metrics (concentration, propagation, dependence).
  - `test_global_cycles.py` – global/regional cycles (business, trade, commodities, inflation, financial conditions).
  - `test_sovereign_esg.py` – sovereign ESG pillar scores and the composite ESG_TOTAL_SOV.

- **API**
  - `test_api_endpoints.py` and `test_api_full.py` – ensure main HTTP endpoints respond and return correctly structured JSON using the FastAPI test client.

- **Foundations**
  - `test_base_client.py` – HTTP client retry/backoff handling.

> If some of these files do not exist yet, they will be added gradually as the implementation matures. The important point is that tests are grouped by **ingest / features / graph / metrics / api / foundations**.

---

## 2. Dependencies and environment

The tests are designed to run on a typical home computer without accessing real external services.

### 2.1 Install dependencies

From the repo root:

```bash
python -m venv .venv
source .venv/bin/activate    # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2.2 Environment variables

Most tests default to an in-memory SQLite database. For API clients, the following environment variables are read but defaulted to dummy values in tests:

```bash
export POSTGRES_URL="sqlite://"
export FRED_API_KEY="dummy"
export EIA_API_KEY="dummy"
export COMTRADE_API_KEY="dummy"
export AISSTREAM_API_KEY="dummy"
```

You can point `POSTGRES_URL` to a local Postgres instance if you want to inspect persisted data, but it is not required for the suite to pass.

---

## 3. Running the tests

From the repo root, with your virtual environment active:

```bash
pytest
```

Common subsets:

```bash
pytest tests/test_ingest_macro_core.py          # Focus on macro/finance ingestion
pytest tests/test_graph.py -k centrality        # Graph schema/centrality checks
pytest tests/test_global_cycles.py              # Cycle extraction and persistence
pytest tests/test_sovereign_esg.py              # ESG pillar and total scores
```

The suite uses small synthetic fixtures and mock HTTP responses, so no external network is needed. SQLite keeps runs self-contained and fast; Postgres-backed runs are helpful when you want to inspect the generated tables.
