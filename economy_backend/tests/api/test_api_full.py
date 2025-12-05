"""Integration coverage for FastAPI endpoints using a migrated database."""


import datetime as dt
import os
from typing import Generator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.orm import Session

pytestmark = pytest.mark.integration

if not os.environ.get("DATABASE_URL"):
    pytest.skip("DATABASE_URL is required for API integration tests", allow_module_level=True)

# Configure environment for Settings defaults used by the API app
os.environ.setdefault("FRED_API_KEY", "test")
os.environ.setdefault("EIA_API_KEY", "test")
os.environ.setdefault("COMTRADE_API_KEY", "test")
os.environ.setdefault("AISSTREAM_API_KEY", "test")

from app.db.engine import get_db  # noqa: E402
from app.db.models import (  # noqa: E402
    Asset,
    Country,
    CountryYearFeatures,
    Edge,
    EdgeType,
    Indicator,
    LayerId,
    Node,
    NodeMetric,
    NodeType,
    TimeSeriesValue,
    TradeFlow,
)
from app.main import app  # noqa: E402


@pytest.fixture()
def seeded_session(db_session: Session) -> Session:
    countries = [
        Country(id="USA", name="United States", region="Americas", income_group="High income"),
        Country(id="DEU", name="Germany", region="Europe", income_group="High income"),
        Country(id="MEX", name="Mexico", region="Americas", income_group="Upper middle income"),
    ]
    db_session.add_all(countries)

    indicators = [
        Indicator(id=1, source="WDI", source_code="NY.GDP.MKTP.KD", canonical_code="GDP_REAL", frequency="annual", unit="USD", category="Macro"),
        Indicator(id=2, source="YF", source_code="SPY", canonical_code="ASSET_SPY", frequency="daily", unit=None, category="Market"),
    ]
    db_session.add_all(indicators)

    db_session.add_all(
        [
            TimeSeriesValue(indicator_id=1, country_id="USA", date=dt.date(2022, 12, 31), value=20000),
            TimeSeriesValue(indicator_id=1, country_id="USA", date=dt.date(2023, 12, 31), value=20500),
            TimeSeriesValue(indicator_id=1, country_id="DEU", date=dt.date(2022, 12, 31), value=15000),
        ]
    )

    asset = Asset(id=1, symbol="SPY", name="SPDR S&P 500 ETF Trust", asset_type="equity", country_id=None, region="US")
    db_session.add(asset)
    db_session.execute(
        text(
            """
            INSERT INTO warehouse.asset_price
            (id, asset_id, date, open, high, low, close, adj_close, volume)
            VALUES
            (:id1, :asset_id, :date1, :open1, :high1, :low1, :close1, :adj_close1, :volume1),
            (:id2, :asset_id, :date2, :open2, :high2, :low2, :close2, :adj_close2, :volume2)
            """
        ),
        {
            "id1": 1,
            "id2": 2,
            "asset_id": 1,
            "date1": dt.date(2024, 1, 2),
            "date2": dt.date(2024, 1, 3),
            "open1": 10,
            "high1": 11,
            "low1": 9,
            "close1": 10.5,
            "adj_close1": 10.4,
            "volume1": 1000,
            "open2": 10.5,
            "high2": 11.2,
            "low2": 10.1,
            "close2": 11,
            "adj_close2": 11,
            "volume2": 1500,
        },
    )

    features = CountryYearFeatures(
        country_id="USA",
        year=2023,
        gdp_real=21000,
        gdp_growth=2.3,
        inflation_cpi=3.1,
        ca_pct_gdp=-2.1,
        debt_pct_gdp=110.0,
        unemployment_rate=4.0,
        co2_per_capita=15.2,
        energy_import_dep=0.25,
        food_import_dep=0.12,
        shipping_activity_level=1.5,
        shipping_activity_change=0.08,
        event_stress_pulse=0.3,
        data_coverage_score=92.0,
        data_freshness_score=88.0,
    )
    db_session.add(features)

    nodes = [
        Node(id=1, name="United States", node_type=NodeType.COUNTRY, ref_type="country", ref_id="USA", label="United States", country_code="USA"),
        Node(id=2, name="Germany", node_type=NodeType.COUNTRY, ref_type="country", ref_id="DEU", label="Germany", country_code="DEU"),
        Node(id=3, name="Mexico", node_type=NodeType.COUNTRY, ref_type="country", ref_id="MEX", label="Mexico", country_code="MEX"),
    ]
    db_session.add_all(nodes)

    db_session.execute(
        text(
            """
            INSERT INTO graph.node_metric (id, node_id, metric_code, as_of_year, value)
            VALUES
            (:id1, :node1, 'CR_RESILIENCE', 2023, 78.5),
            (:id2, :node1, 'NET_SYS_IMPORTANCE', 2023, 88.0),
            (:id3, :node2, 'CR_RESILIENCE', 2023, 80.1)
            """
        ),
        {"id1": 1, "id2": 2, "id3": 3, "node1": 1, "node2": 2},
    )

    db_session.add_all(
        [
            Edge(
                id=1,
                source_node_id=1,
                target_node_id=2,
                edge_type=EdgeType.FLOW,
                layer_id=LayerId.TRADE,
                weight_type="VALUE_USD",
                weight_value=350000000000,
                meta_json={"year": 2023},
            ),
            Edge(
                id=2,
                source_node_id=1,
                target_node_id=3,
                edge_type=EdgeType.FLOW,
                layer_id=LayerId.TRADE,
                weight_type="VALUE_USD",
                weight_value=210000000000,
                meta_json={"year": 2023},
            ),
        ]
    )

    db_session.add_all(
        [
            TradeFlow(reporter_country_id="USA", partner_country_id="DEU", year=2023, hs_section=None, flow_type="import", value_usd=350000000000),
            TradeFlow(reporter_country_id="USA", partner_country_id="MEX", year=2023, hs_section=None, flow_type="import", value_usd=210000000000),
        ]
    )

    db_session.commit()
    return db_session


@pytest.fixture()
def client(seeded_session: Session) -> Generator[TestClient, None, None]:
    """Provide a TestClient wired to the seeded session and clean overrides after use."""

    def override_get_db():
        yield seeded_session

    app.dependency_overrides[get_db] = override_get_db
    client = TestClient(app)
    try:
        yield client
    finally:
        app.dependency_overrides.pop(get_db, None)


def test_reference_countries(client: TestClient):
    response = client.get("/api/reference/countries")
    assert response.status_code == 200
    payload = response.json()
    ids = {c["id"] for c in payload["countries"]}
    assert "USA" in ids
    assert "DEU" in ids


def test_reference_indicators_filter(client: TestClient):
    response = client.get("/api/reference/indicators", params={"category": "Macro"})
    assert response.status_code == 200
    data = response.json()["indicators"]
    assert len(data) == 1
    assert data[0]["canonical_code"] == "GDP_REAL"


def test_timeseries_country(client: TestClient):
    response = client.get("/api/timeseries/country/USA")
    assert response.status_code == 200
    series = response.json()["series"]
    assert any(entry["indicator_code"] == "GDP_REAL" for entry in series)


def test_asset_timeseries(client: TestClient):
    response = client.get("/api/timeseries/asset/SPY")
    assert response.status_code == 200
    prices = response.json()["prices"]
    assert len(prices) == 2


def test_features_endpoint(client: TestClient):
    response = client.get("/api/features/country/USA/2023")
    assert response.status_code == 200
    body = response.json()
    assert body["features"]["gdp_real"] == 21000


def test_metrics_endpoint(client: TestClient):
    response = client.get("/api/metrics/country/USA/2023")
    assert response.status_code == 200
    assert response.json()["metrics"]["CR_RESILIENCE"] == 78.5


def test_webs_trade(client: TestClient):
    response = client.get("/api/webs/trade/2023")
    assert response.status_code == 200
    data = response.json()
    assert len(data["nodes"]) == 3
    assert len(data["edges"]) == 2


def test_dashboard(client: TestClient):
    response = client.get("/api/dashboard/country/USA/2023")
    assert response.status_code == 200
    data = response.json()
    assert data["trade_summary"]["net_system_importance"] == 88.0
    assert len(data["trade_summary"]["top_partners"]) == 2
