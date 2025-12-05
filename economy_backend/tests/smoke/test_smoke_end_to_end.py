"""Integration smoke test that runs a synthetic pipeline end to end."""

import datetime as dt

import pytest
from sqlalchemy import func, text

from app.db.models import (
    Country,
    CountryYearFeatures,
    Edge,
    EdgeMetric,
    EdgeType,
    GlobalCycleIndex,
    Indicator,
    LayerId,
    Node,
    NodeMetric,
    NodeType,
    ShippingCountryMonth,
    SovereignESGRaw,
    TimeSeriesValue,
    WebMetric,
)
from app.features.build_country_year_features import build_country_year_features
from app.metrics.graph_centrality import compute_graph_centrality_and_roles
from app.metrics.sovereign_esg import compute_sovereign_esg_for_year
from app.metrics.utils import get_or_create_country_node

pytestmark = pytest.mark.integration


def _seed_time_series(session, country_code: str, indicator_map: dict[int, tuple[str, float]]) -> None:
    for ind_id, (canonical_code, value) in indicator_map.items():
        session.add(
            Indicator(
                id=ind_id,
                source="TEST",
                source_code=canonical_code,
                canonical_code=canonical_code,
                frequency="annual",
            )
        )
        session.add(
            TimeSeriesValue(
                indicator_id=ind_id,
                country_id=country_code,
                date=dt.date(2023, 12, 31),
                value=value,
            )
        )


def _seed_shipping(session, country_code: str, start_id: int) -> None:
    monthly_values = [10, 15, 18]
    for idx, activity in enumerate(monthly_values, start=start_id):
        session.add(
            ShippingCountryMonth(
                id=idx,
                country_id=country_code,
                year=2023,
                month=idx,
                activity_level=activity,
            )
        )


def _insert_cycle_rows(session) -> None:
    session.add_all(
        [
            GlobalCycleIndex(
                id=1,
                date=dt.date(2023, 1, 1),
                frequency="monthly",
                scope="global",
                cycle_type="credit",
                cycle_score=0.5,
                cycle_regime="expansion",
                method_version="v1",
            ),
            GlobalCycleIndex(
                id=2,
                date=dt.date(2023, 1, 1),
                frequency="monthly",
                scope="region:Europe",
                cycle_type="credit",
                cycle_score=0.3,
                cycle_regime="expansion",
                method_version="v1",
            ),
        ]
    )


@pytest.fixture()
def populated_session(db_session):
    session = db_session
    session.execute(text("SET search_path TO raw,warehouse,graph,public"))
    session.add_all(
        [
            Country(id="USA", name="United States", region="Americas", income_group="High income"),
            Country(id="DEU", name="Germany", region="Europe", income_group="High income"),
        ]
    )
    _seed_time_series(
        session,
        "USA",
        {
            1: ("GDP_REAL", 1000.0),
            2: ("CPI_YOY", 4.0),
            3: ("CA_PCT_GDP", -1.0),
            4: ("DEBT_PCT_GDP", 60.0),
            5: ("UNEMP_RATE", 5.0),
            6: ("CO2_PER_CAPITA", 10.0),
            7: ("ENERGY_IMPORT_DEP", 25.0),
            8: ("FOOD_IMPORT_DEP", 15.0),
            9: ("GDELT_EVENT_COUNT", 4.0),
            10: ("POLICY_RATE_CHANGE_FLAG", 1.0),
        },
    )
    _seed_time_series(
        session,
        "DEU",
        {
            11: ("GDP_REAL", 800.0),
            12: ("CPI_YOY", 3.0),
            13: ("CA_PCT_GDP", 2.0),
            14: ("DEBT_PCT_GDP", 55.0),
            15: ("UNEMP_RATE", 4.5),
            16: ("CO2_PER_CAPITA", 8.0),
            17: ("ENERGY_IMPORT_DEP", 20.0),
            18: ("FOOD_IMPORT_DEP", 10.0),
            19: ("GDELT_EVENT_COUNT", 2.0),
            20: ("POLICY_RATE_CHANGE_FLAG", 1.0),
        },
    )
    _seed_shipping(session, "USA", start_id=1)
    _seed_shipping(session, "DEU", start_id=4)
    session.flush()
    return session


def test_smoke_pipeline(populated_session):
    session = populated_session
    year = 2023

    build_country_year_features(session, year)

    usa_node = get_or_create_country_node(session, "USA", name="United States")
    deu_node = get_or_create_country_node(session, "DEU", name="Germany")
    session.add(
        Edge(
            id=1,
            source_node_id=usa_node.id,
            target_node_id=deu_node.id,
            edge_type=EdgeType.FLOW,
            layer_id=LayerId.TRADE,
            weight_type="VALUE_USD",
            weight_value=100.0,
            meta_json={"year": year},
        )
    )
    session.flush()

    compute_graph_centrality_and_roles(session, year)

    metric_start = session.query(func.coalesce(func.max(NodeMetric.id), 0)).scalar() or 0
    session.add_all(
        [
            NodeMetric(id=metric_start + 1, node_id=usa_node.id, metric_code="CR_RESILIENCE", as_of_year=year, value=75.0),
            NodeMetric(id=metric_start + 2, node_id=usa_node.id, metric_code="CLIMATE_TOTAL_RISK", as_of_year=year, value=15.0),
            NodeMetric(id=metric_start + 3, node_id=usa_node.id, metric_code="HH_STRESS", as_of_year=year, value=20.0),
            NodeMetric(id=metric_start + 4, node_id=usa_node.id, metric_code="RISK_FOOD", as_of_year=year, value=10.0),
            NodeMetric(id=metric_start + 5, node_id=usa_node.id, metric_code="CR_MACRO_FISCAL", as_of_year=year, value=80.0),
            NodeMetric(id=metric_start + 6, node_id=usa_node.id, metric_code="CR_POLITICAL_INSTITUTIONS", as_of_year=year, value=82.0),
        ]
    )

    _insert_cycle_rows(session)

    session.add_all(
        [
            SovereignESGRaw(id=1, country_code="USA", provider="WB_ESG", indicator_code="ENV_CO2_PER_GDP", year=year, value=5.0),
            SovereignESGRaw(id=2, country_code="USA", provider="EPI", indicator_code="EPI_TOTAL", year=year, value=60.0),
            SovereignESGRaw(id=3, country_code="USA", provider="ND_GAIN", indicator_code="ND_GAIN_TOTAL", year=year, value=55.0),
            SovereignESGRaw(id=4, country_code="USA", provider="WB_ESG", indicator_code="SOC_EDU_INDEX", year=year, value=70.0),
            SovereignESGRaw(id=5, country_code="USA", provider="WB_ESG", indicator_code="SOC_HEALTH_INDEX", year=year, value=72.0),
            SovereignESGRaw(id=6, country_code="USA", provider="WGI", indicator_code="CONTROL_OF_CORRUPTION", year=year, value=0.5),
            SovereignESGRaw(id=7, country_code="USA", provider="WGI", indicator_code="RULE_OF_LAW", year=year, value=0.6),
        ]
    )

    session.add_all(
        [
            WebMetric(id=1, web_code="WEB-TRADE", as_of_year=year, metric_code="DENSITY", value=0.5),
            EdgeMetric(
                id=1,
                source_node_id=usa_node.id,
                target_node_id=deu_node.id,
                web_code="WEB-TRADE",
                year=year,
                metric_code="EDGE_WEIGHT",
                value=100.0,
            ),
        ]
    )

    session.commit()
    compute_sovereign_esg_for_year(session, year)

    features = (
        session.query(CountryYearFeatures)
        .filter(CountryYearFeatures.year == year)
        .all()
    )
    assert any(feature.country_id == "USA" for feature in features)
    assert any(feature.country_id == "DEU" for feature in features)

    metrics = session.query(NodeMetric).filter(NodeMetric.metric_code.in_(["CR_RESILIENCE", "CLIMATE_TOTAL_RISK", "ESG_TOTAL_SOV"])).all()
    assert metrics

    cycles = session.query(GlobalCycleIndex).filter(GlobalCycleIndex.scope.in_(["global", "region:Europe"])).all()
    assert len(cycles) == 2

    web_metrics = session.query(WebMetric).filter(WebMetric.web_code == "WEB-TRADE").all()
    edge_metrics = session.query(EdgeMetric).filter(EdgeMetric.web_code == "WEB-TRADE").all()
    assert web_metrics and edge_metrics

    roles = session.query(Node).filter(Node.structural_role != "UNKNOWN").all()
    assert roles
