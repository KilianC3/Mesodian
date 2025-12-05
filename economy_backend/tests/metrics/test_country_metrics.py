"""Computation of economic, ESG, and timing metrics."""


import datetime as dt
import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
pytestmark = pytest.mark.integration
from sqlalchemy.orm import Session, sessionmaker

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.db.models import (
    Base,
    Country,
    CountryYearFeatures,
    Indicator,
    Node,
    NodeMetric,
    NodeMetricContrib,
    TimeSeriesValue,
    TradeFlow,
)
from app.metrics.run_all import compute_all_country_metrics


@pytest.fixture()
def session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS raw"))
        conn.execute(text("ATTACH DATABASE ':memory:' AS warehouse"))
        conn.execute(text("ATTACH DATABASE ':memory:' AS graph"))
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def _seed_reference_data(session: Session) -> None:
    countries = [
        Country(id="USA", name="United States", region="Americas", income_group="High"),
        Country(id="CAN", name="Canada", region="Americas", income_group="High"),
    ]
    session.add_all(countries)

    indicator = Indicator(id=1, source="TEST", source_code="PAT", canonical_code="GREEN_PATENTS")
    session.add(indicator)
    session.commit()


def _seed_features(session: Session, year: int) -> None:
    # Previous year for emission deltas
    session.add_all(
        [
            CountryYearFeatures(country_id="USA", year=year - 1, co2_per_capita=3.0),
            CountryYearFeatures(country_id="CAN", year=year - 1, co2_per_capita=1.5),
        ]
    )

    session.add_all(
        [
            CountryYearFeatures(
                country_id="USA",
                year=year,
                gdp_growth=2.0,
                inflation_cpi=3.0,
                ca_pct_gdp=1.0,
                debt_pct_gdp=50.0,
                unemployment_rate=5.0,
                co2_per_capita=4.0,
                energy_import_dep=30.0,
                food_import_dep=20.0,
                shipping_activity_change=-10.0,
                event_stress_pulse=2.0,
                data_coverage_score=90.0,
                data_freshness_score=95.0,
            ),
            CountryYearFeatures(
                country_id="CAN",
                year=year,
                gdp_growth=4.0,
                inflation_cpi=2.0,
                ca_pct_gdp=3.0,
                debt_pct_gdp=40.0,
                unemployment_rate=6.0,
                co2_per_capita=2.0,
                energy_import_dep=20.0,
                food_import_dep=30.0,
                shipping_activity_change=5.0,
                event_stress_pulse=1.0,
                data_coverage_score=85.0,
                data_freshness_score=90.0,
            ),
        ]
    )
    session.commit()



def _seed_timeseries(session: Session, year: int) -> None:
    session.add_all(
        [
            TimeSeriesValue(
                indicator_id=1,
                country_id="USA",
                date=dt.date(year, 6, 30),
                value=120.0,
                source="TEST",
            ),
            TimeSeriesValue(
                indicator_id=1,
                country_id="CAN",
                date=dt.date(year, 6, 30),
                value=80.0,
                source="TEST",
            ),
        ]
    )
    session.commit()



def _seed_trade_flows(session: Session, year: int) -> None:
    session.add_all(
        [
            TradeFlow(
                reporter_country_id="USA",
                partner_country_id="BRA",
                year=year,
                hs_section=None,
                flow_type="import",
                value_usd=60.0,
            ),
            TradeFlow(
                reporter_country_id="USA",
                partner_country_id="MEX",
                year=year,
                hs_section=None,
                flow_type="import",
                value_usd=40.0,
            ),
            TradeFlow(
                reporter_country_id="CAN",
                partner_country_id="USA",
                year=year,
                hs_section=None,
                flow_type="import",
                value_usd=50.0,
            ),
            TradeFlow(
                reporter_country_id="CAN",
                partner_country_id="MEX",
                year=year,
                hs_section=None,
                flow_type="import",
                value_usd=50.0,
            ),
        ]
    )
    session.commit()


def _get_metric(session: Session, country_id: str, code: str):
    node = session.query(Node).filter(Node.ref_id == country_id, Node.ref_type == "country").one()
    return (
        session.query(NodeMetric)
        .filter(NodeMetric.node_id == node.id, NodeMetric.metric_code == code)
        .one()
    )


def test_compute_all_country_metrics(session: Session) -> None:
    year = 2023
    _seed_reference_data(session)
    _seed_features(session, year)
    _seed_timeseries(session, year)
    _seed_trade_flows(session, year)

    compute_all_country_metrics(session, year)

    # Resilience pillars and aggregate
    usa_resilience = _get_metric(session, "USA", "CR_RESILIENCE")
    can_resilience = _get_metric(session, "CAN", "CR_RESILIENCE")
    assert pytest.approx(float(usa_resilience.value), rel=1e-4) == -0.88
    assert pytest.approx(float(can_resilience.value), rel=1e-4) == 0.88

    # Food and energy risks
    usa_food_risk = _get_metric(session, "USA", "RISK_FOOD")
    can_food_risk = _get_metric(session, "CAN", "RISK_FOOD")
    assert pytest.approx(float(usa_food_risk.value), rel=1e-4) == 27.5
    assert pytest.approx(float(can_food_risk.value), rel=1e-4) == 31.5

    usa_energy_risk = _get_metric(session, "USA", "RISK_ENERGY")
    can_energy_risk = _get_metric(session, "CAN", "RISK_ENERGY")
    assert pytest.approx(float(usa_energy_risk.value), rel=1e-4) == 33.0
    assert pytest.approx(float(can_energy_risk.value), rel=1e-4) == 25.0

    # Transition risk
    usa_transition = _get_metric(session, "USA", "RISKOP_TRANSITION")
    can_transition = _get_metric(session, "CAN", "RISKOP_TRANSITION")
    assert pytest.approx(float(usa_transition.value), rel=1e-3) == 72.844
    assert pytest.approx(float(can_transition.value), rel=1e-3) == 27.152

    # Data quality metrics
    usa_coverage = _get_metric(session, "USA", "DQ_COVERAGE")
    usa_freshness = _get_metric(session, "USA", "DQ_FRESHNESS")
    assert float(usa_coverage.value) == pytest.approx(90.0)
    assert float(usa_freshness.value) == pytest.approx(95.0)

    # Feature level contributions for resilience exist
    resilience_metric = _get_metric(session, "USA", "CR_RESILIENCE")
    contribs = (
        session.query(NodeMetricContrib)
        .filter(NodeMetricContrib.node_metric_id == resilience_metric.id)
        .all()
    )
    assert contribs
