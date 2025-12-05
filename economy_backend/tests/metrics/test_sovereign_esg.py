"""Tests for country, cycle, and ESG metric calculations."""

import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.db.models import Base, Country, SovereignESGRaw
from app.metrics.sovereign_esg import compute_sovereign_esg_for_year
from app.metrics.utils import get_or_create_country_node, upsert_node_metric


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


def _seed_countries(session: Session) -> None:
    session.add_all(
        [
            Country(id="USA", name="United States", region="Americas", income_group="High"),
            Country(id="CAN", name="Canada", region="Americas", income_group="High"),
        ]
    )
    session.commit()


def _seed_internal_metrics(session: Session, year: int) -> None:
    usa_node = get_or_create_country_node(session, "USA")
    can_node = get_or_create_country_node(session, "CAN")

    upsert_node_metric(session, usa_node.id, "CLIMATE_TOTAL_RISK", year, 40.0)
    upsert_node_metric(session, can_node.id, "CLIMATE_TOTAL_RISK", year, 60.0)
    upsert_node_metric(session, usa_node.id, "HH_STRESS", year, 30.0)
    upsert_node_metric(session, can_node.id, "HH_STRESS", year, 50.0)
    upsert_node_metric(session, usa_node.id, "RISK_FOOD", year, 25.0)
    upsert_node_metric(session, can_node.id, "RISK_FOOD", year, 35.0)
    upsert_node_metric(session, usa_node.id, "CR_MACRO_FISCAL", year, 70.0)
    upsert_node_metric(session, can_node.id, "CR_MACRO_FISCAL", year, 80.0)
    upsert_node_metric(session, usa_node.id, "CR_FIN_SYSTEM", year, 65.0)
    upsert_node_metric(session, can_node.id, "CR_FIN_SYSTEM", year, 75.0)
    upsert_node_metric(session, usa_node.id, "POLICY_STANCE", year, 50.0)
    upsert_node_metric(session, can_node.id, "POLICY_STANCE", year, 55.0)
    session.commit()


def _seed_raw_esg(session: Session, year: int) -> None:
    entries = []
    next_id = 0
    for record in [
        ("USA", "WB_ESG", "ENV_CO2_PER_GDP", 1.0),
        ("CAN", "WB_ESG", "ENV_CO2_PER_GDP", 2.0),
        ("USA", "EPI", "EPI_TOTAL", 70.0),
        ("CAN", "EPI", "EPI_TOTAL", 80.0),
        ("USA", "ND_GAIN", "ND_GAIN_TOTAL", 60.0),
        ("CAN", "ND_GAIN", "ND_GAIN_TOTAL", 50.0),
        ("USA", "WB_ESG", "SOC_EDU_INDEX", 0.7),
        ("CAN", "WB_ESG", "SOC_EDU_INDEX", 0.8),
        ("USA", "WB_ESG", "SOC_HEALTH_INDEX", 0.6),
        ("CAN", "WB_ESG", "SOC_HEALTH_INDEX", 0.7),
        ("USA", "WGI", "CONTROL_OF_CORRUPTION", 0.5),
        ("CAN", "WGI", "CONTROL_OF_CORRUPTION", 0.8),
        ("USA", "WGI", "RULE_OF_LAW", 0.6),
        ("CAN", "WGI", "RULE_OF_LAW", 0.9),
    ]:
        next_id += 1
        entries.append(
            SovereignESGRaw(
                id=next_id,
                country_code=record[0],
                year=year,
                provider=record[1],
                indicator_code=record[2],
                value=record[3],
            )
        )
    session.add_all(entries)
    session.commit()


def test_sovereign_esg_scores(session: Session) -> None:
    year = 2023
    _seed_countries(session)
    _seed_internal_metrics(session, year)
    _seed_raw_esg(session, year)

    compute_sovereign_esg_for_year(session, year)

    metrics = {
        (m.metric_code, m.as_of_year): float(m.value)
        for m in session.execute(text("SELECT metric_code, as_of_year, value FROM graph.node_metric")).fetchall()
        if m.metric_code.startswith("ESG")
    }

    assert metrics
    for (metric_code, as_of_year), value in metrics.items():
        assert as_of_year == year
        assert 0.0 <= value <= 100.0
