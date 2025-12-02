import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.db.models import Base, Edge, EdgeMetric, Node, WebMetric
from app.metrics.run_web_metrics import compute_all_web_and_edge_metrics


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


def _seed_graph(session: Session, year: int) -> None:
    nodes = [
        Node(id=1, node_type="country", ref_type="country", ref_id="USA", label="USA"),
        Node(id=2, node_type="country", ref_type="country", ref_id="CAN", label="CAN"),
        Node(id=3, node_type="country", ref_type="country", ref_id="MEX", label="MEX"),
    ]
    session.add_all(nodes)
    edges = [
        Edge(
            id=1,
            source_node_id=1,
            target_node_id=2,
            edge_type="trade_web",
            weight=100.0,
            attrs={"year": year, "web_code": "TRADE"},
        ),
        Edge(
            id=2,
            source_node_id=2,
            target_node_id=3,
            edge_type="trade_web",
            weight=50.0,
            attrs={"year": year, "web_code": "TRADE"},
        ),
        Edge(
            id=3,
            source_node_id=3,
            target_node_id=1,
            edge_type="trade_web",
            weight=25.0,
            attrs={"year": year, "web_code": "TRADE"},
        ),
    ]
    session.add_all(edges)
    session.commit()


def test_web_and_edge_metrics(session: Session) -> None:
    year = 2023
    _seed_graph(session, year)

    compute_all_web_and_edge_metrics(session, year)

    web_metrics = session.query(WebMetric).all()
    assert {wm.metric_code for wm in web_metrics} == {
        "WEB_RISK_SCORE",
        "WEB_CONCENTRATION",
        "WEB_STRATEGIC_IMPORTANCE",
        "WEB_RESILIENCE",
        "WEB_PROPAGATION",
        "WEB_FRAGMENTATION",
    }

    for wm in web_metrics:
        assert wm.as_of_year == year
        assert 0.0 <= float(wm.value) <= 100.0

    edge_metrics = session.query(EdgeMetric).all()
    assert {em.metric_code for em in edge_metrics} == {
        "EDGE_DEPENDENCE",
        "EDGE_CRITICALITY",
        "EDGE_SUBSTITUTABILITY",
    }
    for em in edge_metrics:
        assert em.year == year
        assert 0.0 <= float(em.value) <= 100.0
