import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.db.models import Base, Edge, EdgeMetric, Node  # noqa: E402
from app.metrics.web_relationship_metrics import compute_edge_relationship_metrics_for_year  # noqa: E402


@pytest.fixture()
def session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS raw"))
        conn.execute(text("ATTACH DATABASE ':memory:' AS warehouse"))
        conn.execute(text("ATTACH DATABASE ':memory:' AS graph"))
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def test_edge_metrics_are_computed(session: Session) -> None:
    nodes = [
        Node(id=1, node_type="COUNTRY", ref_type="country", ref_id="USA"),
        Node(id=2, node_type="COUNTRY", ref_type="country", ref_id="DEU"),
        Node(id=3, node_type="COUNTRY", ref_type="country", ref_id="FRA"),
    ]
    session.add_all(nodes)
    session.add_all(
        [
            Edge(id=1, source_node_id=1, target_node_id=2, edge_type="trade", weight=100, attrs={"year": 2023, "web_code": "TRADE"}),
            Edge(id=2, source_node_id=2, target_node_id=3, edge_type="trade", weight=50, attrs={"year": 2023, "web_code": "TRADE"}),
            Edge(id=3, source_node_id=1, target_node_id=3, edge_type="trade", weight=25, attrs={"year": 2023, "web_code": "TRADE"}),
        ]
    )
    session.commit()

    compute_edge_relationship_metrics_for_year(session, 2023)

    metrics = session.query(EdgeMetric).all()
    assert metrics
    codes = {m.metric_code for m in metrics}
    assert {"EDGE_DEPENDENCE", "EDGE_CRITICALITY", "EDGE_SUBSTITUTABILITY"}.issubset(codes)
    for metric in metrics:
        assert 0.0 <= float(metric.value) <= 100.0
        assert metric.year == 2023
