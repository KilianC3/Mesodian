import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.db.models import (
    Base,
    Country,
    Edge,
    Node,
    NodeCategory,
    NodeMetric,
    NodeType,
    ScaleLevel,
    TradeFlow,
    ValueChainPosition,
)  # noqa: E402
from app.graph.schema_helpers import Direction, FlowType, LayerId, RelationshipFamily
from app.graph.algorithms import compute_trade_centrality  # noqa: E402
from app.graph.projection import project_country_nodes, project_trade_edges  # noqa: E402


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
            Country(id="MEX", name="Mexico", region="Americas", income_group="Upper"),
        ]
    )
    session.commit()


def _seed_trade_flows(session: Session, year: int) -> None:
    session.add_all(
        [
            TradeFlow(
                reporter_country_id="USA",
                partner_country_id="CAN",
                year=year,
                hs_section="01",
                flow_type="export",
                value_usd=100.0,
            ),
            TradeFlow(
                reporter_country_id="CAN",
                partner_country_id="MEX",
                year=year,
                hs_section="02",
                flow_type="import",
                value_usd=50.0,
            ),
            TradeFlow(
                reporter_country_id="MEX",
                partner_country_id="USA",
                year=year,
                hs_section="03",
                flow_type="export",
                value_usd=200.0,
            ),
        ]
    )
    session.commit()


def test_project_and_centrality(session: Session) -> None:
    year = 2022
    _seed_countries(session)
    _seed_trade_flows(session, year)

    project_country_nodes(session)
    project_trade_edges(session, year)
    compute_trade_centrality(session, year)

    nodes = session.query(Node).all()
    assert len(nodes) == 3
    assert all(node.node_type == NodeType.COUNTRY for node in nodes)
    assert all(node.node_category == NodeCategory.POLICY_REGULATION for node in nodes)
    assert all(node.value_chain_position == ValueChainPosition.CROSS_CUTTING for node in nodes)
    assert all(node.scale_level == ScaleLevel.MACRO for node in nodes)

    node_map = {node.country_code: node for node in nodes}

    edges = session.query(Edge).all()
    assert len(edges) == 3

    expected_connections = {
        ("USA", "CAN", "01", "export", 100.0),
        ("CAN", "MEX", "02", "import", 50.0),
        ("MEX", "USA", "03", "export", 200.0),
    }
    seen_connections = set()
    for edge in edges:
        source_ref = next(ref for ref, node in node_map.items() if node.id == edge.source_node_id)
        target_ref = next(ref for ref, node in node_map.items() if node.id == edge.target_node_id)
        seen_connections.add(
            (
                source_ref,
                target_ref,
                edge.meta_json.get("hs_section") if edge.meta_json else None,
                edge.meta_json.get("flow_type") if edge.meta_json else None,
                float(edge.weight_value),
            )
        )
        assert edge.meta_json.get("year") == year
        assert edge.rel_family == RelationshipFamily.TRADE
        assert edge.flow_type == FlowType.MONEY
        assert edge.layer_id == LayerId.TRADE
        assert edge.direction == Direction.OUT
    assert seen_connections == expected_connections

    metrics = (
        session.query(NodeMetric)
        .filter(NodeMetric.metric_code == "NET_SYS_IMPORTANCE", NodeMetric.as_of_year == year)
        .all()
    )
    assert len(metrics) == 3

    metric_map = {metric.node_id: float(metric.value) for metric in metrics}
    usa_metric = metric_map[node_map["USA"].id]
    can_metric = metric_map[node_map["CAN"].id]
    mex_metric = metric_map[node_map["MEX"].id]

    assert pytest.approx(usa_metric, rel=1e-4) == 100.0
    assert pytest.approx(can_metric, rel=1e-4) == 0.0
    assert pytest.approx(mex_metric, rel=1e-4) == pytest.approx(66.6667, rel=1e-4)
