import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.db.models import Base, Node, NodeCategory, StructuralRole  # noqa: E402
from app.graph.schema_helpers import (  # noqa: E402
    FlowType,
    LayerId,
    RelationshipFamily,
    make_country_node,
    make_flow_edge,
)
from app.metrics.graph_centrality import compute_graph_centrality_and_roles  # noqa: E402


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
    hub = make_country_node(country_code="HUB", region_code="NA", node_id=1, name="Hub")
    leaf_a = make_country_node(country_code="LA", region_code="NA", node_id=2, name="Leaf A")
    bridge = make_country_node(country_code="BR", region_code="NA", node_id=3, name="Bridge")
    leaf_b = make_country_node(country_code="LB", region_code="NA", node_id=4, name="Leaf B")
    tail = make_country_node(country_code="TL", region_code="NA", node_id=5, name="Tail")
    session.add_all([hub, leaf_a, bridge, leaf_b, tail])

    edges = [
        make_flow_edge(
            edge_id=1,
            source_node_id=hub.id,
            target_node_id=leaf_a.id,
            rel_family=RelationshipFamily.TRADE,
            flow_type=FlowType.MONEY,
            layer_id=LayerId.TRADE,
            weight_type="VALUE_USD",
            weight_value=5.0,
            meta_json={"year": year},
        ),
        make_flow_edge(
            edge_id=2,
            source_node_id=hub.id,
            target_node_id=bridge.id,
            rel_family=RelationshipFamily.TRADE,
            flow_type=FlowType.MONEY,
            layer_id=LayerId.TRADE,
            weight_type="VALUE_USD",
            weight_value=3.0,
            meta_json={"year": year},
        ),
        make_flow_edge(
            edge_id=3,
            source_node_id=bridge.id,
            target_node_id=leaf_b.id,
            rel_family=RelationshipFamily.TRADE,
            flow_type=FlowType.MONEY,
            layer_id=LayerId.TRADE,
            weight_type="VALUE_USD",
            weight_value=2.0,
            meta_json={"year": year},
        ),
        make_flow_edge(
            edge_id=4,
            source_node_id=bridge.id,
            target_node_id=tail.id,
            rel_family=RelationshipFamily.TRADE,
            flow_type=FlowType.MONEY,
            layer_id=LayerId.TRADE,
            weight_type="VALUE_USD",
            weight_value=1.0,
            meta_json={"year": year},
        ),
        make_flow_edge(
            edge_id=5,
            source_node_id=leaf_a.id,
            target_node_id=hub.id,
            rel_family=RelationshipFamily.TRADE,
            flow_type=FlowType.MONEY,
            layer_id=LayerId.TRADE,
            weight_type="VALUE_USD",
            weight_value=0.5,
            meta_json={"year": year},
        ),
    ]
    session.add_all(edges)
    session.commit()


def test_graph_centrality_and_roles(session: Session) -> None:
    year = 2023
    _seed_graph(session, year)

    compute_graph_centrality_and_roles(session, year)

    metrics = session.execute(
        text(
            """
            SELECT metric_code, node_id, value
            FROM graph.node_metric
            WHERE metric_code LIKE 'CENT_%'
            """
        )
    ).fetchall()

    metric_codes = {row[0] for row in metrics}
    assert {
        "CENT_DEGREE_OUT_TRADE",
        "CENT_DEGREE_IN_TRADE",
        "CENT_EIG_TRADE",
        "CENT_BETWEEN_TRADE",
    }.issubset(metric_codes)

    nodes = {node.country_code: node for node in session.query(Node).all()}
    assert nodes["HUB"].structural_role == StructuralRole.CORE
    assert nodes["BR"].structural_role == StructuralRole.BRIDGE
    assert nodes["LB"].structural_role == StructuralRole.LEAF
    assert nodes["TL"].structural_role == StructuralRole.LEAF
    assert nodes["LA"].structural_role in {StructuralRole.PERIPHERY, StructuralRole.LEAF}

    for node in nodes.values():
        assert node.node_category == NodeCategory.POLICY_REGULATION
