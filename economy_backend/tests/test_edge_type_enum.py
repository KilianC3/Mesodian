import sys
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.db.models import Base, Edge, EdgeType, Node, NodeType, StructuralRole  # noqa: E402


def _make_session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS raw"))
        conn.execute(text("ATTACH DATABASE ':memory:' AS warehouse"))
        conn.execute(text("ATTACH DATABASE ':memory:' AS graph"))
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)
    return SessionLocal()


def test_trade_exposure_edge_type_round_trip() -> None:
    session = _make_session()
    try:
        source_node = Node(
            id=1,
            name="Source",
            node_type=NodeType.COUNTRY,
            structural_role=StructuralRole.UNKNOWN,
        )
        target_node = Node(
            id=2,
            name="Target",
            node_type=NodeType.COUNTRY,
            structural_role=StructuralRole.UNKNOWN,
        )
        session.add_all([source_node, target_node])
        session.commit()

        session.add(
            Edge(
                id=1,
                source_node_id=source_node.id,
                target_node_id=target_node.id,
                edge_type=EdgeType.TRADE_EXPOSURE,
            )
        )
        session.commit()

        stored_edge = (
            session.query(Edge)
            .filter(Edge.edge_type == EdgeType.TRADE_EXPOSURE)
            .one()
        )

        assert stored_edge.edge_type == EdgeType.TRADE_EXPOSURE
    finally:
        session.close()
