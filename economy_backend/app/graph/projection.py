from __future__ import annotations

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.db.models import Country, Edge, Node, NodeType, TradeFlow
from app.graph.schema_helpers import (
    FlowType,
    LayerId,
    RelationshipFamily,
    make_country_node,
    make_flow_edge,
)


def project_country_nodes(session: Session) -> None:
    existing_nodes = {node.country_code: node for node in session.query(Node).filter(Node.node_type == NodeType.COUNTRY).all()}
    countries = session.query(Country).all()

    next_id = session.query(func.coalesce(func.max(Node.id), 0)).scalar() or 0

    for country in countries:
        if country.id in existing_nodes:
            continue
        next_id += 1
        node = make_country_node(
            country_code=country.id,
            region_code=country.region,
            node_id=next_id,
            name=country.name,
        )
        session.add(node)
    session.commit()


def project_trade_edges(session: Session, year: int) -> None:
    country_nodes = {
        node.country_code: node for node in session.query(Node).filter(Node.node_type == NodeType.COUNTRY).all()
    }

    trade_flows = session.query(TradeFlow).filter(TradeFlow.year == year).all()
    next_id = session.query(func.coalesce(func.max(Edge.id), 0)).scalar() or 0

    for flow in trade_flows:
        source_node = country_nodes.get(flow.reporter_country_id)
        target_node = country_nodes.get(flow.partner_country_id)
        if not source_node or not target_node:
            continue

        next_id += 1
        edge = make_flow_edge(
            edge_id=next_id,
            source_node_id=source_node.id,
            target_node_id=target_node.id,
            rel_family=RelationshipFamily.TRADE,
            flow_type=FlowType.MONEY,
            layer_id=LayerId.TRADE,
            weight_type="VALUE_USD",
            weight_value=flow.value_usd,
            meta_json={
                "hs_section": flow.hs_section,
                "flow_type": flow.flow_type,
                "year": flow.year,
            },
        )
        session.add(edge)

    session.commit()
