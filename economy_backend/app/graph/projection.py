from __future__ import annotations

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.db.models import Country, Edge, Node, TradeFlow


def project_country_nodes(session: Session) -> None:
    existing_nodes = {
        node.ref_id: node
        for node in session.query(Node)
        .filter(Node.node_type == "Country", Node.ref_type == "Country")
        .all()
    }
    countries = session.query(Country).all()

    next_id = session.query(func.coalesce(func.max(Node.id), 0)).scalar() or 0

    for country in countries:
        if country.id in existing_nodes:
            continue
        next_id += 1
        node = Node(
            id=next_id,
            node_type="Country",
            ref_type="Country",
            ref_id=country.id,
            label=country.name,
            category_role="Macro",
            system_layer="Macro",
        )
        session.add(node)
    session.commit()


def project_trade_edges(session: Session, year: int) -> None:
    country_nodes = {
        node.ref_id: node
        for node in session.query(Node)
        .filter(Node.node_type == "Country", Node.ref_type == "Country")
        .all()
    }

    trade_flows = session.query(TradeFlow).filter(TradeFlow.year == year).all()
    next_id = session.query(func.coalesce(func.max(Edge.id), 0)).scalar() or 0

    for flow in trade_flows:
        source_node = country_nodes.get(flow.reporter_country_id)
        target_node = country_nodes.get(flow.partner_country_id)
        if not source_node or not target_node:
            continue

        next_id += 1
        edge = Edge(
            id=next_id,
            source_node_id=source_node.id,
            target_node_id=target_node.id,
            edge_type="trade_exposure",
            weight=flow.value_usd,
            attrs={
                "hs_section": flow.hs_section,
                "flow_type": flow.flow_type,
                "year": flow.year,
            },
        )
        session.add(edge)

    session.commit()
