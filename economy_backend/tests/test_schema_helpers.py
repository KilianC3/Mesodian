from app.db.models import (
    Certainty,
    Direction,
    EdgeType,
    ImpactStrength,
    ImpactSign,
    NodeType,
    StructuralRole,
)
from app.graph.schema_helpers import (
    FlowType,
    LayerId,
    NodeCategory,
    RelationshipFamily,
    ScaleLevel,
    ValueChainPosition,
    make_country_node,
    make_country_sector_node,
    make_flow_edge,
    make_web_anchor_node,
)


def test_make_country_node_defaults() -> None:
    node = make_country_node("USA", "Americas", name="United States")

    assert node.node_type == NodeType.COUNTRY
    assert node.node_category == NodeCategory.POLICY_REGULATION
    assert node.value_chain_position == ValueChainPosition.CROSS_CUTTING
    assert node.scale_level == ScaleLevel.MACRO
    assert node.structural_role == StructuralRole.UNKNOWN
    assert node.country_code == "USA"
    assert node.region_code == "Americas"


def test_make_country_sector_node_themes_and_position() -> None:
    node = make_country_sector_node(
        country_code="USA",
        sector_code="ENERGY_UTILITIES",
        web_code="WEB-ENERGY",
        value_chain_position=ValueChainPosition.UPSTREAM,
        node_category=NodeCategory.PRODUCTION_SUPPLY,
    )

    assert node.node_type == NodeType.COUNTRY_SECTOR
    assert node.node_category == NodeCategory.PRODUCTION_SUPPLY
    assert node.value_chain_position == ValueChainPosition.UPSTREAM
    assert node.scale_level == ScaleLevel.MESO
    assert "ENERGY" in node.themes


def test_make_web_anchor_node() -> None:
    node = make_web_anchor_node("WEB-1", "Energy Web", ["ENERGY", "CLIMATE"])

    assert node.node_type == NodeType.WEB
    assert node.node_category == NodeCategory.WEB
    assert node.value_chain_position == ValueChainPosition.CROSS_CUTTING
    assert node.scale_level == ScaleLevel.MESO
    assert node.themes == ["ENERGY", "CLIMATE"]


def test_make_flow_edge_defaults() -> None:
    edge = make_flow_edge(
        source_node_id=1,
        target_node_id=2,
        rel_family=RelationshipFamily.TRADE,
        flow_type=FlowType.MONEY,
        layer_id=LayerId.TRADE,
        weight_type="VALUE_USD",
        weight_value=100.0,
        impact_sign=ImpactSign.POSITIVE,
        impact_strength=ImpactStrength.MEDIUM,
    )

    assert edge.edge_type == EdgeType.FLOW
    assert edge.rel_family == RelationshipFamily.TRADE
    assert edge.flow_type == FlowType.MONEY
    assert edge.layer_id == LayerId.TRADE
    assert edge.direction == Direction.OUT
    assert edge.weight_type == "VALUE_USD"
    assert float(edge.weight_value) == 100.0
    assert edge.certainty == Certainty.STRUCTURAL
    assert edge.impact_sign == ImpactSign.POSITIVE
    assert edge.impact_strength == ImpactStrength.MEDIUM
    assert edge.meta_json == {}

