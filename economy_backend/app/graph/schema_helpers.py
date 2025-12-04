"""Schema helper utilities for constructing graph nodes and edges.

These helpers centralise enum validation and deterministic tagging rules so that
graph construction code avoids duplicating string literals. They wrap the core
SQLAlchemy models to ensure nodes and edges are instantiated with consistent
defaults across trade, energy, finance, and macro ingestion pipelines.
"""

from __future__ import annotations

import enum

from typing import List, Optional, Sequence

from app.db.models import (
    Certainty,
    Direction,
    Edge,
    EdgeType,
    FlowType,
    ImpactSign,
    ImpactStrength,
    LayerId,
    Node,
    NodeCategory,
    NodeType,
    RelationshipFamily,
    ScaleLevel,
    StructuralRole,
    ValueChainPosition,
)


def _coerce_enum(value: str | enum.Enum, enum_cls: type[enum.Enum]) -> enum.Enum:
    """Coerce strings into the provided Enum class, raising on invalid values."""

    if isinstance(value, enum_cls):
        return value
    try:
        return enum_cls(value)
    except ValueError as exc:  # pragma: no cover - defensive branch
        raise ValueError(f"Invalid value '{value}' for enum {enum_cls.__name__}") from exc


def _infer_themes_from_sector(sector_code: Optional[str]) -> List[str]:
    """Derive simple thematic tags from a sector code.

    The mapping is intentionally lightweight and deterministic so ingestion
    pipelines can reuse it without relying on external configuration.
    """

    if not sector_code:
        return []

    sector_upper = sector_code.upper()
    if any(keyword in sector_upper for keyword in ("ENERGY", "POWER", "OIL", "GAS")):
        return ["ENERGY"]
    if any(keyword in sector_upper for keyword in ("AGRO", "AGRI", "FOOD")):
        return ["AGRI"]
    if "FIN" in sector_upper:
        return ["FINANCE"]
    if any(keyword in sector_upper for keyword in ("CLIMATE", "ESG", "WATER")):
        return ["CLIMATE", "ESG"]
    return []


def make_country_node(
    country_code: str,
    region_code: Optional[str],
    *,
    node_id: Optional[int] = None,
    name: Optional[str] = None,
    node_category: NodeCategory = NodeCategory.POLICY_REGULATION,
    themes: Optional[Sequence[str]] = None,
    tags_json: Optional[dict] = None,
) -> Node:
    """Create a country node with consistent cross-cutting defaults.

    Defaults follow the schema guidance:
    - node_type: COUNTRY
    - node_category: POLICY_REGULATION (overrideable for finance-centric views)
    - value_chain_position: CROSS_CUTTING
    - scale_level: MACRO
    - structural_role: UNKNOWN
    """

    themes = list(themes) if themes is not None else []
    tags_json = tags_json or {}

    coerced_category = _coerce_enum(node_category, NodeCategory)

    return Node(
        id=node_id,
        name=name or country_code,
        label=name or country_code,
        ref_type="country",
        ref_id=country_code,
        node_type=NodeType.COUNTRY,
        node_category=coerced_category,
        value_chain_position=ValueChainPosition.CROSS_CUTTING,
        scale_level=ScaleLevel.MACRO,
        structural_role=StructuralRole.UNKNOWN,
        country_code=country_code,
        region_code=region_code,
        themes=themes,
        tags_json=tags_json,
    )


def make_country_sector_node(
    country_code: str,
    sector_code: str,
    web_code: Optional[str],
    value_chain_position: ValueChainPosition,
    node_category: NodeCategory,
    *,
    node_id: Optional[int] = None,
    name: Optional[str] = None,
    structural_role: StructuralRole = StructuralRole.UNKNOWN,
) -> Node:
    """Create a country-sector node with deterministic tags and themes."""

    themes = _infer_themes_from_sector(sector_code)
    return Node(
        id=node_id,
        name=name or f"{country_code}-{sector_code}",
        label=name or f"{country_code}-{sector_code}",
        node_type=NodeType.COUNTRY_SECTOR,
        node_category=_coerce_enum(node_category, NodeCategory),
        value_chain_position=_coerce_enum(value_chain_position, ValueChainPosition),
        scale_level=ScaleLevel.MESO,
        structural_role=_coerce_enum(structural_role, StructuralRole),
        country_code=country_code,
        sector_code=sector_code,
        web_code=web_code,
        themes=themes,
        tags_json={},
    )


def make_web_anchor_node(
    web_code: str, name: str, theme_tags: Sequence[str], *, node_id: Optional[int] = None
) -> Node:
    """Create a logical web anchor node for graph construction."""

    return Node(
        id=node_id,
        name=name,
        label=name,
        node_type=NodeType.WEB,
        node_category=NodeCategory.WEB,
        value_chain_position=ValueChainPosition.CROSS_CUTTING,
        scale_level=ScaleLevel.MESO,
        structural_role=StructuralRole.UNKNOWN,
        web_code=web_code,
        themes=list(theme_tags),
        tags_json={},
    )


def make_flow_edge(
    source_node_id: int,
    target_node_id: int,
    rel_family: RelationshipFamily,
    flow_type: FlowType,
    layer_id: LayerId,
    weight_type: str,
    weight_value: Optional[float],
    *,
    edge_id: Optional[int] = None,
    direction: Direction = Direction.OUT,
    impact_sign: Optional[ImpactSign | str] = None,
    impact_strength: Optional[ImpactStrength | str] = None,
    certainty: Optional[Certainty | str] = Certainty.STRUCTURAL,
    meta_json: Optional[dict] = None,
) -> Edge:
    """Create a flow edge with validated enums and default metadata."""

    meta_json = meta_json or {}
    return Edge(
        id=edge_id,
        source_node_id=source_node_id,
        target_node_id=target_node_id,
        edge_type=EdgeType.FLOW,
        rel_family=_coerce_enum(rel_family, RelationshipFamily),
        flow_type=_coerce_enum(flow_type, FlowType),
        layer_id=_coerce_enum(layer_id, LayerId),
        direction=_coerce_enum(direction, Direction),
        weight_type=weight_type,
        weight_value=weight_value,
        meta_json=meta_json,
        impact_sign=_coerce_enum(impact_sign, ImpactSign) if impact_sign else None,
        impact_strength=_coerce_enum(impact_strength, ImpactStrength) if impact_strength else None,
        certainty=_coerce_enum(certainty, Certainty) if certainty else None,
    )

