"""update graph schema with enums and new fields

Revision ID: 0008_update_graph_schema
Revises: 0007_create_sovereign_esg_raw
Create Date: 2025-02-11
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0008_update_graph_schema"
down_revision = "0007_create_sovereign_esg_raw"
branch_labels = None
depends_on = None

GRAPH_SCHEMA = "graph"


node_type_enum = postgresql.ENUM(
    "COUNTRY",
    "COUNTRY_SECTOR",
    "SECTOR_GLOBAL",
    "INFRASTRUCTURE",
    "FIN_SYSTEM",
    "FIN_INSTRUMENT",
    "INSTITUTION",
    "REGIME",
    "INDICATOR",
    "ENV_SYSTEM",
    "WEB",
    "EVENT",
    name="node_type_enum",
    schema=GRAPH_SCHEMA,
)

node_category_enum = postgresql.ENUM(
    "PRODUCTION_SUPPLY",
    "DEMAND_CONSUMPTION",
    "TRANSPORT_INFRA",
    "MARKETS_FINANCE",
    "POLICY_REGULATION",
    "ENVIRONMENT_NATURAL",
    "WEB",
    name="node_category_enum",
    schema=GRAPH_SCHEMA,
)

value_chain_enum = postgresql.ENUM(
    "UPSTREAM",
    "MIDSTREAM",
    "DOWNSTREAM",
    "END_USER",
    "CROSS_CUTTING",
    name="value_chain_position_enum",
    schema=GRAPH_SCHEMA,
)

scale_level_enum = postgresql.ENUM("MACRO", "MESO", "MICRO", name="scale_level_enum", schema=GRAPH_SCHEMA)

structural_role_enum = postgresql.ENUM(
    "CORE",
    "PERIPHERY",
    "BRIDGE",
    "BOTTLENECK",
    "LEAF",
    "UNKNOWN",
    name="structural_role_enum",
    schema=GRAPH_SCHEMA,
)

edge_type_enum = postgresql.ENUM(
    "FLOW",
    "INFLUENCE",
    "MEMBERSHIP",
    "CONSTRAINT",
    name="edge_type_enum",
    schema=GRAPH_SCHEMA,
)

rel_family_enum = postgresql.ENUM(
    "SUPPLY",
    "DEMAND",
    "TRADE",
    "TRANSPORT",
    "FINANCE",
    "OWNERSHIP",
    "PRICE_LINK",
    "POLICY_IMPACT",
    "INFO_LINK",
    "RISK",
    "MEMBERSHIP",
    "OTHER",
    name="rel_family_enum",
    schema=GRAPH_SCHEMA,
)

flow_type_enum = postgresql.ENUM(
    "MATERIAL",
    "ENERGY",
    "MONEY",
    "INFORMATION",
    "PEOPLE",
    "EMISSIONS",
    "RISK",
    name="flow_type_enum",
    schema=GRAPH_SCHEMA,
)

layer_id_enum = postgresql.ENUM(
    "PRODUCTION",
    "TRADE",
    "FINANCIAL",
    "POLICY",
    "INFORMATION",
    "CLIMATE_ESG",
    "RISK_SCENARIO",
    name="layer_id_enum",
    schema=GRAPH_SCHEMA,
)

edge_direction_enum = postgresql.ENUM("OUT", "IN", "BIDIR", name="edge_direction_enum", schema=GRAPH_SCHEMA)

impact_sign_enum = postgresql.ENUM("+", "-", "ambiguous", name="impact_sign_enum", schema=GRAPH_SCHEMA)

impact_strength_enum = postgresql.ENUM("weak", "medium", "strong", name="impact_strength_enum", schema=GRAPH_SCHEMA)

certainty_enum = postgresql.ENUM(
    "STRUCTURAL",
    "SCENARIO",
    "EMPIRICAL",
    name="certainty_enum",
    schema=GRAPH_SCHEMA,
)


def upgrade() -> None:
    bind = op.get_bind()
    for enum_type in [
        node_type_enum,
        node_category_enum,
        value_chain_enum,
        scale_level_enum,
        structural_role_enum,
        edge_type_enum,
        rel_family_enum,
        flow_type_enum,
        layer_id_enum,
        edge_direction_enum,
        impact_sign_enum,
        impact_strength_enum,
        certainty_enum,
    ]:
        enum_type.create(bind, checkfirst=True)

    # Node columns
    op.add_column("node", sa.Column("name", sa.String(), nullable=True), schema=GRAPH_SCHEMA)
    op.add_column("node", sa.Column("node_category", sa.Enum(name="node_category_enum", schema=GRAPH_SCHEMA), nullable=True), schema=GRAPH_SCHEMA)
    op.add_column(
        "node",
        sa.Column("value_chain_position", sa.Enum(name="value_chain_position_enum", schema=GRAPH_SCHEMA), nullable=True),
        schema=GRAPH_SCHEMA,
    )
    op.add_column("node", sa.Column("scale_level", sa.Enum(name="scale_level_enum", schema=GRAPH_SCHEMA), nullable=True), schema=GRAPH_SCHEMA)
    op.add_column(
        "node",
        sa.Column(
            "structural_role",
            sa.Enum(name="structural_role_enum", schema=GRAPH_SCHEMA),
            nullable=False,
            server_default="UNKNOWN",
        ),
        schema=GRAPH_SCHEMA,
    )
    op.add_column("node", sa.Column("country_code", sa.String(length=3), nullable=True), schema=GRAPH_SCHEMA)
    op.add_column("node", sa.Column("region_code", sa.String(), nullable=True), schema=GRAPH_SCHEMA)
    op.add_column("node", sa.Column("sector_code", sa.String(), nullable=True), schema=GRAPH_SCHEMA)
    op.add_column("node", sa.Column("web_code", sa.String(), nullable=True), schema=GRAPH_SCHEMA)
    op.add_column("node", sa.Column("themes", postgresql.JSONB(astext_type=sa.Text()), nullable=True), schema=GRAPH_SCHEMA)
    op.add_column("node", sa.Column("tags_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True), schema=GRAPH_SCHEMA)
    op.add_column(
        "node",
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        schema=GRAPH_SCHEMA,
    )
    op.add_column(
        "node",
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        schema=GRAPH_SCHEMA,
    )

    op.execute("UPDATE graph.node SET name = COALESCE(label, ref_id, 'UNKNOWN') WHERE name IS NULL")
    op.execute("UPDATE graph.node SET node_type = UPPER(node_type) WHERE node_type IS NOT NULL")
    op.execute("UPDATE graph.node SET node_type = 'COUNTRY' WHERE node_type IN ('Country','country')")
    op.alter_column(
        "node",
        "node_type",
        existing_type=sa.String(),
        type_=sa.Enum(name="node_type_enum", schema=GRAPH_SCHEMA),
        nullable=False,
        schema=GRAPH_SCHEMA,
    )
    op.alter_column("node", "name", nullable=False, schema=GRAPH_SCHEMA)

    op.create_index("ix_node_country_code", "node", ["country_code"], schema=GRAPH_SCHEMA)
    op.create_index("ix_node_region_code", "node", ["region_code"], schema=GRAPH_SCHEMA)
    op.create_index("ix_node_sector_code", "node", ["sector_code"], schema=GRAPH_SCHEMA)
    op.create_index("ix_node_web_code", "node", ["web_code"], schema=GRAPH_SCHEMA)

    # Edge columns
    op.add_column(
        "edge",
        sa.Column("rel_family", sa.Enum(name="rel_family_enum", schema=GRAPH_SCHEMA), nullable=True),
        schema=GRAPH_SCHEMA,
    )
    op.add_column("edge", sa.Column("flow_type", sa.Enum(name="flow_type_enum", schema=GRAPH_SCHEMA), nullable=True), schema=GRAPH_SCHEMA)
    op.add_column("edge", sa.Column("layer_id", sa.Enum(name="layer_id_enum", schema=GRAPH_SCHEMA), nullable=True), schema=GRAPH_SCHEMA)
    op.add_column(
        "edge",
        sa.Column("direction", sa.Enum(name="edge_direction_enum", schema=GRAPH_SCHEMA), nullable=True),
        schema=GRAPH_SCHEMA,
    )
    op.add_column("edge", sa.Column("weight_type", sa.String(), nullable=True), schema=GRAPH_SCHEMA)
    op.add_column("edge", sa.Column("weight_value", sa.Numeric(), nullable=True), schema=GRAPH_SCHEMA)
    op.add_column(
        "edge", sa.Column("meta_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True), schema=GRAPH_SCHEMA
    )
    op.add_column("edge", sa.Column("impact_sign", sa.Enum(name="impact_sign_enum", schema=GRAPH_SCHEMA), nullable=True), schema=GRAPH_SCHEMA)
    op.add_column(
        "edge",
        sa.Column("impact_strength", sa.Enum(name="impact_strength_enum", schema=GRAPH_SCHEMA), nullable=True),
        schema=GRAPH_SCHEMA,
    )
    op.add_column("edge", sa.Column("certainty", sa.Enum(name="certainty_enum", schema=GRAPH_SCHEMA), nullable=True), schema=GRAPH_SCHEMA)
    op.add_column("edge", sa.Column("web_code", sa.String(), nullable=True), schema=GRAPH_SCHEMA)
    op.add_column(
        "edge",
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        schema=GRAPH_SCHEMA,
    )
    op.add_column(
        "edge",
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        schema=GRAPH_SCHEMA,
    )

    op.execute("UPDATE graph.edge SET edge_type = 'FLOW'")
    op.alter_column(
        "edge",
        "edge_type",
        existing_type=sa.String(),
        type_=sa.Enum(name="edge_type_enum", schema=GRAPH_SCHEMA),
        nullable=False,
        schema=GRAPH_SCHEMA,
    )

    op.create_index("ix_edge_source_node_id", "edge", ["source_node_id"], schema=GRAPH_SCHEMA)
    op.create_index("ix_edge_target_node_id", "edge", ["target_node_id"], schema=GRAPH_SCHEMA)
    op.create_index("ix_edge_web_layer", "edge", ["web_code", "layer_id"], schema=GRAPH_SCHEMA)

    # Metrics tables
    op.add_column("node_metric", sa.Column("frequency", sa.String(), nullable=True), schema=GRAPH_SCHEMA)
    op.add_column("node_metric", sa.Column("metric_theme", sa.String(), nullable=True), schema=GRAPH_SCHEMA)
    op.add_column("node_metric", sa.Column("meta_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True), schema=GRAPH_SCHEMA)
    op.add_column(
        "node_metric",
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        schema=GRAPH_SCHEMA,
    )
    op.add_column(
        "node_metric",
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        schema=GRAPH_SCHEMA,
    )

    op.add_column("web_metric", sa.Column("meta_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True), schema=GRAPH_SCHEMA)
    op.add_column(
        "web_metric",
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        schema=GRAPH_SCHEMA,
    )
    op.add_column(
        "web_metric",
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        schema=GRAPH_SCHEMA,
    )

    op.add_column("edge_metric", sa.Column("meta_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True), schema=GRAPH_SCHEMA)


def downgrade() -> None:
    op.drop_column("edge_metric", "meta_json", schema=GRAPH_SCHEMA)

    op.drop_column("web_metric", "updated_at", schema=GRAPH_SCHEMA)
    op.drop_column("web_metric", "created_at", schema=GRAPH_SCHEMA)
    op.drop_column("web_metric", "meta_json", schema=GRAPH_SCHEMA)

    op.drop_column("node_metric", "updated_at", schema=GRAPH_SCHEMA)
    op.drop_column("node_metric", "created_at", schema=GRAPH_SCHEMA)
    op.drop_column("node_metric", "meta_json", schema=GRAPH_SCHEMA)
    op.drop_column("node_metric", "metric_theme", schema=GRAPH_SCHEMA)
    op.drop_column("node_metric", "frequency", schema=GRAPH_SCHEMA)

    op.drop_index("ix_edge_web_layer", table_name="edge", schema=GRAPH_SCHEMA)
    op.drop_index("ix_edge_target_node_id", table_name="edge", schema=GRAPH_SCHEMA)
    op.drop_index("ix_edge_source_node_id", table_name="edge", schema=GRAPH_SCHEMA)

    op.alter_column("edge", "edge_type", existing_type=sa.Enum(name="edge_type_enum", schema=GRAPH_SCHEMA), type_=sa.String(), schema=GRAPH_SCHEMA)
    for col in [
        "updated_at",
        "created_at",
        "web_code",
        "certainty",
        "impact_strength",
        "impact_sign",
        "meta_json",
        "weight_value",
        "weight_type",
        "direction",
        "layer_id",
        "flow_type",
        "rel_family",
    ]:
        op.drop_column("edge", col, schema=GRAPH_SCHEMA)

    op.drop_index("ix_node_web_code", table_name="node", schema=GRAPH_SCHEMA)
    op.drop_index("ix_node_sector_code", table_name="node", schema=GRAPH_SCHEMA)
    op.drop_index("ix_node_region_code", table_name="node", schema=GRAPH_SCHEMA)
    op.drop_index("ix_node_country_code", table_name="node", schema=GRAPH_SCHEMA)

    op.alter_column("node", "node_type", existing_type=sa.Enum(name="node_type_enum", schema=GRAPH_SCHEMA), type_=sa.String(), schema=GRAPH_SCHEMA)
    op.alter_column("node", "name", nullable=True, schema=GRAPH_SCHEMA)
    for col in [
        "updated_at",
        "created_at",
        "tags_json",
        "themes",
        "web_code",
        "sector_code",
        "region_code",
        "country_code",
        "structural_role",
        "scale_level",
        "value_chain_position",
        "node_category",
        "name",
    ]:
        op.drop_column("node", col, schema=GRAPH_SCHEMA)

    for enum_type in [
        certainty_enum,
        impact_strength_enum,
        impact_sign_enum,
        edge_direction_enum,
        layer_id_enum,
        flow_type_enum,
        rel_family_enum,
        edge_type_enum,
        structural_role_enum,
        scale_level_enum,
        value_chain_enum,
        node_category_enum,
        node_type_enum,
    ]:
        enum_type.drop(op.get_bind(), checkfirst=True)
