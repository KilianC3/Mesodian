"""Add trade_exposure edge type to enum

Revision ID: 0009_add_trade_exposure_edge_type
Revises: 0008_update_graph_schema
Create Date: 2025-02-12
"""

from alembic import context, op

# revision identifiers, used by Alembic.
revision = "0009_add_trade_exposure_edge_type"
down_revision = "0008_update_graph_schema"
branch_labels = None
depends_on = None


EDGE_TYPE_VALUE = "trade_exposure"

def upgrade() -> None:
    ctx = op.get_context()
    with ctx.autocommit_block():
        op.execute(
            "ALTER TYPE graph.edge_type_enum ADD VALUE IF NOT EXISTS 'trade_exposure'"
        )


def downgrade() -> None:
    # Enum value removals are not supported without recreating the type.
    pass
