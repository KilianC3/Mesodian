"""add trade exposure edge type

Revision ID: 0009
Revises: 0008
Create Date: 2025-03-09
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None

GRAPH_SCHEMA = "graph"
EDGE_TABLE = "edge"
EDGE_TYPE_ENUM = "edge_type_enum"

OLD_EDGE_TYPES = ("FLOW", "INFLUENCE", "MEMBERSHIP", "CONSTRAINT")


def upgrade() -> None:
    # PostgreSQL requires this to be outside a transaction
    bind = op.get_bind()
    if bind.dialect.name == 'postgresql':
        # Use autocommit to execute outside transaction
        with op.get_context().autocommit_block():
            op.execute(
                sa.text(
                    f"ALTER TYPE {GRAPH_SCHEMA}.{EDGE_TYPE_ENUM} "
                    "ADD VALUE IF NOT EXISTS 'TRADE_EXPOSURE'"
                )
            )
    else:
        # For other databases, just execute normally
        op.execute(
            sa.text(
                f"ALTER TYPE {GRAPH_SCHEMA}.{EDGE_TYPE_ENUM} "
                "ADD VALUE IF NOT EXISTS 'TRADE_EXPOSURE'"
            )
        )


def downgrade() -> None:
    bind = op.get_bind()

    op.execute(
        sa.text(
            f"UPDATE {GRAPH_SCHEMA}.{EDGE_TABLE} "
            f"SET edge_type = 'FLOW' WHERE edge_type = 'TRADE_EXPOSURE'"
        )
    )

    temp_enum = postgresql.ENUM(
        *OLD_EDGE_TYPES, name=f"{EDGE_TYPE_ENUM}_old", schema=GRAPH_SCHEMA
    )
    temp_enum.create(bind, checkfirst=True)

    op.execute(
        sa.text(
            f"ALTER TABLE {GRAPH_SCHEMA}.{EDGE_TABLE} "
            f"ALTER COLUMN edge_type TYPE {GRAPH_SCHEMA}.{EDGE_TYPE_ENUM}_old "
            f"USING edge_type::text::{GRAPH_SCHEMA}.{EDGE_TYPE_ENUM}_old"
        )
    )

    op.execute(sa.text(f"DROP TYPE {GRAPH_SCHEMA}.{EDGE_TYPE_ENUM}"))
    op.execute(
        sa.text(
            f"ALTER TYPE {GRAPH_SCHEMA}.{EDGE_TYPE_ENUM}_old RENAME TO {EDGE_TYPE_ENUM}"
        )
    )
