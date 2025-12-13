"""Create edge_metric table

Revision ID: 0005
Revises: 0004
Create Date: 2024-01-01 00:05:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "edge_metric",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("source_node_id", sa.BigInteger(), nullable=False),
        sa.Column("target_node_id", sa.BigInteger(), nullable=False),
        sa.Column("web_code", sa.String(), nullable=True),
        sa.Column("as_of_year", sa.Integer(), nullable=False),
        sa.Column("metric_code", sa.String(), nullable=False),
        sa.Column("value", sa.Numeric(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            onupdate=sa.func.now(),
            nullable=False,
        ),
        sa.UniqueConstraint(
            "source_node_id",
            "target_node_id",
            "web_code",
            "as_of_year",
            "metric_code",
        ),
        schema="graph",
    )


def downgrade() -> None:
    op.drop_table("edge_metric", schema="graph")

