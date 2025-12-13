"""create global cycle index table

Revision ID: 0003
Revises: 0002
Create Date: 2024-07-11
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


WAREHOUSE_SCHEMA = "warehouse"


def upgrade() -> None:
    op.execute(sa.text(f"CREATE SCHEMA IF NOT EXISTS {WAREHOUSE_SCHEMA}"))

    op.create_table(
        "global_cycle_index",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("frequency", sa.String(), nullable=False),
        sa.Column("scope", sa.String(), nullable=False, server_default="global"),
        sa.Column("cycle_type", sa.String(), nullable=False),
        sa.Column("cycle_score", sa.Numeric(), nullable=False),
        sa.Column("cycle_regime", sa.String(), nullable=False),
        sa.Column("method_version", sa.String(), nullable=False),
        sa.Column("coverage_gdp_share", sa.Numeric(), nullable=True),
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
            "date", "frequency", "scope", "cycle_type", "method_version",
            name="uq_global_cycle_index_dimension",
        ),
        schema=WAREHOUSE_SCHEMA,
    )


def downgrade() -> None:
    op.drop_table("global_cycle_index", schema=WAREHOUSE_SCHEMA)
