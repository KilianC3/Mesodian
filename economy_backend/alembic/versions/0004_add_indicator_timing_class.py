"""Add timing_class to indicator

Revision ID: 0004_add_indicator_timing_class
Revises: 0003_create_global_cycle_index
Create Date: 2024-01-01 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0004_add_indicator_timing_class"
down_revision = "0003_create_global_cycle_index"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "indicator",
        sa.Column("timing_class", sa.String(), nullable=True),
        schema="warehouse",
    )


def downgrade() -> None:
    op.drop_column("indicator", "timing_class", schema="warehouse")

