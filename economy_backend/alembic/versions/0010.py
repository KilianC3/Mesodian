"""make time_series_value country_id nullable for global indicators

Revision ID: 0010
Revises: 0009
Create Date: 2025-12-05
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0010"
down_revision = "0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "time_series_value",
        "country_id",
        existing_type=sa.String(length=3),
        nullable=True,
        schema="warehouse",
    )


def downgrade() -> None:
    op.alter_column(
        "time_series_value",
        "country_id",
        existing_type=sa.String(length=3),
        nullable=False,
        schema="warehouse",
    )
