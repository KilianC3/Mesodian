"""Make warehouse.country.region non-nullable

Revision ID: 0002
Revises: 0001
Create Date: 2024-07-09
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column(
        "country",
        "region",
        schema="warehouse",
        existing_type=sa.String(),
        nullable=False,
    )


def downgrade():
    op.alter_column(
        "country",
        "region",
        schema="warehouse",
        existing_type=sa.String(),
        nullable=True,
    )

