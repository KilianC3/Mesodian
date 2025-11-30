"""Make warehouse.country.region non-nullable

Revision ID: 0002_update_country_region_nullable
Revises: 0001_create_schemas_and_tables
Create Date: 2024-07-09
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0002_update_country_region_nullable"
down_revision = "0001_create_schemas_and_tables"
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

