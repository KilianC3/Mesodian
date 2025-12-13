"""add raw_oksurf table

Revision ID: 0012
Revises: 0011
Create Date: 2025-12-12
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


# revision identifiers, used by Alembic.
revision = "0012"
down_revision = "0011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Raw table for ok.surf news API data
    op.create_table(
        "raw_oksurf",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("params", JSONB, nullable=False),  # {sections: list, count: int, fetched_at: str}
        sa.Column("payload", JSONB, nullable=False),  # {articles: [...], meta: {...}}
        schema="raw",
    )
    op.create_index("idx_raw_oksurf_fetched_at", "raw_oksurf", ["fetched_at"], schema="raw")


def downgrade() -> None:
    op.drop_index("idx_raw_oksurf_fetched_at", table_name="raw_oksurf", schema="raw")
    op.drop_table("raw_oksurf", schema="raw")
