"""Rename as_of_year to year on edge_metric"""

from alembic import op
import sqlalchemy as sa

revision = "0006_rename_edge_metric_year"
down_revision = "0005_create_edge_metric"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("edge_metric", schema="graph") as batch_op:
        batch_op.alter_column("as_of_year", new_column_name="year", existing_type=sa.Integer())


def downgrade() -> None:
    with op.batch_alter_table("edge_metric", schema="graph") as batch_op:
        batch_op.alter_column("year", new_column_name="as_of_year", existing_type=sa.Integer())
