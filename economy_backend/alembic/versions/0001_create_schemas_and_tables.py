"""create schemas and tables

Revision ID: 0001_create_schemas_and_tables
Revises: 
Create Date: 2024-07-08
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0001_create_schemas_and_tables"
down_revision = None
branch_labels = None
depends_on = None


RAW_SCHEMA = "raw"
WAREHOUSE_SCHEMA = "warehouse"
GRAPH_SCHEMA = "graph"


def upgrade():
    op.execute(sa.text(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}"))
    op.execute(sa.text(f"CREATE SCHEMA IF NOT EXISTS {WAREHOUSE_SCHEMA}"))
    op.execute(sa.text(f"CREATE SCHEMA IF NOT EXISTS {GRAPH_SCHEMA}"))

    for table_name in [
        "raw_fred",
        "raw_wdi",
        "raw_eurostat",
        "raw_imf",
        "raw_oecd",
        "raw_adb",
        "raw_afdb",
        "raw_comtrade",
        "raw_eia",
        "raw_ember",
        "raw_gcp",
        "raw_ilostat",
        "raw_faostat",
        "raw_bis",
        "raw_unctad",
        "raw_open_alex",
        "raw_patents_view",
        "raw_yfinance",
        "raw_stooq",
        "raw_aisstream",
        "raw_gdelt",
        "raw_rss",
    ]:
        op.create_table(
            table_name,
            sa.Column("id", sa.BigInteger(), primary_key=True),
            sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("params", postgresql.JSONB(astext_type=sa.Text())),
            sa.Column("payload", postgresql.JSONB(astext_type=sa.Text())),
            schema=RAW_SCHEMA,
        )
        op.create_index(
            f"ix_{table_name}_fetched_at",
            table_name,
            ["fetched_at"],
            unique=False,
            schema=RAW_SCHEMA,
        )

    op.create_table(
        "country",
        sa.Column("id", sa.String(length=3), primary_key=True),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("region", sa.String(), nullable=True),
        sa.Column("income_group", sa.String(), nullable=True),
        schema=WAREHOUSE_SCHEMA,
    )

    op.create_table(
        "indicator",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("source_code", sa.String(), nullable=False),
        sa.Column("canonical_code", sa.String(), nullable=True),
        sa.Column("frequency", sa.String(), nullable=True),
        sa.Column("unit", sa.String(), nullable=True),
        sa.Column("category", sa.String(), nullable=True),
        schema=WAREHOUSE_SCHEMA,
    )

    op.create_table(
        "asset",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("symbol", sa.String(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("asset_type", sa.String(), nullable=False),
        sa.Column("country_id", sa.String(length=3), sa.ForeignKey(f"{WAREHOUSE_SCHEMA}.country.id"), nullable=True),
        sa.Column("region", sa.String(), nullable=True),
        sa.UniqueConstraint("symbol", name="uq_asset_symbol"),
        schema=WAREHOUSE_SCHEMA,
    )

    op.create_table(
        "time_series_value",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("indicator_id", sa.Integer(), sa.ForeignKey(f"{WAREHOUSE_SCHEMA}.indicator.id"), nullable=False),
        sa.Column("country_id", sa.String(length=3), sa.ForeignKey(f"{WAREHOUSE_SCHEMA}.country.id"), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("value", sa.Numeric(), nullable=False),
        sa.Column("source", sa.String(), nullable=True),
        sa.Column("ingested_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("indicator_id", "country_id", "date", name="uq_time_series_indicator_country_date"),
        schema=WAREHOUSE_SCHEMA,
    )

    op.create_table(
        "asset_price",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("asset_id", sa.Integer(), sa.ForeignKey(f"{WAREHOUSE_SCHEMA}.asset.id"), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("open", sa.Numeric(), nullable=True),
        sa.Column("high", sa.Numeric(), nullable=True),
        sa.Column("low", sa.Numeric(), nullable=True),
        sa.Column("close", sa.Numeric(), nullable=True),
        sa.Column("adj_close", sa.Numeric(), nullable=True),
        sa.Column("volume", sa.Numeric(), nullable=True),
        sa.UniqueConstraint("asset_id", "date", name="uq_asset_price_asset_date"),
        schema=WAREHOUSE_SCHEMA,
    )

    op.create_table(
        "shipping_country_month",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("country_id", sa.String(length=3), sa.ForeignKey(f"{WAREHOUSE_SCHEMA}.country.id"), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("month", sa.Integer(), nullable=False),
        sa.Column("activity_level", sa.Numeric(), nullable=True),
        sa.Column("transits", sa.Numeric(), nullable=True),
        sa.UniqueConstraint("country_id", "year", "month", name="uq_shipping_country_year_month"),
        schema=WAREHOUSE_SCHEMA,
    )

    op.create_table(
        "trade_flow",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("reporter_country_id", sa.String(length=3), sa.ForeignKey(f"{WAREHOUSE_SCHEMA}.country.id"), nullable=False),
        sa.Column("partner_country_id", sa.String(length=3), sa.ForeignKey(f"{WAREHOUSE_SCHEMA}.country.id"), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("hs_section", sa.String(), nullable=True),
        sa.Column("flow_type", sa.String(), nullable=True),
        sa.Column("value_usd", sa.Numeric(), nullable=True),
        schema=WAREHOUSE_SCHEMA,
    )

    op.create_table(
        "country_year_features",
        sa.Column("country_id", sa.String(length=3), sa.ForeignKey(f"{WAREHOUSE_SCHEMA}.country.id"), primary_key=True),
        sa.Column("year", sa.Integer(), primary_key=True),
        sa.Column("gdp_real", sa.Numeric(), nullable=True),
        sa.Column("gdp_growth", sa.Numeric(), nullable=True),
        sa.Column("inflation_cpi", sa.Numeric(), nullable=True),
        sa.Column("ca_pct_gdp", sa.Numeric(), nullable=True),
        sa.Column("debt_pct_gdp", sa.Numeric(), nullable=True),
        sa.Column("unemployment_rate", sa.Numeric(), nullable=True),
        sa.Column("co2_per_capita", sa.Numeric(), nullable=True),
        sa.Column("energy_import_dep", sa.Numeric(), nullable=True),
        sa.Column("food_import_dep", sa.Numeric(), nullable=True),
        sa.Column("shipping_activity_level", sa.Numeric(), nullable=True),
        sa.Column("shipping_activity_change", sa.Numeric(), nullable=True),
        sa.Column("event_stress_pulse", sa.Numeric(), nullable=True),
        sa.Column("data_coverage_score", sa.Numeric(), nullable=True),
        sa.Column("data_freshness_score", sa.Numeric(), nullable=True),
        schema=WAREHOUSE_SCHEMA,
    )

    op.create_table(
        "node",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("node_type", sa.String(), nullable=False),
        sa.Column("ref_type", sa.String(), nullable=True),
        sa.Column("ref_id", sa.String(), nullable=True),
        sa.Column("label", sa.String(), nullable=True),
        sa.Column("category_role", sa.String(), nullable=True),
        sa.Column("system_layer", sa.String(), nullable=True),
        schema=GRAPH_SCHEMA,
    )

    op.create_table(
        "edge",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("source_node_id", sa.BigInteger(), sa.ForeignKey(f"{GRAPH_SCHEMA}.node.id"), nullable=False),
        sa.Column("target_node_id", sa.BigInteger(), sa.ForeignKey(f"{GRAPH_SCHEMA}.node.id"), nullable=False),
        sa.Column("edge_type", sa.String(), nullable=False),
        sa.Column("weight", sa.Numeric(), nullable=True),
        sa.Column("attrs", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        schema=GRAPH_SCHEMA,
    )

    op.create_table(
        "node_metric",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("node_id", sa.BigInteger(), sa.ForeignKey(f"{GRAPH_SCHEMA}.node.id"), nullable=False),
        sa.Column("metric_code", sa.String(), nullable=False),
        sa.Column("as_of_year", sa.Integer(), nullable=False),
        sa.Column("value", sa.Numeric(), nullable=True),
        sa.UniqueConstraint("node_id", "metric_code", "as_of_year", name="uq_node_metric_node_code_year"),
        schema=GRAPH_SCHEMA,
    )

    op.create_table(
        "node_metric_contrib",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("node_metric_id", sa.BigInteger(), sa.ForeignKey(f"{GRAPH_SCHEMA}.node_metric.id"), nullable=False),
        sa.Column("feature_name", sa.String(), nullable=False),
        sa.Column("contribution", sa.Numeric(), nullable=True),
        schema=GRAPH_SCHEMA,
    )

    op.create_table(
        "web_metric",
        sa.Column("id", sa.BigInteger(), primary_key=True),
        sa.Column("web_code", sa.String(), nullable=False),
        sa.Column("as_of_year", sa.Integer(), nullable=False),
        sa.Column("metric_code", sa.String(), nullable=False),
        sa.Column("value", sa.Numeric(), nullable=True),
        sa.UniqueConstraint("web_code", "metric_code", "as_of_year", name="uq_web_metric_code_year"),
        schema=GRAPH_SCHEMA,
    )


def downgrade():
    op.drop_table("web_metric", schema=GRAPH_SCHEMA)
    op.drop_table("node_metric_contrib", schema=GRAPH_SCHEMA)
    op.drop_table("node_metric", schema=GRAPH_SCHEMA)
    op.drop_table("edge", schema=GRAPH_SCHEMA)
    op.drop_table("node", schema=GRAPH_SCHEMA)

    op.drop_table("country_year_features", schema=WAREHOUSE_SCHEMA)
    op.drop_table("trade_flow", schema=WAREHOUSE_SCHEMA)
    op.drop_table("shipping_country_month", schema=WAREHOUSE_SCHEMA)
    op.drop_table("asset_price", schema=WAREHOUSE_SCHEMA)
    op.drop_table("time_series_value", schema=WAREHOUSE_SCHEMA)
    op.drop_table("asset", schema=WAREHOUSE_SCHEMA)
    op.drop_table("indicator", schema=WAREHOUSE_SCHEMA)
    op.drop_table("country", schema=WAREHOUSE_SCHEMA)

    for table_name in [
        "raw_rss",
        "raw_gdelt",
        "raw_aisstream",
        "raw_stooq",
        "raw_yfinance",
        "raw_patents_view",
        "raw_open_alex",
        "raw_unctad",
        "raw_bis",
        "raw_faostat",
        "raw_ilostat",
        "raw_gcp",
        "raw_ember",
        "raw_eia",
        "raw_comtrade",
        "raw_afdb",
        "raw_adb",
        "raw_oecd",
        "raw_imf",
        "raw_eurostat",
        "raw_wdi",
        "raw_fred",
    ]:
        op.drop_table(table_name, schema=RAW_SCHEMA)

    op.execute(sa.text(f"DROP SCHEMA IF EXISTS {GRAPH_SCHEMA} CASCADE"))
    op.execute(sa.text(f"DROP SCHEMA IF EXISTS {WAREHOUSE_SCHEMA} CASCADE"))
    op.execute(sa.text(f"DROP SCHEMA IF EXISTS {RAW_SCHEMA} CASCADE"))
