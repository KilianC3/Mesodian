"""create equity fundamentals and analyst data tables

Revision ID: 0011
Revises: 0010
Create Date: 2025-12-08
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB


# revision identifiers, used by Alembic.
revision = "0011"
down_revision = "0010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Raw table for FinViz scraped data
    op.create_table(
        "raw_finviz",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("fetched_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("params", JSONB, nullable=False),  # {ticker: str}
        sa.Column("payload", JSONB, nullable=False),  # Full scraped data
        schema="raw",
    )
    op.create_index("idx_raw_finviz_fetched_at", "raw_finviz", ["fetched_at"], schema="raw")
    op.create_index("idx_raw_finviz_ticker", "raw_finviz", [sa.text("(params->>'ticker')")], schema="raw")

    # Equity fundamentals table (snapshot data)
    op.create_table(
        "equity_fundamentals",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("ticker", sa.String(10), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("market_cap", sa.Float()),
        sa.Column("pe_ratio", sa.Float()),
        sa.Column("forward_pe", sa.Float()),
        sa.Column("peg_ratio", sa.Float()),
        sa.Column("ps_ratio", sa.Float()),
        sa.Column("pb_ratio", sa.Float()),
        sa.Column("pc_ratio", sa.Float()),
        sa.Column("pfcf_ratio", sa.Float()),
        sa.Column("dividend_yield", sa.Float()),
        sa.Column("payout_ratio", sa.Float()),
        sa.Column("eps_ttm", sa.Float()),
        sa.Column("eps_next_y", sa.Float()),
        sa.Column("eps_next_q", sa.Float()),
        sa.Column("eps_this_y", sa.Float()),
        sa.Column("eps_next_5y", sa.Float()),
        sa.Column("eps_past_5y", sa.Float()),
        sa.Column("sales_past_5y", sa.Float()),
        sa.Column("sales_qq", sa.Float()),
        sa.Column("eps_qq", sa.Float()),
        sa.Column("roa", sa.Float()),
        sa.Column("roe", sa.Float()),
        sa.Column("roi", sa.Float()),
        sa.Column("gross_margin", sa.Float()),
        sa.Column("operating_margin", sa.Float()),
        sa.Column("net_margin", sa.Float()),
        sa.Column("debt_equity", sa.Float()),
        sa.Column("lt_debt_equity", sa.Float()),
        sa.Column("current_ratio", sa.Float()),
        sa.Column("quick_ratio", sa.Float()),
        sa.Column("beta", sa.Float()),
        sa.Column("atr", sa.Float()),
        sa.Column("volatility", sa.Float()),
        sa.Column("insider_own", sa.Float()),
        sa.Column("insider_trans", sa.Float()),
        sa.Column("inst_own", sa.Float()),
        sa.Column("inst_trans", sa.Float()),
        sa.Column("short_float", sa.Float()),
        sa.Column("short_ratio", sa.Float()),
        sa.Column("target_price", sa.Float()),
        sa.Column("recommendation", sa.Float()),  # 1.0=Strong Buy, 5.0=Sell
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now(), onupdate=sa.func.now()),
        schema="warehouse",
    )
    op.create_index("idx_equity_fundamentals_ticker_date", "equity_fundamentals", ["ticker", "date"], unique=True, schema="warehouse")
    op.create_index("idx_equity_fundamentals_date", "equity_fundamentals", ["date"], schema="warehouse")

    # Stock news table
    op.create_table(
        "stock_news",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("ticker", sa.String(10), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("headline", sa.Text(), nullable=False),
        sa.Column("source", sa.String(100)),
        sa.Column("url", sa.Text()),
        sa.Column("sentiment_score", sa.Float()),  # Optional: for future NLP analysis
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        schema="warehouse",
    )
    op.create_index("idx_stock_news_ticker_timestamp", "stock_news", ["ticker", "timestamp"], schema="warehouse")
    op.create_index("idx_stock_news_timestamp", "stock_news", ["timestamp"], schema="warehouse")

    # Analyst ratings table
    op.create_table(
        "analyst_rating",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("ticker", sa.String(10), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("action", sa.String(50)),  # Upgrade, Downgrade, Initiate, Reiterate
        sa.Column("firm", sa.String(100)),
        sa.Column("from_rating", sa.String(50)),
        sa.Column("to_rating", sa.String(50)),
        sa.Column("price_target", sa.Float()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        schema="warehouse",
    )
    op.create_index("idx_analyst_rating_ticker_date", "analyst_rating", ["ticker", "date"], schema="warehouse")
    op.create_index("idx_analyst_rating_date", "analyst_rating", ["date"], schema="warehouse")

    # Insider trading table
    op.create_table(
        "insider_trade",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("ticker", sa.String(10), nullable=False),
        sa.Column("insider_name", sa.String(100)),
        sa.Column("relationship", sa.String(100)),  # CEO, CFO, Director, etc.
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("transaction_type", sa.String(50)),  # Buy, Sale, Option Exercise
        sa.Column("cost", sa.Float()),  # Price per share
        sa.Column("shares", sa.Float()),
        sa.Column("value", sa.Float()),  # Total transaction value
        sa.Column("shares_total", sa.Float()),  # Total shares owned after transaction
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        schema="warehouse",
    )
    op.create_index("idx_insider_trade_ticker_date", "insider_trade", ["ticker", "date"], schema="warehouse")
    op.create_index("idx_insider_trade_date", "insider_trade", ["date"], schema="warehouse")

    # Equity financial statements (time series)
    op.create_table(
        "equity_financials",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("ticker", sa.String(10), nullable=False),
        sa.Column("statement_type", sa.String(50), nullable=False),  # income, balance_sheet, cash_flow
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("line_item", sa.String(100), nullable=False),
        sa.Column("value", sa.Float()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now(), onupdate=sa.func.now()),
        schema="warehouse",
    )
    op.create_index(
        "idx_equity_financials_ticker_year_type",
        "equity_financials",
        ["ticker", "year", "statement_type"],
        schema="warehouse",
    )


def downgrade() -> None:
    op.drop_table("equity_financials", schema="warehouse")
    op.drop_table("insider_trade", schema="warehouse")
    op.drop_table("analyst_rating", schema="warehouse")
    op.drop_table("stock_news", schema="warehouse")
    op.drop_table("equity_fundamentals", schema="warehouse")
    op.drop_table("raw_finviz", schema="raw")
