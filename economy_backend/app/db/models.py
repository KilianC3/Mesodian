from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class RawBase(Base):
    __abstract__ = True

    id = Column(BigInteger, primary_key=True)
    fetched_at = Column(DateTime(timezone=True), index=True)
    params = Column(JSONB)
    payload = Column(JSONB)
    __table_args__ = {"schema": "raw"}


class RawFred(RawBase):
    __tablename__ = "raw_fred"


class RawWdi(RawBase):
    __tablename__ = "raw_wdi"


class RawEurostat(RawBase):
    __tablename__ = "raw_eurostat"


class RawImf(RawBase):
    __tablename__ = "raw_imf"


class RawOecd(RawBase):
    __tablename__ = "raw_oecd"


class RawAdb(RawBase):
    __tablename__ = "raw_adb"


class RawAfdb(RawBase):
    __tablename__ = "raw_afdb"


class RawComtrade(RawBase):
    __tablename__ = "raw_comtrade"


class RawEia(RawBase):
    __tablename__ = "raw_eia"


class RawEmber(RawBase):
    __tablename__ = "raw_ember"


class RawGcp(RawBase):
    __tablename__ = "raw_gcp"


class RawIlostat(RawBase):
    __tablename__ = "raw_ilostat"


class RawFaostat(RawBase):
    __tablename__ = "raw_faostat"


class RawBis(RawBase):
    __tablename__ = "raw_bis"


class RawUnctad(RawBase):
    __tablename__ = "raw_unctad"


class RawOpenAlex(RawBase):
    __tablename__ = "raw_open_alex"


class RawPatentsView(RawBase):
    __tablename__ = "raw_patents_view"


class RawYfinance(RawBase):
    __tablename__ = "raw_yfinance"


class RawStooq(RawBase):
    __tablename__ = "raw_stooq"


class RawAisstream(RawBase):
    __tablename__ = "raw_aisstream"


class RawGdelt(RawBase):
    __tablename__ = "raw_gdelt"


class RawRss(RawBase):
    __tablename__ = "raw_rss"


class Country(Base):
    __tablename__ = "country"
    __table_args__ = {"schema": "warehouse"}

    id = Column(String(3), primary_key=True)
    name = Column(String, nullable=False)
    region = Column(String, nullable=True)
    income_group = Column(String, nullable=True)


class Indicator(Base):
    __tablename__ = "indicator"
    __table_args__ = {"schema": "warehouse"}

    id = Column(Integer, primary_key=True)
    source = Column(String, nullable=False)
    source_code = Column(String, nullable=False)
    canonical_code = Column(String, nullable=True)
    frequency = Column(String, nullable=True)
    unit = Column(String, nullable=True)
    category = Column(String, nullable=True)


class Asset(Base):
    __tablename__ = "asset"
    __table_args__ = {"schema": "warehouse"}

    id = Column(Integer, primary_key=True)
    symbol = Column(String, unique=True, nullable=False)
    name = Column(String, nullable=False)
    asset_type = Column(String, nullable=False)
    country_id = Column(String(3), ForeignKey("warehouse.country.id"), nullable=True)
    region = Column(String, nullable=True)


class TimeSeriesValue(Base):
    __tablename__ = "time_series_value"
    __table_args__ = (
        UniqueConstraint("indicator_id", "country_id", "date"),
        {"schema": "warehouse"},
    )

    id = Column(BigInteger, primary_key=True)
    indicator_id = Column(Integer, ForeignKey("warehouse.indicator.id"), nullable=False)
    country_id = Column(String(3), ForeignKey("warehouse.country.id"), nullable=False)
    date = Column(Date, nullable=False)
    value = Column(Numeric, nullable=False)
    source = Column(String, nullable=True)
    ingested_at = Column(DateTime(timezone=True), nullable=True)


class AssetPrice(Base):
    __tablename__ = "asset_price"
    __table_args__ = (
        UniqueConstraint("asset_id", "date"),
        {"schema": "warehouse"},
    )

    id = Column(BigInteger, primary_key=True)
    asset_id = Column(Integer, ForeignKey("warehouse.asset.id"), nullable=False)
    date = Column(Date, nullable=False)
    open = Column(Numeric, nullable=True)
    high = Column(Numeric, nullable=True)
    low = Column(Numeric, nullable=True)
    close = Column(Numeric, nullable=True)
    adj_close = Column(Numeric, nullable=True)
    volume = Column(Numeric, nullable=True)


class ShippingCountryMonth(Base):
    __tablename__ = "shipping_country_month"
    __table_args__ = (
        UniqueConstraint("country_id", "year", "month"),
        {"schema": "warehouse"},
    )

    id = Column(BigInteger, primary_key=True)
    country_id = Column(String(3), ForeignKey("warehouse.country.id"), nullable=False)
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    activity_level = Column(Numeric, nullable=True)
    transits = Column(Numeric, nullable=True)


class TradeFlow(Base):
    __tablename__ = "trade_flow"
    __table_args__ = {"schema": "warehouse"}

    id = Column(BigInteger, primary_key=True)
    reporter_country_id = Column(String(3), ForeignKey("warehouse.country.id"), nullable=False)
    partner_country_id = Column(String(3), ForeignKey("warehouse.country.id"), nullable=False)
    year = Column(Integer, nullable=False)
    hs_section = Column(String, nullable=True)
    flow_type = Column(String, nullable=True)
    value_usd = Column(Numeric, nullable=True)


class CountryYearFeatures(Base):
    __tablename__ = "country_year_features"
    __table_args__ = (
        {"schema": "warehouse"},
    )

    country_id = Column(String(3), ForeignKey("warehouse.country.id"), primary_key=True)
    year = Column(Integer, primary_key=True)
    gdp_real = Column(Numeric, nullable=True)
    gdp_growth = Column(Numeric, nullable=True)
    inflation_cpi = Column(Numeric, nullable=True)
    ca_pct_gdp = Column(Numeric, nullable=True)
    debt_pct_gdp = Column(Numeric, nullable=True)
    unemployment_rate = Column(Numeric, nullable=True)
    co2_per_capita = Column(Numeric, nullable=True)
    energy_import_dep = Column(Numeric, nullable=True)
    food_import_dep = Column(Numeric, nullable=True)
    shipping_activity_level = Column(Numeric, nullable=True)
    shipping_activity_change = Column(Numeric, nullable=True)
    event_stress_pulse = Column(Numeric, nullable=True)
    data_coverage_score = Column(Numeric, nullable=True)
    data_freshness_score = Column(Numeric, nullable=True)


class Node(Base):
    __tablename__ = "node"
    __table_args__ = {"schema": "graph"}

    id = Column(BigInteger, primary_key=True)
    node_type = Column(String, nullable=False)
    ref_type = Column(String, nullable=True)
    ref_id = Column(String, nullable=True)
    label = Column(String, nullable=True)
    category_role = Column(String, nullable=True)
    system_layer = Column(String, nullable=True)


class Edge(Base):
    __tablename__ = "edge"
    __table_args__ = {"schema": "graph"}

    id = Column(BigInteger, primary_key=True)
    source_node_id = Column(BigInteger, ForeignKey("graph.node.id"), nullable=False)
    target_node_id = Column(BigInteger, ForeignKey("graph.node.id"), nullable=False)
    edge_type = Column(String, nullable=False)
    weight = Column(Numeric, nullable=True)
    attrs = Column(JSONB, nullable=True)


class NodeMetric(Base):
    __tablename__ = "node_metric"
    __table_args__ = (
        UniqueConstraint("node_id", "metric_code", "as_of_year"),
        {"schema": "graph"},
    )

    id = Column(BigInteger, primary_key=True)
    node_id = Column(BigInteger, ForeignKey("graph.node.id"), nullable=False)
    metric_code = Column(String, nullable=False)
    as_of_year = Column(Integer, nullable=False)
    value = Column(Numeric, nullable=True)


class NodeMetricContrib(Base):
    __tablename__ = "node_metric_contrib"
    __table_args__ = {"schema": "graph"}

    id = Column(BigInteger, primary_key=True)
    node_metric_id = Column(BigInteger, ForeignKey("graph.node_metric.id"), nullable=False)
    feature_name = Column(String, nullable=False)
    contribution = Column(Numeric, nullable=True)


class WebMetric(Base):
    __tablename__ = "web_metric"
    __table_args__ = (
        UniqueConstraint("web_code", "metric_code", "as_of_year"),
        {"schema": "graph"},
    )

    id = Column(BigInteger, primary_key=True)
    web_code = Column(String, nullable=False)
    as_of_year = Column(Integer, nullable=False)
    metric_code = Column(String, nullable=False)
    value = Column(Numeric, nullable=True)
