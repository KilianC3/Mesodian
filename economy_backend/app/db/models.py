from __future__ import annotations

"""
SQLAlchemy ORM models for raw ingestion, warehouse panels, and graph schemas.

This module anchors the storage layer for the backend, covering raw provider
payloads, normalized warehouse entities (countries, indicators, features),
global cycle scores, and the graph tables used by web and edge metrics. It is
imported by ingestion jobs, feature builders, metrics pipelines, and API
routers whenever database objects are created or queried.
"""

import enum

from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    JSON,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base

JSONType = JSONB().with_variant(JSON, "sqlite")
BigIntPKType = BigInteger().with_variant(Integer, "sqlite")

Base = declarative_base()


class RawBase(Base):
    __abstract__ = True

    """Base class for raw provider payload tables in the ``raw`` schema."""

    id = Column(Integer, primary_key=True, autoincrement=True)
    fetched_at = Column(DateTime(timezone=True), index=True)
    params = Column(JSONType)
    payload = Column(JSONType)
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


class RawEcb(RawBase):
    __tablename__ = "raw_ecb"


class RawOns(RawBase):
    __tablename__ = "raw_ons"


class RawDbnomics(RawBase):
    __tablename__ = "raw_dbnomics"


class Country(Base):
    __tablename__ = "country"
    __table_args__ = {"schema": "warehouse", "sqlite_autoincrement": True}

    """Reference country table shared across warehouse and graph layers."""

    id = Column(String(3), primary_key=True)
    name = Column(String, nullable=False)
    region = Column(String, nullable=False)
    income_group = Column(String, nullable=True)


class Indicator(Base):
    __tablename__ = "indicator"
    __table_args__ = {"schema": "warehouse"}

    """Reference table mapping provider series to canonical indicator codes."""

    id = Column(Integer, primary_key=True)
    source = Column(String, nullable=False)
    source_code = Column(String, nullable=False)
    canonical_code = Column(String, nullable=True)
    frequency = Column(String, nullable=True)
    unit = Column(String, nullable=True)
    category = Column(String, nullable=True)
    timing_class = Column(String, nullable=True)


class Asset(Base):
    __tablename__ = "asset"
    __table_args__ = {"schema": "warehouse"}

    """Registry of financial assets such as equities or indices."""

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

    """Normalized macro/market time series values keyed by indicator and country."""

    id = Column(Integer, primary_key=True, autoincrement=True)
    indicator_id = Column(Integer, ForeignKey("warehouse.indicator.id"), nullable=False)
    country_id = Column(String(3), ForeignKey("warehouse.country.id"), nullable=False)
    date = Column(Date, nullable=False)
    value = Column(Numeric, nullable=False)
    source = Column(String, nullable=True)
    ingested_at = Column(DateTime(timezone=True), nullable=True)


class GlobalCycleIndex(Base):
    __tablename__ = "global_cycle_index"
    __table_args__ = (
        UniqueConstraint(
            "date", "frequency", "scope", "cycle_type", "method_version"
        ),
        {"schema": "warehouse", "sqlite_autoincrement": True},
    )

    """Principal-component cycle scores for global and regional aggregates."""

    id = Column(BigIntPKType, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    frequency = Column(String, nullable=False)
    scope = Column(String, nullable=False, default="global")
    cycle_type = Column(String, nullable=False)
    cycle_score = Column(Numeric, nullable=False)
    cycle_regime = Column(String, nullable=False)
    method_version = Column(String, nullable=False)
    coverage_gdp_share = Column(Numeric, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class AssetPrice(Base):
    __tablename__ = "asset_price"
    __table_args__ = (
        UniqueConstraint("asset_id", "date"),
        {"schema": "warehouse"},
    )

    """Daily OHLCV-style observations for tracked assets."""

    id = Column(BigIntPKType, primary_key=True, autoincrement=True)
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

    """Monthly AIS-derived shipping intensity metrics for each country."""

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    country_id = Column(String(3), ForeignKey("warehouse.country.id"), nullable=False)
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    activity_level = Column(Numeric, nullable=True)
    transits = Column(Numeric, nullable=True)


class TradeFlow(Base):
    __tablename__ = "trade_flow"
    __table_args__ = {"schema": "warehouse"}

    """Bilateral trade flows with HS section and flow direction metadata."""

    id = Column(Integer, primary_key=True, autoincrement=True)
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

    """Aggregated macro/climate/shipping features per country and year."""

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


class NodeType(str, enum.Enum):
    COUNTRY = "COUNTRY"
    COUNTRY_SECTOR = "COUNTRY_SECTOR"
    SECTOR_GLOBAL = "SECTOR_GLOBAL"
    INFRASTRUCTURE = "INFRASTRUCTURE"
    FIN_SYSTEM = "FIN_SYSTEM"
    FIN_INSTRUMENT = "FIN_INSTRUMENT"
    INSTITUTION = "INSTITUTION"
    REGIME = "REGIME"
    INDICATOR = "INDICATOR"
    ENV_SYSTEM = "ENV_SYSTEM"
    WEB = "WEB"
    EVENT = "EVENT"


class NodeCategory(str, enum.Enum):
    PRODUCTION_SUPPLY = "PRODUCTION_SUPPLY"
    DEMAND_CONSUMPTION = "DEMAND_CONSUMPTION"
    TRANSPORT_INFRA = "TRANSPORT_INFRA"
    MARKETS_FINANCE = "MARKETS_FINANCE"
    POLICY_REGULATION = "POLICY_REGULATION"
    ENVIRONMENT_NATURAL = "ENVIRONMENT_NATURAL"
    WEB = "WEB"


class ValueChainPosition(str, enum.Enum):
    UPSTREAM = "UPSTREAM"
    MIDSTREAM = "MIDSTREAM"
    DOWNSTREAM = "DOWNSTREAM"
    END_USER = "END_USER"
    CROSS_CUTTING = "CROSS_CUTTING"


class ScaleLevel(str, enum.Enum):
    MACRO = "MACRO"
    MESO = "MESO"
    MICRO = "MICRO"


class StructuralRole(str, enum.Enum):
    CORE = "CORE"
    PERIPHERY = "PERIPHERY"
    BRIDGE = "BRIDGE"
    BOTTLENECK = "BOTTLENECK"
    LEAF = "LEAF"
    UNKNOWN = "UNKNOWN"


class Node(Base):
    __tablename__ = "node"
    __table_args__ = (
        Index("ix_node_country_code", "country_code"),
        Index("ix_node_region_code", "region_code"),
        Index("ix_node_sector_code", "sector_code"),
        Index("ix_node_web_code", "web_code"),
        {"schema": "graph"},
    )

    id = Column(BigInteger, primary_key=True)
    name = Column(String, nullable=False)
    ref_type = Column(String, nullable=True)
    ref_id = Column(String, nullable=True)
    label = Column(String, nullable=True)
    category_role = Column(String, nullable=True)
    system_layer = Column(String, nullable=True)
    node_type = Column(Enum(NodeType, name="node_type_enum", schema="graph"), nullable=False)
    node_category = Column(Enum(NodeCategory, name="node_category_enum", schema="graph"), nullable=True)
    value_chain_position = Column(
        Enum(ValueChainPosition, name="value_chain_position_enum", schema="graph"), nullable=True
    )
    scale_level = Column(Enum(ScaleLevel, name="scale_level_enum", schema="graph"), nullable=True)
    structural_role = Column(
        Enum(StructuralRole, name="structural_role_enum", schema="graph"),
        nullable=False,
        default=StructuralRole.UNKNOWN,
        server_default=StructuralRole.UNKNOWN.value,
    )
    country_code = Column(String(3), nullable=True)
    region_code = Column(String, nullable=True)
    sector_code = Column(String, nullable=True)
    web_code = Column(String, nullable=True)
    themes = Column(JSONType, nullable=True)
    tags_json = Column(JSONType, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    """Graph node with classification metadata for webs across domains."""


class EdgeType(str, enum.Enum):
    TRADE_EXPOSURE = "trade_exposure"
    FLOW = "FLOW"
    INFLUENCE = "INFLUENCE"
    MEMBERSHIP = "MEMBERSHIP"
    CONSTRAINT = "CONSTRAINT"


class RelationshipFamily(str, enum.Enum):
    SUPPLY = "SUPPLY"
    DEMAND = "DEMAND"
    TRADE = "TRADE"
    TRANSPORT = "TRANSPORT"
    FINANCE = "FINANCE"
    OWNERSHIP = "OWNERSHIP"
    PRICE_LINK = "PRICE_LINK"
    POLICY_IMPACT = "POLICY_IMPACT"
    INFO_LINK = "INFO_LINK"
    RISK = "RISK"
    MEMBERSHIP = "MEMBERSHIP"
    OTHER = "OTHER"


class FlowType(str, enum.Enum):
    MATERIAL = "MATERIAL"
    ENERGY = "ENERGY"
    MONEY = "MONEY"
    INFORMATION = "INFORMATION"
    PEOPLE = "PEOPLE"
    EMISSIONS = "EMISSIONS"
    RISK = "RISK"


class LayerId(str, enum.Enum):
    PRODUCTION = "PRODUCTION"
    TRADE = "TRADE"
    FINANCIAL = "FINANCIAL"
    POLICY = "POLICY"
    INFORMATION = "INFORMATION"
    CLIMATE_ESG = "CLIMATE_ESG"
    RISK_SCENARIO = "RISK_SCENARIO"


class Direction(str, enum.Enum):
    OUT = "OUT"
    IN = "IN"
    BIDIR = "BIDIR"


class ImpactSign(str, enum.Enum):
    POSITIVE = "+"
    NEGATIVE = "-"
    AMBIGUOUS = "ambiguous"


class ImpactStrength(str, enum.Enum):
    WEAK = "weak"
    MEDIUM = "medium"
    STRONG = "strong"


class Certainty(str, enum.Enum):
    STRUCTURAL = "STRUCTURAL"
    SCENARIO = "SCENARIO"
    EMPIRICAL = "EMPIRICAL"


class Edge(Base):
    __tablename__ = "edge"
    __table_args__ = (
        Index("ix_edge_source_node_id", "source_node_id"),
        Index("ix_edge_target_node_id", "target_node_id"),
        Index("ix_edge_web_layer", "web_code", "layer_id"),
        {"schema": "graph"},
    )

    id = Column(BigInteger, primary_key=True)
    source_node_id = Column(BigInteger, ForeignKey("graph.node.id"), nullable=False)
    target_node_id = Column(BigInteger, ForeignKey("graph.node.id"), nullable=False)
    edge_type = Column(Enum(EdgeType, name="edge_type_enum", schema="graph"), nullable=False)
    rel_family = Column(Enum(RelationshipFamily, name="rel_family_enum", schema="graph"), nullable=True)
    flow_type = Column(Enum(FlowType, name="flow_type_enum", schema="graph"), nullable=True)
    layer_id = Column(Enum(LayerId, name="layer_id_enum", schema="graph"), nullable=True)
    direction = Column(Enum(Direction, name="edge_direction_enum", schema="graph"), nullable=True)
    weight_type = Column(String, nullable=True)
    weight_value = Column(Numeric, nullable=True)
    meta_json = Column(JSONType, nullable=True)
    impact_sign = Column(Enum(ImpactSign, name="impact_sign_enum", schema="graph"), nullable=True)
    impact_strength = Column(
        Enum(ImpactStrength, name="impact_strength_enum", schema="graph"), nullable=True
    )
    certainty = Column(Enum(Certainty, name="certainty_enum", schema="graph"), nullable=True)
    web_code = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    """Directed or bidirectional relationship between graph nodes."""


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
    frequency = Column(String, nullable=True)
    metric_theme = Column(String, nullable=True)
    meta_json = Column(JSONType, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    """Quantitative metric attached to a specific node for a given year."""


class NodeMetricContrib(Base):
    __tablename__ = "node_metric_contrib"
    __table_args__ = {"schema": "graph"}

    """Optional feature contribution breakdown for node metrics."""

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
    meta_json = Column(JSONType, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    """Metric summarizing an entire web for a specific year (e.g., centrality)."""


class EdgeMetric(Base):
    __tablename__ = "edge_metric"
    __table_args__ = (
        UniqueConstraint(
            "source_node_id",
            "target_node_id",
            "web_code",
            "year",
            "metric_code",
        ),
        {"schema": "graph"},
    )

    id = Column(BigInteger, primary_key=True)
    source_node_id = Column(BigInteger, ForeignKey("graph.node.id"), nullable=False)
    target_node_id = Column(BigInteger, ForeignKey("graph.node.id"), nullable=False)
    web_code = Column(String, nullable=True)
    year = Column(Integer, nullable=False)
    metric_code = Column(String, nullable=False)
    value = Column(Numeric, nullable=True)
    meta_json = Column(JSONType, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    """Metric capturing the strength or stress of a particular edge."""


class SovereignESGRaw(Base):
    __tablename__ = "sovereign_esg_raw"
    __table_args__ = {"schema": "warehouse"}

    """Raw ESG pillar inputs staged before aggregation to sovereign scores."""

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    country_code = Column(String(3), nullable=False)
    year = Column(Integer, nullable=False)
    provider = Column(String, nullable=False)
    indicator_code = Column(String, nullable=False)
    value = Column(Numeric, nullable=False)
    data_metadata = Column("metadata", JSONType, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )
