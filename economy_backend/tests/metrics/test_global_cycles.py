"""Computation of economic, ESG, and timing metrics."""


import sys
from datetime import date
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
pytestmark = pytest.mark.integration
from sqlalchemy.orm import Session, sessionmaker

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.cycles.global_business_cycle import compute_gbc_index, gbc_regime_from_z, write_gbc_to_db
from app.cycles.global_trade_cycle import compute_gtmc_index, gtmc_regime_from_z, write_gtmc_to_db
from app.cycles.global_commodity_cycles import (
    COMMODITY_CODES,
    commodity_regime_from_z,
    compute_commodity_cycles,
    write_commodity_cycles_to_db,
)
from app.cycles.global_inflation_cycle import compute_gic_index, gic_regime_from_z, write_gic_to_db
from app.cycles.global_financial_cycle import compute_gfc_index, gfc_regime_from_z, write_gfc_to_db
from app.db.models import Base, Country, GlobalCycleIndex, Indicator, TimeSeriesValue
from app.metrics.indicator_timing import TIMING_CLASS_MAP


@pytest.fixture()
def session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS raw"))
        conn.execute(text("ATTACH DATABASE ':memory:' AS warehouse"))
        conn.execute(text("ATTACH DATABASE ':memory:' AS graph"))
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _add_indicator(session: Session, canonical_code: str, frequency: str = "monthly") -> Indicator:
    indicator = Indicator(
        source="test",
        source_code=canonical_code,
        canonical_code=canonical_code,
        frequency=frequency,
        timing_class=TIMING_CLASS_MAP.get(canonical_code),
    )
    session.add(indicator)
    session.flush()
    return indicator


def _add_time_series(
    session: Session, indicator: Indicator, country_id: str, date_value: date, value: float
) -> None:
    session.add(
        TimeSeriesValue(
            indicator_id=indicator.id,
            country_id=country_id,
            date=date_value,
            value=value,
        )
    )


def _seed_countries(session: Session, country_ids: list[str]) -> None:
    for cid in country_ids:
        session.add(Country(id=cid, name=cid, region="test", income_group="test"))
    session.flush()


def _seed_gdp_and_inflation(session: Session) -> None:
    countries = ["USA", "DEU", "JPN"]
    _seed_countries(session, countries)

    gdp_growth = _add_indicator(session, "GDP_REAL_YOY", frequency="annual")
    gdp_nominal = _add_indicator(session, "GDP_CURRENT_USD", frequency="annual")
    inflation = _add_indicator(session, "CPI_HEADLINE_YOY", frequency="quarterly")

    growth_values = {
        "USA": [2.0, 2.5, 3.0, 2.8],
        "DEU": [1.0, 1.5, 2.0, 2.2],
        "JPN": [0.5, 1.0, 1.2, 1.5],
    }
    nominal_values = {cid: [2e4 + i * 100 for i in range(4)] for cid in countries}
    inflation_values = {
        "USA": [1.5, 2.0, 2.5, 2.8],
        "DEU": [1.0, 1.4, 1.8, 2.0],
        "JPN": [0.5, 0.8, 1.0, 1.2],
    }

    years = [2018, 2019, 2020, 2021]
    for idx, year in enumerate(years):
        for cid in countries:
            _add_time_series(session, gdp_growth, cid, date(year, 12, 31), growth_values[cid][idx])
            _add_time_series(session, gdp_nominal, cid, date(year, 12, 31), nominal_values[cid][idx])
            _add_time_series(session, inflation, cid, date(year, 12, 31), inflation_values[cid][idx])


def _seed_trade(session: Session) -> None:
    for code in [
        "WORLD_TRADE_VOL",
        "WORLD_IP",
        "GLOBAL_MANUF_PMI",
        "GLOBAL_EXPORT_ORDERS_PMI",
    ]:
        indicator = _add_indicator(session, code)
        for offset, value in enumerate([100, 101, 102, 103]):
            _add_time_series(
                session,
                indicator,
                "WLD",
                date(2020, offset + 1, 1),
                value + offset,
            )


def _seed_commodities(session: Session) -> None:
    start_dates = pd.date_range("2020-01-01", periods=12, freq="MS")
    for code in COMMODITY_CODES.values():
        indicator = _add_indicator(session, code)
        for idx, dt in enumerate(start_dates):
            _add_time_series(
                session,
                indicator,
                "WLD",
                dt.date(),
                90 + idx + (idx % 3),
            )


def _seed_financial(session: Session) -> None:
    dates = pd.date_range("2020-01-01", periods=12, freq="MS")
    for code in [
        "POLICY_RATE_US",
        "GLOBAL_IG_SPREAD",
        "GLOBAL_HY_SPREAD",
        "EMBIG_SPREAD",
        "VIX",
        "GLOBAL_EQUITY_RETURN",
    ]:
        indicator = _add_indicator(session, code)
        for idx, dt in enumerate(dates):
            value = 1.0 + idx * 0.1
            if code == "GLOBAL_EQUITY_RETURN":
                value = 0.5 + idx * 0.05
            _add_time_series(session, indicator, "WLD", dt.date(), value)


def test_global_cycles_computation_and_persistence(session: Session) -> None:
    _seed_gdp_and_inflation(session)
    _seed_trade(session)
    _seed_commodities(session)
    _seed_financial(session)
    session.commit()

    gbc_df = compute_gbc_index(session)
    assert not gbc_df.empty
    write_gbc_to_db(session, gbc_df, frequency="annual")

    gtmc_df = compute_gtmc_index(session)
    assert not gtmc_df.empty
    write_gtmc_to_db(session, gtmc_df, frequency="monthly")

    commodity_df = compute_commodity_cycles(session)
    assert {"commodity_energy_z", "commodity_metals_z", "commodity_agri_z"}.issubset(commodity_df.columns)
    write_commodity_cycles_to_db(session, commodity_df, frequency="monthly")

    gic_df = compute_gic_index(session)
    assert not gic_df.empty
    write_gic_to_db(session, gic_df, frequency="quarterly")

    gfc_df = compute_gfc_index(session)
    assert not gfc_df.empty
    write_gfc_to_db(session, gfc_df, frequency="monthly")
    session.commit()

    rows = session.query(GlobalCycleIndex).all()
    assert rows, "Global cycle index rows should be persisted"

    sample_gbc = session.query(GlobalCycleIndex).filter_by(cycle_type="business").first()
    assert sample_gbc is not None
    assert sample_gbc.cycle_regime == gbc_regime_from_z(float(sample_gbc.cycle_score))

    sample_trade = session.query(GlobalCycleIndex).filter_by(cycle_type="trade").first()
    assert sample_trade.cycle_regime == gtmc_regime_from_z(float(sample_trade.cycle_score))

    sample_com = session.query(GlobalCycleIndex).filter_by(cycle_type="commodity_energy").first()
    assert sample_com.cycle_regime == commodity_regime_from_z(float(sample_com.cycle_score))

    sample_gic = session.query(GlobalCycleIndex).filter_by(cycle_type="inflation").first()
    assert sample_gic.cycle_regime == gic_regime_from_z(float(sample_gic.cycle_score))

    sample_gfc = session.query(GlobalCycleIndex).filter_by(cycle_type="financial_conditions").first()
    assert sample_gfc.cycle_regime == gfc_regime_from_z(float(sample_gfc.cycle_score))

