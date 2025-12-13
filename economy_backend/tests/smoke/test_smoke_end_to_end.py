"""End-to-end pipeline validation on synthetic data."""

import datetime as dt
from typing import Dict, List

import pandas as pd
import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.cycles.global_trade_cycle import GLOBAL_TRADE_CODES, compute_gtmc_index, write_gtmc_to_db
from app.features import build_country_year_features
from app.graph.algorithms import compute_trade_centrality
from app.graph.projection import project_country_nodes, project_trade_edges
from app.graph.schema_helpers import StructuralRole
from app.metrics.run_all import compute_all_country_metrics
from app.metrics.run_web_metrics import compute_all_web_and_edge_metrics
from app.db.models import (
    Country,
    CountryYearFeatures,
    EdgeMetric,
    GlobalCycleIndex,
    Indicator,
    Node,
    NodeMetric,
    TimeSeriesValue,
    TradeFlow,
)

pytestmark = pytest.mark.integration


def _seed_countries(session: Session) -> None:
    session.add_all(
        [
            Country(id="USA", name="United States", region="Americas", income_group="High"),
            Country(id="CAN", name="Canada", region="Americas", income_group="High"),
        ]
    )
    session.commit()


def _seed_indicators(session: Session) -> Dict[str, int]:
    indicators: List[Indicator] = []
    code_to_id: Dict[str, int] = {}
    next_id = 1
    feature_codes = [
        "GDP_REAL",
        "GDP_GROWTH",
        "INFLATION_CPI",
        "CA_PCT_GDP",
        "DEBT_PCT_GDP",
        "UNEMP_RATE",
        "CO2_PC",
        "ENERGY_IMP",
        "FOOD_IMP",
    ]
    for code in feature_codes + ["GREEN_PATENTS"] + GLOBAL_TRADE_CODES:
        indicators.append(
            Indicator(
                id=next_id,
                source="TEST",
                source_code=code,
                canonical_code=code,
                frequency="annual",
                category="macro",
            )
        )
        code_to_id[code] = next_id
        next_id += 1
    session.add_all(indicators)
    session.commit()
    return code_to_id


def _seed_timeseries(session: Session, code_to_id: Dict[str, int], year: int) -> None:
    def add_ts(code: str, country: str, value: float, month: int = 12) -> None:
        session.add(
            TimeSeriesValue(
                indicator_id=code_to_id[code],
                country_id=country,
                date=dt.date(year, month, 28),
                value=value,
                source="TEST",
            )
        )

    for country in ("USA", "CAN"):
        add_ts("GDP_REAL", country, 21000.0 if country == "USA" else 18000.0)
        add_ts("GDP_GROWTH", country, 2.5 if country == "USA" else 2.0)
        add_ts("INFLATION_CPI", country, 3.0 if country == "USA" else 2.5)
        add_ts("CA_PCT_GDP", country, -1.0 if country == "USA" else 0.5)
        add_ts("DEBT_PCT_GDP", country, 90.0 if country == "USA" else 60.0)
        add_ts("UNEMP_RATE", country, 4.0 if country == "USA" else 5.0)
        add_ts("CO2_PC", country, 10.0 if country == "USA" else 7.0)
        add_ts("ENERGY_IMP", country, 25.0 if country == "USA" else 18.0)
        add_ts("FOOD_IMP", country, 12.0 if country == "USA" else 10.0)
        add_ts("GREEN_PATENTS", country, 120.0 if country == "USA" else 80.0, month=6)

    for code in GLOBAL_TRADE_CODES:
        session.add(
            TimeSeriesValue(
                indicator_id=code_to_id[code],
                country_id=None,
                date=dt.date(year, 6, 30),
                value=100.0,
                source="TEST",
            )
        )
        session.add(
            TimeSeriesValue(
                indicator_id=code_to_id[code],
                country_id=None,
                date=dt.date(year, 9, 30),
                value=105.0,
                source="TEST",
            )
        )
    session.commit()


def _seed_trade_flows(session: Session, year: int) -> None:
    session.add_all(
        [
            TradeFlow(
                reporter_country_id="USA",
                partner_country_id="CAN",
                year=year,
                hs_section="01",
                flow_type="export",
                value_usd=150.0,
            ),
            TradeFlow(
                reporter_country_id="CAN",
                partner_country_id="USA",
                year=year,
                hs_section="02",
                flow_type="import",
                value_usd=90.0,
            ),
        ]
    )
    session.commit()


def test_end_to_end_pipeline(db_session: Session, monkeypatch: pytest.MonkeyPatch) -> None:
    year = 2023
    _seed_countries(db_session)
    code_to_id = _seed_indicators(db_session)
    _seed_timeseries(db_session, code_to_id, year)
    _seed_trade_flows(db_session, year)

    feature_map = {
        "gdp_real": "GDP_REAL",
        "gdp_growth": "GDP_GROWTH",
        "inflation_cpi": "INFLATION_CPI",
        "ca_pct_gdp": "CA_PCT_GDP",
        "debt_pct_gdp": "DEBT_PCT_GDP",
        "unemployment_rate": "UNEMP_RATE",
        "co2_per_capita": "CO2_PC",
        "energy_import_dep": "ENERGY_IMP",
        "food_import_dep": "FOOD_IMP",
    }

    monkeypatch.setattr(build_country_year_features, "COUNTRY_UNIVERSE", ["USA", "CAN"])  # type: ignore[attr-defined]
    monkeypatch.setattr(build_country_year_features, "FEATURE_INDICATORS", feature_map)  # type: ignore[attr-defined]
    monkeypatch.setattr(build_country_year_features, "STRESS_INDICATORS", [])  # type: ignore[attr-defined]

    def fake_fetch_series(_session: Session, _country_id: str, indicator_code: str, *_: object, **__: object) -> pd.Series:
        values = {
            "GDP_REAL": {year: 21000.0},
            "GDP_GROWTH": {year: 2.5},
            "INFLATION_CPI": {year: 3.0},
            "CA_PCT_GDP": {year: -1.0},
            "DEBT_PCT_GDP": {year: 90.0},
            "UNEMP_RATE": {year: 4.0},
            "CO2_PC": {year: 10.0},
            "ENERGY_IMP": {year: 25.0},
            "FOOD_IMP": {year: 12.0},
        }
        data = values.get(indicator_code, {})
        return pd.Series(data=data, index=[pd.to_datetime(dt.date(y, 12, 31)) for y in data])

    monkeypatch.setattr(build_country_year_features, "_fetch_series", fake_fetch_series)
    monkeypatch.setattr(
        build_country_year_features,
        "get_shipping_features_for_country",
        lambda _session, _country_id, _year: {
            "shipping_activity_level": 1.0,
            "shipping_activity_change": 0.05,
        },
    )

    build_country_year_features.build_country_year_features(db_session, year)
    compute_all_country_metrics(db_session, year)

    project_country_nodes(db_session)
    project_trade_edges(db_session, year)
    compute_trade_centrality(db_session, year)
    compute_all_web_and_edge_metrics(db_session, year)

    cycle_df = compute_gtmc_index(db_session)
    write_gtmc_to_db(db_session, cycle_df, frequency="monthly")

    first_node = db_session.execute(select(Node).limit(1)).scalar_one()
    first_node.structural_role = StructuralRole.CORE

    db_session.commit()

    feature_rows = db_session.execute(select(CountryYearFeatures)).scalars().all()
    assert {row.country_id for row in feature_rows} == {"USA", "CAN"}

    metric_codes = {
        metric.metric_code
        for metric in db_session.execute(select(NodeMetric)).scalars().all()
        if metric.metric_code in {"CR_RESILIENCE", "RISK_ENERGY", "RISK_FOOD", "ESG_TOTAL_SOV"}
    }
    assert metric_codes >= {"CR_RESILIENCE", "RISK_ENERGY"}

    cycles = db_session.execute(select(GlobalCycleIndex)).scalars().all()
    assert cycles, "global cycle index should persist"

    assert db_session.execute(select(EdgeMetric)).scalars().all()

    roles = {
        node.structural_role
        for node in db_session.execute(select(Node)).scalars().all()
    }
    assert StructuralRole.CORE in roles
