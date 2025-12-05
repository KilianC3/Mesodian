"""Ingestion clients and data fetching helpers."""


import datetime as dt

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
pytestmark = pytest.mark.integration
from sqlalchemy.orm import Session, sessionmaker

from app.db.models import (
    Base,
    Country,
    Indicator,
    RawBis,
    RawComtrade,
    RawFaostat,
    RawIlostat,
    RawOpenAlex,
    RawPatentsView,
    RawUnctad,
    TimeSeriesValue,
    TradeFlow,
)
from app.ingest import (
    bis_client,
    comtrade_client,
    faostat_client,
    ilostat_client,
    openalex_client,
    patentsview_client,
    unctad_client,
)


def setup_session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS raw"))
        conn.execute(text("ATTACH DATABASE ':memory:' AS warehouse"))
        conn.execute(text("ATTACH DATABASE ':memory:' AS graph"))
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    session.add_all(
        [
            Country(id="USA", name="United States", region="Americas", income_group="High"),
            Country(id="CAN", name="Canada", region="Americas", income_group="High"),
        ]
    )
    indicators = [
        Indicator(id=1, source="BIS", source_code="WS_CREDIT", canonical_code="BIS_CREDIT_PRIVATE"),
        Indicator(id=2, source="BIS", source_code="LOC_CLAIMS", canonical_code="BIS_CROSS_BORDER_CLAIMS"),
        Indicator(id=3, source="FAO", source_code="PROD", canonical_code="FAOSTAT_PRODUCTION"),
        Indicator(id=4, source="ILOSTAT", source_code="UNE_TUNE_RT_A", canonical_code="ILOSTAT_UNEMPLOYMENT_RATE"),
        Indicator(id=5, source="UNCTAD", source_code="FDI_FLOW_INWARD", canonical_code="UNCTAD_FDI_FLOW_INWARD"),
        Indicator(id=6, source="OPENALEX", source_code="WORKS", canonical_code="OPENALEX_WORKS_COUNT"),
        Indicator(id=7, source="USPTO", source_code="PATENTS", canonical_code="PATENTS_COUNT"),
    ]
    session.add_all(indicators)
    session.commit()
    return session


def test_comtrade_ingest(monkeypatch: pytest.MonkeyPatch) -> None:
    session = setup_session()

    async def fake_fetch_raw_trade(reporter: str, partner: str, year: int, section: str, *, client=None):
        assert reporter == "USA"
        assert partner == "CAN"
        assert year == 2022
        return {
            "data": [
                {
                    "reporter": reporter,
                    "partner": partner,
                    "period": year,
                    "cmdDescE": section,
                    "flowDesc": "Export",
                    "primaryValue": 123.4,
                }
            ]
        }

    monkeypatch.setattr(comtrade_client, "fetch_raw_trade", fake_fetch_raw_trade)

    comtrade_client.ingest_full(
        session,
        reporter_subset=["USA"],
        partner_subset=["CAN"],
        year_subset=[2022],
        section_subset=["AGRICULTURE"],
    )

    trades = session.query(TradeFlow).all()
    assert len(trades) == 1
    assert trades[0].reporter_country_id == "USA"
    assert float(trades[0].value_usd) == pytest.approx(123.4)

    raw = session.query(RawComtrade).all()
    assert len(raw) == 1


def test_timeseries_ingestion(monkeypatch: pytest.MonkeyPatch) -> None:
    session = setup_session()

    def fake_bis_fetch(base_url: str, dataset_code: str, params=None):
        return pd.DataFrame(
            [
                {"LOCATION": "USA", "time": dt.date(2020, 12, 31), "value": 10.0},
            ]
        )

    def fake_faostat(domain: str, params):
        return {
            "data": [
                {"Area Code (M49)": "USA", "Value": 50, "Year": 2021},
            ]
        }

    def fake_ilostat(base_url: str, dataset_code: str, params=None):
        return pd.DataFrame(
            [
                {"LOCATION": "USA", "time": "2022", "value": 6.5},
            ]
        )

    def fake_unctad(indicator: str, country: str):
        return {"data": [{"Country": country, "Year": 2020, "Value": 99.9}]}

    async def fake_openalex(concept_id: str, country: str, year: int, *, client=None):
        return {"meta": {"count": 5}}

    async def fake_patents(technology: str, country: str, year: int, *, client=None):
        return {"total": 7}

    monkeypatch.setattr(bis_client, "fetch_sdmx_dataset", fake_bis_fetch)
    monkeypatch.setattr(faostat_client, "fetch_faostat", fake_faostat)
    monkeypatch.setattr(ilostat_client, "fetch_sdmx_dataset", fake_ilostat)
    monkeypatch.setattr(unctad_client, "fetch_unctad", fake_unctad)
    monkeypatch.setattr(openalex_client, "fetch_openalex_works", fake_openalex)
    monkeypatch.setattr(patentsview_client, "fetch_patents", fake_patents)

    bis_client.ingest_full(session, dataset_subset=["BIS:WS_CREDIT"], country_subset=["USA"])
    faostat_client.ingest_full(session, indicator_subset=["production"], country_subset=["USA"])
    ilostat_client.ingest_full(session, series_subset=["UNE_TUNE_RT_A"], country_subset=["USA"])
    unctad_client.ingest_full(session, indicator_subset=["FDI_FLOW_INWARD"], country_subset=["USA"])
    openalex_client.ingest_full(session, country_subset=["USA"], year_subset=[2020], concept_subset=["C154945302"])
    patentsview_client.ingest_full(session, country_subset=["USA"], technology_subset=["computers"], year_subset=[2020])

    values = session.query(TimeSeriesValue).order_by(TimeSeriesValue.indicator_id.asc()).all()
    assert len(values) == 6
    # BIS
    assert values[0].indicator_id == 1 and float(values[0].value) == pytest.approx(10.0)
    # FAOSTAT
    assert values[1].indicator_id == 3 and float(values[1].value) == pytest.approx(50)
    # ILOSTAT
    assert values[2].indicator_id == 4 and float(values[2].value) == pytest.approx(6.5)
    # UNCTAD
    assert values[3].indicator_id == 5 and float(values[3].value) == pytest.approx(99.9)
    # OPENALEX
    assert values[4].indicator_id == 6 and float(values[4].value) == pytest.approx(5)
    # PATENTSVIEW
    assert values[5].indicator_id == 7 and float(values[5].value) == pytest.approx(7)

    assert session.query(RawBis).count() == 1
    assert session.query(RawFaostat).count() == 1
    assert session.query(RawIlostat).count() == 1
    assert session.query(RawUnctad).count() == 1
    assert session.query(RawOpenAlex).count() == 1
    assert session.query(RawPatentsView).count() == 1

