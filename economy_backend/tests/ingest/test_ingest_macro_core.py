"""Tests covering ingestion clients, catalogue helpers, and DB.nomics loaders."""

import datetime as dt

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from app.db.models import Base, Country, Indicator, TimeSeriesValue
from app.ingest import fred_client, wdi_client


def setup_session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS raw"))
        conn.execute(text("ATTACH DATABASE ':memory:' AS warehouse"))
        conn.execute(text("ATTACH DATABASE ':memory:' AS graph"))
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()
    session.add(Country(id="USA", name="United States", region="Americas", income_group="High"))
    session.add(Indicator(id=1, source="FRED", source_code="CPIAUCSL", canonical_code="CPI_USA_MONTHLY"))
    session.add(Indicator(id=2, source="WDI", source_code="NY.GDP.MKTP.KD", canonical_code="GDP_REAL"))
    session.commit()
    return session


def test_restricted_ingest(monkeypatch: pytest.MonkeyPatch) -> None:
    session = setup_session()

    async def fake_fred(series_id: str, **_: object):
        return {"observations": [{"date": "2023-01-01", "value": "123.4"}]}

    async def fake_wdi(country: str, indicator: str):
        assert country == "usa"
        assert indicator == "NY.GDP.MKTP.KD"
        return [{}, [{"date": "2022", "value": 456.7}]]

    monkeypatch.setattr(fred_client, "fetch_series", fake_fred)
    monkeypatch.setattr(wdi_client, "fetch_indicator", fake_wdi)

    fred_client.ingest_full(session, country_subset=["USA"], series_subset=["CPIAUCSL"])
    wdi_client.ingest_full(session, country_subset=["USA"], indicator_subset=["NY.GDP.MKTP.KD"])

    values = session.query(TimeSeriesValue).order_by(TimeSeriesValue.date.asc()).all()
    assert len(values) == 2

    assert values[0].indicator_id == 2
    assert values[0].country_id == "USA"
    assert values[0].date == dt.date(2022, 12, 31)
    assert float(values[0].value) == pytest.approx(456.7)

    assert values[1].indicator_id == 1
    assert values[1].country_id == "USA"
    assert values[1].date == dt.date(2023, 1, 1)
    assert float(values[1].value) == pytest.approx(123.4)

