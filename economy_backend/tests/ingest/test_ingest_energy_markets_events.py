"""Ingestion clients and data fetching helpers."""


import datetime as dt

import pandas as pd
import pytest
from pathlib import Path
pytestmark = pytest.mark.integration
import sys
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

sys.path.append(str(Path(__file__).resolve().parents[2]))

from app.db.models import (
    Asset,
    AssetPrice,
    Base,
    Country,
    CountryYearFeatures,
    Indicator,
    RawAisstream,
    RawEia,
    RawEmber,
    RawGcp,
    RawGdelt,
    RawRss,
    RawStooq,
    RawYfinance,
    ShippingCountryMonth,
    TimeSeriesValue,
)
from app.ingest import aisstream_client, eia_client, ember_client, gcp_client, gdelt_client, rss_client, stooq_client, yfinance_client


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
        Indicator(id=1, source="EIA", source_code="TOTAL.CONS_TOT.A", canonical_code="EIA_ENERGY_CONSUMPTION_TOTAL"),
        Indicator(id=2, source="EIA", source_code="PET.RWTC.D", canonical_code="EIA_WTI_PRICE"),
        Indicator(id=3, source="EMBER", source_code="SOLAR", canonical_code="EMBER_ELECTRICITY_SOLAR"),
        Indicator(id=4, source="EMBER", source_code="WIND", canonical_code="EMBER_ELECTRICITY_WIND"),
        Indicator(id=5, source="OWID", source_code="CO2", canonical_code="CO2_TOTAL"),
        Indicator(id=6, source="OWID", source_code="CO2_PC", canonical_code="CO2_PER_CAPITA"),
        Indicator(id=7, source="GDELT", source_code="COUNT", canonical_code="GDELT_EVENT_COUNT"),
        Indicator(id=8, source="GDELT", source_code="TONE", canonical_code="GDELT_GOLDSTEIN"),
        Indicator(id=9, source="RSS", source_code="POLICY", canonical_code="POLICY_RATE_CHANGE_FLAG"),
    ]
    session.add_all(indicators)
    session.commit()
    return session


def test_eia_ember_co2_ingestion(monkeypatch: pytest.MonkeyPatch) -> None:
    session = setup_session()

    async def fake_eia(series_id: str):
        return {"series": [{"data": [["2023-01-01", 10.0], ["2023-02-01", 12.0]]}]}

    monkeypatch.setattr(eia_client, "fetch_series", fake_eia)
    eia_client.ingest_full(session, country_subset=["USA"], series_subset=["PET.RWTC.D"])

    df = pd.DataFrame(
        [
            {"Country code": "USA", "Technology": "Solar", "TWh": 5, "Year": 2022},
            {"Country code": "USA", "Technology": "Wind", "TWh": 7, "Year": 2022},
        ]
    )
    monkeypatch.setattr(ember_client, "fetch_csv", lambda url: df)
    ember_client.ingest_full(session, country_subset=["USA"], technology_subset=["Solar", "Wind"])

    co2_df = pd.DataFrame(
        [{"iso_code": "USA", "year": 2020, "co2": 100.0, "co2_per_capita": 2.5}]
    )
    monkeypatch.setattr(gcp_client, "fetch_co2_dataset", lambda: co2_df)
    gcp_client.ingest_full(session, country_subset=["USA"], year_subset=[2020])

    values = session.query(TimeSeriesValue).order_by(TimeSeriesValue.indicator_id.asc()).all()
    assert len(values) == 6
    assert float(values[0].value) == pytest.approx(10.0)
    assert float(values[1].value) == pytest.approx(12.0)
    assert float(values[2].value) == pytest.approx(5)
    assert float(values[3].value) == pytest.approx(7)
    assert float(values[4].value) == pytest.approx(100.0)
    assert float(values[5].value) == pytest.approx(2.5)

    assert session.query(RawEia).count() == 1
    assert session.query(RawEmber).count() == 1
    assert session.query(RawGcp).count() == 1


def test_market_price_ingestion(monkeypatch: pytest.MonkeyPatch) -> None:
    session = setup_session()

    price_df = pd.DataFrame(
        {
            "Open": [1.0, 2.0],
            "High": [1.1, 2.2],
            "Low": [0.9, 1.9],
            "Close": [1.05, 2.1],
            "Adj Close": [1.05, 2.1],
            "Volume": [100, 200],
        },
        index=pd.to_datetime(["2023-01-01", "2023-01-02"]),
    )

    monkeypatch.setattr(yfinance_client, "get_all_tickers", lambda: ["ABC"])
    monkeypatch.setattr(yfinance_client, "fetch_prices", lambda tickers, start, end: {"ABC": price_df})

    yfinance_client.ingest_full(session, lookback_days=5, batch_size=1, throttle_seconds=0)

    stooq_df = pd.DataFrame(
        {
            "Date": ["2023-01-01"],
            "Open": [10.0],
            "High": [11.0],
            "Low": [9.5],
            "Close": [10.5],
            "Volume": [1000],
        }
    )
    monkeypatch.setattr(stooq_client, "fetch_stooq_csv", lambda symbol: stooq_df)

    stooq_client.ingest_full(session, symbol_subset=["^SPX"])

    assets = session.query(Asset).all()
    prices = session.query(AssetPrice).all()
    assert len(assets) == 2
    assert len(prices) == 3
    assert session.query(RawYfinance).count() == 1
    assert session.query(RawStooq).count() == 1


def test_events_ingestion(monkeypatch: pytest.MonkeyPatch) -> None:
    session = setup_session()

    class DummySettings:
        aisstream_api_key = "test"

    async def fake_source():
        for msg in [
            {"timestamp": "2023-01-15T00:00:00", "country": "USA", "mmsi": "1"},
            {"timestamp": "2023-01-20T00:00:00", "country": "USA", "mmsi": "2"},
        ]:
            yield msg

    monkeypatch.setattr(aisstream_client, "get_settings", lambda: DummySettings())
    aisstream_client.ingest_full(session, duration_seconds=1, message_source=fake_source())

    gdelt_df = pd.DataFrame(
        [
            {"Actor1Geo_CountryCode": "USA", "SQLDATE": 20220101, "NumEvents": 3, "GoldsteinScale": 2.0},
            {"Actor1Geo_CountryCode": "USA", "SQLDATE": 20220601, "NumEvents": 2, "GoldsteinScale": -1.0},
        ]
    )
    monkeypatch.setattr(gdelt_client, "fetch_gdelt_events", lambda params=None: gdelt_df)
    gdelt_client.ingest_full(session, country_subset=["USA"], year_subset=[2022])

    monkeypatch.setattr(rss_client, "fetch_feed", lambda url: [{"title": "Rate decision", "published": "2023-02-01"}])
    rss_client.ingest_full(session, country_subset=["USA"])

    shipping = session.query(ShippingCountryMonth).one()
    assert shipping.country_id == "USA"
    assert float(shipping.activity_level) == pytest.approx(2)
    assert float(shipping.transits) == pytest.approx(2)

    stress = session.query(CountryYearFeatures).one()
    assert float(stress.event_stress_pulse) == pytest.approx(5)

    rss_rows = session.query(TimeSeriesValue).filter(TimeSeriesValue.indicator_id == 9).all()
    assert len(rss_rows) == 1

    assert session.query(RawAisstream).count() == 1
    assert session.query(RawGdelt).count() == 1
    assert session.query(RawRss).count() == 1

