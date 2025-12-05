"""Feature assembly logic for country-year records."""


import datetime as dt
import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
pytestmark = pytest.mark.integration
from sqlalchemy.orm import Session, sessionmaker

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.config.country_universe import COUNTRY_UNIVERSE
from app.db.models import Base, Country, CountryYearFeatures, Indicator, ShippingCountryMonth, TimeSeriesValue
from app.features.build_country_year_features import build_country_year_features
from app.features.data_quality import compute_data_freshness_score


@pytest.fixture()
def session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        conn.execute(text("ATTACH DATABASE ':memory:' AS raw"))
        conn.execute(text("ATTACH DATABASE ':memory:' AS warehouse"))
        conn.execute(text("ATTACH DATABASE ':memory:' AS graph"))
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()

    countries = [Country(id=iso, name=iso, region="Test", income_group="Test") for iso in COUNTRY_UNIVERSE]
    session.add_all(countries)

    indicators = [
        Indicator(id=1, source="TEST", source_code="GDP", canonical_code="GDP_REAL"),
        Indicator(id=2, source="TEST", source_code="CPI", canonical_code="CPI_YOY"),
        Indicator(id=3, source="TEST", source_code="CA", canonical_code="CA_PCT_GDP"),
        Indicator(id=4, source="TEST", source_code="DEBT", canonical_code="DEBT_PCT_GDP"),
        Indicator(id=5, source="TEST", source_code="UNEMP", canonical_code="UNEMP_RATE"),
        Indicator(id=6, source="TEST", source_code="CO2", canonical_code="CO2_PER_CAPITA"),
        Indicator(id=7, source="TEST", source_code="ENERGY", canonical_code="ENERGY_IMPORT_DEP"),
        Indicator(id=8, source="TEST", source_code="FOOD", canonical_code="FOOD_IMPORT_DEP"),
        Indicator(id=9, source="TEST", source_code="GDELT", canonical_code="GDELT_EVENT_COUNT"),
        Indicator(id=10, source="TEST", source_code="POLICY", canonical_code="POLICY_RATE_CHANGE_FLAG"),
    ]
    session.add_all(indicators)

    values = [
        TimeSeriesValue(indicator_id=1, country_id="USA", date=dt.date(2022, 12, 31), value=1000.0),
        TimeSeriesValue(indicator_id=1, country_id="USA", date=dt.date(2023, 12, 31), value=1100.0),
        TimeSeriesValue(indicator_id=2, country_id="USA", date=dt.date(2023, 6, 30), value=5.0),
        TimeSeriesValue(indicator_id=3, country_id="USA", date=dt.date(2023, 12, 31), value=2.0),
        TimeSeriesValue(indicator_id=4, country_id="USA", date=dt.date(2023, 12, 31), value=60.0),
        TimeSeriesValue(indicator_id=5, country_id="USA", date=dt.date(2023, 12, 31), value=6.0),
        TimeSeriesValue(indicator_id=6, country_id="USA", date=dt.date(2023, 12, 31), value=4.0),
        TimeSeriesValue(indicator_id=7, country_id="USA", date=dt.date(2023, 12, 31), value=30.0),
        TimeSeriesValue(indicator_id=8, country_id="USA", date=dt.date(2023, 12, 31), value=20.0),
        TimeSeriesValue(indicator_id=9, country_id="USA", date=dt.date(2023, 1, 15), value=2.0),
        TimeSeriesValue(indicator_id=9, country_id="USA", date=dt.date(2023, 6, 1), value=3.0),
    ]
    session.add_all(values)

    shipping_rows = [
        ShippingCountryMonth(id=1, country_id="USA", year=2022, month=1, activity_level=10),
        ShippingCountryMonth(id=2, country_id="USA", year=2022, month=2, activity_level=20),
        ShippingCountryMonth(id=3, country_id="USA", year=2022, month=3, activity_level=90),
        ShippingCountryMonth(id=4, country_id="USA", year=2023, month=1, activity_level=50),
        ShippingCountryMonth(id=5, country_id="USA", year=2023, month=2, activity_level=60),
        ShippingCountryMonth(id=6, country_id="USA", year=2023, month=3, activity_level=70),
    ]
    session.add_all(shipping_rows)
    session.commit()
    try:
        yield session
    finally:
        session.close()


def test_build_country_year_features(session: Session) -> None:
    target_year = 2023
    build_country_year_features(session, target_year)

    usa_features = (
        session.query(CountryYearFeatures)
        .filter(CountryYearFeatures.country_id == "USA", CountryYearFeatures.year == target_year)
        .one()
    )

    assert float(usa_features.gdp_real) == pytest.approx(1100.0)
    assert float(usa_features.gdp_growth) == pytest.approx(10.0)
    assert float(usa_features.inflation_cpi) == pytest.approx(5.0)
    assert float(usa_features.ca_pct_gdp) == pytest.approx(2.0)
    assert float(usa_features.debt_pct_gdp) == pytest.approx(60.0)
    assert float(usa_features.unemployment_rate) == pytest.approx(6.0)
    assert float(usa_features.co2_per_capita) == pytest.approx(4.0)
    assert float(usa_features.energy_import_dep) == pytest.approx(30.0)
    assert float(usa_features.food_import_dep) == pytest.approx(20.0)
    assert float(usa_features.shipping_activity_level) == pytest.approx(180.0)
    assert float(usa_features.shipping_activity_change) == pytest.approx(50.0)
    assert float(usa_features.event_stress_pulse) == pytest.approx(5.0)

    assert float(usa_features.data_coverage_score) == pytest.approx(100.0)

    latest_obs_date = dt.date(2023, 12, 31)
    expected_freshness = compute_data_freshness_score(latest_obs_date, dt.date.today())
    assert float(usa_features.data_freshness_score) == pytest.approx(expected_freshness)

    other_countries = (
        session.query(CountryYearFeatures)
        .filter(CountryYearFeatures.country_id != "USA", CountryYearFeatures.year == target_year)
        .all()
    )
    assert all(feature.data_coverage_score == 0 for feature in other_countries)
