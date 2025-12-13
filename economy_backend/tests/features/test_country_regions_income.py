"""Feature assembly logic for country-year records."""


import logging
import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
pytestmark = pytest.mark.integration
from sqlalchemy.orm import Session, sessionmaker

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.db.models import Base, Country
from app.db import seed_countries


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


def test_seed_assigns_regions_and_income(monkeypatch, session: Session) -> None:
    monkeypatch.setattr(seed_countries, "COUNTRY_UNIVERSE", ["USA", "IND", "AUS"])

    def fake_income() -> dict[str, str]:
        return {"USA": "High income", "IND": "Lower middle income", "AUS": "High income"}

    def fake_names() -> dict[str, str]:
        return {"USA": "United States", "IND": "India", "AUS": "Australia"}

    monkeypatch.setattr(seed_countries, "fetch_worldbank_income_table", fake_income)
    monkeypatch.setattr(seed_countries, "fetch_worldbank_country_names", fake_names)

    seed_countries.seed_or_refresh_countries(session=session)

    usa = session.get(Country, "USA")
    ind = session.get(Country, "IND")
    aus = session.get(Country, "AUS")

    assert usa.region == "North America"
    assert ind.region == "Emerging Asia"
    assert aus.region == "Oceania"

    assert usa.income_group == "High income"
    assert ind.income_group == "Lower middle income"
    assert aus.income_group == "High income"


def test_seed_handles_missing_income(monkeypatch, session: Session) -> None:
    monkeypatch.setattr(seed_countries, "COUNTRY_UNIVERSE", ["USA", "IND"])

    def fake_income() -> dict[str, str]:
        return {"USA": "High income"}

    def fake_names() -> dict[str, str]:
        return {"USA": "United States", "IND": "India"}

    monkeypatch.setattr(seed_countries, "fetch_worldbank_income_table", fake_income)
    monkeypatch.setattr(seed_countries, "fetch_worldbank_country_names", fake_names)

    seed_countries.seed_or_refresh_countries(session=session)

    usa = session.get(Country, "USA")
    ind = session.get(Country, "IND")

    assert usa.income_group == "High income"
    assert ind.income_group == "Unknown"

