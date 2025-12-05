"""Ingestion clients and data fetching helpers."""


import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
pytestmark = pytest.mark.integration
from sqlalchemy.orm import Session, sessionmaker

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.db.models import Base, SovereignESGRaw
from app.ingest.esg_external import ingest_epi, ingest_nd_gain, ingest_wgi, ingest_world_bank_esg


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
    try:
        yield session
    finally:
        session.close()


def test_ingest_functions_accept_records(session: Session) -> None:
    wb_records = [
        {
            "country_code": "USA",
            "year": 2023,
            "indicator_code": "ENV_CO2_PER_GDP",
            "value": 1.5,
            "metadata": {"unit": "kg per gdp"},
        }
    ]
    wgi_records = [
        {
            "country_code": "USA",
            "year": 2023,
            "indicator_code": "CONTROL_OF_CORRUPTION",
            "value": 0.5,
            "metadata": {},
        }
    ]
    nd_gain_records = [
        {
            "country_code": "USA",
            "year": 2023,
            "indicator_code": "ND_GAIN_TOTAL",
            "value": 60.0,
            "metadata": {},
        }
    ]
    epi_records = [
        {
            "country_code": "USA",
            "year": 2023,
            "indicator_code": "EPI_TOTAL",
            "value": 75.0,
            "metadata": {},
        }
    ]

    ingest_world_bank_esg(session, records=wb_records)
    ingest_wgi(session, records=wgi_records)
    ingest_nd_gain(session, records=nd_gain_records)
    ingest_epi(session, records=epi_records)
    session.commit()

    records = session.query(SovereignESGRaw).all()
    assert len(records) == 4
    indicators = {rec.indicator_code for rec in records}
    assert indicators == {"ENV_CO2_PER_GDP", "CONTROL_OF_CORRUPTION", "ND_GAIN_TOTAL", "EPI_TOTAL"}
