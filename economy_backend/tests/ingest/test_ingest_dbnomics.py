"""Unit tests for DB.nomics ingestion pipeline using stubbed client responses."""

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from app.db.models import Base, RawDbnomics  # noqa: E402
from app.ingest.dbnomics_client import SeriesInfo  # noqa: E402
from app.ingest.dbnomics_national_stats import ingest_national_stats  # noqa: E402


class StubClient:
    def search_series(self, query: str, provider_code=None, dataset_code=None, limit: int = 100):
        return [SeriesInfo(series_code="P/D/S", provider_code="P", dataset_code="D", name="stub", metadata={})]

    def fetch_series(self, series_code: str, frequency: str | None = None) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "provider": ["P"],
                "dataset": ["D"],
                "series_code": [series_code],
                "date": [pd.to_datetime("2023-01-01")],
                "value": [1.23],
                "metadata": [{"frequency": frequency}],
            }
        )


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


def test_dbnomics_ingestion_records_payload(session: Session) -> None:
    ingest_national_stats(session, client=StubClient())
    records = session.query(RawDbnomics).all()
    assert records
    payload = records[0].payload[0]
    assert payload["provider"] == "P"
    assert "date" in payload
