<<<<<<<< HEAD:economy_backend/tests/features/test_indicator_timing.py
"""Tests for feature construction and supporting configuration utilities."""
========
"""Computation of economic, ESG, and timing metrics."""

>>>>>>>> codex/create-alembic-migration-for-edge_type_enum:economy_backend/tests/metrics/test_indicator_timing.py

import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
pytestmark = pytest.mark.unit
from sqlalchemy.orm import Session, sessionmaker

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.db.models import Base, Indicator  # noqa: E402
from app.metrics.indicator_timing import TIMING_CLASS_MAP, apply_timing_classification  # noqa: E402


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


def test_timing_classes_are_applied(session: Session) -> None:
    for idx, code in enumerate(TIMING_CLASS_MAP.keys(), start=1):
        session.add(
            Indicator(
                id=idx,
                source="unit-test",
                source_code=code,
                canonical_code=code,
                frequency="monthly",
            )
        )
    session.commit()

    apply_timing_classification(session)
    session.commit()

    stored = {
        ind.canonical_code: ind.timing_class
        for ind in session.query(Indicator).filter(Indicator.canonical_code.in_(TIMING_CLASS_MAP))
    }
    assert stored
    for code, timing in TIMING_CLASS_MAP.items():
        assert stored[code] == timing
