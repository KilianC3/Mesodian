<<<<<<<< HEAD:economy_backend/tests/ingest/test_catalogue_and_timing.py
"""Unit tests for indicator catalogue validation and timing classification."""
========
"""Computation of economic, ESG, and timing metrics."""


import sys
from pathlib import Path
>>>>>>>> codex/create-alembic-migration-for-edge_type_enum:economy_backend/tests/metrics/test_catalogue_and_timing.py

import pytest
from sqlalchemy import create_engine, text
pytestmark = pytest.mark.integration
from sqlalchemy.orm import Session, sessionmaker

<<<<<<<< HEAD:economy_backend/tests/ingest/test_catalogue_and_timing.py
========
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

>>>>>>>> codex/create-alembic-migration-for-edge_type_enum:economy_backend/tests/metrics/test_catalogue_and_timing.py
from app.db.models import Base, Indicator
from app.metrics.catalogue import MetricScope, get_metric_scope, is_valid_metric_code
from app.metrics.indicator_timing import TIMING_CLASS_MAP, apply_timing_classification


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


def test_catalogue_contains_expected_scopes():
    # Ensure a few representative codes resolve to the right scope
    assert is_valid_metric_code("GBC")
    assert get_metric_scope("GBC") == MetricScope.GLOBAL
    assert get_metric_scope("CR_RESILIENCE") == MetricScope.COUNTRY
    assert get_metric_scope("WEB_RISK_SCORE") == MetricScope.WEB
    assert get_metric_scope("EDGE_DEPENDENCE") == MetricScope.EDGE


def test_apply_timing_classification(session: Session):
    # Seed minimal indicators
    for idx, (code, timing) in enumerate(TIMING_CLASS_MAP.items(), start=1):
        session.add(
            Indicator(
                id=idx,
                source="test",
                source_code=code,
                canonical_code=code,
                frequency="monthly",
                unit="index",
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
    for code, timing_class in TIMING_CLASS_MAP.items():
        assert stored[code] == timing_class


def test_unknown_metric_scope_raises():
    with pytest.raises(ValueError):
        get_metric_scope("UNKNOWN_METRIC")

