"""
Database engine and session utilities for SQLAlchemy.

This infrastructure module builds the SQLAlchemy engine from configured
environment variables, exposes session helpers for dependency injection in
FastAPI and scripts, and validates connectivity up front. It is used throughout
ingestion, metrics, and API layers wherever a database session is required.
"""

from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker

from app.config import get_settings
from app.db.models import Base  # noqa: F401


def _build_engine() -> Engine:
    """Construct an engine from environment settings with basic validation."""
    settings = get_settings()
    if not settings.database_url:
        raise RuntimeError("DATABASE_URL must be configured for database access.")

    try:
        engine = create_engine(settings.database_url, pool_pre_ping=True, future=True)
    except Exception as exc:  # pragma: no cover - safety
        raise RuntimeError(f"Failed to create engine: {exc}") from exc

    return engine


engine = _build_engine()
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True, class_=Session)
metadata = Base.metadata


def get_db() -> Generator[Session, None, None]:
    """Yield a SQLAlchemy session for FastAPI dependency injection."""
    db = SessionLocal()
    try:
        # Validate connectivity early to surface misconfiguration.
        db.execute(text("SELECT 1"))
        yield db
    except OperationalError as exc:
        db.close()
        raise RuntimeError(f"Database connection failed: {exc}") from exc
    finally:
        db.close()


@contextmanager
def db_session() -> Generator[Session, None, None]:
    """Context manager that opens and closes a SQLAlchemy session."""
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
