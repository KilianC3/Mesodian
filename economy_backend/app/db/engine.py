from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker

from app.config import get_settings


def _build_engine() -> Engine:
    settings = get_settings()
    if not settings.postgres_url:
        raise RuntimeError("POSTGRES_URL must be configured for database access.")

    try:
        engine = create_engine(settings.postgres_url, pool_pre_ping=True, future=True)
    except Exception as exc:  # pragma: no cover - safety
        raise RuntimeError(f"Failed to create engine: {exc}") from exc

    return engine


engine = _build_engine()
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True, class_=Session)


def get_db() -> Generator[Session, None, None]:
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
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
