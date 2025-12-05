"""Shared pytest fixtures and markers for database-backed and unit tests."""
import os
from typing import Generator

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

DEFAULT_DB_URL = "postgresql+psycopg2://economy:economy@postgres:5432/economy_dev"


@pytest.fixture(scope="session")
def database_url() -> str:
    """Provide the database URL for tests, defaulting to the compose Postgres service."""
    return os.environ.get("DATABASE_URL", DEFAULT_DB_URL)


@pytest.fixture(scope="session")
def migrated_engine(database_url: str):
    """Apply Alembic migrations once and yield a ready SQLAlchemy engine."""
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(config, "head")
    engine = create_engine(database_url, future=True)
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture()
def db_session(migrated_engine) -> Generator[Session, None, None]:
    """Create a transactional session against the migrated database for integration tests."""
    connection = migrated_engine.connect()
    transaction = connection.begin()
    SessionLocal = sessionmaker(bind=connection, autoflush=False, autocommit=False, future=True)
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
        transaction.rollback()
        connection.close()


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Default all tests to the ``unit`` marker unless explicitly marked otherwise."""
    for item in items:
        if "integration" in item.keywords or "unit" in item.keywords:
            continue
        item.add_marker(pytest.mark.unit)
