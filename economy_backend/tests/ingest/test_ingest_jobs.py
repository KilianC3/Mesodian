"""Tests covering ingestion clients, catalogue helpers, and DB.nomics loaders."""

from typing import Dict

from unittest.mock import MagicMock

import pytest

from app.ingest import jobs


@pytest.fixture
def session_mock() -> MagicMock:
    return MagicMock()


def test_ingest_all_health_check_success(monkeypatch: pytest.MonkeyPatch, session_mock: MagicMock) -> None:
    calls = []

    for provider, module in jobs.PROVIDERS:
        monkeypatch.setattr(module, "ingest_full", lambda session, name=provider: calls.append(name))

    result = jobs.ingest_all_health_check(session_mock)

    assert set(result) == {name for name, _ in jobs.PROVIDERS}
    assert all(status["ok"] for status in result.values())
    assert all(status["error"] is None for status in result.values())
    assert set(calls) == {name for name, _ in jobs.PROVIDERS}


def test_ingest_all_health_check_failure(monkeypatch: pytest.MonkeyPatch, session_mock: MagicMock) -> None:
    calls: Dict[str, int] = {name: 0 for name, _ in jobs.PROVIDERS}

    def failing(*_, **__):
        calls["FRED"] += 1
        raise RuntimeError("boom")

    for provider, module in jobs.PROVIDERS:
        if provider == "FRED":
            monkeypatch.setattr(module, "ingest_full", failing)
        else:
            monkeypatch.setattr(module, "ingest_full", lambda session, name=provider: calls.__setitem__(name, calls[name] + 1))

    result = jobs.ingest_all_health_check(session_mock)

    assert result["FRED"]["ok"] is False
    assert "boom" in result["FRED"]["error"]
    assert session_mock.rollback.called is True

    for provider, count in calls.items():
        assert count == 1
        if provider != "FRED":
            assert result[provider]["ok"] is True
            assert result[provider]["error"] is None
