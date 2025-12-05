"""Unit tests for the ingestion HTTP client wrapper with mocked responses."""

import asyncio
import random
from typing import Any, Dict, List, Optional

import httpx
import pytest

from app.ingest.base_client import AsyncHttpClient, HttpClientError


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


class MockAsyncClient:
    def __init__(self, responses: List[httpx.Response]) -> None:
        self.responses = responses
        self.calls: List[Dict[str, Any]] = []
        self.closed = False

    async def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> httpx.Response:
        self.calls.append({"path": path, "params": params})
        if not self.responses:
            raise RuntimeError("No more responses configured")
        return self.responses.pop(0)

    async def aclose(self) -> None:
        self.closed = True


@pytest.mark.anyio
async def test_get_json_success(monkeypatch: pytest.MonkeyPatch) -> None:
    responses = [
        httpx.Response(
            200,
            json={"result": "ok"},
            request=httpx.Request("GET", "https://example.com/data"),
        )
    ]
    mock_client = MockAsyncClient(responses)
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: mock_client)

    async with AsyncHttpClient("https://example.com") as client:
        payload = await client.get_json("/data", params={"q": 1})

    assert payload == {"result": "ok"}
    assert mock_client.calls == [{"path": "/data", "params": {"q": 1}}]
    assert mock_client.closed is True


@pytest.mark.anyio
async def test_retries_on_server_error(monkeypatch: pytest.MonkeyPatch) -> None:
    responses = [
        httpx.Response(500, request=httpx.Request("GET", "https://example.com/data")),
        httpx.Response(
            200,
            json={"result": "ok"},
            request=httpx.Request("GET", "https://example.com/data"),
        ),
    ]
    mock_client = MockAsyncClient(responses)
    sleep_calls: List[float] = []

    async def fake_sleep(duration: float) -> None:
        sleep_calls.append(duration)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: mock_client)
    monkeypatch.setattr(random, "uniform", lambda a, b: 0)

    async with AsyncHttpClient(
        "https://example.com", max_retries=2, backoff_base_seconds=0.1
    ) as client:
        payload = await client.get_json("/data")

    assert payload == {"result": "ok"}
    assert len(mock_client.calls) == 2
    assert pytest.approx(sleep_calls[0], rel=1e-3) == 0.1


@pytest.mark.anyio
async def test_raises_after_exhausting_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    responses = [
        httpx.Response(500, request=httpx.Request("GET", "https://example.com/data")),
        httpx.Response(502, request=httpx.Request("GET", "https://example.com/data")),
    ]
    mock_client = MockAsyncClient(responses)
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: mock_client)
    monkeypatch.setattr(random, "uniform", lambda a, b: 0)

    async def no_sleep(_duration: float) -> None:
        return None

    monkeypatch.setattr(asyncio, "sleep", no_sleep)

    async with AsyncHttpClient("https://example.com", max_retries=1) as client:
        with pytest.raises(HttpClientError):
            await client.get_json("/data")

    assert len(mock_client.calls) == 2
