import os

os.environ.setdefault("DATABASE_URL", "sqlite://")
os.environ.setdefault("FRED_API_KEY", "dummy")
os.environ.setdefault("EIA_API_KEY", "dummy")
os.environ.setdefault("COMTRADE_API_KEY", "dummy")
os.environ.setdefault("AISSTREAM_API_KEY", "dummy")

from fastapi.testclient import TestClient

from app.main import app


def test_root_endpoint_returns_metadata():
    client = TestClient(app)
    resp = client.get("/")
    assert resp.status_code == 200
    payload = resp.json()
    assert "app_name" in payload


def test_health_router_available():
    client = TestClient(app)
    resp = client.get("/health")
    assert resp.status_code in (200, 503)
