"""Ingestion helper for national statistics via DB.nomics."""
from __future__ import annotations

import datetime as dt
import datetime as dt
from pathlib import Path
from typing import Any, Dict, Iterable

import pandas as pd
import yaml
from sqlalchemy.orm import Session

from app.db.engine import SessionLocal
from app.db.models import RawDbnomics
from app.ingest.dbnomics_client import DBNomicsClient, SeriesInfo

CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "dbnomics.yml"


def _load_config() -> Dict[str, Any]:
    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _iter_indicator_configs(group: str) -> Iterable[Dict[str, Any]]:
    cfg = _load_config()
    for indicator in cfg.get("indicators", []):
        preferred = indicator.get("preferred_source")
        if preferred == group:
            yield indicator


def _persist_payload(session: Session, indicator_code: str, series: SeriesInfo, df: pd.DataFrame) -> None:
    serialisable = df.copy()
    if "date" in serialisable.columns:
        serialisable["date"] = pd.to_datetime(serialisable["date"]).dt.strftime("%Y-%m-%d")
    record = RawDbnomics(
        fetched_at=dt.datetime.utcnow(),
        params={"indicator_code": indicator_code, "series_code": series.series_code},
        payload=serialisable.to_dict(orient="records"),
    )
    session.add(record)


def ingest_national_stats(session: Session, client: DBNomicsClient | None = None) -> None:
    cfg = _load_config()
    base_url = cfg.get("default_base_url")
    client = client or DBNomicsClient(base_url=base_url)

    for indicator in _iter_indicator_configs("national_stats"):
        search = indicator.get("dbnomics_search", {})
        query = search.get("query")
        series_list = client.search_series(query, limit=5) if query else []
        if not series_list:
            continue
        series = series_list[0]
        df = client.fetch_series(series.series_code, frequency=search.get("filters", {}).get("frequency"))
        if df.empty:
            continue
        _persist_payload(session, indicator.get("code"), series, df)

    session.commit()


if __name__ == "__main__":  # pragma: no cover - CLI entry
    with SessionLocal() as session:
        ingest_national_stats(session)
        print("Ingested DB.nomics national statistics payloads")
