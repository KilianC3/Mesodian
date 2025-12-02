"""Ingestion helper for thematic DB.nomics providers (energy, trade, etc.)."""
from __future__ import annotations

from sqlalchemy.orm import Session

from app.db.engine import SessionLocal
from app.ingest.dbnomics_client import DBNomicsClient
from app.ingest.dbnomics_national_stats import _iter_indicator_configs, _load_config, _persist_payload


def ingest_thematic_series(session: Session, client: DBNomicsClient | None = None) -> None:
    cfg = _load_config()
    client = client or DBNomicsClient(base_url=cfg.get("default_base_url"))

    for indicator in _iter_indicator_configs("thematic"):
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
        ingest_thematic_series(session)
        print("Ingested DB.nomics thematic payloads")
