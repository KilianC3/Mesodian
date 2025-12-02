from __future__ import annotations

"""Ingestion of external sovereign ESG sources.

The ingestion functions intentionally accept optional pre-parsed records so they
can be exercised in tests without performing HTTP calls. In production they rely
on DB.Nomics mirrors where available and fall back to official APIs/CSV
endpoints for resilience.
"""

import logging
from typing import Iterable, List, Mapping, MutableMapping, Optional

import httpx
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.db.models import SovereignESGRaw
from app.ingest.dbnomics_client import DBNomicsClient

logger = logging.getLogger(__name__)

RawRecord = Mapping[str, object]


def _write_records(
    session: Session, provider: str, records: Iterable[RawRecord]
) -> List[SovereignESGRaw]:
    """Persist a collection of raw ESG records to ``sovereign_esg_raw``.

    Parameters
    ----------
    session:
        Active database session.
    provider:
        Provider code being ingested (e.g. ``"WB_ESG"`` or ``"WGI"``).
    records:
        Iterable of mappings containing ``country_code``, ``year``,
        ``indicator_code``, ``value`` and optional ``metadata``.
    """

    created: List[SovereignESGRaw] = []
    next_id = session.query(func.coalesce(func.max(SovereignESGRaw.id), 0)).scalar() or 0
    for record in records:
        next_id += 1
        raw = SovereignESGRaw(
            id=int(next_id),
            country_code=str(record["country_code"]),
            year=int(record["year"]),
            provider=provider,
            indicator_code=str(record["indicator_code"]),
            value=record["value"],
            data_metadata=record.get("metadata"),
        )
        session.add(raw)
        created.append(raw)
    session.flush()
    return created


def _fetch_dbnomics_series(
    client: DBNomicsClient, provider: str, dataset: str, series_code: str
) -> List[MutableMapping[str, object]]:
    """Fetch a single series via DB.Nomics and format records for storage."""

    series_id = f"{provider}/{dataset}/{series_code}"
    df = client.fetch_series(series_id)
    records: List[MutableMapping[str, object]] = []
    for _, row in df.iterrows():
        records.append(
            {
                "country_code": row.get("metadata", {}).get("country_code")
                or row.get("metadata", {}).get("iso3")
                or "UNK",
                "year": row["date"].year if hasattr(row["date"], "year") else row["date"],
                "indicator_code": series_code,
                "value": row["value"],
                "metadata": row.get("metadata", {}),
            }
        )
    return records


def ingest_world_bank_esg(
    session: Session,
    client: Optional[DBNomicsClient] = None,
    records: Optional[Iterable[RawRecord]] = None,
) -> List[SovereignESGRaw]:
    """Ingest World Bank ESG series, preferring DB.Nomics mirrors.

    Parameters are injectable to facilitate testing without external calls.
    """

    if records is not None:
        return _write_records(session, "WB_ESG", records)

    fetched: List[RawRecord] = []
    if client:
        try:
            fetched.extend(
                _fetch_dbnomics_series(client, "WB", "ESG", "ENV.CO2.PP.GDP")
            )
        except Exception as exc:  # pragma: no cover - defensive path
            logger.warning("DB.Nomics ESG fetch failed: %s", exc)

    if not fetched:
        url = "https://api.worldbank.org/v2/country/all/indicator/EN.ATM.CO2E.PP.GD?format=json"
        try:
            response = httpx.get(url, timeout=30.0)
            response.raise_for_status()
            payload = response.json()[1]
        except Exception as exc:  # pragma: no cover - network path
            logger.error("World Bank ESG download failed: %s", exc)
            payload = []

        for row in payload:
            if row.get("value") is None:
                continue
            fetched.append(
                {
                    "country_code": row.get("countryiso3code") or "UNK",
                    "year": int(row.get("date")),
                    "indicator_code": "ENV_CO2_PER_GDP",
                    "value": row.get("value"),
                    "metadata": {"source_indicator": row.get("indicator", {}).get("id")},
                }
            )

    return _write_records(session, "WB_ESG", fetched)


def ingest_wgi(
    session: Session,
    client: Optional[DBNomicsClient] = None,
    records: Optional[Iterable[RawRecord]] = None,
) -> List[SovereignESGRaw]:
    """Ingest Worldwide Governance Indicators using DB.Nomics when mirrored."""

    if records is not None:
        return _write_records(session, "WGI", records)

    fetched: List[RawRecord] = []
    if client:
        try:
            fetched.extend(_fetch_dbnomics_series(client, "WGI", "WGI", "RULE_OF_LAW"))
        except Exception as exc:  # pragma: no cover - defensive path
            logger.warning("DB.Nomics WGI fetch failed: %s", exc)

    if not fetched:
        url = "https://api.worldbank.org/v2/en/indicator/CC.EST?downloadformat=json"
        try:
            response = httpx.get(url, timeout=30.0)
            response.raise_for_status()
            payload = response.json()[1]
        except Exception as exc:  # pragma: no cover
            logger.error("WGI download failed: %s", exc)
            payload = []

        for row in payload:
            if row.get("value") is None:
                continue
            fetched.append(
                {
                    "country_code": row.get("countryiso3code") or "UNK",
                    "year": int(row.get("date")),
                    "indicator_code": "CONTROL_OF_CORRUPTION",
                    "value": row.get("value"),
                    "metadata": {"source_indicator": row.get("indicator", {}).get("id")},
                }
            )

    return _write_records(session, "WGI", fetched)


def ingest_nd_gain(
    session: Session,
    client: Optional[DBNomicsClient] = None,
    records: Optional[Iterable[RawRecord]] = None,
) -> List[SovereignESGRaw]:
    """Ingest ND-GAIN country index values."""

    if records is not None:
        return _write_records(session, "ND_GAIN", records)

    fetched: List[RawRecord] = []
    if client:
        try:
            fetched.extend(_fetch_dbnomics_series(client, "NDGAIN", "INDEX", "TOTAL"))
        except Exception as exc:  # pragma: no cover - defensive path
            logger.warning("DB.Nomics ND-GAIN fetch failed: %s", exc)

    if not fetched:
        url = "https://gain.nd.edu/assets/225476/nd_gain_country_index.csv"
        try:
            response = httpx.get(url, timeout=30.0)
            response.raise_for_status()
            text = response.text.splitlines()
        except Exception as exc:  # pragma: no cover
            logger.error("ND-GAIN download failed: %s", exc)
            text = []

        for line in text[1:]:  # skip header
            parts = line.split(",")
            if len(parts) < 4:
                continue
            try:
                year = int(parts[0])
                country_code = parts[1]
                value = float(parts[3])
            except ValueError:
                continue
            fetched.append(
                {
                    "country_code": country_code,
                    "year": year,
                    "indicator_code": "ND_GAIN_TOTAL",
                    "value": value,
                    "metadata": {"source_row": line},
                }
            )

    return _write_records(session, "ND_GAIN", fetched)


def ingest_epi(
    session: Session,
    client: Optional[DBNomicsClient] = None,
    records: Optional[Iterable[RawRecord]] = None,
) -> List[SovereignESGRaw]:
    """Ingest Yale Environmental Performance Index data."""

    if records is not None:
        return _write_records(session, "EPI", records)

    fetched: List[RawRecord] = []
    if client:
        try:
            fetched.extend(_fetch_dbnomics_series(client, "EPI", "SCORES", "EPI_TOTAL"))
        except Exception as exc:  # pragma: no cover
            logger.warning("DB.Nomics EPI fetch failed: %s", exc)

    if not fetched:
        url = "https://epi.yale.edu/downloads/epi2022results.csv"
        try:
            response = httpx.get(url, timeout=30.0)
            response.raise_for_status()
            text = response.text.splitlines()
        except Exception as exc:  # pragma: no cover
            logger.error("EPI download failed: %s", exc)
            text = []

        for line in text[1:]:
            parts = line.split(",")
            if len(parts) < 3:
                continue
            country_code = parts[0]
            try:
                score = float(parts[2])
            except ValueError:
                continue
            fetched.append(
                {
                    "country_code": country_code,
                    "year": 2022,
                    "indicator_code": "EPI_TOTAL",
                    "value": score,
                    "metadata": {"source_row": line},
                }
            )

    return _write_records(session, "EPI", fetched)


if __name__ == "__main__":  # pragma: no cover - CLI entry
    from app.db.engine import SessionLocal

    session = SessionLocal()
    try:
        client = DBNomicsClient()
        ingest_world_bank_esg(session, client=client)
        ingest_wgi(session, client=client)
        ingest_nd_gain(session, client=client)
        ingest_epi(session, client=client)
        session.commit()
        print("ESG ingestion completed")
    finally:
        session.close()
