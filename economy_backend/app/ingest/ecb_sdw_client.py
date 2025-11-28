import asyncio
import datetime as dt
import logging
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy.orm import Session

from app.db.models import RawEcb
from app.ingest.base_client import fetch_sdmx_dataset
from app.ingest.utils import bulk_upsert_timeseries, ensure_date, resolve_indicator_id, store_raw_payload

logger = logging.getLogger(__name__)


ECB_BASE_URL = "https://sdw-wsrest.ecb.europa.eu/service"

ECB_SERIES_CONFIG: Dict[str, Dict[str, Any]] = {
    # FX reference rates
    "EXR.D.USD.EUR.SP00.A": {
        "canonical_indicator": "FX_USD_EUR",
        "frequency": "D",
        "countries": ["EMU"],
    },
    "EXR.D.GBP.EUR.SP00.A": {
        "canonical_indicator": "FX_GBP_EUR",
        "frequency": "D",
        "countries": ["EMU"],
    },
}


def parse_ecb_dataframe(
    df: Any,
    *,
    indicator_id: int,
    country_id: str,
    source: str,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if df is None:
        return rows
    for _, row in df.iterrows():  # type: ignore[call-arg]
        value = row.get("value")
        time_value = row.get("time")
        if value is None or time_value is None:
            continue
        try:
            date = ensure_date(time_value)
            numeric_value = float(value)
        except Exception as exc:  # pragma: no cover - data dependent
            logger.warning("Skipping ECB row %s: %s", row, exc)
            continue
        rows.append(
            {
                "indicator_id": indicator_id,
                "country_id": country_id,
                "date": date,
                "value": numeric_value,
                "source": source,
                "ingested_at": dt.datetime.utcnow(),
            }
        )
    return rows


def ingest_full(
    session: Session,
    *,
    series_subset: Optional[Iterable[str]] = None,
) -> None:
    selected_series = set(series_subset) if series_subset else None

    async def _run() -> None:
        for dataset_code, cfg in ECB_SERIES_CONFIG.items():
            if selected_series and dataset_code not in selected_series:
                continue
            indicator_id = resolve_indicator_id(session, cfg["canonical_indicator"])
            country_id = cfg.get("countries", ["EMU"])[0]
            df = fetch_sdmx_dataset(ECB_BASE_URL, dataset_code)
            store_raw_payload(
                session,
                RawEcb,
                params={"dataset": dataset_code},
                payload=df.to_dict() if hasattr(df, "to_dict") else None,
            )
            rows = parse_ecb_dataframe(
                df,
                indicator_id=indicator_id,
                country_id=country_id,
                source="ECB_SDW",
            )
            bulk_upsert_timeseries(session, rows)

    asyncio.run(_run())
    session.commit()

