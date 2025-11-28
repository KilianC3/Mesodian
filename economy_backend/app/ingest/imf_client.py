import asyncio
import datetime as dt
import logging
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy.orm import Session

from app.db.models import RawImf
from app.ingest.base_client import fetch_sdmx_dataset
from app.ingest.utils import bulk_upsert_timeseries, ensure_date, resolve_indicator_id, store_raw_payload

logger = logging.getLogger(__name__)


IMF_IFS_INDICATORS: Dict[str, Dict[str, Any]] = {
    "NGDP_R_SA_IX": {
        "canonical_indicator": "GDP_REAL_INDEX",
        "frequency": "Q",
    },
    "PCPI_IX": {
        "canonical_indicator": "CPI_INDEX",
        "frequency": "M",
    },
    "RNF_PA": {
        "canonical_indicator": "RESERVES_NFA",
        "frequency": "M",
    },
}

IMF_BASE_URL = "https://dataservices.imf.org/REST/SDMX_JSON.svc"


def _build_dataset_code(indicator_code: str, country: str, frequency: str) -> str:
    return f"IFS/{frequency}.{country}.{indicator_code}.A"  # Dataset path format used by IMF


def parse_imf_dataframe(
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
        time_value = row.get("time")
        value = row.get("value")
        if time_value is None or value is None:
            continue
        try:
            date = ensure_date(str(time_value))
            numeric_value = float(value)
        except Exception as exc:  # pragma: no cover - data dependent
            logger.warning("Skipping IMF row %s: %s", row, exc)
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
    country_subset: Optional[Iterable[str]] = None,
    indicator_subset: Optional[Iterable[str]] = None,
) -> None:
    selected_countries = set(country_subset) if country_subset else None
    selected_indicators = set(indicator_subset) if indicator_subset else None

    async def _run() -> None:
        for series_code, cfg in IMF_IFS_INDICATORS.items():
            if selected_indicators and series_code not in selected_indicators:
                continue
            indicator_id = resolve_indicator_id(session, cfg["canonical_indicator"])
            countries = selected_countries or []
            if not countries:
                from app.config.country_universe import COUNTRY_UNIVERSE

                countries = set(COUNTRY_UNIVERSE)
            for country_id in countries:
                dataset_code = _build_dataset_code(series_code, country_id, cfg.get("frequency", "M"))
                df = fetch_sdmx_dataset(IMF_BASE_URL, dataset_code)
                store_raw_payload(
                    session,
                    RawImf,
                    params={"dataset": dataset_code},
                    payload=df.to_dict() if hasattr(df, "to_dict") else None,
                )
                rows = parse_imf_dataframe(
                    df,
                    indicator_id=indicator_id,
                    country_id=country_id,
                    source="IMF_IFS",
                )
                bulk_upsert_timeseries(session, rows)

    asyncio.run(_run())
    session.commit()

