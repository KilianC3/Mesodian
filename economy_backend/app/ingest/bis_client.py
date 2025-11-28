import datetime as dt
import logging
from typing import Any, Dict, Iterable, Optional

from sqlalchemy.orm import Session

from app.config.country_universe import COUNTRY_UNIVERSE
from app.db.models import RawBis
from app.ingest.base_client import fetch_sdmx_dataset
from app.ingest.utils import (
    bulk_upsert_timeseries,
    parse_timeseries_rows,
    resolve_indicator_id,
    store_raw_payload,
)

logger = logging.getLogger(__name__)


BIS_BASE_URL = "https://stats.bis.org/api/v1"

BIS_SERIES: Dict[str, Dict[str, Any]] = {
    "BIS:WS_CREDIT": {
        "canonical_indicator": "BIS_CREDIT_PRIVATE",
        "countries": COUNTRY_UNIVERSE,
    },
    "BIS:LOC_CLAIMS": {
        "canonical_indicator": "BIS_CROSS_BORDER_CLAIMS",
        "countries": COUNTRY_UNIVERSE,
    },
}


def ingest_full(
    session: Session,
    *,
    dataset_subset: Optional[Iterable[str]] = None,
    country_subset: Optional[Iterable[str]] = None,
) -> None:
    selected_datasets = set(dataset_subset) if dataset_subset else None
    selected_countries = set(country_subset) if country_subset else None

    for dataset_code, cfg in BIS_SERIES.items():
        if selected_datasets and dataset_code not in selected_datasets:
            continue
        indicator_id = resolve_indicator_id(session, cfg["canonical_indicator"])
        countries = cfg.get("countries", COUNTRY_UNIVERSE)
        for country_id in countries:
            if selected_countries and country_id not in selected_countries:
                continue
            df = fetch_sdmx_dataset(BIS_BASE_URL, dataset_code, params={"c": country_id})
            payload_df = df.copy()
            if hasattr(payload_df, "columns"):
                for column in payload_df.columns:
                    payload_df[column] = payload_df[column].apply(
                        lambda v: v.isoformat() if isinstance(v, dt.date) else v
                    )
            store_raw_payload(
                session,
                RawBis,
                params={"dataset": dataset_code, "country": country_id},
                payload=payload_df.to_dict() if hasattr(payload_df, "to_dict") else None,
            )
            rows = parse_timeseries_rows(
                df,
                indicator_id=indicator_id,
                country_id=country_id,
                source="BIS",
            )
            # BIS datasets may not carry a source date; stamp ingestion time
            for row in rows:
                row.setdefault("ingested_at", dt.datetime.utcnow())
            bulk_upsert_timeseries(session, rows)

    session.commit()

