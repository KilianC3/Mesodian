import datetime as dt
import logging
from typing import Any, Dict, Iterable, Optional

from sqlalchemy.orm import Session

from app.config.country_universe import COUNTRY_UNIVERSE
from app.db.models import RawIlostat
from app.ingest.base_client import fetch_sdmx_dataset
from app.ingest.utils import (
    bulk_upsert_timeseries,
    parse_timeseries_rows,
    resolve_indicator_id,
    store_raw_payload,
)

logger = logging.getLogger(__name__)


ILOSTAT_BASE_URL = "https://api.ilo.org/v2/sdmx"

ILOSTAT_SERIES: Dict[str, Dict[str, Any]] = {
    "UNE_TUNE_RT_A": {
        "canonical_indicator": "ILOSTAT_UNEMPLOYMENT_RATE",
        "countries": COUNTRY_UNIVERSE,
    },
    "LFS_LFPR_T_A": {
        "canonical_indicator": "ILOSTAT_LABOUR_FORCE_PARTICIPATION",
        "countries": COUNTRY_UNIVERSE,
    },
}


def ingest_full(
    session: Session,
    *,
    series_subset: Optional[Iterable[str]] = None,
    country_subset: Optional[Iterable[str]] = None,
) -> None:
    selected_series = set(series_subset) if series_subset else None
    selected_countries = set(country_subset) if country_subset else None

    for series_code, cfg in ILOSTAT_SERIES.items():
        if selected_series and series_code not in selected_series:
            continue
        indicator_id = resolve_indicator_id(session, cfg["canonical_indicator"])
        for country_id in cfg.get("countries", COUNTRY_UNIVERSE):
            if selected_countries and country_id not in selected_countries:
                continue
            df = fetch_sdmx_dataset(
                ILOSTAT_BASE_URL,
                f"{series_code}/{country_id}.A",
                params={"contentType": "csv"},
            )
            store_raw_payload(
                session,
                RawIlostat,
                params={"series": series_code, "country": country_id},
                payload=df.to_dict() if hasattr(df, "to_dict") else None,
            )
            rows = parse_timeseries_rows(
                df,
                indicator_id=indicator_id,
                country_id=country_id,
                source="ILOSTAT",
                time_column="time",
                value_column="value",
                location_fields=("LOCATION", "ref_area", "REF_AREA"),
            )
            for row in rows:
                row.setdefault("ingested_at", dt.datetime.utcnow())
            bulk_upsert_timeseries(session, rows)

    session.commit()

