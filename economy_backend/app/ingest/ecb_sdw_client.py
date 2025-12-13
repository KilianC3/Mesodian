import asyncio
import datetime as dt
import logging
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy.orm import Session

from app.db.models import RawEcb
from app.ingest.base_client import fetch_sdmx_dataset
from app.ingest.sample_mode import (
    SampleConfig,
    validate_timeseries_data,
    IngestionError,
)
from app.ingest.utils import bulk_upsert_timeseries, ensure_date, resolve_indicator_id, store_raw_payload

logger = logging.getLogger(__name__)


# ECB migrated to new API endpoint in 2024
# Old: https://sdw-wsrest.ecb.europa.eu/service
# New: https://data-api.ecb.europa.eu/service
# API format: {base_url}/data/{dataflow}/{key}
# Example: https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A
ECB_BASE_URL = "https://data-api.ecb.europa.eu/service"

ECB_SERIES_CONFIG: Dict[str, Dict[str, Any]] = {
    # FX reference rates - format: EXR/{frequency}.{currency}.{currency_denom}.{type}.{suffix}
    "EXR/D.USD.EUR.SP00.A": {
        "canonical_indicator": "FX_USD_EUR",
        "frequency": "D",
        "countries": ["EMU"],
    },
    "EXR/D.GBP.EUR.SP00.A": {
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
    sample_config: Optional[SampleConfig] = None,
) -> List[Dict[str, Any]]:
    """Parse ECB SDMX dataframe with strict validation."""
    sample_config = sample_config or SampleConfig()
    rows: List[Dict[str, Any]] = []
    
    if df is None or (hasattr(df, 'empty') and df.empty):
        if sample_config.fail_on_empty:
            raise IngestionError("ECB_SDW", country_id, "Empty dataframe")
        return rows
    
    for _, row in df.iterrows():  # type: ignore[call-arg]
        value = row.get("value")
        time_value = row.get("time")
        if value is None or time_value is None:
            if sample_config.strict_validation:
                logger.warning(f"ECB_SDW: Missing value or time in row")
            continue
        try:
            date = ensure_date(time_value)
            numeric_value = float(value)
        except Exception as exc:
            logger.error(f"ECB_SDW: Parse error for {country_id}: {exc}")
            if sample_config.strict_validation:
                raise IngestionError("ECB_SDW", country_id, f"Parse error: {exc}")
            continue
        rows.append(
            {
                "indicator_id": indicator_id,
                "country_id": country_id,
                "date": date,
                "value": numeric_value,
                "source": source,
                "ingested_at": dt.datetime.now(dt.timezone.utc),
            }
        )
    
    # Validate results
    if sample_config.enabled and rows:
        validation = validate_timeseries_data(
            rows,
            expected_countries=[country_id],
            sample_config=sample_config
        )
        if sample_config.strict_validation:
            validation.raise_if_invalid()
    
    return rows


def ingest_full(
    session: Session,
    *,
    series_subset: Optional[Iterable[str]] = None,
    sample_config: Optional[SampleConfig] = None,
) -> None:
    """Ingest ECB SDW data with optional sample mode."""
    sample_config = sample_config or SampleConfig()
    selected_series = set(series_subset) if series_subset else None
    
    # Limit series in sample mode
    series_to_fetch = list(ECB_SERIES_CONFIG.items())
    if selected_series:
        series_to_fetch = [(k, v) for k, v in series_to_fetch if k in selected_series]
    if sample_config.enabled:
        series_to_fetch = series_to_fetch[:2]

    async def _run() -> None:
        for dataset_code, cfg in series_to_fetch:
            try:
                indicator_id = resolve_indicator_id(session, cfg["canonical_indicator"])
                country_id = cfg.get("countries", ["EMU"])[0]
                df = fetch_sdmx_dataset(ECB_BASE_URL, dataset_code, sample_config=sample_config)
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
                    sample_config=sample_config,
                )
                bulk_upsert_timeseries(session, rows)
                logger.info(f"ECB_SDW: Ingested {len(rows)} records for {dataset_code}")
            except IngestionError:
                raise
            except Exception as e:
                logger.error(f"ECB_SDW: Failed for {dataset_code}: {e}")
                if sample_config.strict_validation:
                    raise IngestionError("ECB_SDW", "N/A", f"Failed for {dataset_code}: {e}")

    asyncio.run(_run())
    session.commit()

