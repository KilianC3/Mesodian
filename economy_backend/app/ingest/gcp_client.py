import datetime as dt
import logging
from io import StringIO
from typing import Iterable, Optional

import pandas as pd
from sqlalchemy.orm import Session

from app.config.country_universe import COUNTRY_UNIVERSE
from app.db.models import RawGcp
from app.ingest.sample_mode import (
    SampleConfig,
    validate_timeseries_data,
    limit_dataframe_by_country,
    IngestionError,
)
from app.ingest.utils import bulk_upsert_timeseries, resolve_indicator_id, store_raw_payload

logger = logging.getLogger(__name__)


CO2_DATA_URL = "https://raw.githubusercontent.com/owid/co2-data/master/owid-co2-data.csv"


def fetch_co2_dataset() -> pd.DataFrame:
    """Fetch CO2 dataset from OWID GitHub repository with proper error handling."""
    try:
        import httpx
    except ImportError as exc:  # pragma: no cover - dependency missing
        raise RuntimeError("httpx is required to fetch CO2 data") from exc

    try:
        response = httpx.get(CO2_DATA_URL, timeout=90.0, follow_redirects=True)
        response.raise_for_status()
        df = pd.read_csv(StringIO(response.text))
        # CRITICAL: Reset index immediately after CSV read to avoid duplicate index issues
        df = df.reset_index(drop=True)
        return df
    except httpx.HTTPStatusError as e:
        raise IngestionError("GCP", "N/A", f"HTTP {e.response.status_code}: {e.response.text[:200]}")
    except Exception as e:
        raise IngestionError("GCP", "N/A", f"Fetch error: {e}")


def ingest_full(
    session: Session,
    *,
    country_subset: Optional[Iterable[str]] = None,
    year_subset: Optional[Iterable[int]] = None,
    sample_config: Optional[SampleConfig] = None,
) -> None:
    """Ingest GCP CO2 data with optional sample mode."""
    sample_config = sample_config or SampleConfig()
    selected_countries = set(country_subset) if country_subset else set(COUNTRY_UNIVERSE)
    if sample_config.enabled:
        selected_countries = set(list(selected_countries)[:5])
    
    selected_years = set(year_subset) if year_subset else None

    try:
        df = fetch_co2_dataset()
        if df.empty and sample_config.fail_on_empty:
            raise IngestionError("GCP", "N/A", "Empty dataset")
        
        # CSV already has 'country' column (full name) - drop it and use iso_code
        df = df.drop(columns=["country"], errors="ignore")
        df = df.rename(columns={"iso_code": "country", "year": "date"})
        
        # Filter by country (now using ISO codes)
        df = df[df["country"].isin(selected_countries)]
        
        # Filter by years if specified
        if selected_years:
            df = df[df["date"].isin(selected_years)]
        
        # CRITICAL: Reset index BEFORE limiting to avoid duplicate index
        df = df.reset_index(drop=True)
        
        # Limit to sample size AFTER reset
        if sample_config.enabled and not df.empty:
            # Group by country and take last N records per country
            df = df.groupby("country", group_keys=False).tail(sample_config.max_records_per_country)
            df = df.reset_index(drop=True)

        indicator_total = resolve_indicator_id(session, "CO2_TOTAL")
        indicator_per_capita = resolve_indicator_id(session, "CO2_PER_CAPITA")
    except Exception as e:
        logger.error(f"GCP: Failed to fetch/resolve: {e}")
        if sample_config.strict_validation:
            raise IngestionError("GCP", "N/A", f"Init error: {e}")
        return

    # Convert NaN to None before JSON serialization to avoid "NaN is not valid JSON" errors
    import math
    def clean_nan(obj):
        """Recursively replace NaN with None in nested structures."""
        if isinstance(obj, dict):
            return {k: clean_nan(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [clean_nan(item) for item in obj]
        elif isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        else:
            return obj
    
    payload_dict = df.to_dict()
    payload_clean = clean_nan(payload_dict)
    store_raw_payload(session, RawGcp, params={"type": "co2"}, payload=payload_clean)

    rows = []
    for _, row in df.iterrows():
        try:
            date = dt.date(int(row["date"]), 12, 31)
        except Exception as exc:
            logger.error(f"GCP: Bad date {row.get('date')}: {exc}")
            if sample_config.strict_validation:
                raise IngestionError("GCP", str(row.get("country")), f"Date parse error: {exc}")
            continue
        for indicator_id, column in [
            (indicator_total, "co2"),
            (indicator_per_capita, "co2_per_capita"),
        ]:
            value = row.get(column)
            if pd.isna(value):
                continue
            rows.append(
                {
                    "indicator_id": indicator_id,
                    "country_id": row["country"],
                    "date": date,
                    "value": float(value),
                    "source": "OWID_CO2",
                    "ingested_at": dt.datetime.now(dt.timezone.utc),
                }
            )
    
    # Validate results
    if sample_config.enabled and rows:
        validation = validate_timeseries_data(
            rows,
            expected_countries=list(selected_countries),
            sample_config=sample_config
        )
        if sample_config.strict_validation:
            validation.raise_if_invalid()

    bulk_upsert_timeseries(session, rows)
    logger.info(f"GCP: Ingested {len(rows)} records")
    session.commit()

