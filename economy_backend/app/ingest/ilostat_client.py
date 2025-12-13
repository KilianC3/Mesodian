import datetime as dt
import logging
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import httpx
from sqlalchemy.orm import Session

from app.config.country_universe import COUNTRY_UNIVERSE
from app.db.models import RawIlostat
from app.ingest.rate_limiter import ILOSTAT_LIMITER
from app.ingest.sample_mode import (
    SampleConfig,
    validate_timeseries_data,
    IngestionError,
)
from app.ingest.utils import (
    bulk_upsert_timeseries,
    ensure_date,
    resolve_indicator_id,
    store_raw_payload,
)

logger = logging.getLogger(__name__)


# ILOSTAT rplumber API
# IMPORTANT: Can fetch MULTIPLE countries at once with + separator:
# Example: https://rplumber.ilo.org/data/indicator/?id=SDG_0922_NOC_RT_A&ref_area=CHN+HKG+JPN+KOR+MAC+MNG+TWN&timefrom=2014&timeto=2024&type=label&format=.csv
# Documentation: https://rplumber.ilo.org/__docs__/
ILOSTAT_BASE_URL = "https://rplumber.ilo.org/data/indicator/"

ILOSTAT_SERIES: Dict[str, Dict[str, Any]] = {
    "SDG_0922_NOC_RT_A": {
        "canonical_indicator": "ILOSTAT_UNEMPLOYMENT_RATE",
        "countries": COUNTRY_UNIVERSE,
    },
    "SDG_0831_SEX_ECO_NB_A": {
        "canonical_indicator": "ILOSTAT_LABOUR_FORCE_PARTICIPATION",
        "countries": COUNTRY_UNIVERSE,
    },
}


def fetch_ilostat_csv(indicator_id_str: str, country_ids: List[str], sample_config: Optional[SampleConfig] = None) -> pd.DataFrame:
    """
    Fetch data from ILOSTAT rplumber API with retry logic.
    
    CRITICAL: Can fetch MULTIPLE countries at once with + separator!
    Example: ?id=SDG_0922_NOC_RT_A&ref_area=CHN+HKG+JPN+KOR+MAC+MNG+TWN&format=.csv
    Documentation: https://rplumber.ilo.org/__docs__/
    
    Args:
        indicator_id_str: ILOSTAT indicator code
        country_ids: List of country codes (will be joined with +)
        sample_config: Optional sampling config
    
    Returns:
        DataFrame with data for all requested countries
    
    Note: Requires User-Agent header to avoid 403 Forbidden.
    """
    sample_config = sample_config or SampleConfig()
    
    # Apply rate limiting
    ILOSTAT_LIMITER.acquire()
    
    # Join multiple countries with + separator
    ref_area = "+".join(country_ids)
    
    # Build URL with query parameters
    url = f"{ILOSTAT_BASE_URL}?id={indicator_id_str}&ref_area={ref_area}&format=.csv"
    
    # Retry up to 3 times with increasing timeout
    max_retries = 3
    timeouts = [60.0, 90.0, 120.0]  # Progressive timeout increase
    
    for attempt in range(max_retries):
        try:
            # Use httpx with proper headers and generous timeout
            headers = {'User-Agent': 'Mozilla/5.0 (compatible; Mesodian/1.0)'}
            timeout_val = timeouts[attempt]
            logger.debug(f"ILOSTAT: Attempt {attempt + 1}/{max_retries} for {len(country_ids)} countries with {timeout_val}s timeout")
            
            response = httpx.get(url, headers=headers, timeout=timeout_val, follow_redirects=True)
            response.raise_for_status()
            
            # Parse CSV from response text
            from io import StringIO
            df = pd.read_csv(StringIO(response.text))
            logger.info(f"ILOSTAT: Successfully fetched {len(df)} rows for {indicator_id_str}/{len(country_ids)} countries")
            return df
            
        except (httpx.ReadTimeout, httpx.ConnectTimeout) as e:
            if attempt < max_retries - 1:
                logger.warning(f"ILOSTAT: Timeout on attempt {attempt + 1} for {len(country_ids)} countries, retrying...")
                continue
            else:
                # Final attempt failed - log and skip
                logger.warning(f"ILOSTAT: All {max_retries} attempts timed out for {len(country_ids)} countries, skipping")
                return pd.DataFrame()  # Return empty dataframe instead of raising
                
        except httpx.HTTPStatusError as e:
            raise IngestionError("ILOSTAT", ref_area, f"HTTP {e.response.status_code}: {e.response.text[:200]}")
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"ILOSTAT: Error on attempt {attempt + 1} for {len(country_ids)} countries: {e}, retrying...")
                continue
            else:
                raise IngestionError("ILOSTAT", ref_area, f"Fetch failed: {e}")


def parse_ilostat_df(
    df: pd.DataFrame,
    indicator_id: int,
    country_id: str,
    sample_config: Optional[SampleConfig] = None,
) -> List[Dict[str, Any]]:
    """Parse ILOSTAT dataframe."""
    sample_config = sample_config or SampleConfig()
    rows = []
    
    if df.empty:
        if sample_config.fail_on_empty:
            raise IngestionError("ILOSTAT", country_id, "Empty dataframe")
        return rows
    
    for _, row in df.iterrows():
        time_val = row.get("time")
        obs_val = row.get("obs_value")
        
        if pd.isna(time_val) or pd.isna(obs_val):
            continue
        
        try:
            date = ensure_date(f"{time_val}-12-31")
            value = float(obs_val)
        except Exception as e:
            logger.warning(f"ILOSTAT: Parse error for {country_id}/{time_val}: {e}")
            if sample_config.strict_validation:
                raise IngestionError("ILOSTAT", country_id, f"Parse error: {e}")
            continue
        
        rows.append({
            "indicator_id": indicator_id,
            "country_id": country_id,
            "date": date,
            "value": value,
            "source": "ILOSTAT",
            "ingested_at": dt.datetime.now(dt.timezone.utc),
        })
    
    return rows


def ingest_full(
    session: Session,
    *,
    series_subset: Optional[Iterable[str]] = None,
    country_subset: Optional[Iterable[str]] = None,
    sample_config: Optional[SampleConfig] = None,
) -> None:
    """
    Ingest ILOSTAT data from rplumber API.
    
    Fetches data in BATCHES of countries to minimize API calls and avoid timeouts.
    """
    sample_config = sample_config or SampleConfig()
    selected_series = set(series_subset) if series_subset else None
    selected_countries = set(country_subset) if country_subset else None

    series_to_fetch = list(ILOSTAT_SERIES.items())
    if selected_series:
        series_to_fetch = [(k, v) for k, v in series_to_fetch if k in selected_series]
    if sample_config.enabled:
        series_to_fetch = series_to_fetch[:1]  # Test with 1 series in sample mode

    for series_code, cfg in series_to_fetch:
        indicator_id = resolve_indicator_id(session, cfg["canonical_indicator"])
        countries = cfg.get("countries", COUNTRY_UNIVERSE)
        
        # Limit countries in sample mode
        if selected_countries:
            countries = [c for c in countries if c in selected_countries]
        if sample_config.enabled:
            countries = countries[:5]  # Test with 5 countries
        
        # BATCH countries to minimize API calls (fetch 10 at a time)
        batch_size = 10
        for i in range(0, len(countries), batch_size):
            batch_countries = countries[i:i + batch_size]
            
            try:
                # Fetch ALL countries in this batch with ONE API call
                df = fetch_ilostat_csv(series_code, batch_countries, sample_config)
                
                if df.empty:
                    logger.warning(f"ILOSTAT: Empty result for {series_code} batch {i//batch_size + 1}")
                    continue
                
                # Convert dataframe to dict for storage
                payload = df.to_dict() if hasattr(df, "to_dict") else None
                if payload:
                    import math
                    def replace_nan(obj):
                        if isinstance(obj, dict):
                            return {k: replace_nan(v) for k, v in obj.items()}
                        elif isinstance(obj, list):
                            return [replace_nan(v) for v in obj]
                        elif isinstance(obj, float) and math.isnan(obj):
                            return None
                        return obj
                    payload = replace_nan(payload)
                
                # Store raw payload for the batch
                store_raw_payload(
                    session,
                    RawIlostat,
                    params={"series": series_code, "countries": "+".join(batch_countries)},
                    payload=payload,
                )
                
                # Parse and store data for each country in the batch
                for country_id in batch_countries:
                    country_df = df[df['ref_area'] == country_id] if 'ref_area' in df.columns else df
                    
                    if country_df.empty:
                        continue
                    
                    rows = parse_ilostat_df(country_df, indicator_id, country_id, sample_config)
                    
                    if rows:
                        bulk_upsert_timeseries(session, rows)
                        logger.info(f"ILOSTAT: Ingested {len(rows)} records for {series_code}/{country_id}")
                
            except IngestionError:
                raise
            except Exception as e:
                logger.error(f"ILOSTAT: Failed for {series_code} batch {i//batch_size + 1}: {e}")
                if sample_config.strict_validation:
                    raise IngestionError("ILOSTAT", "+".join(batch_countries), f"Batch failed: {e}")

    session.commit()

