"""
IMF Data Ingestion using official sdmx1 Python client.

This module uses the sdmx.Client("IMF_DATA") pattern for both:
- Public unauthenticated access (default)
- Authenticated access (when MSAL credentials are provided)

Configuration is loaded from config/catalogs/providers.yaml
"""

import asyncio
import datetime as dt
import logging
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import yaml
from sqlalchemy.orm import Session

from app.db.models import RawImf
from app.ingest.sample_mode import (
    SampleConfig,
    validate_timeseries_data,
    IngestionError,
)
from app.ingest.utils import bulk_upsert_timeseries, ensure_date, resolve_indicator_id, store_raw_payload

logger = logging.getLogger(__name__)


def _load_imf_config() -> Dict[str, Any]:
    """Load IMF configuration from providers catalog."""
    catalog_path = Path(__file__).resolve().parents[2] / "config" / "catalogs" / "providers.yaml"
    if not catalog_path.exists():
        logger.warning("providers.yaml not found, using defaults")
        return {}
    try:
        with open(catalog_path, "r", encoding="utf-8") as f:
            catalog = yaml.safe_load(f) or {}
            return catalog.get("IMF", {})
    except Exception as e:
        logger.error(f"Failed to load IMF config: {e}")
        return {}


IMF_CONFIG = _load_imf_config()
IMF_DATASETS = IMF_CONFIG.get("datasets", {})


def _get_msal_token() -> Optional[Dict[str, str]]:
    """
    Acquire MSAL token for authenticated IMF data access.
    
    Returns None if MSAL credentials are not configured (will use public access).
    Set IMF_USE_MSAL=true to enable authentication.
    """
    use_msal = os.getenv("IMF_USE_MSAL", "false").lower() == "true"
    if not use_msal:
        return None
    
    try:
        from msal import PublicClientApplication
    except ImportError:
        logger.warning("msal not installed, falling back to public access")
        return None
    
    client_id = os.getenv("IMF_CLIENT_ID", "446ce2fa-88b1-436c-b8e6-94491ca4f6fb")
    authority = os.getenv(
        "IMF_AUTHORITY",
        "https://imfprdb2c.b2clogin.com/imfprdb2c.onmicrosoft.com/b2c_1a_signin_aad_simple_user_journey/"
    )
    scope = os.getenv(
        "IMF_SCOPE",
        "https://imfprdb2c.onmicrosoft.com/4042e178-3e2f-4ff9-ac38-1276c901c13d/iData.Login"
    )
    
    try:
        app = PublicClientApplication(client_id, authority=authority)
        token = app.acquire_token_interactive(scopes=[scope])
        
        if "access_token" in token:
            return {"Authorization": f"{token['token_type']} {token['access_token']}"}
        else:
            logger.error(f"MSAL token acquisition failed: {token.get('error_description', 'Unknown error')}")
            return None
    except Exception as e:
        logger.error(f"MSAL authentication failed: {e}")
        return None


def fetch_imf_data_sdmx(
    dataset: str,
    key: str,
    params: Optional[Dict[str, Any]] = None,
    *,
    sample_config: Optional[SampleConfig] = None,
) -> Any:
    """
    Fetch IMF data using sdmx1 client.
    
    Args:
        dataset: Dataset code (e.g., "CPI", "IFS", "BOP")
        key: Series key (e.g., "USA+CAN.CPI.CP01.IX.M")
        params: Query parameters (e.g., {"startPeriod": 2018})
        sample_config: Sample mode configuration
        
    Returns:
        pandas DataFrame with time series data
    """
    sample_config = sample_config or SampleConfig()
    
    try:
        import sdmx
    except ImportError as exc:
        raise IngestionError("IMF", "N/A", "sdmx1 package not installed") from exc
    
    # Initialize IMF_DATA client
    IMF_DATA = sdmx.Client("IMF_DATA")
    
    # Get MSAL token if configured
    headers = _get_msal_token()
    
    # Apply sample mode limits
    if sample_config.enabled:
        params = params or {}
        # Limit to recent data in sample mode
        current_year = dt.datetime.now().year
        params.setdefault("startPeriod", str(current_year - 2))
    
    try:
        logger.info(f"Fetching IMF data: dataset={dataset}, key={key}, params={params}")
        
        # Fetch data with optional authentication headers
        data_msg = IMF_DATA.data(
            dataset,
            key=key,
            params=params,
            headers=headers,
        )
        
        # Convert to pandas DataFrame
        df = sdmx.to_pandas(data_msg)
        
        if df is None or df.empty:
            if sample_config.fail_on_empty:
                raise IngestionError("IMF", key, "Empty response from IMF")
            return None
        
        return df
    except Exception as e:
        error_msg = f"SDMX fetch failed for {dataset}/{key}: {e}"
        logger.error(error_msg)
        if sample_config.strict_validation:
            raise IngestionError("IMF", key, error_msg)
        return None




def parse_imf_dataframe(
    df: Any,
    *,
    indicator_id: int,
    country_id: str,
    source: str,
    sample_config: Optional[SampleConfig] = None,
) -> List[Dict[str, Any]]:
    """Parse IMF DataFrame into timeseries records."""
    sample_config = sample_config or SampleConfig()
    rows: List[Dict[str, Any]] = []
    
    if df is None or (hasattr(df, 'empty') and df.empty):
        if sample_config.enabled:
            logger.error("IMF dataframe is None or empty for country=%s", country_id)
            if sample_config.strict_validation:
                raise IngestionError("IMF", country_id, "Empty dataframe received")
        return rows
    
    # sdmx.to_pandas can return different structures depending on the data
    # Handle both Series and DataFrame
    if hasattr(df, 'to_frame'):
        df = df.to_frame()
    
    # Reset index to make date/time accessible
    if hasattr(df, 'reset_index'):
        df = df.reset_index()
    
    for idx, row in df.iterrows():
        # Try different column names for time dimension
        time_value = None
        for time_col in ['TIME_PERIOD', 'time', 'TIME', 'period', 'date', 'index']:
            if time_col in row:
                time_value = row[time_col]
                break
        
        # Try different column names for value
        value = None
        for val_col in ['value', 'OBS_VALUE', 'VALUE', 0]:
            if val_col in row:
                value = row[val_col]
                break
        
        if time_value is None or value is None:
            continue
        
        try:
            date = ensure_date(str(time_value))
            numeric_value = float(value)
        except Exception as exc:
            logger.error("Skipping IMF row %s: %s", row, exc)
            if sample_config.strict_validation:
                raise IngestionError("IMF", country_id, f"Parse error: {exc}")
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
    
    # Limit records in sample mode
    if sample_config.enabled and len(rows) > sample_config.max_records_per_country:
        # Sort by date descending and take most recent records
        rows = sorted(rows, key=lambda r: r['date'], reverse=True)[:sample_config.max_records_per_country]
        logger.info(f"Sample mode: Limited to {len(rows)} most recent records for {country_id}")
    
    # Validate before returning
    if sample_config.enabled and rows:
        validation = validate_timeseries_data(
            rows,
            expected_countries=[country_id],
            sample_config=sample_config,
        )
        logger.info("IMF parsed %d records for country=%s", validation.record_count, country_id)
        if sample_config.strict_validation:
            validation.raise_if_invalid()
    
    return rows


def ingest_full(
    session: Session,
    *,
    country_subset: Optional[Iterable[str]] = None,
    indicator_subset: Optional[Iterable[str]] = None,
    dataset_subset: Optional[Iterable[str]] = None,
    sample_config: Optional[SampleConfig] = None,
) -> None:
    """
    Ingest IMF data using sdmx1 client with configurable datasets.
    
    Args:
        session: Database session
        country_subset: Optional subset of countries to ingest
        indicator_subset: Optional subset of indicator codes to ingest
        dataset_subset: Optional subset of dataset codes (CPI, QNEA, etc.)
        sample_config: Sample mode configuration
    """
    sample_config = sample_config or SampleConfig()
    selected_countries = set(country_subset) if country_subset else None
    selected_indicators = set(indicator_subset) if indicator_subset else None
    selected_datasets = set(dataset_subset) if dataset_subset else None

    async def _run() -> None:
        # Iterate through configured datasets
        for dataset_code, dataset_config in IMF_DATASETS.items():
            if selected_datasets and dataset_code not in selected_datasets:
                continue
            
            # Get canonical indicator for this dataset
            canonical_indicator = dataset_config.get("canonical_indicator")
            if not canonical_indicator:
                logger.warning(f"No canonical_indicator for dataset {dataset_code}, skipping")
                continue
            
            if selected_indicators and canonical_indicator not in selected_indicators:
                continue
            
            # Resolve indicator in our database
            try:
                indicator_id = resolve_indicator_id(session, canonical_indicator)
            except Exception as e:
                logger.error(f"Could not resolve indicator {canonical_indicator}: {e}")
                continue
            
            # Get countries to process
            countries = selected_countries or set()
            if not countries:
                from app.config.country_universe import COUNTRY_UNIVERSE
                countries = set(COUNTRY_UNIVERSE)
            
            # Limit in sample mode
            if sample_config.enabled:
                countries = set(list(countries)[:2])
            
            for country_id in countries:
                try:
                    # Build SDMX key pattern: COUNTRY (simple key for most IMF dataflows)
                    # For CPI: just "USA" or "USA+CAN" for multiple countries
                    key = country_id
                    key_suffix = dataset_config.get("key_suffix", "")
                    if key_suffix:
                        key = f"{key}.{key_suffix}"
                    
                    # Set query parameters
                    params = {"startPeriod": "2018"}  # Get recent data
                    
                    # Fetch data using sdmx1 client
                    df = fetch_imf_data_sdmx(
                        dataset_code,
                        key,
                        params=params,
                        sample_config=sample_config,
                    )
                    
                    if df is None or (hasattr(df, 'empty') and df.empty):
                        logger.warning(f"No data returned for {dataset_code}/{country_id}")
                        continue
                    
                    # Store raw payload
                    payload_dict = None
                    if df is not None and hasattr(df, "to_dict"):
                        try:
                            # Reset index before converting to dict to avoid tuple key issues
                            if hasattr(df, 'reset_index'):
                                payload_dict = df.reset_index().to_dict(orient='records')
                            else:
                                payload_dict = df.to_dict(orient='records')
                            
                            # Clean NaN values before JSON serialization
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
                            payload_dict = clean_nan(payload_dict)
                            
                        except Exception as e:
                            logger.warning(f"Could not convert DataFrame to dict: {e}")
                            payload_dict = {"records": len(df), "error": str(e)}
                    
                    store_raw_payload(
                        session,
                        RawImf,
                        params={
                            "dataset": dataset_code,
                            "key": key,
                            "params": str(params),
                        },
                        payload=payload_dict,
                    )
                    
                    # Parse and insert
                    rows = parse_imf_dataframe(
                        df,
                        indicator_id=indicator_id,
                        country_id=country_id,
                        source=f"IMF_{dataset_code}",
                        sample_config=sample_config,
                    )
                    
                    # Deduplicate rows before bulk insert (IMF returns multiple series per country/date)
                    dedupe_dict = {}
                    for row in rows:
                        key = (row["indicator_id"], row["country_id"], row["date"])
                        dedupe_dict[key] = row  # Last occurrence wins
                    deduped_rows = list(dedupe_dict.values())
                    
                    if len(rows) != len(deduped_rows):
                        logger.warning(
                            f"IMF: Deduplicated {len(rows)} -> {len(deduped_rows)} rows for {country_id}/{dataset_code}"
                        )
                    
                    bulk_upsert_timeseries(session, deduped_rows)
                    
                    logger.info(
                        "IMF: Ingested %d records for %s/%s/%s",
                        len(deduped_rows), dataset_code, canonical_indicator, country_id
                    )
                    
                except IngestionError:
                    raise
                except Exception as e:
                    logger.error(
                        "IMF ingestion failed for %s/%s/%s: %s",
                        dataset_code, country_id, canonical_indicator, e
                    )
                    if sample_config.strict_validation:
                        raise IngestionError(
                            f"IMF_{dataset_code}",
                            country_id,
                            f"Ingestion failed: {e}"
                        )

    asyncio.run(_run())
    session.commit()

