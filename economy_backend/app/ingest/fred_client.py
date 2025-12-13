import asyncio
import datetime as dt
import logging
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy.orm import Session

from app.config import get_settings
from app.db.models import RawFred
from app.ingest.base_client import get_provider_client
from app.ingest.sample_mode import (
    SampleConfig,
    validate_timeseries_data,
    IngestionError,
)
from app.ingest.utils import bulk_upsert_timeseries, ensure_date, resolve_indicator_id, store_raw_payload

logger = logging.getLogger(__name__)


FRED_SERIES_CONFIG: Dict[str, Dict[str, Any]] = {
    # USA Economic Indicators (Original)
    "CPIAUCSL": {
        "canonical_indicator": "CPI_USA_MONTHLY",
        "countries": ["USA"],
        "frequency": "M",
        "unit": "Index",
    },
    "UNRATE": {
        "canonical_indicator": "UNEMP_RATE_USA",
        "countries": ["USA"],
        "frequency": "M",
        "unit": "Percent",
    },
    
    # G20 Exchange Rates (vs USD)
    "DEXCHUS": {
        "canonical_indicator": "FX_CNY_USD_FRED",
        "countries": ["CHN"],
        "frequency": "D",
        "unit": "Rate",
    },
    "DEXJPUS": {
        "canonical_indicator": "FX_JPY_USD_FRED",
        "countries": ["JPN"],
        "frequency": "D",
        "unit": "Rate",
    },
    "DEXUSEU": {
        "canonical_indicator": "FX_USD_EUR_FRED",
        "countries": ["EMU"],
        "frequency": "D",
        "unit": "Rate",
    },
    "DEXUSUK": {
        "canonical_indicator": "FX_USD_GBP_FRED",
        "countries": ["GBR"],
        "frequency": "D",
        "unit": "Rate",
    },
    "DEXCAUS": {
        "canonical_indicator": "FX_CAD_USD_FRED",
        "countries": ["CAN"],
        "frequency": "D",
        "unit": "Rate",
    },
    "DEXMXUS": {
        "canonical_indicator": "FX_MXN_USD_FRED",
        "countries": ["MEX"],
        "frequency": "D",
        "unit": "Rate",
    },
    "DEXBZUS": {
        "canonical_indicator": "FX_BRL_USD_FRED",
        "countries": ["BRA"],
        "frequency": "D",
        "unit": "Rate",
    },
    "DEXINUS": {
        "canonical_indicator": "FX_INR_USD_FRED",
        "countries": ["IND"],
        "frequency": "D",
        "unit": "Rate",
    },
    "DEXKOUS": {
        "canonical_indicator": "FX_KRW_USD_FRED",
        "countries": ["KOR"],
        "frequency": "D",
        "unit": "Rate",
    },
    "DEXUSAL": {
        "canonical_indicator": "FX_USD_AUD_FRED",
        "countries": ["AUS"],
        "frequency": "D",
        "unit": "Rate",
    },
    "DEXSZUS": {
        "canonical_indicator": "FX_CHF_USD_FRED",
        "countries": ["CHE"],
        "frequency": "D",
        "unit": "Rate",
    },
    "DEXSDUS": {
        "canonical_indicator": "FX_SEK_USD_FRED",
        "countries": ["SWE"],
        "frequency": "D",
        "unit": "Rate",
    },
    
    # International CPI
    "CHNCPIALLMINMEI": {
        "canonical_indicator": "CPI_CHN_FRED",
        "countries": ["CHN"],
        "frequency": "M",
        "unit": "Index",
    },
    "JPNCPIALLMINMEI": {
        "canonical_indicator": "CPI_JPN_FRED",
        "countries": ["JPN"],
        "frequency": "M",
        "unit": "Index",
    },
    "DEUCPIALLMINMEI": {
        "canonical_indicator": "CPI_DEU_FRED",
        "countries": ["DEU"],
        "frequency": "M",
        "unit": "Index",
    },
    "GBRCPIALLMINMEI": {
        "canonical_indicator": "CPI_GBR_FRED",
        "countries": ["GBR"],
        "frequency": "M",
        "unit": "Index",
    },
    "CANCPIALLMINMEI": {
        "canonical_indicator": "CPI_CAN_FRED",
        "countries": ["CAN"],
        "frequency": "M",
        "unit": "Index",
    },
    
    # International Interest Rates
    "INTGSTJPM193N": {
        "canonical_indicator": "INT_RATE_JPN_FRED",
        "countries": ["JPN"],
        "frequency": "M",
        "unit": "Percent",
    },
    "INTGSTGBM193N": {
        "canonical_indicator": "INT_RATE_GBR_FRED",
        "countries": ["GBR"],
        "frequency": "M",
        "unit": "Percent",
    },
    
    # Commodity Prices (Global)
    "DCOILWTICO": {
        "canonical_indicator": "OIL_WTI_FRED",
        "countries": ["WLD"],
        "frequency": "D",
        "unit": "USD_PER_BARREL",
    },
    "DCOILBRENTEU": {
        "canonical_indicator": "OIL_BRENT_FRED",
        "countries": ["WLD"],
        "frequency": "D",
        "unit": "USD_PER_BARREL",
    },
}


async def fetch_series(
    series_id: str,
    *,
    observation_start: Optional[str] = None,
    observation_end: Optional[str] = None,
    sample_config: Optional[SampleConfig] = None,
) -> Dict[str, Any]:
    sample_config = sample_config or SampleConfig()
    settings = get_settings()
    params = {
        "series_id": series_id,
        "api_key": settings.fred_api_key,
        "file_type": "json",
    }
    if observation_start:
        params["observation_start"] = observation_start
    if observation_end:
        params["observation_end"] = observation_end
    
    # Sample mode: limit to recent observations
    if sample_config.enabled:
        params["limit"] = str(sample_config.max_records_per_country)
        params["sort_order"] = "desc"
    
    try:
        async with get_provider_client("FRED", "https://api.stlouisfed.org/fred") as client:
            return await client.get_json("/series/observations", params=params)
    except Exception as e:
        raise IngestionError("FRED", "N/A", f"Fetch error: {e}")


def parse_observations(
    payload: Dict[str, Any],
    *,
    indicator_id: int,
    country_id: str,
    source: str,
    sample_config: Optional[SampleConfig] = None,
) -> List[Dict[str, Any]]:
    sample_config = sample_config or SampleConfig()
    observations = payload.get("observations", [])
    
    if not observations and sample_config.fail_on_empty:
        raise IngestionError("FRED", country_id, "Empty observations")
    
    rows: List[Dict[str, Any]] = []
    for obs in observations:
        try:
            date = ensure_date(obs.get("date"))
            value_str = obs.get("value")
            if value_str in {"." , None, ""}:
                continue
            value = float(value_str)
        except Exception as exc:
            logger.error(f"FRED: Parse error for {country_id}: {exc}")
            if sample_config.strict_validation:
                raise IngestionError("FRED", country_id, f"Parse error: {exc}")
            continue

        rows.append(
            {
                "indicator_id": indicator_id,
                "country_id": country_id,
                "date": date,
                "value": value,
                "source": source,
                "ingested_at": dt.datetime.now(dt.timezone.utc),
            }
        )
    
    # Limit to sample size
    if sample_config.enabled and len(rows) > sample_config.max_records_per_country:
        rows = rows[:sample_config.max_records_per_country]
    
    # Validate results
    if sample_config.enabled and rows:
        validation = validate_timeseries_data(rows, expected_countries=[country_id], sample_config=sample_config)
        if sample_config.strict_validation:
            validation.raise_if_invalid()
    
    return rows


def ingest_full(
    session: Session,
    *,
    country_subset: Optional[Iterable[str]] = None,
    series_subset: Optional[Iterable[str]] = None,
    sample_config: Optional[SampleConfig] = None,
) -> None:
    sample_config = sample_config or SampleConfig()
    selected_countries = set(country_subset) if country_subset else None
    selected_series = set(series_subset) if series_subset else None
    
    # Limit series in sample mode
    series_to_fetch = list(FRED_SERIES_CONFIG.items())
    if selected_series:
        series_to_fetch = [(k, v) for k, v in series_to_fetch if k in selected_series]
    if sample_config.enabled:
        series_to_fetch = series_to_fetch[:2]

    async def _run() -> None:
        for series_id, cfg in series_to_fetch:
            try:
                indicator_id = resolve_indicator_id(session, cfg["canonical_indicator"])
            except Exception as e:
                logger.error(f"FRED: Failed to resolve indicator: {e}")
                if sample_config.strict_validation:
                    raise IngestionError("FRED", "N/A", f"Indicator resolution failed: {e}")
                continue
            
            for country_id in cfg.get("countries", []):
                if selected_countries and country_id not in selected_countries:
                    continue
                try:
                    payload = await fetch_series(series_id, sample_config=sample_config)
                    store_raw_payload(
                        session,
                        RawFred,
                        params={"series_id": series_id, "country_id": country_id},
                        payload=payload,
                    )
                    rows = parse_observations(
                        payload,
                        indicator_id=indicator_id,
                        country_id=country_id,
                        source="FRED",
                        sample_config=sample_config,
                    )
                    bulk_upsert_timeseries(session, rows)
                    logger.info(f"FRED: Ingested {len(rows)} records for {country_id}/{series_id}")
                except IngestionError:
                    raise
                except Exception as e:
                    logger.error(f"FRED: Failed for {country_id}/{series_id}: {e}")
                    if sample_config.strict_validation:
                        raise IngestionError("FRED", country_id, f"Failed: {e}")

    asyncio.run(_run())
    session.commit()

