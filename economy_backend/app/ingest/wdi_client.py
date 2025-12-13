import asyncio
import datetime as dt
import logging
from typing import Any, Dict, Iterable, List, Optional

import httpx
from sqlalchemy.orm import Session

from app.config.country_universe import COUNTRY_UNIVERSE
from app.db.models import RawWdi
from app.ingest.sample_mode import (
    SampleConfig,
    validate_timeseries_data,
    IngestionError,
)
from app.ingest.utils import bulk_upsert_timeseries, ensure_date, resolve_indicator_id, store_raw_payload

logger = logging.getLogger(__name__)


WDI_INDICATORS: Dict[str, Dict[str, str]] = {
    # Core Economic (Original)
    "NY.GDP.MKTP.KD": {"canonical_indicator": "GDP_REAL"},
    "FP.CPI.TOTL.ZG": {"canonical_indicator": "CPI_YOY"},
    "SL.UEM.TOTL.ZS": {"canonical_indicator": "UNEMP_RATE"},
    
    # National Accounts - GDP Components
    "NE.CON.TOTL.KD": {"canonical_indicator": "CONSUMPTION_TOTAL_WDI"},
    "NE.CON.PRVT.KD": {"canonical_indicator": "CONSUMPTION_PRIVATE_WDI"},
    "NE.CON.GOVT.KD": {"canonical_indicator": "CONSUMPTION_GOVT_WDI"},
    "NE.GDI.TOTL.KD": {"canonical_indicator": "INVESTMENT_TOTAL_WDI"},
    "NE.EXP.GNFS.KD": {"canonical_indicator": "EXPORTS_GOODS_SERVICES_WDI"},
    "NE.IMP.GNFS.KD": {"canonical_indicator": "IMPORTS_GOODS_SERVICES_WDI"},
    
    # National Accounts - Per Capita & Growth
    "NY.GDP.PCAP.KD": {"canonical_indicator": "GDP_PER_CAPITA_WDI"},
    "NY.GDP.MKTP.KD.ZG": {"canonical_indicator": "GDP_GROWTH_WDI"},
    "NY.GNP.MKTP.CD": {"canonical_indicator": "GNI_CURRENT_WDI"},
    
    # Demographics
    "SP.POP.TOTL": {"canonical_indicator": "POPULATION_TOTAL_WDI"},
    "SP.POP.GROW": {"canonical_indicator": "POPULATION_GROWTH_WDI"},
    "SP.URB.TOTL.IN.ZS": {"canonical_indicator": "URBAN_POPULATION_PCT_WDI"},
    "SL.TLF.TOTL.IN": {"canonical_indicator": "LABOR_FORCE_TOTAL_WDI"},
    
    # Trade
    "NE.EXP.GNFS.ZS": {"canonical_indicator": "EXPORTS_PCT_GDP_WDI"},
    "NE.IMP.GNFS.ZS": {"canonical_indicator": "IMPORTS_PCT_GDP_WDI"},
    "BN.CAB.XOKA.GD.ZS": {"canonical_indicator": "CURRENT_ACCOUNT_PCT_GDP_WDI"},
    
    # Finance
    "FS.AST.DOMS.GD.ZS": {"canonical_indicator": "DOMESTIC_CREDIT_PCT_GDP_WDI"},
    "CM.MKT.LCAP.GD.ZS": {"canonical_indicator": "MARKET_CAP_PCT_GDP_WDI"},
    "BX.KLT.DINV.WD.GD.ZS": {"canonical_indicator": "FDI_NET_INFLOWS_PCT_GDP_WDI"},
    
    # Infrastructure & Technology
    "IT.NET.USER.ZS": {"canonical_indicator": "INTERNET_USERS_PCT_WDI"},
    "EG.ELC.ACCS.ZS": {"canonical_indicator": "ELECTRICITY_ACCESS_PCT_WDI"},
    "IT.CEL.SETS.P2": {"canonical_indicator": "MOBILE_SUBSCRIPTIONS_PER100_WDI"},
    
    # Health & Education
    "SP.DYN.LE00.IN": {"canonical_indicator": "LIFE_EXPECTANCY_WDI"},
    "SH.DYN.MORT": {"canonical_indicator": "INFANT_MORTALITY_WDI"},
    "SE.PRM.ENRR": {"canonical_indicator": "PRIMARY_SCHOOL_ENROLLMENT_WDI"},
    
    # Environment
    "EG.USE.ELEC.KH.PC": {"canonical_indicator": "ELECTRICITY_USE_PER_CAPITA_WDI"},
    "EN.ATM.CO2E.PC": {"canonical_indicator": "CO2_EMISSIONS_PER_CAPITA_WDI"},
    "EG.FEC.RNEW.ZS": {"canonical_indicator": "RENEWABLE_ENERGY_PCT_WDI"},
}


async def fetch_indicator(
    country: str,
    indicator: str,
    *,
    sample_config: Optional[SampleConfig] = None,
) -> Dict[str, Any]:
    url = f"https://api.worldbank.org/v2/country/{country}/indicator/{indicator}"
    per_page = 50 if sample_config and sample_config.enabled else 20000
    params = {"format": "json", "per_page": per_page}
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.get(url, params=params)
        response.raise_for_status()
        return response.json()


def parse_wdi(
    payload: Dict[str, Any],
    *,
    indicator_id: int,
    country_id: str,
    source: str,
    sample_config: Optional[SampleConfig] = None,
) -> List[Dict[str, Any]]:
    sample_config = sample_config or SampleConfig()
    
    if not isinstance(payload, list) or len(payload) < 2:
        if sample_config.enabled and not payload:
            logger.error("WDI payload empty for country=%s", country_id)
            if sample_config.strict_validation:
                raise IngestionError("WDI", country_id, "Empty payload received")
        return []
    data = payload[1] or []
    rows: List[Dict[str, Any]] = []
    for entry in data:
        value = entry.get("value")
        year = entry.get("date")
        if value is None or year is None:
            continue
        try:
            date = ensure_date(f"{year}-12-31")
            numeric_value = float(value)
        except Exception as exc:  # pragma: no cover - data dependent
            logger.error("Failed to parse WDI entry %s: %s", entry, exc)
            if sample_config.strict_validation:
                raise IngestionError("WDI", country_id, f"Parse error: {exc}")
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
    if sample_config.enabled and sample_config.max_records_per_country:
        rows = rows[:sample_config.max_records_per_country]
    
    # Validate before returning
    if sample_config.enabled and rows:
        validation = validate_timeseries_data(
            rows,
            expected_countries=[country_id],
            sample_config=sample_config,
        )
        logger.info("WDI parsed %d records for country=%s", validation.record_count, country_id)
        if sample_config.strict_validation:
            validation.raise_if_invalid()
    
    return rows


def ingest_full(
    session: Session,
    *,
    country_subset: Optional[Iterable[str]] = None,
    indicator_subset: Optional[Iterable[str]] = None,
    sample_config: Optional[SampleConfig] = None,
) -> None:
    sample_config = sample_config or SampleConfig()
    selected_countries = set(country_subset) if country_subset else set(COUNTRY_UNIVERSE)
    selected_indicators = set(indicator_subset) if indicator_subset else set(WDI_INDICATORS)
    
    # Limit in sample mode
    if sample_config.enabled:
        selected_countries = set(list(selected_countries)[:2])
        selected_indicators = set(list(selected_indicators)[:2])

    async def _run() -> None:
        for indicator_code, cfg in WDI_INDICATORS.items():
            if indicator_code not in selected_indicators:
                continue
            indicator_id = resolve_indicator_id(session, cfg["canonical_indicator"])
            for country_id in selected_countries:
                try:
                    payload = await fetch_indicator(
                        country_id.lower(),
                        indicator_code,
                        sample_config=sample_config,
                    )
                    store_raw_payload(
                        session,
                        RawWdi,
                        params={"indicator": indicator_code, "country": country_id},
                        payload=payload,
                    )
                    rows = parse_wdi(
                        payload,
                        indicator_id=indicator_id,
                        country_id=country_id,
                        source="WDI",
                        sample_config=sample_config,
                    )
                    bulk_upsert_timeseries(session, rows)
                except IngestionError:
                    raise
                except Exception as e:
                    logger.error("WDI ingestion failed for %s/%s: %s", country_id, indicator_code, e)
                    if sample_config.strict_validation:
                        raise IngestionError("WDI", country_id, f"Ingestion failed: {e}")

    asyncio.run(_run())
    session.commit()

