import asyncio
import logging
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import httpx
import yaml
from sqlalchemy.orm import Session

from app.config.country_universe import COUNTRY_UNIVERSE
from app.db.models import RawComtrade
from app.ingest.utils import bulk_upsert_tradeflows, store_raw_payload
from app.ingest.sample_mode import SampleConfig, validate_trade_flows, IngestionError
from app.ingest.rate_limiter import COMTRADE_LIMITER

logger = logging.getLogger(__name__)

# ISO3 to COMTRADE numeric code mapping
# Generated from https://comtradeapi.un.org/files/v1/app/reference/Reporters.json
ISO3_TO_COMTRADE_CODE = {
    'ARE': 784, 'ARG': 32, 'AUS': 36, 'AUT': 40, 'BEL': 56, 'BGD': 50,
    'BRA': 76, 'CAN': 124, 'CHE': 757, 'CHL': 152, 'CHN': 156, 'COL': 170,
    'CZE': 203, 'DEU': 276, 'DNK': 208, 'EGY': 818, 'ESP': 724, 'ETH': 231,
    'FRA': 251, 'GBR': 826, 'GHA': 288, 'HKG': 344, 'HUN': 348, 'IDN': 360,
    'IND': 699, 'IRL': 372, 'ISR': 376, 'ITA': 380, 'JPN': 392, 'KEN': 404,
    'KOR': 410, 'KWT': 414, 'MAR': 504, 'MEX': 484, 'MYS': 458, 'NGA': 566,
    'NLD': 528, 'NOR': 579, 'NZL': 554, 'PAK': 586, 'PER': 604, 'PHL': 608,
    'POL': 616, 'QAT': 634, 'ROU': 642, 'SAU': 682, 'SGP': 702, 'SWE': 752,
    'THA': 764, 'TUR': 792, 'USA': 842, 'VNM': 704, 'ZAF': 710,
}


# UN COMTRADE API v1 - Trade statistics
# Official Base: https://comtradeapi.un.org/public/v1
# Pattern: /preview/C/A/HS?reporterCode={CODE}&partnerCode=0&cmdCode=TOTAL&flowCode=1&timePeriod={YEAR}
# Example: /preview/C/A/HS?reporterCode=840&partnerCode=0&cmdCode=TOTAL&flowCode=1&timePeriod=2023
# STATUS: API DEPRECATED - Returns HTTP 404 for all v1 endpoints
# TESTED: 2025-12-09 - Multiple endpoint patterns all return 404
# Alternative needed: WITS (World Bank), OECD Trade, or bulk downloads from comtradeplus.un.org
COMTRADE_BASE_URL = "https://comtradeapi.un.org/public/v1/preview/C/A/HS"


def _load_provider_catalog() -> Dict[str, Any]:
    catalog_path = Path(__file__).resolve().parents[2] / "config" / "catalogs" / "providers.yaml"
    if not catalog_path.exists():
        return {}
    try:
        with open(catalog_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


CATALOG = _load_provider_catalog()
CATALOG_COMTRADE = CATALOG.get("COMTRADE", {}) if isinstance(CATALOG, dict) else {}

COMTRADE_CONFIG: Dict[str, Any] = {
    "reporters": COUNTRY_UNIVERSE,
    "partners": ["0"],  # partnerCode 0 = world; expand with partners if needed
    "hs_codes": CATALOG_COMTRADE.get(
        "hs_codes",
        [
            "2709",
            "2710",
            "2711",
            "2701",
            "2702",
            "2601",
            "2606",
            "2603",
            "1001",
            "1005",
            "1006",
            "1201",
            "1511",
            "1205",
            "8703",
            "8500",
        ],
    ),
    "flows": CATALOG_COMTRADE.get("flows", ["M", "X"]),  # "M"=imports, "X"=exports (API uses text codes now)
    "years": list(range(2020, 2025)),
    "throttle_seconds": CATALOG_COMTRADE.get("throttle", {}).get("per_request_sleep_seconds", 1.0),
}


async def fetch_raw_trade(
    reporter: str,
    partner: str,
    year: int,
    hs_code: str,
    flow_code: str,  # Changed from int to str ("M" or "X")
    *,
    client=None,
    sample_config: Optional[SampleConfig] = None
) -> Dict[str, Any]:
    """
    Fetch raw trade data from UN Comtrade API preview endpoint.
    
    Docs: https://comtradeapi.un.org/
    Endpoint pattern: /preview?reporterCode={CODE}&partnerCode=0&flowCode={M|X}&cmdCode={HS}&period={year}
    
    Note: API requires numeric country codes, not ISO3 codes.
    Note: API requires text flow codes ("M"=Import, "X"=Export), not numeric.
    """
    sample_config = sample_config or SampleConfig()
    
    # Convert ISO3 to numeric code
    reporter_code = ISO3_TO_COMTRADE_CODE.get(reporter)
    if not reporter_code:
        raise IngestionError("COMTRADE", reporter, f"No COMTRADE code mapping for {reporter}")
    
    url = COMTRADE_BASE_URL
    api_key = os.getenv("COMTRADE_API_KEY")

    params = {
        "reporterCode": str(reporter_code),  # Use numeric code
        "partnerCode": partner,
        "period": str(year),
        "cmdCode": hs_code,
        "flowCode": flow_code,  # Now a string like "M" or "X"
    }
    if api_key:
        params["subscription-key"] = api_key  # COMTRADE uses subscription-key, not token
    
    if sample_config.enabled:
        params["maxRecords"] = str(sample_config.max_records_per_country)

    async with httpx.AsyncClient(timeout=30.0) as http_client:
        try:
            # Rate limiting: 100 requests/hour
            COMTRADE_LIMITER.acquire()
            await asyncio.sleep(COMTRADE_CONFIG["throttle_seconds"])
            response = await http_client.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            raise IngestionError("COMTRADE", reporter, f"HTTP {e.response.status_code}: {e.response.text[:200]}")
        except Exception as e:
            raise IngestionError("COMTRADE", reporter, f"Fetch failed: {e}")


def parse_to_trade_flows(
    raw: Dict[str, Any],
    *,
    sample_config: Optional[SampleConfig] = None
) -> List[Dict[str, Any]]:
    """Parse COMTRADE response with strict validation."""
    sample_config = sample_config or SampleConfig()
    flows: List[Dict[str, Any]] = []
    
    if not raw:
        if sample_config.fail_on_empty:
            raise IngestionError("COMTRADE", "UNKNOWN", "Empty response")
        return flows

    data = raw.get("data", [])
    if not data and sample_config.strict_validation:
        raise IngestionError("COMTRADE", "UNKNOWN", "No data array in response")
    
    for item in data:
        # Use explicit None check for partnerCode since 0 is a valid value (world)
        reporter = item.get("reporterCode")
        if reporter is None:
            reporter = item.get("reporter") or item.get("rtCode")
        
        partner = item.get("partnerCode")
        if partner is None:
            partner = item.get("partner") or item.get("ptCode")
        
        year = item.get("period") or item.get("year") or item.get("refPeriodId")
        hs_section = item.get("cmdCode") or item.get("cmdDescE")
        flow_type = item.get("flowCode") or item.get("flowDesc") or item.get("flow")
        value = (
            item.get("fobvalue") or
            item.get("cifvalue") or
            item.get("primaryValue") or
            item.get("primaryValueUsd") or
            item.get("TradeValue") or
            item.get("tradeValue")
        )

        if reporter is None:
            logger.warning(f"COMTRADE: Missing reporter in item: {item}")
            if sample_config.strict_validation:
                continue
        if partner is None:
            logger.warning(f"COMTRADE: Missing partner in item: {item}")
            if sample_config.strict_validation:
                continue
        if not year:
            logger.warning(f"COMTRADE: Missing year in item: {item}")
            if sample_config.strict_validation:
                continue
        
        try:
            year_int = int(str(year)[0:4])
            value_numeric = float(value) if value is not None else None
            
            # Convert numeric codes back to ISO3
            # Reporter: reverse lookup in ISO3_TO_COMTRADE_CODE mapping
            reporter_iso3 = None
            for iso3, code in ISO3_TO_COMTRADE_CODE.items():
                if code == reporter:
                    reporter_iso3 = iso3
                    break
            if not reporter_iso3:
                # If not in our mapping, just use the numeric code as string
                reporter_iso3 = str(reporter).upper()
            
            # Partner: 0 means "World" = WLD
            partner_iso3 = "WLD" if partner == 0 else str(partner).upper()
            
        except Exception as exc:
            logger.error(f"COMTRADE: Parse error for item {item}: {exc}")
            if sample_config.strict_validation:
                raise IngestionError("COMTRADE", str(reporter), f"Parse error: {exc}")
            continue

        flows.append(
            {
                "reporter_country_id": reporter_iso3,
                "partner_country_id": partner_iso3,
                "year": year_int,
                "hs_section": str(hs_section) if hs_section else None,
                "flow_type": str(flow_type) if flow_type else None,
                "value_usd": value_numeric,
            }
        )
    
    return flows


def ingest_full(
    session: Session,
    *,
    reporter_subset: Optional[Iterable[str]] = None,
    partner_subset: Optional[Iterable[str]] = None,
    year_subset: Optional[Iterable[int]] = None,
    section_subset: Optional[Iterable[str]] = None,
    sample_config: Optional[SampleConfig] = None,
) -> None:
    """
    Ingest COMTRADE data with optional sample mode.
    
    Args:
        session: Database session
        reporter_subset: Optional subset of reporter countries
        partner_subset: Optional subset of partner countries
        year_subset: Optional subset of years
        section_subset: Optional subset of HS sections
        sample_config: Sample mode configuration for testing
    """
    sample_config = sample_config or SampleConfig()
    
    # Filter reporters to only those with COMTRADE codes (excludes TWN)
    valid_reporters = [r for r in COMTRADE_CONFIG["reporters"] if r in ISO3_TO_COMTRADE_CODE]
    reporters = [r for r in valid_reporters if not reporter_subset or r in reporter_subset]
    partners = [p for p in COMTRADE_CONFIG["partners"] if not partner_subset or p in partner_subset]
    years = [y for y in COMTRADE_CONFIG["years"] if not year_subset or y in year_subset]
    hs_codes = [c for c in COMTRADE_CONFIG["hs_codes"] if not section_subset or c in section_subset]
    flows = [f for f in COMTRADE_CONFIG["flows"] if not section_subset or str(f) in {str(x) for x in section_subset}]
    
    # Limit iterations in sample mode
    if sample_config.enabled:
        reporters = reporters[:3]  # Only test 3 reporters
        partners = partners[:2]  # Only test 2 partners
        years = years[-sample_config.max_years:]  # Only recent years
        hs_codes = hs_codes[:2]
        flows = flows[:2]

    async def _run() -> None:
        for reporter in reporters:
            for partner in partners:
                if reporter == partner:
                    continue
                for year in years:
                    for hs_code in hs_codes:
                        for flow_code in flows:
                            try:
                                raw = await fetch_raw_trade(
                                    reporter,
                                    partner,
                                    year,
                                    hs_code,
                                    flow_code,
                                    sample_config=sample_config,
                                )
                                store_raw_payload(
                                    session,
                                    RawComtrade,
                                    params={
                                        "reporter": reporter,
                                        "partner": partner,
                                        "year": year,
                                        "hs_code": hs_code,
                                        "flow": flow_code,
                                    },
                                    payload=raw,
                                )
                                flows_parsed = parse_to_trade_flows(raw, sample_config=sample_config)

                                if sample_config.enabled:
                                    validation = validate_trade_flows(
                                        flows_parsed,
                                        expected_reporters=[reporter],
                                        sample_config=sample_config,
                                    )
                                    if sample_config.strict_validation:
                                        validation.raise_if_invalid()

                                bulk_upsert_tradeflows(session, flows_parsed)
                                logger.info(
                                    "COMTRADE: Ingested %s flows for %s->%s %s hs=%s flow=%s",
                                    len(flows_parsed), reporter, partner, year, hs_code, flow_code,
                                )
                                await asyncio.sleep(COMTRADE_CONFIG["throttle_seconds"])
                            except IngestionError:
                                raise
                            except Exception as e:
                                error_msg = f"Failed {reporter}->{partner} {year} hs={hs_code} flow={flow_code}: {e}"
                                logger.error(f"COMTRADE: {error_msg}")
                                if sample_config.strict_validation:
                                    raise IngestionError("COMTRADE", reporter, error_msg)

    asyncio.run(_run())
    session.commit()

