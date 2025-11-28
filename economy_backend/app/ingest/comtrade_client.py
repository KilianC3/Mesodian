import asyncio
import logging
import os
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy.orm import Session

from app.config.country_universe import COUNTRY_UNIVERSE
from app.db.models import RawComtrade
from app.ingest.base_client import get_provider_client
from app.ingest.utils import bulk_upsert_tradeflows, store_raw_payload

logger = logging.getLogger(__name__)


COMTRADE_BASE_URL = "https://api.un.org/Comtrade/v1"

COMTRADE_CONFIG: Dict[str, Any] = {
    "reporters": COUNTRY_UNIVERSE,
    "partners": COUNTRY_UNIVERSE + ["WLD"],
    "hs_sections": {
        "AGRICULTURE": "01",
        "ENERGY": "27",
        "METALS": "72",
        "MACHINERY": "84",
    },
    "years": list(range(2019, 2024)),
}


async def fetch_raw_trade(
    reporter: str, partner: str, year: int, section: str, *, client=None
) -> Dict[str, Any]:
    """Fetch raw trade data from Comtrade API v1."""

    if client is None:
        client = get_provider_client("COMTRADE", COMTRADE_BASE_URL)

    api_key = os.getenv("COMTRADE_API_KEY")
    params = {
        "reporterCode": reporter,
        "partnerCode": partner,
        "cmdCode": section,
        "period": year,
        "flowCode": "M,X",
    }
    if api_key:
        params["api_key"] = api_key

    async with client as http_client:
        return await http_client.get_json("/get/hs", params=params)


def parse_to_trade_flows(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    flows: List[Dict[str, Any]] = []
    if not raw:
        return flows

    for item in raw.get("data", []):
        reporter = item.get("reporter") or item.get("rtCode") or item.get("reporterCode")
        partner = item.get("partner") or item.get("ptCode") or item.get("partnerCode")
        year = item.get("period") or item.get("year")
        hs_section = item.get("cmdDescE") or item.get("cmdCode")
        flow_type = item.get("flowDesc") or item.get("flowCode") or item.get("flow")
        value = (
            item.get("primaryValue")
            or item.get("primaryValueUsd")
            or item.get("TradeValue")
            or item.get("tradeValue")
        )

        if not reporter or not partner or not year:
            continue
        try:
            year_int = int(str(year)[0:4])
            value_numeric = float(value) if value is not None else None
        except Exception as exc:  # pragma: no cover - data dependent
            logger.warning("Skipping trade row %s: %s", item, exc)
            continue

        flows.append(
            {
                "reporter_country_id": str(reporter).upper(),
                "partner_country_id": str(partner).upper(),
                "year": year_int,
                "hs_section": hs_section,
                "flow_type": flow_type,
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
) -> None:
    reporters = [r for r in COMTRADE_CONFIG["reporters"] if not reporter_subset or r in reporter_subset]
    partners = [p for p in COMTRADE_CONFIG["partners"] if not partner_subset or p in partner_subset]
    years = [y for y in COMTRADE_CONFIG["years"] if not year_subset or y in year_subset]
    sections = {
        name: code
        for name, code in COMTRADE_CONFIG["hs_sections"].items()
        if not section_subset or name in section_subset or code in section_subset
    }

    async def _run() -> None:
        for reporter in reporters:
            for partner in partners:
                if reporter == partner:
                    continue
                for year in years:
                    for section_name, section_code in sections.items():
                        raw = await fetch_raw_trade(reporter, partner, year, section_code)
                        store_raw_payload(
                            session,
                            RawComtrade,
                            params={
                                "reporter": reporter,
                                "partner": partner,
                                "year": year,
                                "section": section_name,
                            },
                            payload=raw,
                        )
                        flows = parse_to_trade_flows(raw)
                        bulk_upsert_tradeflows(session, flows)
                        await asyncio.sleep(0.2)

    asyncio.run(_run())
    session.commit()

