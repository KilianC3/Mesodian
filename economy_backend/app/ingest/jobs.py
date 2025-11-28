from typing import Dict

from sqlalchemy.orm import Session

from app.ingest import (
    adb_client,
    afdb_client,
    aisstream_client,
    bis_client,
    comtrade_client,
    ecb_sdw_client,
    eia_client,
    ember_client,
    eurostat_client,
    faostat_client,
    fred_client,
    gcp_client,
    gdelt_client,
    ilostat_client,
    imf_client,
    oecd_client,
    ons_client,
    openalex_client,
    patentsview_client,
    rss_client,
    stooq_client,
    unctad_client,
    wdi_client,
    yfinance_client,
)


PROVIDERS = [
    ("FRED", fred_client),
    ("WDI", wdi_client),
    ("IMF", imf_client),
    ("ECB_SDW", ecb_sdw_client),
    ("EUROSTAT", eurostat_client),
    ("ONS", ons_client),
    ("OECD", oecd_client),
    ("ADB", adb_client),
    ("AFDB", afdb_client),
    ("COMTRADE", comtrade_client),
    ("BIS", bis_client),
    ("FAOSTAT", faostat_client),
    ("ILOSTAT", ilostat_client),
    ("UNCTAD", unctad_client),
    ("OPENALEX", openalex_client),
    ("PATENTSVIEW", patentsview_client),
    ("EIA", eia_client),
    ("EMBER", ember_client),
    ("GCP", gcp_client),
    ("YFINANCE", yfinance_client),
    ("STOOQ", stooq_client),
    ("AISSTREAM", aisstream_client),
    ("GDELT", gdelt_client),
    ("RSS", rss_client),
]


def ingest_all_full(session: Session) -> None:
    """Run a full ingestion cycle for every provider."""

    for _, module in PROVIDERS:
        module.ingest_full(session)


def ingest_all_health_check(session: Session) -> Dict[str, Dict[str, object]]:
    """Run lightweight ingestion/health checks for all providers.

    Returns a mapping of provider name to an object describing whether the
    ingestion call completed successfully.
    """

    status: Dict[str, Dict[str, object]] = {}
    for provider, module in PROVIDERS:
        try:
            module.ingest_full(session)
        except Exception as exc:  # pragma: no cover - exercised via tests
            session.rollback()
            status[provider] = {"ok": False, "error": str(exc)}
        else:
            status[provider] = {"ok": True, "error": None}
    return status
