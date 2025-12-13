#!/usr/bin/env python
"""Seed all required indicators for data ingestion."""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy.orm import Session

from app.db.engine import get_db
from app.db.models import Indicator


# Mapping of canonical codes to indicator metadata
INDICATORS = {
    # FRED - USA (Original)
    "CPI_USA_MONTHLY": {
        "source": "FRED",
        "source_code": "CPIAUCSL",
        "category": "Inflation",
        "unit": "Index",
        "frequency": "M",
    },
    "UNEMP_RATE_USA": {
        "source": "FRED",
        "source_code": "UNRATE",
        "category": "Labor",
        "unit": "Percent",
        "frequency": "M",
    },
    # FRED - G20 Exchange Rates
    "FX_CNY_USD_FRED": {
        "source": "FRED",
        "source_code": "DEXCHUS",
        "category": "Exchange Rates",
        "unit": "Rate",
        "frequency": "D",
    },
    "FX_JPY_USD_FRED": {
        "source": "FRED",
        "source_code": "DEXJPUS",
        "category": "Exchange Rates",
        "unit": "Rate",
        "frequency": "D",
    },
    "FX_USD_EUR_FRED": {
        "source": "FRED",
        "source_code": "DEXUSEU",
        "category": "Exchange Rates",
        "unit": "Rate",
        "frequency": "D",
    },
    "FX_USD_GBP_FRED": {
        "source": "FRED",
        "source_code": "DEXUSUK",
        "category": "Exchange Rates",
        "unit": "Rate",
        "frequency": "D",
    },
    "FX_CAD_USD_FRED": {
        "source": "FRED",
        "source_code": "DEXCAUS",
        "category": "Exchange Rates",
        "unit": "Rate",
        "frequency": "D",
    },
    "FX_MXN_USD_FRED": {
        "source": "FRED",
        "source_code": "DEXMXUS",
        "category": "Exchange Rates",
        "unit": "Rate",
        "frequency": "D",
    },
    "FX_BRL_USD_FRED": {
        "source": "FRED",
        "source_code": "DEXBZUS",
        "category": "Exchange Rates",
        "unit": "Rate",
        "frequency": "D",
    },
    "FX_INR_USD_FRED": {
        "source": "FRED",
        "source_code": "DEXINUS",
        "category": "Exchange Rates",
        "unit": "Rate",
        "frequency": "D",
    },
    "FX_KRW_USD_FRED": {
        "source": "FRED",
        "source_code": "DEXKOUS",
        "category": "Exchange Rates",
        "unit": "Rate",
        "frequency": "D",
    },
    "FX_USD_AUD_FRED": {
        "source": "FRED",
        "source_code": "DEXUSAL",
        "category": "Exchange Rates",
        "unit": "Rate",
        "frequency": "D",
    },
    "FX_CHF_USD_FRED": {
        "source": "FRED",
        "source_code": "DEXSZUS",
        "category": "Exchange Rates",
        "unit": "Rate",
        "frequency": "D",
    },
    "FX_SEK_USD_FRED": {
        "source": "FRED",
        "source_code": "DEXSDUS",
        "category": "Exchange Rates",
        "unit": "Rate",
        "frequency": "D",
    },
    # FRED - International CPI
    "CPI_CHN_FRED": {
        "source": "FRED",
        "source_code": "CHNCPIALLMINMEI",
        "category": "Inflation",
        "unit": "Index",
        "frequency": "M",
    },
    "CPI_JPN_FRED": {
        "source": "FRED",
        "source_code": "JPNCPIALLMINMEI",
        "category": "Inflation",
        "unit": "Index",
        "frequency": "M",
    },
    "CPI_DEU_FRED": {
        "source": "FRED",
        "source_code": "DEUCPIALLMINMEI",
        "category": "Inflation",
        "unit": "Index",
        "frequency": "M",
    },
    "CPI_GBR_FRED": {
        "source": "FRED",
        "source_code": "GBRCPIALLMINMEI",
        "category": "Inflation",
        "unit": "Index",
        "frequency": "M",
    },
    "CPI_CAN_FRED": {
        "source": "FRED",
        "source_code": "CANCPIALLMINMEI",
        "category": "Inflation",
        "unit": "Index",
        "frequency": "M",
    },
    # FRED - International Interest Rates
    "INT_RATE_CHN_FRED": {
        "source": "FRED",
        "source_code": "INTGSTCNM193N",
        "category": "Interest Rates",
        "unit": "Percent",
        "frequency": "M",
    },
    "INT_RATE_JPN_FRED": {
        "source": "FRED",
        "source_code": "INTGSTJPM193N",
        "category": "Interest Rates",
        "unit": "Percent",
        "frequency": "M",
    },
    "INT_RATE_GBR_FRED": {
        "source": "FRED",
        "source_code": "INTGSTGBM193N",
        "category": "Interest Rates",
        "unit": "Percent",
        "frequency": "M",
    },
    # FRED - Commodity Prices (Global)
    "OIL_WTI_FRED": {
        "source": "FRED",
        "source_code": "DCOILWTICO",
        "category": "Commodities",
        "unit": "USD_PER_BARREL",
        "frequency": "D",
    },
    "OIL_BRENT_FRED": {
        "source": "FRED",
        "source_code": "DCOILBRENTEU",
        "category": "Commodities",
        "unit": "USD_PER_BARREL",
        "frequency": "D",
    },
    "GOLD_PRICE_FRED": {
        "source": "FRED",
        "source_code": "GOLDPMGBD228NLBM",
        "category": "Commodities",
        "unit": "USD_PER_OZ",
        "frequency": "D",
    },
    # WDI - Core Economic (Original)
    "GDP_REAL": {
        "source": "WDI",
        "source_code": "NY.GDP.MKTP.KD",
        "category": "Economic Growth",
        "unit": "USD",
        "frequency": "A",
    },
    "CPI_YOY": {
        "source": "WDI",
        "source_code": "FP.CPI.TOTL.ZG",
        "category": "Inflation",
        "unit": "Percent",
        "frequency": "A",
    },
    "UNEMP_RATE": {
        "source": "WDI",
        "source_code": "SL.UEM.TOTL.ZS",
        "category": "Labor",
        "unit": "Percent",
        "frequency": "A",
    },
    # WDI - National Accounts
    "CONSUMPTION_TOTAL_WDI": {
        "source": "WDI",
        "source_code": "NE.CON.TOTL.KD",
        "category": "National Accounts",
        "unit": "USD",
        "frequency": "A",
    },
    "CONSUMPTION_PRIVATE_WDI": {
        "source": "WDI",
        "source_code": "NE.CON.PRVT.KD",
        "category": "National Accounts",
        "unit": "USD",
        "frequency": "A",
    },
    "CONSUMPTION_GOVT_WDI": {
        "source": "WDI",
        "source_code": "NE.CON.GOVT.KD",
        "category": "National Accounts",
        "unit": "USD",
        "frequency": "A",
    },
    "INVESTMENT_TOTAL_WDI": {
        "source": "WDI",
        "source_code": "NE.GDI.TOTL.KD",
        "category": "National Accounts",
        "unit": "USD",
        "frequency": "A",
    },
    "EXPORTS_GOODS_SERVICES_WDI": {
        "source": "WDI",
        "source_code": "NE.EXP.GNFS.KD",
        "category": "Trade",
        "unit": "USD",
        "frequency": "A",
    },
    "IMPORTS_GOODS_SERVICES_WDI": {
        "source": "WDI",
        "source_code": "NE.IMP.GNFS.KD",
        "category": "Trade",
        "unit": "USD",
        "frequency": "A",
    },
    "GDP_PER_CAPITA_WDI": {
        "source": "WDI",
        "source_code": "NY.GDP.PCAP.KD",
        "category": "Economic Growth",
        "unit": "USD",
        "frequency": "A",
    },
    "GDP_GROWTH_WDI": {
        "source": "WDI",
        "source_code": "NY.GDP.MKTP.KD.ZG",
        "category": "Economic Growth",
        "unit": "Percent",
        "frequency": "A",
    },
    "GNI_CURRENT_WDI": {
        "source": "WDI",
        "source_code": "NY.GNP.MKTP.CD",
        "category": "Economic Growth",
        "unit": "USD",
        "frequency": "A",
    },
    # WDI - Demographics
    "POPULATION_TOTAL_WDI": {
        "source": "WDI",
        "source_code": "SP.POP.TOTL",
        "category": "Demographics",
        "unit": "Count",
        "frequency": "A",
    },
    "POPULATION_GROWTH_WDI": {
        "source": "WDI",
        "source_code": "SP.POP.GROW",
        "category": "Demographics",
        "unit": "Percent",
        "frequency": "A",
    },
    "URBAN_POPULATION_PCT_WDI": {
        "source": "WDI",
        "source_code": "SP.URB.TOTL.IN.ZS",
        "category": "Demographics",
        "unit": "Percent",
        "frequency": "A",
    },
    "LABOR_FORCE_TOTAL_WDI": {
        "source": "WDI",
        "source_code": "SL.TLF.TOTL.IN",
        "category": "Labor",
        "unit": "Count",
        "frequency": "A",
    },
    # WDI - Trade
    "EXPORTS_PCT_GDP_WDI": {
        "source": "WDI",
        "source_code": "NE.EXP.GNFS.ZS",
        "category": "Trade",
        "unit": "Percent",
        "frequency": "A",
    },
    "IMPORTS_PCT_GDP_WDI": {
        "source": "WDI",
        "source_code": "NE.IMP.GNFS.ZS",
        "category": "Trade",
        "unit": "Percent",
        "frequency": "A",
    },
    "CURRENT_ACCOUNT_PCT_GDP_WDI": {
        "source": "WDI",
        "source_code": "BN.CAB.XOKA.GD.ZS",
        "category": "Trade",
        "unit": "Percent",
        "frequency": "A",
    },
    # WDI - Finance
    "DOMESTIC_CREDIT_PCT_GDP_WDI": {
        "source": "WDI",
        "source_code": "FS.AST.DOMS.GD.ZS",
        "category": "Finance",
        "unit": "Percent",
        "frequency": "A",
    },
    "MARKET_CAP_PCT_GDP_WDI": {
        "source": "WDI",
        "source_code": "CM.MKT.LCAP.GD.ZS",
        "category": "Finance",
        "unit": "Percent",
        "frequency": "A",
    },
    "FDI_NET_INFLOWS_PCT_GDP_WDI": {
        "source": "WDI",
        "source_code": "BX.KLT.DINV.WD.GD.ZS",
        "category": "Finance",
        "unit": "Percent",
        "frequency": "A",
    },
    # WDI - Infrastructure
    "INTERNET_USERS_PCT_WDI": {
        "source": "WDI",
        "source_code": "IT.NET.USER.ZS",
        "category": "Infrastructure",
        "unit": "Percent",
        "frequency": "A",
    },
    "ELECTRICITY_ACCESS_PCT_WDI": {
        "source": "WDI",
        "source_code": "EG.ELC.ACCS.ZS",
        "category": "Infrastructure",
        "unit": "Percent",
        "frequency": "A",
    },
    "MOBILE_SUBSCRIPTIONS_PER100_WDI": {
        "source": "WDI",
        "source_code": "IT.CEL.SETS.P2",
        "category": "Infrastructure",
        "unit": "Per 100 people",
        "frequency": "A",
    },
    # WDI - Health & Education
    "LIFE_EXPECTANCY_WDI": {
        "source": "WDI",
        "source_code": "SP.DYN.LE00.IN",
        "category": "Health",
        "unit": "Years",
        "frequency": "A",
    },
    "INFANT_MORTALITY_WDI": {
        "source": "WDI",
        "source_code": "SH.DYN.MORT",
        "category": "Health",
        "unit": "Per 1000 births",
        "frequency": "A",
    },
    "PRIMARY_SCHOOL_ENROLLMENT_WDI": {
        "source": "WDI",
        "source_code": "SE.PRM.ENRR",
        "category": "Education",
        "unit": "Percent",
        "frequency": "A",
    },
    # WDI - Environment
    "ELECTRICITY_USE_PER_CAPITA_WDI": {
        "source": "WDI",
        "source_code": "EG.USE.ELEC.KH.PC",
        "category": "Energy",
        "unit": "kWh per capita",
        "frequency": "A",
    },
    "CO2_EMISSIONS_PER_CAPITA_WDI": {
        "source": "WDI",
        "source_code": "EN.ATM.CO2E.PC",
        "category": "Environment",
        "unit": "Metric tons per capita",
        "frequency": "A",
    },
    "RENEWABLE_ENERGY_PCT_WDI": {
        "source": "WDI",
        "source_code": "EG.FEC.RNEW.ZS",
        "category": "Energy",
        "unit": "Percent",
        "frequency": "A",
    },
    # IMF - Economic Indicators
    "GDP_REAL_INDEX": {
        "source": "IMF",
        "source_code": "NGDP_R",
        "category": "Economic Growth",
        "unit": "Index",
        "frequency": "A",
    },
    "IMF_CPI_INDEX": {
        "source": "IMF",
        "source_code": "CPI",
        "category": "Inflation",
        "unit": "Index",
        "frequency": "M",
    },
    "IMF_CPI_2010": {
        "source": "IMF",
        "source_code": "PCPI",
        "category": "Inflation",
        "unit": "Index",
        "frequency": "A",
    },
    "IMF_INFLATION_PCT": {
        "source": "IMF",
        "source_code": "PCPIPCH",
        "category": "Inflation",
        "unit": "Percent",
        "frequency": "A",
    },
    "IMF_GDP_CURRENT": {
        "source": "IMF",
        "source_code": "NGDP",
        "category": "Economic Growth",
        "unit": "National Currency",
        "frequency": "A",
    },
    "IMF_GDP_PER_CAPITA_PPP": {
        "source": "IMF",
        "source_code": "NGDPRPPPPC",
        "category": "Economic Growth",
        "unit": "USD_PPP",
        "frequency": "A",
    },
    # IMF - Balance of Payments
    "IMF_CURRENT_ACCOUNT": {
        "source": "IMF",
        "source_code": "BCA",
        "category": "Balance of Payments",
        "unit": "USD_Billions",
        "frequency": "A",
    },
    "IMF_CURRENT_ACCOUNT_PCT_GDP": {
        "source": "IMF",
        "source_code": "BCA_NGDPD",
        "category": "Balance of Payments",
        "unit": "Percent",
        "frequency": "A",
    },
    # IMF - Monetary & Financial
    "IMF_M2": {
        "source": "IMF",
        "source_code": "FM",
        "category": "Monetary",
        "unit": "National Currency",
        "frequency": "A",
    },
    "IMF_POLICY_RATE": {
        "source": "IMF",
        "source_code": "FPOLM",
        "category": "Interest Rates",
        "unit": "Percent",
        "frequency": "M",
    },
    "IMF_LENDING_RATE": {
        "source": "IMF",
        "source_code": "FILR",
        "category": "Interest Rates",
        "unit": "Percent",
        "frequency": "A",
    },
    # IMF - External Sector
    "IMF_EXTERNAL_DEBT": {
        "source": "IMF",
        "source_code": "ENDA",
        "category": "External Debt",
        "unit": "USD_Billions",
        "frequency": "A",
    },
    "IMF_EXTERNAL_DEBT_PCT_GDP": {
        "source": "IMF",
        "source_code": "ENDA_NGDPD",
        "category": "External Debt",
        "unit": "Percent",
        "frequency": "A",
    },
    # IMF - Fiscal (Government Finance)
    "IMF_GOVT_REVENUE": {
        "source": "IMF",
        "source_code": "GGR",
        "category": "Fiscal",
        "unit": "National Currency",
        "frequency": "A",
    },
    "IMF_GOVT_BALANCE": {
        "source": "IMF",
        "source_code": "GGXCNL",
        "category": "Fiscal",
        "unit": "National Currency",
        "frequency": "A",
    },
    "IMF_GOVT_DEBT": {
        "source": "IMF",
        "source_code": "GGXWDG",
        "category": "Fiscal",
        "unit": "National Currency",
        "frequency": "A",
    },
    "IMF_GOVT_DEBT_PCT_GDP": {
        "source": "IMF",
        "source_code": "GGXWDG_NGDP",
        "category": "Fiscal",
        "unit": "Percent",
        "frequency": "A",
    },
    # IMF - Trade & Reserves
    "IMF_EXPORTS": {
        "source": "IMF",
        "source_code": "TX",
        "category": "Trade",
        "unit": "National Currency",
        "frequency": "A",
    },
    "IMF_IMPORTS": {
        "source": "IMF",
        "source_code": "TM",
        "category": "Trade",
        "unit": "National Currency",
        "frequency": "A",
    },
    "IMF_RESERVES_TOTAL": {
        "source": "IMF",
        "source_code": "FDIR",
        "category": "Reserves",
        "unit": "USD_Billions",
        "frequency": "M",
    },
    "IMF_RESERVES_MONTHS_IMPORTS": {
        "source": "IMF",
        "source_code": "FIDR",
        "category": "Reserves",
        "unit": "Months",
        "frequency": "A",
    },
    "IMF_FX_RATE_USD": {
        "source": "IMF",
        "source_code": "ENDA_XDC_USD",
        "category": "Exchange Rates",
        "unit": "Rate",
        "frequency": "A",
    },
    "IMF_UNEMPLOYMENT_RATE": {
        "source": "IMF",
        "source_code": "LUR",
        "category": "Labor",
        "unit": "Percent",
        "frequency": "A",
    },
    # ECB_SDW
    "FX_USD_EUR": {
        "source": "ECB_SDW",
        "source_code": "EURUSD",
        "category": "Exchange Rates",
        "unit": "Rate",
        "frequency": "D",
    },
    "FX_GBP_EUR": {
        "source": "ECB_SDW",
        "source_code": "GBPEUR",
        "category": "Exchange Rates",
        "unit": "Rate",
        "frequency": "D",
    },
    # EUROSTAT
    "HICP_YOY": {
        "source": "EUROSTAT",
        "source_code": "PRC_HICP_YY",
        "category": "Inflation",
        "unit": "Percent",
        "frequency": "M",
    },
    "GDP_GROWTH_QOQ": {
        "source": "EUROSTAT",
        "source_code": "GDP_QOQ",
        "category": "Economic Growth",
        "unit": "Percent",
        "frequency": "Q",
    },
    # ONS
    "CPIH_UK": {
        "source": "ONS",
        "source_code": "CPIH",
        "category": "Inflation",
        "unit": "Index",
        "frequency": "M",
    },
    "GDP_GROWTH_UK": {
        "source": "ONS",
        "source_code": "GDP_GROWTH",
        "category": "Economic Growth",
        "unit": "Percent",
        "frequency": "Q",
    },
    # OECD
    "GDP_PER_CAPITA_OECD": {
        "source": "OECD",
        "source_code": "GDP_PC",
        "category": "Economic Growth",
        "unit": "USD",
        "frequency": "A",
    },
    # ADB
    "ADB_GDP_GROWTH": {
        "source": "ADB",
        "source_code": "NY.GDP.MKTP.KD.ZG",
        "category": "Economic Growth",
        "unit": "Percent",
        "frequency": "A",
    },
    # BIS
    "BIS_CREDIT_PRIVATE": {
        "source": "BIS",
        "source_code": "CREDIT_PRIV",
        "category": "Credit",
        "unit": "USD",
        "frequency": "A",
    },
    "BIS_CREDIT_PRIVATE_PCT_GDP": {
        "source": "BIS",
        "source_code": "CREDIT_PRIV_PCT_GDP",
        "category": "Credit",
        "unit": "Percent",
        "frequency": "A",
    },
    "BIS_CPI": {
        "source": "BIS",
        "source_code": "BIS_CPI",
        "category": "Inflation",
        "unit": "Index",
        "frequency": "M",
    },
    "BIS_POLICY_RATE": {
        "source": "BIS",
        "source_code": "BIS_POLICY_RATE",
        "category": "Monetary Policy",
        "unit": "Percent",
        "frequency": "M",
    },
    "BIS_DEBT_SERVICE_RATIO": {
        "source": "BIS",
        "source_code": "BIS_DSR",
        "category": "Credit",
        "unit": "Percent",
        "frequency": "Q",
    },
    # FAOSTAT
    "FAOSTAT_PRODUCTION": {
        "source": "FAOSTAT",
        "source_code": "PRODUCTION",
        "category": "Agriculture",
        "unit": "Tonnes",
        "frequency": "A",
    },
    # ILOSTAT
    "ILOSTAT_UNEMPLOYMENT_RATE": {
        "source": "ILOSTAT",
        "source_code": "UNE_RATE",
        "category": "Labor",
        "unit": "Percent",
        "frequency": "A",
    },
    # UNCTAD
    "UNCTAD_FDI_FLOW_INWARD": {
        "source": "UNCTAD",
        "source_code": "FDI_INWARD",
        "category": "Investment",
        "unit": "USD",
        "frequency": "A",
    },
    # OPENALEX
    "OPENALEX_WORKS_COUNT": {
        "source": "OPENALEX",
        "source_code": "WORKS_COUNT",
        "category": "Research",
        "unit": "Count",
        "frequency": "A",
    },
    # PATENTSVIEW
    "PATENTS_COUNT": {
        "source": "PATENTSVIEW",
        "source_code": "PATENTS",
        "category": "Innovation",
        "unit": "Count",
        "frequency": "A",
    },
    # EIA
    "EIA_ENERGY_CONSUMPTION_TOTAL": {
        "source": "EIA",
        "source_code": "CONSUMPTION",
        "category": "Energy",
        "unit": "BTU",
        "frequency": "A",
    },
    "EIA_ENERGY_PRODUCTION_TOTAL": {
        "source": "EIA",
        "source_code": "PRODUCTION",
        "category": "Energy",
        "unit": "BTU",
        "frequency": "A",
    },
    "EIA_WTI_PRICE": {
        "source": "EIA",
        "source_code": "WTI",
        "category": "Energy",
        "unit": "USD/barrel",
        "frequency": "D",
    },
    "BRENT_OIL_SPOT_PRICE": {
        "source": "EIA",
        "source_code": "BRENT_SPOT",
        "category": "Energy",
        "unit": "USD/barrel",
        "frequency": "D",
    },
    "HENRY_HUB_GAS_PRICE": {
        "source": "EIA",
        "source_code": "HENRY_HUB",
        "category": "Energy",
        "unit": "USD/MMBtu",
        "frequency": "D",
    },
    # EMBER
    "EMBER_ELECTRICITY_GENERATION": {
        "source": "EMBER",
        "source_code": "GENERATION",
        "category": "Energy",
        "unit": "GWh",
        "frequency": "M",
    },
    "EMBER_ELECTRICITY_SOLAR": {
        "source": "EMBER",
        "source_code": "SOLAR",
        "category": "Energy",
        "unit": "TWh",
        "frequency": "A",
    },
    "EMBER_ELECTRICITY_WIND": {
        "source": "EMBER",
        "source_code": "WIND",
        "category": "Energy",
        "unit": "TWh",
        "frequency": "A",
    },
    "EMBER_ELECTRICITY_COAL": {
        "source": "EMBER",
        "source_code": "COAL",
        "category": "Energy",
        "unit": "TWh",
        "frequency": "A",
    },
    "EMBER_ELECTRICITY_GAS": {
        "source": "EMBER",
        "source_code": "GAS",
        "category": "Energy",
        "unit": "TWh",
        "frequency": "A",
    },
    "EMBER_ELECTRICITY_HYDRO": {
        "source": "EMBER",
        "source_code": "HYDRO",
        "category": "Energy",
        "unit": "TWh",
        "frequency": "A",
    },
    # GCP
    "GCP_EMISSIONS_SECTOR": {
        "source": "GCP",
        "source_code": "EMISSIONS",
        "category": "Environment",
        "unit": "MtCO2",
        "frequency": "A",
    },
    "CO2_TOTAL": {
        "source": "GCP",
        "source_code": "CO2_TOTAL",
        "category": "Environment",
        "unit": "MtCO2",
        "frequency": "A",
    },
    "CO2_PER_CAPITA": {
        "source": "GCP",
        "source_code": "CO2_PER_CAPITA",
        "category": "Environment",
        "unit": "tCO2/person",
        "frequency": "A",
    },
    # YFINANCE
    "EQUITY_PRICE": {
        "source": "YFINANCE",
        "source_code": "EQUITY",
        "category": "Financial Markets",
        "unit": "USD",
        "frequency": "D",
    },
    # STOOQ
    "STOCK_PRICE": {
        "source": "STOOQ",
        "source_code": "STOCK",
        "category": "Financial Markets",
        "unit": "USD",
        "frequency": "D",
    },
    # AISSTREAM
    "VESSEL_POSITION": {
        "source": "AISSTREAM",
        "source_code": "VESSEL",
        "category": "Maritime",
        "unit": "Coordinates",
        "frequency": "RT",
    },
    # GDELT
    "GDELT_EVENT_COUNT": {
        "source": "GDELT",
        "source_code": "EVENTS",
        "category": "Geopolitics",
        "unit": "Count",
        "frequency": "D",
    },
    # RSS
    "POLICY_RATE_CHANGE_FLAG": {
        "source": "RSS",
        "source_code": "POLICY_RATE",
        "category": "Monetary Policy",
        "unit": "Flag",
        "frequency": "D",
    },
    # COMTRADE
    "TRADE_FLOW_VALUE": {
        "source": "COMTRADE",
        "source_code": "TRADE",
        "category": "Trade",
        "unit": "USD",
        "frequency": "A",
    },
}


def seed_indicators(session: Session) -> None:
    """Seed all required indicators."""
    
    for canonical_code, metadata in INDICATORS.items():
        # Check if indicator already exists
        existing = session.query(Indicator).filter(
            Indicator.canonical_code == canonical_code
        ).one_or_none()
        
        if existing:
            print(f"  Skipping {canonical_code} (already exists)")
            continue
        
        # Create new indicator
        indicator = Indicator(
            canonical_code=canonical_code,
            source=metadata["source"],
            source_code=metadata["source_code"],
            category=metadata["category"],
            unit=metadata["unit"],
            frequency=metadata["frequency"],
        )
        session.add(indicator)
        print(f"  ✓ Added {canonical_code}")
    
    session.commit()
    print(f"\n✓ {len(INDICATORS)} indicators seeded")


if __name__ == "__main__":
    print("\nSeeding indicators...")
    session = next(get_db())
    try:
        seed_indicators(session)
    finally:
        session.close()

