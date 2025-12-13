"""
Seed all UNCTAD and WTO indicators for comprehensive data coverage.
Covers ~40 datasets across trade, economy, maritime, and policy.
"""

from app.db.engine import get_db
from app.db.models import Indicator

# All UNCTAD indicators (25 datasets)
UNCTAD_INDICATORS = [
    # === A. INTERNATIONAL TRADE ===
    ("UNCTAD", "US.TradeMerchTotal", "UNCTAD_TRADE_MERCH_TOTAL", "annual", "USD", "trade"),
    ("UNCTAD", "US.TradeGoods", "UNCTAD_TRADE_GOODS", "annual", "USD", "trade"),
    ("UNCTAD", "US.TradeServCatByPartner", "UNCTAD_TRADE_SERV_PARTNER", "annual", "USD", "trade"),
    ("UNCTAD", "US.CreativeServ_Indiv_Tot", "UNCTAD_CREATIVE_SERV_TOTAL", "annual", "USD", "trade"),
    ("UNCTAD", "US.CreativeServ_Group_E", "UNCTAD_CREATIVE_SERV_EXPORTS", "annual", "USD", "trade"),
    
    # === B. ECONOMY, INVESTMENT & FINANCE ===
    ("UNCTAD", "US.FDI", "UNCTAD_FDI_FLOW_INWARD", "annual", "USD", "finance"),
    ("UNCTAD", "US.GDPTotal", "UNCTAD_GDP_CURRENT", "annual", "USD", "economy"),
    ("UNCTAD", "US.GDPGrowth", "UNCTAD_GDP_GROWTH", "annual", "percent", "economy"),
    ("UNCTAD", "US.CapitalFormation", "UNCTAD_GFCF", "annual", "USD", "economy"),
    ("UNCTAD", "US.ProductiveCapacity", "UNCTAD_PCI", "annual", "index", "economy"),
    ("UNCTAD", "US.Inflation", "UNCTAD_CPI", "annual", "index", "economy"),
    ("UNCTAD", "US.ExchangeRate", "UNCTAD_FX_RATE", "annual", "rate", "finance"),
    ("UNCTAD", "US.CommodityPrices", "UNCTAD_COMMODITY_PRICE_INDEX", "monthly", "index", "economy"),
    
    # === C. MARITIME & TRANSPORT ===
    ("UNCTAD", "US.MerchantFleet", "UNCTAD_FLEET_FLAG", "annual", "DWT", "maritime"),
    ("UNCTAD", "US.FleetOwnership", "UNCTAD_FLEET_OWNERSHIP", "annual", "DWT", "maritime"),
    ("UNCTAD", "US.FleetValue", "UNCTAD_FLEET_VALUE_SHARE", "annual", "percent", "maritime"),
    ("UNCTAD", "US.SeaborneTrade", "UNCTAD_SEABORNE_TRADE", "annual", "tonnes", "maritime"),
    ("UNCTAD", "US.PortCalls", "UNCTAD_PORT_CALLS", "annual", "count", "maritime"),
    ("UNCTAD", "US.PortCallsArrivals_S", "UNCTAD_PORT_ARRIVALS", "annual", "count", "maritime"),
    ("UNCTAD", "US.LSCI", "UNCTAD_LSCI_QUARTERLY", "quarterly", "index", "maritime"),
    ("UNCTAD", "US.LSCI_M", "UNCTAD_LSCI_MONTHLY", "monthly", "index", "maritime"),
    ("UNCTAD", "US.PLSCI", "UNCTAD_PLSCI", "quarterly", "index", "maritime"),
    ("UNCTAD", "US.Seafarers", "UNCTAD_SEAFARER_SUPPLY", "annual", "count", "maritime"),
    ("UNCTAD", "US.TransportCost", "UNCTAD_FREIGHT_COST", "annual", "percent", "maritime"),
]

# All WTO indicators (15 indicators)
WTO_INDICATORS = [
    # === A. TRADE FLOWS ===
    ("WTO", "ITS_MTV_AM", "WTO_TRADE_MERCH_VALUE", "annual", "USD", "trade"),
    ("WTO", "ITS_MTV_QM", "WTO_TRADE_MERCH_VALUE_Q", "quarterly", "USD", "trade"),
    ("WTO", "ITS_IVM_AM", "WTO_TRADE_VOLUME_INDEX", "annual", "index", "trade"),
    ("WTO", "ITS_IVI_AM", "WTO_TRADE_VALUE_INDEX", "annual", "index", "trade"),
    ("WTO", "TCS_AM", "WTO_SERVICES_TOTAL", "annual", "USD", "trade"),
    ("WTO", "TCS_QM", "WTO_SERVICES_TOTAL_Q", "quarterly", "USD", "trade"),
    ("WTO", "DDS_AM", "WTO_DIGITAL_SERVICES", "annual", "USD", "trade"),
    
    # === B. TARIFFS & MARKET ACCESS ===
    ("WTO", "TP_A_0010", "WTO_TARIFF_AVG_MFN", "annual", "percent", "trade"),
    ("WTO", "TP_A_0020", "WTO_TARIFF_AVG_MFN_AGR", "annual", "percent", "trade"),
    ("WTO", "TP_A_0030", "WTO_TARIFF_AVG_MFN_NONAGR", "annual", "percent", "trade"),
    ("WTO", "TP_A_0050", "WTO_TARIFF_WEIGHTED_MFN", "annual", "percent", "trade"),
    ("WTO", "TP_A_0100", "WTO_DUTY_FREE_SHARE", "annual", "percent", "trade"),
    ("WTO", "TP_A_0200", "WTO_TARIFF_BOUND_AVG", "annual", "percent", "trade"),
    ("WTO", "TP_A_0300", "WTO_TARIFF_PEAKS", "annual", "percent", "trade"),
]


def seed_all_indicators():
    """Seed all UNCTAD and WTO indicators."""
    session = next(get_db())
    
    all_indicators = UNCTAD_INDICATORS + WTO_INDICATORS
    added_count = 0
    skipped_count = 0
    
    print(f"Seeding {len(all_indicators)} indicators...")
    print(f"  - UNCTAD: {len(UNCTAD_INDICATORS)} datasets")
    print(f"  - WTO: {len(WTO_INDICATORS)} indicators")
    print()
    
    for source, source_code, canonical_code, frequency, unit, category in all_indicators:
        existing = session.query(Indicator).filter_by(canonical_code=canonical_code).first()
        
        if existing:
            print(f"✓ {canonical_code} (already exists)")
            skipped_count += 1
        else:
            new_indicator = Indicator(
                source=source,
                source_code=source_code,
                canonical_code=canonical_code,
                frequency=frequency,
                unit=unit,
                category=category,
            )
            session.add(new_indicator)
            print(f"+ {canonical_code}")
            added_count += 1
    
    session.commit()
    print()
    print(f"✅ Seeding complete:")
    print(f"   Added: {added_count}")
    print(f"   Skipped (existing): {skipped_count}")
    print(f"   Total: {len(all_indicators)}")
    session.close()


if __name__ == "__main__":
    seed_all_indicators()
