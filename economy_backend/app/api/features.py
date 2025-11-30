"""Feature endpoints."""

from typing import Dict

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.config.country_universe import COUNTRY_UNIVERSE
from app.db.engine import get_db
from app.db.models import CountryYearFeatures


router = APIRouter()


@router.get("/features/country/{country_id}/{year}")
def country_features(country_id: str, year: int, db: Session = Depends(get_db)) -> Dict:
    country_id = country_id.upper()
    if country_id not in COUNTRY_UNIVERSE:
        raise HTTPException(status_code=404, detail="Country not supported")

    features = (
        db.query(CountryYearFeatures)
        .filter(
            CountryYearFeatures.country_id == country_id,
            CountryYearFeatures.year == year,
        )
        .first()
    )
    if not features:
        raise HTTPException(status_code=404, detail="Features not found")

    feature_payload = {
        "gdp_real": _to_float(features.gdp_real),
        "gdp_growth": _to_float(features.gdp_growth),
        "inflation_cpi": _to_float(features.inflation_cpi),
        "ca_pct_gdp": _to_float(features.ca_pct_gdp),
        "debt_pct_gdp": _to_float(features.debt_pct_gdp),
        "unemployment_rate": _to_float(features.unemployment_rate),
        "co2_per_capita": _to_float(features.co2_per_capita),
        "energy_import_dep": _to_float(features.energy_import_dep),
        "food_import_dep": _to_float(features.food_import_dep),
        "shipping_activity_level": _to_float(features.shipping_activity_level),
        "shipping_activity_change": _to_float(features.shipping_activity_change),
        "event_stress_pulse": _to_float(features.event_stress_pulse),
        "data_coverage_score": _to_float(features.data_coverage_score),
        "data_freshness_score": _to_float(features.data_freshness_score),
    }

    return {"country_id": country_id, "year": year, "features": feature_payload}


def _to_float(value):
    return float(value) if value is not None else None


features_router = router
