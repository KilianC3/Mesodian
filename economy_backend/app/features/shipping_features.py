from __future__ import annotations

from typing import Dict, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.db.models import ShippingCountryMonth


def get_shipping_features_for_country(session: Session, country_id: str, year: int) -> Dict[str, Optional[float]]:
    """Compute shipping activity level and year-on-year change for a country."""

    activity_sum = (
        session.query(func.sum(ShippingCountryMonth.activity_level))
        .filter(ShippingCountryMonth.country_id == country_id, ShippingCountryMonth.year == year)
        .scalar()
    )
    activity_value: Optional[float] = float(activity_sum) if activity_sum is not None else None

    prev_sum = (
        session.query(func.sum(ShippingCountryMonth.activity_level))
        .filter(
            ShippingCountryMonth.country_id == country_id,
            ShippingCountryMonth.year == year - 1,
        )
        .scalar()
    )
    prev_value: Optional[float] = float(prev_sum) if prev_sum is not None else None

    change: Optional[float]
    if activity_value is None or prev_value is None or prev_value == 0:
        change = None
    else:
        change = ((activity_value - prev_value) / prev_value) * 100.0

    return {
        "shipping_activity_level": activity_value,
        "shipping_activity_change": change,
    }
