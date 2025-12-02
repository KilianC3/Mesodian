"""Physical climate and water stress metric."""
from __future__ import annotations

from sqlalchemy.orm import Session

from app.db.models import CountryYearFeatures
from app.metrics.utils import get_or_create_country_node, upsert_node_metric


def _clamp(value: float) -> float:
    return max(0.0, min(100.0, value))


def compute_physical_climate_water_stress_for_year(session: Session, year: int) -> None:
    rows = (
        session.query(CountryYearFeatures)
        .filter(CountryYearFeatures.year == year)
        .all()
    )
    for row in rows:
        node = get_or_create_country_node(session, row.country_id)
        emissions = float(row.co2_per_capita or 0.0)
        energy_dep = float(row.energy_import_dep or 0.0)
        stress = 0.7 * emissions + 0.3 * (energy_dep / 2)
        upsert_node_metric(session, node.id, "PHYSICAL_CLIMATE_WATER_STRESS", year, _clamp(stress))
    session.commit()
