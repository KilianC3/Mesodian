"""Critical import dependence metric using food and energy import shares."""
from __future__ import annotations

from sqlalchemy.orm import Session

from app.db.models import CountryYearFeatures
from app.metrics.utils import get_or_create_country_node, upsert_node_metric


def compute_critical_import_dependence_for_year(session: Session, year: int) -> None:
    rows = (
        session.query(CountryYearFeatures)
        .filter(CountryYearFeatures.year == year)
        .all()
    )
    for row in rows:
        node = get_or_create_country_node(session, row.country_id)
        energy_dep = float(row.energy_import_dep or 0.0)
        food_dep = float(row.food_import_dep or 0.0)
        score = min(100.0, max(0.0, 0.6 * energy_dep + 0.4 * food_dep))
        upsert_node_metric(session, node.id, "CRITICAL_IMPORT_DEPENDENCE", year, score)
    session.commit()
