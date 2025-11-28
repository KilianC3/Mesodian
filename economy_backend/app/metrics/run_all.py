from __future__ import annotations

from sqlalchemy.orm import Session

from app.metrics.country_resilience import compute_country_resilience_for_year
from app.metrics.data_quality_metrics import write_data_quality_metrics_for_year
from app.metrics.risk_energy import compute_energy_risk_for_year
from app.metrics.risk_food import compute_food_risk_for_year
from app.metrics.transition_risk import compute_transition_risk_for_year


def compute_all_country_metrics(session: Session, year: int) -> None:
    compute_country_resilience_for_year(session, year)
    compute_food_risk_for_year(session, year)
    compute_energy_risk_for_year(session, year)
    compute_transition_risk_for_year(session, year)
    write_data_quality_metrics_for_year(session, year)
