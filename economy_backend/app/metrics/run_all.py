from __future__ import annotations

import argparse

from sqlalchemy.orm import Session

from app.graph.algorithms import compute_trade_centrality
from app.metrics.composite_climate_total_risk import compute_climate_total_risk_for_year
from app.metrics.composite_household_housing import compute_household_housing_composite_for_year
from app.metrics.country_resilience import compute_country_resilience_for_year
from app.metrics.credit_excess import compute_credit_excess_for_year
from app.metrics.critical_import_dependence import compute_critical_import_dependence_for_year
from app.metrics.data_quality_metrics import write_data_quality_metrics_for_year
from app.metrics.housing_stress import compute_housing_stress_for_year
from app.metrics.household_stress import compute_household_stress_for_year
from app.metrics.inflation_pressure import compute_inflation_pressure_for_year
from app.metrics.physical_climate_water import (
    compute_physical_climate_water_stress_for_year,
)
from app.metrics.policy_stance import compute_policy_stance_for_year
from app.metrics.recession_indicator import compute_recession_indicator_for_year
from app.metrics.risk_energy import compute_energy_risk_for_year
from app.metrics.risk_food import compute_food_risk_for_year
from app.metrics.sovereign_esg import compute_sovereign_esg_for_year
from app.metrics.transition_risk import compute_transition_risk_for_year


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute all country metrics")
    parser.add_argument("--year", type=int, required=True, help="Target year")
    return parser.parse_args()


def compute_all_country_metrics(session: Session, year: int) -> None:
    compute_country_resilience_for_year(session, year)
    compute_food_risk_for_year(session, year)
    compute_energy_risk_for_year(session, year)
    compute_transition_risk_for_year(session, year)
    compute_housing_stress_for_year(session, year)
    compute_household_stress_for_year(session, year)
    compute_physical_climate_water_stress_for_year(session, year)
    compute_critical_import_dependence_for_year(session, year)
    compute_policy_stance_for_year(session, year)
    compute_recession_indicator_for_year(session, year)
    compute_inflation_pressure_for_year(session, year)
    compute_credit_excess_for_year(session, year)
    write_data_quality_metrics_for_year(session, year)
    compute_trade_centrality(session, year)
    compute_household_housing_composite_for_year(session, year)
    compute_climate_total_risk_for_year(session, year)
    compute_sovereign_esg_for_year(session, year)


if __name__ == "__main__":  # pragma: no cover - CLI entry
    import argparse

    from app.db.engine import SessionLocal

    args = _parse_args()
    session = SessionLocal()
    try:
        compute_all_country_metrics(session, args.year)
        session.commit()
        print(f"Computed all country metrics for {args.year}")
    finally:
        session.close()
