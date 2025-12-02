"""CLI entry for computing regional cycles."""
from __future__ import annotations

import argparse

from app.db.engine import SessionLocal
from app.cycles.regional_cycles import compute_all_regional_cycles


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute regional cycles")
    parser.add_argument("--year", type=int, default=2023)
    args = parser.parse_args()

    with SessionLocal() as session:
        compute_all_regional_cycles(session, args.year)
        session.commit()
        print(f"Computed regional cycles for {args.year}")


if __name__ == "__main__":  # pragma: no cover - CLI helper
    main()
