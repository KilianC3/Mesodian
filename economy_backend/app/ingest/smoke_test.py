def main() -> None:
    from app.db.engine import db_session
    from app.ingest.jobs import ingest_all_health_check

    with db_session() as session:
        statuses = ingest_all_health_check(session)

    for provider, state in statuses.items():
        status_text = "ok" if state.get("ok") else f"error: {state.get('error')}"
        print(f"{provider}: {status_text}")


if __name__ == "__main__":
    main()
