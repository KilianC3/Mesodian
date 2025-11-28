from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.db.engine import db_session, get_db
from app.ingest.jobs import ingest_all_health_check


router = APIRouter()


@router.get("/health")
def health_check(db: Session = Depends(get_db)):
    return {"status": "ok"}


@router.get("/health/data-sources")
def data_source_health():
    with db_session() as session:
        return ingest_all_health_check(session)
