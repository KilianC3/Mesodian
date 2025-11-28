from fastapi import Depends, FastAPI, HTTPException
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.config import get_settings
from app.db.engine import get_db


settings = get_settings()
app = FastAPI(title=settings.app_name, debug=settings.debug)


@app.get("/")
def read_root():
    return {"app_name": settings.app_name, "environment": settings.env}


@app.get("/health")
def health_check(db: Session = Depends(get_db)):
    try:
        db.execute(text("SELECT 1"))
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail=f"Database unavailable: {exc}") from exc
    return {"status": "ok"}
