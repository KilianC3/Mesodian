from fastapi import FastAPI

from app.config import get_settings
from app.api.health import router as health_router


settings = get_settings()
app = FastAPI(title=settings.app_name, debug=settings.debug)


@app.get("/")
def read_root():
    return {"app_name": settings.app_name, "environment": settings.env}


app.include_router(health_router)
