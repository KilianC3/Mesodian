from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api import (
    dashboard_router,
    features_router,
    health_router,
    metrics_router,
    reference_router,
    timeseries_router,
    webs_router,
)
from app.config import get_settings


settings = get_settings()
app = FastAPI(title=settings.app_name, debug=settings.debug)

origins = [
    "http://localhost:3000",
    "https://localhost:3000",
    "http://127.0.0.1:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/")
def read_root():
    return {"app_name": settings.app_name, "environment": settings.env}


app.include_router(reference_router, prefix="/api")
app.include_router(timeseries_router, prefix="/api")
app.include_router(features_router, prefix="/api")
app.include_router(metrics_router, prefix="/api")
app.include_router(webs_router, prefix="/api")
app.include_router(dashboard_router, prefix="/api")
app.include_router(health_router)
