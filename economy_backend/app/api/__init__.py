"""API routers for the application."""

from app.api.dashboard import router as dashboard_router
from app.api.features import router as features_router
from app.api.health import router as health_router
from app.api.metrics import router as metrics_router
from app.api.reference import router as reference_router
from app.api.time_series import router as timeseries_router
from app.api.webs import router as webs_router

__all__ = [
    "dashboard_router",
    "features_router",
    "health_router",
    "metrics_router",
    "reference_router",
    "timeseries_router",
    "webs_router",
]

