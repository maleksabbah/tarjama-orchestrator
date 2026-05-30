# app/Routes/__init__.py
from app.Routes.JobRoutes import router as job_router
from app.Routes.WsRoutes import router as ws_router
__all__ = ["job_router", "ws_router"]