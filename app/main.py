"""
ASR Orchestrator Service
=========================
The pipeline brain. Manages job lifecycle, dispatches tasks
to workers via Redis queues, and listens for completions.

Run:
  uvicorn app.main:app --host 0.0.0.0 --port 8001 --reload
"""
import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.Config import config
from app.Database import init_db, close_db
from app.Redis_client import init_redis, close_redis
from app.Event_listener import event_listener
from app.Routes import router


listener_task = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global listener_task

    # Startup
    print("Starting Orchestrator...")
    await init_db()
    print("  PostgreSQL connected")
    await init_redis()
    print("  Redis connected")

    # Start the event listener in the background
    listener_task = asyncio.create_task(event_listener())
    print("  Event listener started")
    print("Orchestrator ready.")

    yield

    # Shutdown
    print("Shutting down Orchestrator...")
    if listener_task:
        listener_task.cancel()
        try:
            await listener_task
        except asyncio.CancelledError:
            pass
    await close_db()
    await close_redis()
    print("Orchestrator stopped.")


app = FastAPI(
    title="ASR Orchestrator",
    description="Arabic ASR Pipeline Orchestrator",
    version="1.0.0",
    lifespan=lifespan,
)


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"},
    )


# Include routes
app.include_router(router)


# Health check
@app.get("/health")
async def health():
    return {"status": "ok", "service": "orchestrator"}


@app.get("/")
async def root():
    return {
        "service": "ASR Orchestrator",
        "version": "1.0.0",
        "docs": "/docs",
    }