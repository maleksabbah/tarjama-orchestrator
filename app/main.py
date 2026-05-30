"""
Orchestrator entrypoint.
- Wires the FastAPI app
- Starts the Kafka producer + DB pool on startup
- Spawns the completion consumer loop as a background task
- Cleans everything up on shutdown
"""
import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.Config.Config import config
from app.Config.Database import SessionLocal, close_db
from app.Config.Redis import get_redis, close_redis
from app.Config.Kafka import get_producer, close_producer, make_consumer
from app.Repositories import (
    JobRepository,
    TaskRepository,
    ProgressRepository,
    EventPublisher,
    EventConsumer,
)
from app.Services.PipelineService import PipelineService
from app.ExceptionHandler import register_exception_handlers
from app.Routes.JobRoutes import router as jobs_router


# ─── Consumer loop ──────────────────────────────────────────────────────

async def run_completion_consumer() -> None:
    """
    Reads completion events off Kafka and drives PipelineService.
    Builds a fresh DB session + repos per message so each handler runs in
    its own transaction. Commits the Kafka offset only after both the DB
    and the handler succeed — so a crash mid-handle replays the message.
    """
    consumer = EventConsumer(
        make_consumer(
            topics=[config.TOPIC_COMPLETED],
            group_id=config.GROUP_ORCHESTRATOR,
        )
    )
    producer = await get_producer()
    redis_client = get_redis()
    events = EventPublisher(producer)

    await consumer.start()
    print(f"  [MAIN] Consumer started on topic '{config.TOPIC_COMPLETED}'")

    try:
        async for message in consumer.messages():
            session = SessionLocal()
            try:
                jobs = JobRepository(session)
                tasks = TaskRepository(session)
                progress = ProgressRepository(redis_client)
                pipeline = PipelineService(
                    jobs=jobs, tasks=tasks, progress=progress, events=events,
                )

                await pipeline.handle_completion(message)
                await session.commit()
                await consumer.commit()
            except Exception as e:
                await session.rollback()
                print(f"  [MAIN] Handler error, message will be redelivered: {e}")
                # don't commit Kafka offset — Kafka will redeliver on next poll
            finally:
                await session.close()
    except asyncio.CancelledError:
        print("  [MAIN] Consumer loop cancelled")
        raise
    finally:
        await consumer.stop()
        print("  [MAIN] Consumer stopped")


# ─── Lifespan ───────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: warm the producer pool, then spawn the consumer loop.
    await get_producer()
    consumer_task = asyncio.create_task(run_completion_consumer())
    print("  [MAIN] Orchestrator started")

    try:
        yield
    finally:
        # Shutdown: cancel the consumer, then close everything else.
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass

        await close_producer()
        await close_redis()
        await close_db()
        print("  [MAIN] Orchestrator stopped")


# ─── App ────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Tarjama Orchestrator",
    version="2.0.0",
    lifespan=lifespan,
)

register_exception_handlers(app)
app.include_router(jobs_router)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}