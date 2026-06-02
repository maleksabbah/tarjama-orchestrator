"""
HTTP controllers. Thin layer — converts HTTP <-> DTOs and calls JobService.
"""
from fastapi import APIRouter, Query

from app.Config.Database import get_session
from app.Config.Redis import get_redis
from app.Config.Kafka import get_producer
from app.Dtos import (
    CreateJobRequest,
    JobResponse,
    JobListResponse,
    JobProgressResponse,
)
from app.Repositories import (
    JobRepository,
    TaskRepository,
    ProgressRepository,
    EventPublisher,
)
from app.Services.JobService import JobService
from app.Services.GPUWaker import wake_gpu
import asyncio


router = APIRouter(prefix="/jobs", tags=["jobs"])


async def _service() -> JobService:
    """Assemble a JobService inside an open session."""
    async with get_session() as session:
        producer = await get_producer()
        return JobService(
            jobs=JobRepository(session),
            tasks=TaskRepository(session),
            progress=ProgressRepository(get_redis()),
            events=EventPublisher(producer),
        )


@router.post("", response_model=JobResponse, status_code=201)
async def create_job(body: CreateJobRequest) -> JobResponse:
    async with get_session() as session:
        producer = await get_producer()
        service = JobService(
            jobs=JobRepository(session),
            tasks=TaskRepository(session),
            progress=ProgressRepository(get_redis()),
            events=EventPublisher(producer),
        )
        job = await service.create_job(
            user_id=body.user_id,
            file_path=body.file_path,
            dialect=body.dialect or "auto",
            output_type=body.output_type or "all",
            subtitle_format=body.subtitle_format or "srt",
            burn_subtitles=bool(body.burn_subtitles),
        )
        # Best-effort: wake the GPU transcription box. Task already queued in
        # Kafka; the worker drains it once it boots. Never blocks job creation.
        asyncio.create_task(wake_gpu())
        return JobResponse.model_validate(job)


@router.get("/{job_id}", response_model=JobResponse)
async def get_job(job_id: str, user_id: int = Query(...)) -> JobResponse:
    async with get_session() as session:
        producer = await get_producer()
        service = JobService(
            jobs=JobRepository(session),
            tasks=TaskRepository(session),
            progress=ProgressRepository(get_redis()),
            events=EventPublisher(producer),
        )
        job = await service.get_job(job_id, user_id)
        return JobResponse.model_validate(job)


@router.get("", response_model=JobListResponse)
async def list_jobs(
    user_id: int = Query(...),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> JobListResponse:
    async with get_session() as session:
        producer = await get_producer()
        service = JobService(
            jobs=JobRepository(session),
            tasks=TaskRepository(session),
            progress=ProgressRepository(get_redis()),
            events=EventPublisher(producer),
        )
        jobs, total = await service.list_jobs(user_id, limit=limit, offset=offset)
        return JobListResponse(
            jobs=[JobResponse.model_validate(j) for j in jobs],
            total=total,
        )


@router.get("/{job_id}/progress", response_model=JobProgressResponse)
async def get_progress(job_id: str, user_id: int = Query(...)) -> JobProgressResponse:
    async with get_session() as session:
        producer = await get_producer()
        service = JobService(
            jobs=JobRepository(session),
            tasks=TaskRepository(session),
            progress=ProgressRepository(get_redis()),
            events=EventPublisher(producer),
        )
        progress = await service.get_progress(job_id, user_id)
        return JobProgressResponse(**progress)


@router.post("/{job_id}/cancel")
async def cancel_job(job_id: str, user_id: int = Query(...)) -> dict:
    async with get_session() as session:
        producer = await get_producer()
        service = JobService(
            jobs=JobRepository(session),
            tasks=TaskRepository(session),
            progress=ProgressRepository(get_redis()),
            events=EventPublisher(producer),
        )
        return await service.cancel_job(job_id, user_id)
