"""
Job lifecycle service.
Handles user-facing operations: create, get, list, cancel, progress.
Receives pre-built repositories — does not know about Postgres, Redis, or Kafka.
"""
from app.Entities import Job
from app.Repositories import (
    JobRepository,
    TaskRepository,
    ProgressRepository,
    EventPublisher,
)
from app.ExceptionHandler import JobNotFound, JobForbidden, JobAlreadyTerminal


class JobService:
    def __init__(
        self,
        jobs: JobRepository,
        tasks: TaskRepository,
        progress: ProgressRepository,
        events: EventPublisher,
    ):
        self.jobs = jobs
        self.tasks = tasks
        self.progress = progress
        self.events = events

    async def create_job(
        self,
        user_id: int,
        file_path: str,
        dialect: str = "auto",
        output_type: str = "all",
        subtitle_format: str = "srt",
        burn_subtitles: bool = False,
    ) -> Job:
        job = await self.jobs.create(
            user_id=user_id,
            input_file_path=file_path,
            dialect=dialect,
            output_type=output_type,
            subtitle_format=subtitle_format,
            burn_subtitles=burn_subtitles,
        )

        task = await self.tasks.create(
            job_id=job.id,
            task_type="extract",
            input_path=file_path,
        )

        await self.jobs.update_status(job.id, "extracting")

        await self.events.publish_media_task(
            task_id=task.id,
            job_id=job.id,
            input_path=file_path,
        )

        await self.progress.set(job.id, {
            "status": "extracting",
            "total_chunks": 0,
            "completed_chunks": 0,
            "failed_chunks": 0,
        })

        print(f"  [JOB] Created job {job.id} for user {user_id}")
        return job

    async def get_job(self, job_id: str, user_id: int) -> Job:
        job = await self.jobs.get(job_id)
        if not job:
            raise JobNotFound(job_id)
        if job.user_id != user_id:
            raise JobForbidden(job_id)
        return job

    async def list_jobs(
        self, user_id: int, limit: int = 50, offset: int = 0
    ) -> tuple[list[Job], int]:
        jobs = await self.jobs.list_by_user(user_id, limit=limit, offset=offset)
        total = await self.jobs.count_by_user(user_id)
        return list(jobs), total

    async def get_progress(self, job_id: str, user_id: int) -> dict:
        job = await self.get_job(job_id, user_id)

        live = await self.progress.get(job_id)
        if live:
            return {
                "job_id": job_id,
                "status": live.get("status", job.status),
                "total_chunks": int(live.get("total_chunks", 0)),
                "completed_chunks": int(live.get("completed_chunks", 0)),
                "failed_chunks": int(live.get("failed_chunks", 0)),
            }
        return {
            "job_id": job_id,
            "status": job.status,
            "total_chunks": 0,
            "completed_chunks": 0,
            "failed_chunks": 0,
        }

    async def cancel_job(self, job_id: str, user_id: int) -> dict:
        job = await self.get_job(job_id, user_id)

        if job.status in ("completed", "failed", "cancelled"):
            raise JobAlreadyTerminal(job_id, job.status)

        await self.jobs.update_status(job_id, "cancelled")
        await self.progress.delete(job_id)

        print(f"  [JOB] Cancelled job {job_id}")
        return {"status": "cancelled"}



















