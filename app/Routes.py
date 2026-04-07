"""
Orchestrator Routes
API endpoints called by the Gateway via HTTP.
"""
from fastapi import APIRouter, HTTPException, Request

from app import Database as db
from app import Redis_client as rc
from app.Config import config
from app.Schemas import CreateJobRequest, JobResponse, ProgressResponse

router = APIRouter()


def get_user_id(request: Request) -> int:
    """Extract user_id from X-User-ID header (set by Gateway)."""
    user_id = request.headers.get("X-User-ID")
    if not user_id:
        raise HTTPException(status_code=401, detail="Missing X-User-ID header")
    return int(user_id)


# =============================================================================
# Job endpoints
# =============================================================================

@router.post("/jobs")
async def create_job(req: CreateJobRequest) -> JobResponse:
    """Create a new transcription job and start the pipeline."""
    # Create job in database
    job = await db.create_job(
        user_id=req.user_id,
        input_file_path=req.file_path,
        dialect=req.dialect,
        output_type=req.output_type,
        subtitle_format=req.subtitle_format,
        burn_subtitles=req.burn_subtitles,
    )
    # Create the first task: media extraction

    task = await db.create_task(
        job_id = job["id"],
        task_type = "extract",
        input_path = req.file_path,
    )

    # Update job status
    await db.update_job_status(job["id"],"extracting")

    # Push to media queue
    await rc.push_media_task(
        task_id = task["id"],
        job_id = job["id"],
        input_path = req.file_path,
    )

    # Set initial progress
    await rc.set_progress(job["id"], {
        "status":"extracting",
        "total_chunks":0,
        "completed_chunks":0,
        "failed_chunks":0,

    })
    print(f"  [API] Created job {job['id']} for user {req.user_id}")
    return job


@router.get("/jobs")
async def list_jobs(request: Request, user_id: int = None,
                    limit: int = 50, offset: int = 0):
    """List jobs for a user."""
    uid = user_id or get_user_id(request)
    jobs = await db.get_jobs_by_user(uid, limit=limit, offset=offset)
    total = await db.count_user_jobs(uid)
    return {"jobs": jobs, "total": total}

@router.get("/jobs/{job_id}")
async def get_job(job_id:str,request:Request):
    """Get a job by ID."""
    job = await db.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # Verify ownership
    user_id = get_user_id(request)
    if job["user_id"] != user_id:
        raise HTTPException(status_code=403, detail="Not your job")
    return job
@router.get("/jobs/{job_id}/progress")
async def get_job_progress(job_id:str,request:Request):
    """Get real time progress for a job."""
    job = await db.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    user_id = get_user_id(request)
    if job["user_id"] != user_id:
        raise HTTPException(status_code=403, detail="Not your job")

    # Get progress from Redis
    progress = await rc.get_progress(job_id)

    if progress:
        return {
            "job_id": job_id,
            "status": progress.get("status", job["status"]),
            "total_chunks": int(progress.get("total_chunks", 0)),
            "completed_chunks": int(progress.get("completed_chunks", 0)),
            "failed_chunks": int(progress.get("failed_chunks", 0)),
        }
    # No progress in Redis, return from database
    return {
        "job_id": job_id,
        "status": job["status"],
        "total_chunks": 0,
        "completed_chunks": 0,
        "failed_chunks": 0,
    }

@router.get("/jobs/{job_id}/tasks")
async def get_tasks(job_id: str, request: Request):
    """Get all tasks for a job."""
    job = await db.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    user_id = get_user_id(request)
    if job["user_id"] != user_id:
        raise HTTPException(status_code=403, detail="Not your job")

    tasks = await db.get_tasks_by_job(job_id)
    return {
        "tasks": tasks, "total": len(tasks),
    }

@router.post("/jobs/{job_id}/cancel")
async def cancel_job(job_id: str, request: Request):
    """Cancel a job - remove pending tasks from queue"""
    job = await db.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    user_id = get_user_id(request)
    if job["user_id"] != user_id:
        raise HTTPException(status_code=403, detail="Not your job")
    if job["status"] in ("completed", "failed", "cancelled"):
        raise HTTPException(status_code=400, detail=f"Job already {job['status']}")

    # Remove pending tasks from all queues
    removed = 0
    removed += await rc.remove_pending_tasks(job_id, config.QUEUE_MEDIA)
    removed += await rc.remove_pending_tasks(job_id, config.QUEUE_TRANSCRIBE)
    removed += await rc.remove_pending_tasks(job_id, config.QUEUE_SUBTITLE)

    # Update job and tasks
    await db.update_job_status(job_id, "cancelled")
    await rc.delete_progress(job_id)

    print(f"  [API] Cancelled job {job_id}, removed {removed} pending tasks")
    return {"status": "cancelled", "removed_tasks": removed}









