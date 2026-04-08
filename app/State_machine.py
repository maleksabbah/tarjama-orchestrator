"""
Orchestrator State Machine
==========================
Pipeline logic: decides what happens after each step.

Job lifecycle:
  queued → extracting → transcribing → subtitling → completed
               ↓              ↓             ↓
             failed         failed        failed
"""
from app.Config import config
from app import Database as db
from app import Redis_client as rc


async def handle_completion(message: dict):
    """Route a completed task to the right handler."""
    msg_type = message.get("type")
    status = message.get("status")
    job_id = message.get("job_id")
    task_id = message.get("task_id")

    if not all([msg_type, status, job_id, task_id]):
        print(f"  [STATE] Invalid message: {message}")
        return

    if status == "failed":
        await handle_failure(message)
        return

    if msg_type == "media":
        await handle_media_completed(message)
    elif msg_type == "transcribe":
        await handle_transcribe_completed(message)
    elif msg_type == "subtitle":
        await handle_subtitle_completed(message)
    else:
        print(f"  [STATE] Unknown task type: {msg_type}")


# =============================================================================
# Media extraction completed
# =============================================================================

async def handle_media_completed(message: dict):
    """Media done → create transcription tasks, push to queue."""
    job_id = message["job_id"]
    task_id = message["task_id"]
    chunks = message.get("chunks", [])
    total_chunks = message.get("total_chunks", len(chunks))

    print(f"  [STATE] Media done for job {job_id}: {total_chunks} chunks")

    # Update the media task
    await db.update_task_status(task_id, "completed")

    # Update job status
    job = await db.get_job(job_id)
    if not job or job["status"] == "cancelled":
        return

    await db.update_job_status(job_id, "transcribing")

    # Set progress
    await rc.set_progress(job_id, {
        "status": "transcribing",
        "total_chunks": total_chunks,
        "completed_chunks": 0,
        "failed_chunks": 0,
    })

    # Create transcription tasks and push to queue
    for i, chunk_path in enumerate(chunks):
        task = await db.create_task(
            job_id=job_id,
            task_type="transcribe",
            input_path=chunk_path,
            chunk_index=i,
        )
        await rc.push_transcribe_task(
            task_id=task["id"],
            job_id=job_id,
            chunk_path=chunk_path,
            dialect=job.get("dialect", "auto"),
            chunk_index=i,
        )

    print(f"  [STATE] Pushed {total_chunks} transcription tasks for job {job_id}")


# =============================================================================
# Transcription chunk completed
# =============================================================================

async def handle_transcribe_completed(message: dict):
    """One chunk transcribed → update progress, check if all done."""
    job_id = message["job_id"]
    task_id = message["task_id"]
    output_path = message.get("output")

    # Update the task
    await db.update_task_status(task_id, "completed", output_path=output_path)

    # Increment progress
    await rc.increment_progress(job_id, "completed_chunks")

    # Check if all chunks are done
    counts = await db.count_tasks_by_status(job_id)
    total_transcribe = sum(
        v for k, v in counts.items()
        if k != "pending" or True
    )

    # Only count transcription tasks
    tasks = await db.get_tasks_by_job(job_id)
    transcribe_tasks = [t for t in tasks if t["type"] == "transcribe"]
    completed = sum(1 for t in transcribe_tasks if t["status"] == "completed")
    failed = sum(1 for t in transcribe_tasks if t["status"] == "failed")
    total = len(transcribe_tasks)

    print(f"  [STATE] Transcription {completed}/{total} for job {job_id}")

    if completed + failed >= total:
        if failed > 0 and completed == 0:
            # All failed
            await db.update_job_status(job_id, "failed",
                                       error=f"All {failed} transcription tasks failed")
            await rc.delete_progress(job_id)
        else:
            # All done (or some failed but we have results) → move to subtitling
            await start_subtitling(job_id)


async def start_subtitling(job_id: str):
    """Create subtitle task and push to queue."""
    job = await db.get_job(job_id)
    if not job or job["status"] == "cancelled":
        return

    await db.update_job_status(job_id, "subtitling")
    await rc.update_progress(job_id, "status", "subtitling")

    # Determine results directory from job_id
    results_dir = f"results/{job_id}/"

    task = await db.create_task(
        job_id=job_id,
        task_type="subtitle",
        input_path=results_dir,
    )

   await rc.push_subtitle_task(
        task_id=task["id"],
        job_id=job_id,
        results_dir=results_dir,
        original_video=job["input_file_path"],
        subtitle_format=job.get("subtitle_format", "srt"),
        burn=job.get("burn_subtitles", False),
        user_id=job["user_id"],
    )

    print(f"  [STATE] Pushed subtitle task for job {job_id}")


# =============================================================================
# Subtitle generation completed
# =============================================================================

async def handle_subtitle_completed(message: dict):
    """Subtitles done → mark job completed."""
    job_id = message["job_id"]
    task_id = message["task_id"]
    outputs = message.get("outputs", {})

    print(f"  [STATE] Subtitles done for job {job_id}")

    # Update the task
    await db.update_task_status(task_id, "completed")

    # Update job with output paths
    update_fields = {}
    if "transcript" in outputs:
        update_fields["transcript_path"] = outputs["transcript"]
    if "srt" in outputs or "vtt" in outputs:
        update_fields["subtitle_path"] = outputs.get("srt", outputs.get("vtt"))
    if "video" in outputs:
        update_fields["video_output_path"] = outputs["video"]

    await db.update_job_status(job_id, "completed", **update_fields)
    await rc.delete_progress(job_id)

    print(f"  [STATE] Job {job_id} COMPLETED")


# =============================================================================
# Failure handling
# =============================================================================

async def handle_failure(message: dict):
    """Handle a failed task — retry or mark job as failed."""
    job_id = message["job_id"]
    task_id = message["task_id"]
    msg_type = message.get("type")
    error = message.get("error", "Unknown error")

    print(f"  [STATE] Task {task_id} failed: {error}")

    # Get current task to check retries
    tasks = await db.get_tasks_by_job(job_id)
    task = next((t for t in tasks if t["id"] == task_id), None)

    if not task:
        return

    retries = task.get("retries", 0)

    if retries < config.MAX_RETRIES:
        # Retry the task
        await db.update_task_status(task_id, "pending", retries=retries + 1)
        print(f"  [STATE] Retrying task {task_id} (attempt {retries + 2})")

        if msg_type == "media":
            job = await db.get_job(job_id)
            await rc.push_media_task(task_id, job_id, job["input_file_path"])
        elif msg_type == "transcribe":
            await rc.push_transcribe_task(
                task_id, job_id, task["input_path"],
                chunk_index=task.get("chunk_index", 0),
            )
        elif msg_type == "subtitle":
            job = await db.get_job(job_id)
            await rc.push_subtitle_task(
                task_id, job_id, task["input_path"],
                original_video=job["input_file_path"],
            )
    else:
        # Max retries exceeded
        await db.update_task_status(task_id, "failed", error=error)

        if msg_type == "transcribe":
            # For transcription, increment failed counter and check if all done
            await rc.increment_progress(job_id, "failed_chunks")
            # Re-check completion
            tasks = await db.get_tasks_by_job(job_id)
            transcribe_tasks = [t for t in tasks if t["type"] == "transcribe"]
            completed = sum(1 for t in transcribe_tasks if t["status"] == "completed")
            failed = sum(1 for t in transcribe_tasks if t["status"] == "failed")
            total = len(transcribe_tasks)

            if completed + failed >= total:
                if completed > 0:
                    await start_subtitling(job_id)
                else:
                    await db.update_job_status(job_id, "failed", error="All chunks failed")
                    await rc.delete_progress(job_id)
        else:
            # Media or subtitle failure → whole job fails
            await db.update_job_status(job_id, "failed", error=error)
            await rc.delete_progress(job_id)