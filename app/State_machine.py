"""
Orchestrator State Machine
Routes completed task messages and advances the pipeline.
Updated: media now produces one full audio file, not chunks.
Transcription is one task per job (not per chunk).
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


async def handle_media_completed(message: dict):
    job_id = message["job_id"]
    task_id = message["task_id"]
    audio_path = message.get("audio_path")
    video_meta_path = message.get("video_meta_path")
    duration = message.get("duration", 0)
    fps = message.get("fps", 0)

    print(f"  [STATE] Media done for job {job_id}: {duration:.1f}s @ {fps:.3f}fps")

    await db.update_task_status(task_id, "completed")

    job = await db.get_job(job_id)
    if not job or job["status"] == "cancelled":
        return

    await db.update_job_status(job_id, "transcribing")
    await rc.set_progress(job_id, {
        "status": "transcribing",
        "total_chunks": 1,
        "completed_chunks": 0,
        "failed_chunks": 0,
    })

    # One transcription task for the full audio
    task = await db.create_task(
        job_id=job_id,
        task_type="transcribe",
        input_path=audio_path,
    )

    await rc.push_transcribe_task(
        task_id=task["id"],
        job_id=job_id,
        audio_path=audio_path,
        video_meta_path=video_meta_path,
        dialect=job.get("dialect", "auto"),
    )

    print(f"  [STATE] Pushed transcription task for job {job_id}")


async def handle_transcribe_completed(message: dict):
    job_id = message["job_id"]
    task_id = message["task_id"]
    output_path = message.get("output")

    await db.update_task_status(task_id, "completed", output_path=output_path)
    await rc.increment_progress(job_id, "completed_chunks")

    print(f"  [STATE] Transcription done for job {job_id}")
    await start_subtitling(job_id)


async def start_subtitling(job_id: str):
    job = await db.get_job(job_id)
    if not job or job["status"] == "cancelled":
        return

    await db.update_job_status(job_id, "subtitling")
    await rc.update_progress(job_id, "status", "subtitling")

    task = await db.create_task(
        job_id=job_id,
        task_type="subtitle",
        input_path=f"results/{job_id}/",
    )

    await rc.push_subtitle_task(
        task_id=task["id"],
        job_id=job_id,
        results_dir=f"results/{job_id}/",
        original_video=job["input_file_path"],
        subtitle_format=job.get("subtitle_format", "srt"),
        burn=job.get("burn_subtitles", False),
        user_id=job["user_id"],
    )

    print(f"  [STATE] Pushed subtitle task for job {job_id}")


async def handle_subtitle_completed(message: dict):
    job_id = message["job_id"]
    task_id = message["task_id"]
    outputs = message.get("outputs", {})

    print(f"  [STATE] Subtitles done for job {job_id}")

    await db.update_task_status(task_id, "completed")

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


async def handle_failure(message: dict):
    job_id = message["job_id"]
    task_id = message["task_id"]
    msg_type = message.get("type")
    error = message.get("error", "Unknown error")

    print(f"  [STATE] Task {task_id} failed: {error}")

    tasks = await db.get_tasks_by_job(job_id)
    task = next((t for t in tasks if t["id"] == task_id), None)
    if not task:
        return

    retries = task.get("retries", 0)

    if retries < config.MAX_RETRIES:
        await db.update_task_status(task_id, "pending", retries=retries + 1)
        print(f"  [STATE] Retrying task {task_id} (attempt {retries + 2})")

        if msg_type == "media":
            job = await db.get_job(job_id)
            await rc.push_media_task(task_id, job_id, job["input_file_path"])
        elif msg_type == "transcribe":
            job = await db.get_job(job_id)
            await rc.push_transcribe_task(
                task_id, job_id,
                audio_path=task["input_path"],
                dialect=job.get("dialect", "auto"),
            )
        elif msg_type == "subtitle":
            job = await db.get_job(job_id)
            await rc.push_subtitle_task(
                task_id, job_id, task["input_path"],
                original_video=job["input_file_path"],
                user_id=job["user_id"],
            )
    else:
        await db.update_task_status(task_id, "failed", error=error)
        await db.update_job_status(job_id, "failed", error=error)
        await rc.delete_progress(job_id)
