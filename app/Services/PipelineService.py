"""
Pipeline coordination service.
Consumes worker completion events and advances jobs through stages:
  extract → transcribe → subtitle (→ burn) → completed

Pure DI: receives pre-built repositories, no session/redis/kafka leaks.
Raises nothing user-facing — this runs inside the consumer loop, not HTTP.
"""
from typing import Any

from app.Config.Config import config
from app.Entities import Task
from app.Repositories import (
    JobRepository,
    TaskRepository,
    ProgressRepository,
    EventPublisher,
)


# Terminal job statuses — once a job is here, ignore further worker events.
TERMINAL_STATUSES = {"completed", "failed", "cancelled"}


class PipelineService:
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

    # ─── Entry point ────────────────────────────────────────────────────

    async def handle_completion(self, message: dict) -> None:
        """
        Single entry point for the Kafka consumer loop.
        Routes by message type and status.
        """
        msg_type = message.get("type")
        status = message.get("status")
        job_id = message.get("job_id")
        task_id = message.get("task_id")

        if not all([msg_type, status, job_id, task_id]):
            print(f"  [PIPELINE] Invalid message: {message}")
            return

        # Cancelled jobs don't advance — drop late events from workers.
        job = await self.jobs.get(job_id)
        if not job:
            print(f"  [PIPELINE] Completion for unknown job {job_id}, dropping")
            return
        if job.status in TERMINAL_STATUSES:
            print(f"  [PIPELINE] Job {job_id} is {job.status}, dropping completion")
            return

        if status == "failed":
            await self._handle_failure(message)
            return

        if msg_type == "media":
            await self._handle_media_completed(message)
        elif msg_type == "transcribe":
            await self._handle_transcribe_completed(message)
        elif msg_type == "subtitle":
            await self._handle_subtitle_completed(message)
        else:
            print(f"  [PIPELINE] Unknown task type '{msg_type}' for job {job_id}")

    # ─── Stage handlers ─────────────────────────────────────────────────

    async def _handle_media_completed(self, message: dict) -> None:
        job_id: str = message["job_id"]
        task_id: str = message["task_id"]
        audio_path: str = message.get("audio_path") or ""
        video_meta_path: str | None = message.get("video_meta_path")
        duration: float = float(message.get("duration") or 0)
        fps: float = float(message.get("fps") or 0)

        print(f"  [PIPELINE] Media done for job {job_id}: {duration:.1f}s @ {fps:.3f}fps")

        await self.tasks.update_status(task_id, "completed", output_path=audio_path)

        job = await self.jobs.get(job_id)
        if not job or job.status in TERMINAL_STATUSES:
            return

        # Persist duration on the job so we can show it in the UI later.
        job_updates: dict[str, Any] = {}
        if duration > 0:
            job_updates["input_duration"] = duration

        await self.jobs.update_status(job_id, "transcribing", **job_updates)
        await self.progress.set(job_id, {
            "status": "transcribing",
            "total_chunks": 1,
            "completed_chunks": 0,
            "failed_chunks": 0,
        })

        task = await self.tasks.create(
            job_id=job_id,
            task_type="transcribe",
            input_path=audio_path,
        )
        await self.events.publish_transcribe_task(
            task_id=task.id,
            job_id=job_id,
            audio_path=audio_path,
            video_meta_path=video_meta_path,
            dialect=job.dialect or "auto",
        )
        print(f"  [PIPELINE] Published transcribe task for job {job_id}")

    async def _handle_transcribe_completed(self, message: dict) -> None:
        job_id: str = message["job_id"]
        task_id: str = message["task_id"]
        output_path: str | None = message.get("output")

        await self.tasks.update_status(
            task_id, "completed", output_path=output_path,
        )
        await self.progress.increment(job_id, "completed_chunks")

        print(f"  [PIPELINE] Transcribe done for job {job_id}")
        await self._start_subtitling(job_id)

    async def _start_subtitling(self, job_id: str) -> None:
        job = await self.jobs.get(job_id)
        if not job or job.status in TERMINAL_STATUSES:
            return

        results_dir = f"results/{job_id}/"

        await self.jobs.update_status(job_id, "subtitling")
        await self.progress.update_field(job_id, "status", "subtitling")

        task = await self.tasks.create(
            job_id=job_id,
            task_type="subtitle",
            input_path=results_dir,
        )
        await self.events.publish_subtitle_task(
            task_id=task.id,
            job_id=job_id,
            results_dir=results_dir,
            original_video=job.input_file_path,
            subtitle_format=job.subtitle_format or "srt",
            burn=bool(job.burn_subtitles),
            user_id=job.user_id,
        )
        print(f"  [PIPELINE] Published subtitle task for job {job_id}")

    async def _handle_subtitle_completed(self, message: dict) -> None:
        """
        Two-stage completion:
          stage="subtitles" + final=True   → job done (burn was off)
          stage="subtitles" + final=False  → SRT/JSON done, burn pending
          stage="burn"                     → burned video done, job complete
        """
        job_id: str = message["job_id"]
        task_id: str = message["task_id"]
        stage: str = message.get("stage", "subtitles")
        is_final: bool = bool(message.get("final", True))
        outputs: dict = message.get("outputs") or {}

        print(f"  [PIPELINE] Subtitle stage='{stage}' done for job {job_id} (final={is_final})")

        # Map worker output keys to Job columns.
        update_fields: dict[str, Any] = {}
        if "transcript" in outputs:
            update_fields["transcript_path"] = outputs["transcript"]
        if "srt" in outputs or "vtt" in outputs:
            update_fields["subtitle_path"] = outputs.get("srt") or outputs.get("vtt")
        if "video" in outputs:
            update_fields["video_output_path"] = outputs["video"]

        if stage == "subtitles":
            if is_final:
                # No burn requested — task and job both done.
                await self.tasks.update_status(task_id, "completed")
                await self.jobs.update_status(job_id, "completed", **update_fields)
                await self.progress.delete(job_id)
                print(f"  [PIPELINE] Job {job_id} COMPLETED (no burn)")
            else:
                # Subtitles registered, burn still running on same task.
                # Don't close the task yet — worker will emit stage="burn" next.
                await self.jobs.update_status(job_id, "burning", **update_fields)
                await self.progress.update_field(job_id, "status", "burning")
                print(f"  [PIPELINE] Job {job_id} moved to BURNING stage")

        elif stage == "burn":
            await self.tasks.update_status(task_id, "completed")
            await self.jobs.update_status(job_id, "completed", **update_fields)
            await self.progress.delete(job_id)
            print(f"  [PIPELINE] Job {job_id} COMPLETED (burn done)")

        else:
            print(f"  [PIPELINE] Unknown subtitle stage '{stage}' for job {job_id}")

    # ─── Failure & retry ────────────────────────────────────────────────

    async def _handle_failure(self, message: dict) -> None:
        job_id: str = message["job_id"]
        task_id: str = message["task_id"]
        msg_type: str = message.get("type", "")
        error: str = message.get("error") or "Unknown error"

        print(f"  [PIPELINE] Task {task_id} ({msg_type}) failed for job {job_id}: {error}")

        task = await self.tasks.get(task_id)
        if not task:
            print(f"  [PIPELINE] Failure for unknown task {task_id}, dropping")
            return

        retries = task.retries or 0

        if retries < config.MAX_RETRIES:
            new_retries = retries + 1
            await self.tasks.update_status(
                task_id, "pending", retries=new_retries, error=error,
            )
            print(f"  [PIPELINE] Retrying task {task_id} (attempt {new_retries + 1}/{config.MAX_RETRIES + 1})")
            refreshed = await self.tasks.get(task_id)
            if refreshed is not None:
                await self._republish(msg_type, refreshed)
            return

        # Out of retries — fail task and job.
        await self.tasks.update_status(task_id, "failed", error=error)
        await self.jobs.update_status(job_id, "failed", error=error)
        await self.progress.update_field(job_id, "status", "failed")
        await self.progress.increment(job_id, "failed_chunks")
        # Keep progress hash briefly so the UI can show the failure;
        # the hash TTL will reap it.

    async def _republish(self, msg_type: str, task: Task) -> None:
        job = await self.jobs.get(task.job_id)
        if not job or job.status in TERMINAL_STATUSES:
            print(f"  [PIPELINE] Skipping republish for task {task.id} — job is terminal")
            return

        if msg_type == "media":
            await self.events.publish_media_task(
                task_id=task.id,
                job_id=job.id,
                input_path=job.input_file_path,
            )
        elif msg_type == "transcribe":
            await self.events.publish_transcribe_task(
                task_id=task.id,
                job_id=job.id,
                audio_path=task.input_path or "",
                dialect=job.dialect or "auto",
            )
        elif msg_type == "subtitle":
            await self.events.publish_subtitle_task(
                task_id=task.id,
                job_id=job.id,
                results_dir=task.input_path or f"results/{job.id}/",
                original_video=job.input_file_path,
                subtitle_format=job.subtitle_format or "srt",
                burn=bool(job.burn_subtitles),
                user_id=job.user_id,
            )
        else:
            print(f"  [PIPELINE] Cannot republish unknown task type '{msg_type}' for task {task.id}")







