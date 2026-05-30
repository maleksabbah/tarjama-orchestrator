"""
Publishes events to Kafka topics.
Replaces the old QueueRepository.push_*_task methods.
"""
import json
from typing import Optional

from aiokafka import AIOKafkaProducer

from app.Config.Config import config


class EventPublisher:
    def __init__(self, producer: AIOKafkaProducer):
        self.producer = producer

    async def _publish(self, topic: str, key: str, payload: dict) -> None:
        """Send a JSON-encoded message keyed by job_id (preserves per-job ordering)."""
        await self.producer.send_and_wait(
            topic=topic,
            key=key.encode("utf-8"),
            value=json.dumps(payload).encode("utf-8"),
        )

    # ─── Task dispatch (orchestrator → workers) ─────────────────────────

    async def publish_media_task(
        self, task_id: str, job_id: str, input_path: str
    ) -> None:
        await self._publish(
            topic=config.TOPIC_MEDIA_TASKS,
            key=job_id,
            payload={
                "task_id": task_id,
                "job_id": job_id,
                "input_path": input_path,
            },
        )

    async def publish_transcribe_task(
        self,
        task_id: str,
        job_id: str,
        audio_path: str,
        video_meta_path: Optional[str] = None,
        dialect: str = "auto",
    ) -> None:
        await self._publish(
            topic=config.TOPIC_TRANSCRIBE_TASKS,
            key=job_id,
            payload={
                "task_id": task_id,
                "job_id": job_id,
                "audio_path": audio_path,
                "video_meta_path": video_meta_path,
                "dialect": dialect,
            },
        )

    async def publish_subtitle_task(
        self,
        task_id: str,
        job_id: str,
        results_dir: str,
        original_video: str,
        subtitle_format: str = "srt",
        burn: bool = False,
        user_id: int = 0,
    ) -> None:
        await self._publish(
            topic=config.TOPIC_SUBTITLE_TASKS,
            key=job_id,
            payload={
                "task_id": task_id,
                "job_id": job_id,
                "user_id": user_id,
                "results_dir": results_dir,
                "original_video": original_video,
                "format": subtitle_format,
                "burn": burn,
            },
        )

    # ─── Worker → orchestrator completion events ────────────────────────

    async def publish_completion(self, payload: dict) -> None:
        """
        Used by workers to signal a task has completed (or failed).
        Required keys in payload: task_id, job_id, type, status.
        Optional: stage, final, outputs, output, error, audio_path,
                  video_meta_path, duration, fps, text_preview, etc.
        """
        job_id = payload.get("job_id")
        if not job_id:
            raise ValueError("publish_completion requires 'job_id' in payload")
        await self._publish(
            topic=config.TOPIC_COMPLETED,
            key=job_id,
            payload=payload,
        )