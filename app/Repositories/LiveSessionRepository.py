# orchestrator: app/Repositories/LiveSessionRepository.py
"""
Live transcription session storage in Redis.
Single repository for both the live-mic and file-upload WebSocket flows.

Key shapes:
  live:audio:{session_id}        list of audio chunks (RPUSH/LPOP)
  live:result:{session_id}       list of result JSONs (LPUSH by worker, LPOP by orchestrator)
  live:notify:{session_id}       pub/sub channel for worker signalling
  transcribe:audio:{session_id}  appended audio bytes (APPEND)
  transcribe:result:{session_id} list of result JSONs
  queue:live_transcribe          work queue read by the GPU worker
"""
from typing import Optional

from redis import asyncio as aioredis


class LiveSessionRepository:
    def __init__(self, client: aioredis.Redis):
        self.client = client

    # ─── Live mic flow ──────────────────────────────────────────────

    async def append_live_audio(
        self, session_id: str, chunk: bytes, ttl_seconds: int,
    ) -> None:
        key = f"live:audio:{session_id}"
        await self.client.rpush(key, chunk)
        await self.client.expire(key, ttl_seconds)

    async def notify_live(self, session_id: str, event: str) -> None:
        await self.client.publish(f"live:notify:{session_id}", event)

    async def pop_live_result(self, session_id: str) -> Optional[str]:
        return await self.client.lpop(f"live:result:{session_id}")

    async def clear_live_session(self, session_id: str) -> None:
        await self.client.delete(
            f"live:audio:{session_id}",
            f"live:result:{session_id}",
        )

    # ─── File transcribe flow ───────────────────────────────────────

    async def append_file_audio(
        self, session_id: str, chunk: bytes, ttl_seconds: int,
    ) -> None:
        key = f"transcribe:audio:{session_id}"
        await self.client.append(key, chunk)
        await self.client.expire(key, ttl_seconds)

    async def enqueue_transcribe_job(self, payload: str) -> None:
        await self.client.rpush("queue:live_transcribe", payload)

    async def pop_file_result(self, session_id: str) -> Optional[str]:
        return await self.client.lpop(f"transcribe:result:{session_id}")

    async def clear_file_session(self, session_id: str) -> None:
        await self.client.delete(
            f"transcribe:audio:{session_id}",
            f"transcribe:result:{session_id}",
        )