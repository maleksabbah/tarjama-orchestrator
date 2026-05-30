"""
Per-job progress tracking stored as a Redis hash.
Key shape: progress:{job_id}
"""
from typing import Optional

from redis import asyncio as aioredis


PROGRESS_TTL_SECONDS = 86400  # 24 hours


class ProgressRepository:
    def __init__(self, client: aioredis.Redis):
        self.client = client

    def _key(self, job_id: str) -> str:
        return f"progress:{job_id}"

    async def set(self, job_id: str, data: dict) -> None:
        key = self._key(job_id)
        await self.client.hset(key, mapping={k: str(v) for k, v in data.items()})
        await self.client.expire(key, PROGRESS_TTL_SECONDS)

    async def update_field(self, job_id: str, field: str, value) -> None:
        await self.client.hset(self._key(job_id), field, str(value))

    async def increment(self, job_id: str, field: str, amount: int = 1) -> None:
        await self.client.hincrby(self._key(job_id), field, amount)

    async def get(self, job_id: str) -> Optional[dict]:
        data = await self.client.hgetall(self._key(job_id))
        return data if data else None

    async def delete(self, job_id: str) -> None:
        await self.client.delete(self._key(job_id))