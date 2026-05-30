"""
Async Redis client setup.
"""
from redis import asyncio as aioredis

from app.Config.Config import config


redis_pool = aioredis.ConnectionPool.from_url(
    config.REDIS_URL,
    decode_responses=True,
    max_connections=20,
)


def get_redis() -> aioredis.Redis:
    """Returns a Redis client backed by the shared pool."""
    return aioredis.Redis(connection_pool=redis_pool)


async def close_redis() -> None:
    """Disconnect the pool. Call on shutdown."""
    await redis_pool.aclose()