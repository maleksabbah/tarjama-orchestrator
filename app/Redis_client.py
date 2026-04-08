"""
Orchestrator Redis Client
Queue operations + progress tracking.
"""
import json
import redis.asyncio as redis
from app.Config import config


client: redis.Redis = None


async def init_redis():
    global client
    client = redis.from_url(config.REDIS_URL,decode_responses=True)


async def close_redis():
    global client
    if client:
        await client.close()

# =============================================================================
# Queue operations
# =============================================================================

async def push_task(queue_name: str, message: dict):
    """Push a task message to a queue."""
    await client.lpush(queue_name, json.dumps(message))


async def pop_completed(timeout: int = 5) -> dict | None:
    """Pop from the completed queue. Blocks for `timeout` seconds."""
    result = await client.brpop(config.QUEUE_COMPLETED, timeout=timeout)
    if result:
        _, data = result
        return json.loads(data)
    return None
async def push_media_task(task_id: str, job_id: str, input_path: str):
    await push_task(config.QUEUE_MEDIA, {
        "task_id": task_id,
        "job_id": job_id,
        "input_path": input_path,
    })

async def push_subtitle_task(task_id: str, job_id: str, results_dir: str,
                              original_video: str, subtitle_format: str = "srt",
                              burn: bool = False, user_id: int = 0):
    await push_task(config.QUEUE_SUBTITLE, {
        "task_id": task_id,
        "job_id": job_id,
        "user_id": user_id,
        "results_dir": results_dir,
        "original_video": original_video,
        "format": subtitle_format,
        "burn": burn,
    })
async def push_subtitle_task(task_id: str, job_id: str, results_dir: str,
                              original_video: str, subtitle_format: str = "srt",
                              burn: bool = False):
    await push_task(config.QUEUE_SUBTITLE, {
        "task_id": task_id,
        "job_id": job_id,
        "results_dir": results_dir,
        "original_video": original_video,
        "format": subtitle_format,
        "burn": burn,
    })


# =============================================================================
# Progress tracking
# =============================================================================

async def set_progress(job_id: str, data: dict):
    """Set progress hash for a job."""
    key = f"progress:{job_id}"
    await client.hset(key, mapping={k: str(v) for k, v in data.items()})
    await client.expire(key, 86400)  # 24 hour TTL


async def update_progress(job_id: str, field: str, value):
    """Update a single progress field."""
    key = f"progress:{job_id}"
    await client.hset(key, field, str(value))

async def increment_progress(job_id: str, field: str, amount: int = 1):
    """Increment a progress counter."""
    key = f"progress:{job_id}"
    await client.hincrby(key, field, amount)


async def get_progress(job_id: str) -> dict | None:
    """Get progress for a job."""
    key = f"progress:{job_id}"
    data = await client.hgetall(key)
    return data if data else None


async def delete_progress(job_id: str):
    """Clean up progress when job is done."""
    await client.delete(f"progress:{job_id}")


# =============================================================================
# Queue management
# =============================================================================

async def get_queue_length(queue_name: str) -> int:
    return await client.llen(queue_name)

async def remove_pending_tasks(job_id: str, queue_name: str) -> int:
    """Remove all pending tasks for a job from a queue (for cancellation)."""
    removed = 0
    length = await client.llen(queue_name)
    for _ in range(length):
        item = await client.rpop(queue_name)
        if item:
            msg = json.loads(item)
            if msg.get("job_id") != job_id:
                # Put it back if it's not for this job
                await client.lpush(queue_name, item)
            else:
                removed += 1
    return removed


