"""
Best-effort GPU box waker.

When a job is created, we ping a Lambda that starts the GPU transcription
box if it's stopped. The Kafka task buffers in the topic while the box
boots, so transcription just picks up once the worker joins. Failure here
never blocks job creation.

Calling a Lambda (rather than boto3 directly) keeps AWS credentials out of
the orchestrator container — the Lambda holds the EC2 permission.
"""
import asyncio
import os
import time
import urllib.request

# Lambda Function URL that starts the GPU box (override via env if needed).
GPU_WAKE_URL = os.getenv(
    "GPU_WAKE_URL",
    "https://rup3pazxvq6wk4lish2c6k6dua0utcrd.lambda-url.us-east-1.on.aws/",
)

# Throttle: don't hit the Lambda on every single job.
_WAKE_COOLDOWN_SEC = 60
_last_wake = 0.0


def _wake_sync() -> None:
    if not GPU_WAKE_URL:
        return
    req = urllib.request.Request(GPU_WAKE_URL, method="GET")
    with urllib.request.urlopen(req, timeout=8) as resp:
        body = resp.read().decode("utf-8", "ignore")
        print(f"  [GPU-WAKE] {resp.status} {body}")


async def wake_gpu() -> None:
    """Fire-and-forget. Never raises into the caller."""
    global _last_wake
    if not GPU_WAKE_URL:
        return
    now = time.time()
    if now - _last_wake < _WAKE_COOLDOWN_SEC:
        return
    _last_wake = now
    try:
        await asyncio.to_thread(_wake_sync)
    except Exception as e:
        print(f"  [GPU-WAKE] failed (non-fatal): {e}")