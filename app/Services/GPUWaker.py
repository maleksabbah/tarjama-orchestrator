"""
Best-effort GPU box waker.

When a job is created, we ensure the GPU transcription box is starting.
The Kafka task buffers in the topic while the box boots, so transcription
just picks up once the worker joins. Failure here never blocks job creation.
"""
import asyncio
import os
import time

try:
    import boto3
except ImportError:  # boto3 optional; if missing, waking is a no-op
    boto3 = None

GPU_INSTANCE_ID = os.getenv("GPU_INSTANCE_ID", "")
GPU_REGION = os.getenv("GPU_REGION", "us-east-1")

# Don't hammer StartInstances on every job — throttle to once per N seconds.
_WAKE_COOLDOWN_SEC = 60
_last_wake = 0.0


def _wake_sync() -> None:
    if not boto3 or not GPU_INSTANCE_ID:
        return
    ec2 = boto3.client("ec2", region_name=GPU_REGION)
    # Only start if it's stopped/stopping; skip if already running/pending.
    resp = ec2.describe_instances(InstanceIds=[GPU_INSTANCE_ID])
    state = resp["Reservations"][0]["Instances"][0]["State"]["Name"]
    if state in ("stopped", "stopping"):
        ec2.start_instances(InstanceIds=[GPU_INSTANCE_ID])
        print(f"  [GPU-WAKE] start_instances sent ({state} -> pending)")
    else:
        print(f"  [GPU-WAKE] GPU already {state}, no action")


async def wake_gpu() -> None:
    """Fire-and-forget. Never raises into the caller."""
    global _last_wake
    if not GPU_INSTANCE_ID:
        return
    now = time.time()
    if now - _last_wake < _WAKE_COOLDOWN_SEC:
        return
    _last_wake = now
    try:
        await asyncio.to_thread(_wake_sync)
    except Exception as e:
        print(f"  [GPU-WAKE] failed (non-fatal): {e}")