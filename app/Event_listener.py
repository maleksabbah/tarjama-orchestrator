"""
Orchestrator Event Listener
============================
Background loop: BRPOP queue:completed → pass to state machine.
Runs in a separate asyncio task alongside the FastAPI server.
"""
import asyncio
from app import Redis_client as rc
from app.State_machine import handle_completion


async def event_listener():
    """Main event loop — listens for worker completions forever."""
    print("  [LISTENER] Started, waiting for completions...")

    while True:
        try:
            message = await rc.pop_completed(timeout=5)
            if message:
                print(f"  [LISTENER] Received: {message.get('type')} for job {message.get('job_id')}")
                await handle_completion(message)
        except asyncio.CancelledError:
            print("  [LISTENER] Shutting down...")
            break
        except Exception as e:
            print(f"  [LISTENER] Error: {e}")
            await asyncio.sleep(1)  # Brief pause before retrying