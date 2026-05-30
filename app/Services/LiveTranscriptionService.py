# orchestrator: app/Services/LiveTranscriptionService.py
"""
Live transcription service.
Handles both /ws/live (mic streaming) and /ws/transcribe (file upload-then-stream)
flows. Owns the policy: TTLs, session-id format, queue names.
"""
import json
from typing import Optional

from app.Repositories import LiveSessionRepository


# Live mic chunks expire after 2 minutes of inactivity.
LIVE_AUDIO_TTL_SECONDS = 120

# File-upload buffer expires after 5 minutes.
UPLOAD_TTL_SECONDS = 300

# How many results to drain per receive cycle (cap so we don't hold the loop).
MAX_RESULTS_PER_DRAIN = 10


class LiveTranscriptionService:
    def __init__(self, sessions: LiveSessionRepository):
        self.sessions = sessions

    # ─── Session IDs ────────────────────────────────────────────────

    @staticmethod
    def make_live_session_id(user_id: str, ws_handle: int) -> str:
        return f"live:{user_id}:{ws_handle}"

    @staticmethod
    def make_file_session_id(user_id: str, ws_handle: int) -> str:
        return f"transcribe:{user_id}:{ws_handle}"

    # ─── Live mic flow ──────────────────────────────────────────────

    async def push_live_audio(self, session_id: str, chunk: bytes) -> None:
        """Buffer a mic audio chunk for the worker and signal it."""
        await self.sessions.append_live_audio(
            session_id, chunk, LIVE_AUDIO_TTL_SECONDS,
        )
        await self.sessions.notify_live(session_id, "chunk")

    async def signal_live_end(self, session_id: str) -> None:
        """Tell the worker the client is done sending."""
        await self.sessions.notify_live(session_id, "end")

    async def drain_live_results(self, session_id: str) -> list[str]:
        """Pull whatever results the worker has produced. Caller decodes JSON."""
        results: list[str] = []
        for _ in range(MAX_RESULTS_PER_DRAIN):
            raw = await self.sessions.pop_live_result(session_id)
            if raw is None:
                break
            results.append(raw)
        return results

    async def cleanup_live(self, session_id: str) -> None:
        await self.sessions.clear_live_session(session_id)

    # ─── File upload flow ───────────────────────────────────────────

    async def append_file_chunk(self, session_id: str, chunk: bytes) -> None:
        """Append bytes to the in-progress upload buffer."""
        await self.sessions.append_file_audio(
            session_id, chunk, UPLOAD_TTL_SECONDS,
        )

    async def submit_file(self, session_id: str, user_id: str) -> None:
        """Mark the upload complete and queue it for the worker."""
        payload = json.dumps({
            "session_id": session_id,
            "audio_key": f"transcribe:audio:{session_id}",
            "result_key": f"transcribe:result:{session_id}",
            "user_id": user_id,
        })
        await self.sessions.enqueue_transcribe_job(payload)

    async def pop_file_result(self, session_id: str) -> Optional[str]:
        return await self.sessions.pop_file_result(session_id)

    async def cleanup_file(self, session_id: str) -> None:
        await self.sessions.clear_file_session(session_id)