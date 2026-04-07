"""
Orchestrator Unit Tests
========================
Tests state machine logic, routes, and Redis operations.

Run:
  pytest Tests.py -v
"""
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, patch, MagicMock
from fastapi import HTTPException
from httpx import AsyncClient, ASGITransport

from app.main import app
from app.State_machine import (
    handle_completion,
    handle_media_completed,
    handle_transcribe_completed,
    handle_subtitle_completed,
    handle_failure,
)


# =============================================================================
# Fixtures
# =============================================================================

@pytest_asyncio.fixture
async def client():
    """Create test client with mocked database and Redis."""
    with patch("app.main.init_db", new_callable=AsyncMock), \
         patch("app.main.close_db", new_callable=AsyncMock), \
         patch("app.main.init_redis", new_callable=AsyncMock), \
         patch("app.main.close_redis", new_callable=AsyncMock), \
         patch("app.main.event_listener", new_callable=AsyncMock):

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac


def mock_job(job_id="j_123", user_id=1, status="queued"):
    return {
        "id": job_id,
        "user_id": user_id,
        "status": status,
        "input_file_path": "uploads/j_123/video.mp4",
        "input_duration": None,
        "dialect": "lebanese",
        "output_type": "all",
        "subtitle_format": "srt",
        "burn_subtitles": False,
        "transcript_path": None,
        "subtitle_path": None,
        "video_output_path": None,
        "error": None,
        "created_at": "2026-03-13T00:00:00",
        "started_at": None,
        "completed_at": None,
        "updated_at": "2026-03-13T00:00:00",
    }


def mock_task(task_id="t_001", job_id="j_123", task_type="extract",
              status="pending", chunk_index=None, retries=0):
    return {
        "id": task_id,
        "job_id": job_id,
        "type": task_type,
        "status": status,
        "input_path": f"chunks/{job_id}/chunk_{chunk_index or 0}.wav",
        "output_path": None,
        "chunk_index": chunk_index,
        "retries": retries,
        "error": None,
        "created_at": "2026-03-13T00:00:00",
        "started_at": None,
        "completed_at": None,
    }


# =============================================================================
# Health endpoints
# =============================================================================

@pytest.mark.asyncio
class TestHealthEndpoints:
    async def test_health(self, client):
        resp = await client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["service"] == "orchestrator"

    async def test_root(self, client):
        resp = await client.get("/")
        assert resp.status_code == 200
        data = resp.json()
        assert data["service"] == "ASR Orchestrator"


# =============================================================================
# Job creation
# =============================================================================

@pytest.mark.asyncio
class TestCreateJob:
    async def test_create_job_success(self, client):
        job = mock_job()
        task = mock_task()

        with patch("app.Routes.db.create_job", new_callable=AsyncMock, return_value=job), \
             patch("app.Routes.db.create_task", new_callable=AsyncMock, return_value=task), \
             patch("app.Routes.db.update_job_status", new_callable=AsyncMock, return_value=job), \
             patch("app.Routes.rc.push_media_task", new_callable=AsyncMock), \
             patch("app.Routes.rc.set_progress", new_callable=AsyncMock):
            resp = await client.post("/jobs", json={
                "user_id": 1,
                "file_path": "uploads/j_123/video.mp4",
                "dialect": "lebanese",
            })
            assert resp.status_code == 200
            data = resp.json()
            assert data["id"] == "j_123"
            assert data["dialect"] == "lebanese"

    async def test_create_job_starts_pipeline(self, client):
        """Verify that creating a job pushes a media task to Redis."""
        job = mock_job()
        task = mock_task()

        mock_push = AsyncMock()

        with patch("app.Routes.db.create_job", new_callable=AsyncMock, return_value=job), \
             patch("app.Routes.db.create_task", new_callable=AsyncMock, return_value=task), \
             patch("app.Routes.db.update_job_status", new_callable=AsyncMock, return_value=job), \
             patch("app.Routes.rc.push_media_task", mock_push), \
             patch("app.Routes.rc.set_progress", new_callable=AsyncMock):
            await client.post("/jobs", json={
                "user_id": 1,
                "file_path": "uploads/j_123/video.mp4",
            })
            mock_push.assert_called_once()


# =============================================================================
# Job retrieval
# =============================================================================

@pytest.mark.asyncio
class TestGetJob:
    async def test_get_job_success(self, client):
        job = mock_job()

        with patch("app.Routes.db.get_job", new_callable=AsyncMock, return_value=job):
            resp = await client.get("/jobs/j_123", headers={"X-User-ID": "1"})
            assert resp.status_code == 200
            assert resp.json()["id"] == "j_123"

    async def test_get_job_not_found(self, client):
        with patch("app.Routes.db.get_job", new_callable=AsyncMock, return_value=None):
            resp = await client.get("/jobs/j_999", headers={"X-User-ID": "1"})
            assert resp.status_code == 404

    async def test_get_job_wrong_user(self, client):
        job = mock_job(user_id=1)

        with patch("app.Routes.db.get_job", new_callable=AsyncMock, return_value=job):
            resp = await client.get("/jobs/j_123", headers={"X-User-ID": "999"})
            assert resp.status_code == 403

    async def test_get_job_missing_user_header(self, client):
        job = mock_job()

        with patch("app.Routes.db.get_job", new_callable=AsyncMock, return_value=job):
            resp = await client.get("/jobs/j_123")
            assert resp.status_code == 401


# =============================================================================
# Job listing
# =============================================================================

@pytest.mark.asyncio
class TestListJobs:
    async def test_list_jobs(self, client):
        jobs = [mock_job("j_1"), mock_job("j_2"), mock_job("j_3")]

        with patch("app.Routes.db.get_jobs_by_user", new_callable=AsyncMock, return_value=jobs), \
             patch("app.Routes.db.count_user_jobs", new_callable=AsyncMock, return_value=3):
            resp = await client.get("/jobs?user_id=1", headers={"X-User-ID": "1"})
            assert resp.status_code == 200
            data = resp.json()
            assert data["total"] == 3
            assert len(data["jobs"]) == 3


# =============================================================================
# Job progress
# =============================================================================

@pytest.mark.asyncio
class TestJobProgress:
    async def test_progress_from_redis(self, client):
        job = mock_job(status="transcribing")
        progress = {
            "status": "transcribing",
            "total_chunks": "120",
            "completed_chunks": "45",
            "failed_chunks": "2",
        }

        with patch("app.Routes.db.get_job", new_callable=AsyncMock, return_value=job), \
             patch("app.Routes.rc.get_progress", new_callable=AsyncMock, return_value=progress):
            resp = await client.get("/jobs/j_123/progress", headers={"X-User-ID": "1"})
            assert resp.status_code == 200
            data = resp.json()
            assert data["total_chunks"] == 120
            assert data["completed_chunks"] == 45
            assert data["failed_chunks"] == 2

    async def test_progress_fallback_to_db(self, client):
        job = mock_job(status="queued")

        with patch("app.Routes.db.get_job", new_callable=AsyncMock, return_value=job), \
             patch("app.Routes.rc.get_progress", new_callable=AsyncMock, return_value=None):
            resp = await client.get("/jobs/j_123/progress", headers={"X-User-ID": "1"})
            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] == "queued"
            assert data["total_chunks"] == 0


# =============================================================================
# Job cancellation
# =============================================================================

@pytest.mark.asyncio
class TestCancelJob:
    async def test_cancel_active_job(self, client):
        job = mock_job(status="transcribing")

        with patch("app.Routes.db.get_job", new_callable=AsyncMock, return_value=job), \
             patch("app.Routes.rc.remove_pending_tasks", new_callable=AsyncMock, return_value=5), \
             patch("app.Routes.db.update_job_status", new_callable=AsyncMock), \
             patch("app.Routes.rc.delete_progress", new_callable=AsyncMock):
            resp = await client.post("/jobs/j_123/cancel", headers={"X-User-ID": "1"})
            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] == "cancelled"
            assert data["removed_tasks"] == 15  # 5 from each of 3 queues

    async def test_cancel_completed_job_fails(self, client):
        job = mock_job(status="completed")

        with patch("app.Routes.db.get_job", new_callable=AsyncMock, return_value=job):
            resp = await client.post("/jobs/j_123/cancel", headers={"X-User-ID": "1"})
            assert resp.status_code == 400

    async def test_cancel_wrong_user(self, client):
        job = mock_job(user_id=1)

        with patch("app.Routes.db.get_job", new_callable=AsyncMock, return_value=job):
            resp = await client.post("/jobs/j_123/cancel", headers={"X-User-ID": "999"})
            assert resp.status_code == 403


# =============================================================================
# State machine — media completed
# =============================================================================

@pytest.mark.asyncio
class TestStateMachineMedia:
    async def test_media_completed_creates_transcription_tasks(self):
        job = mock_job(status="extracting")
        task = mock_task(task_id="t_new", task_type="transcribe")

        message = {
            "type": "media",
            "status": "completed",
            "job_id": "j_123",
            "task_id": "t_001",
            "chunks": [
                "chunks/j_123/chunk_0.wav",
                "chunks/j_123/chunk_1.wav",
                "chunks/j_123/chunk_2.wav",
            ],
            "total_chunks": 3,
        }

        with patch("app.State_machine.db.update_task_status", new_callable=AsyncMock), \
             patch("app.State_machine.db.get_job", new_callable=AsyncMock, return_value=job), \
             patch("app.State_machine.db.update_job_status", new_callable=AsyncMock), \
             patch("app.State_machine.rc.set_progress", new_callable=AsyncMock), \
             patch("app.State_machine.db.create_task", new_callable=AsyncMock, return_value=task), \
             patch("app.State_machine.rc.push_transcribe_task", new_callable=AsyncMock) as mock_push:
            await handle_media_completed(message)
            assert mock_push.call_count == 3

    async def test_media_completed_skips_cancelled_job(self):
        job = mock_job(status="cancelled")

        message = {
            "type": "media",
            "status": "completed",
            "job_id": "j_123",
            "task_id": "t_001",
            "chunks": ["chunk_0.wav"],
            "total_chunks": 1,
        }

        with patch("app.State_machine.db.update_task_status", new_callable=AsyncMock), \
             patch("app.State_machine.db.get_job", new_callable=AsyncMock, return_value=job), \
             patch("app.State_machine.db.update_job_status", new_callable=AsyncMock) as mock_update, \
             patch("app.State_machine.rc.set_progress", new_callable=AsyncMock):
            await handle_media_completed(message)
            mock_update.assert_not_called()


# =============================================================================
# State machine — transcription completed
# =============================================================================

@pytest.mark.asyncio
class TestStateMachineTranscribe:
    async def test_transcribe_increments_progress(self):
        message = {
            "type": "transcribe",
            "status": "completed",
            "job_id": "j_123",
            "task_id": "t_042",
            "output": "results/j_123/chunk_042.json",
        }

        # 3 tasks: 2 completed, 1 pending → not done yet
        tasks = [
            mock_task("t_041", task_type="transcribe", status="completed", chunk_index=0),
            mock_task("t_042", task_type="transcribe", status="completed", chunk_index=1),
            mock_task("t_043", task_type="transcribe", status="pending", chunk_index=2),
        ]

        with patch("app.State_machine.db.update_task_status", new_callable=AsyncMock), \
             patch("app.State_machine.rc.increment_progress", new_callable=AsyncMock) as mock_incr, \
             patch("app.State_machine.db.count_tasks_by_status", new_callable=AsyncMock, return_value={"completed": 2, "pending": 1}), \
             patch("app.State_machine.db.get_tasks_by_job", new_callable=AsyncMock, return_value=tasks):
            await handle_transcribe_completed(message)
            mock_incr.assert_called_once_with("j_123", "completed_chunks")

    async def test_all_transcriptions_done_starts_subtitling(self):
        message = {
            "type": "transcribe",
            "status": "completed",
            "job_id": "j_123",
            "task_id": "t_003",
            "output": "results/j_123/chunk_2.json",
        }

        # All 3 completed
        tasks = [
            mock_task("t_001", task_type="transcribe", status="completed", chunk_index=0),
            mock_task("t_002", task_type="transcribe", status="completed", chunk_index=1),
            mock_task("t_003", task_type="transcribe", status="completed", chunk_index=2),
        ]
        job = mock_job(status="transcribing")
        subtitle_task = mock_task("t_sub", task_type="subtitle")

        with patch("app.State_machine.db.update_task_status", new_callable=AsyncMock), \
             patch("app.State_machine.rc.increment_progress", new_callable=AsyncMock), \
             patch("app.State_machine.db.count_tasks_by_status", new_callable=AsyncMock, return_value={"completed": 3}), \
             patch("app.State_machine.db.get_tasks_by_job", new_callable=AsyncMock, return_value=tasks), \
             patch("app.State_machine.db.get_job", new_callable=AsyncMock, return_value=job), \
             patch("app.State_machine.db.update_job_status", new_callable=AsyncMock), \
             patch("app.State_machine.rc.update_progress", new_callable=AsyncMock), \
             patch("app.State_machine.db.create_task", new_callable=AsyncMock, return_value=subtitle_task), \
             patch("app.State_machine.rc.push_subtitle_task", new_callable=AsyncMock) as mock_push:
            await handle_transcribe_completed(message)
            mock_push.assert_called_once()

    async def test_all_transcriptions_failed(self):
        message = {
            "type": "transcribe",
            "status": "completed",
            "job_id": "j_123",
            "task_id": "t_003",
        }

        # All 3 failed
        tasks = [
            mock_task("t_001", task_type="transcribe", status="failed", chunk_index=0),
            mock_task("t_002", task_type="transcribe", status="failed", chunk_index=1),
            mock_task("t_003", task_type="transcribe", status="failed", chunk_index=2),
        ]

        with patch("app.State_machine.db.update_task_status", new_callable=AsyncMock), \
             patch("app.State_machine.rc.increment_progress", new_callable=AsyncMock), \
             patch("app.State_machine.db.count_tasks_by_status", new_callable=AsyncMock, return_value={"failed": 3}), \
             patch("app.State_machine.db.get_tasks_by_job", new_callable=AsyncMock, return_value=tasks), \
             patch("app.State_machine.db.update_job_status", new_callable=AsyncMock) as mock_update, \
             patch("app.State_machine.rc.delete_progress", new_callable=AsyncMock):
            await handle_transcribe_completed(message)
            mock_update.assert_called_with("j_123", "failed", error="All 3 transcription tasks failed")


# =============================================================================
# State machine — subtitle completed
# =============================================================================

@pytest.mark.asyncio
class TestStateMachineSubtitle:
    async def test_subtitle_completed_marks_job_done(self):
        message = {
            "type": "subtitle",
            "status": "completed",
            "job_id": "j_123",
            "task_id": "t_200",
            "outputs": {
                "transcript": "results/j_123/transcript.json",
                "srt": "results/j_123/subtitles.srt",
                "video": "results/j_123/video_sub.mp4",
            },
        }

        with patch("app.State_machine.db.update_task_status", new_callable=AsyncMock), \
             patch("app.State_machine.db.update_job_status", new_callable=AsyncMock) as mock_update, \
             patch("app.State_machine.rc.delete_progress", new_callable=AsyncMock):
            await handle_subtitle_completed(message)
            mock_update.assert_called_once_with(
                "j_123", "completed",
                transcript_path="results/j_123/transcript.json",
                subtitle_path="results/j_123/subtitles.srt",
                video_output_path="results/j_123/video_sub.mp4",
            )


# =============================================================================
# State machine — failure handling
# =============================================================================

@pytest.mark.asyncio
class TestStateMachineFailure:
    async def test_failure_retries_task(self):
        message = {
            "type": "transcribe",
            "status": "failed",
            "job_id": "j_123",
            "task_id": "t_042",
            "error": "GPU OOM",
        }

        tasks = [
            mock_task("t_042", task_type="transcribe", status="running", chunk_index=5, retries=0),
        ]

        with patch("app.State_machine.db.get_tasks_by_job", new_callable=AsyncMock, return_value=tasks), \
             patch("app.State_machine.db.update_task_status", new_callable=AsyncMock) as mock_update, \
             patch("app.State_machine.rc.push_transcribe_task", new_callable=AsyncMock) as mock_push:
            await handle_failure(message)
            mock_update.assert_called_with("t_042", "pending", retries=1)
            mock_push.assert_called_once()

    async def test_failure_max_retries_exceeded(self):
        message = {
            "type": "media",
            "status": "failed",
            "job_id": "j_123",
            "task_id": "t_001",
            "error": "FFmpeg crashed",
        }

        tasks = [
            mock_task("t_001", task_type="extract", status="running", retries=3),
        ]

        with patch("app.State_machine.db.get_tasks_by_job", new_callable=AsyncMock, return_value=tasks), \
             patch("app.State_machine.db.update_task_status", new_callable=AsyncMock), \
             patch("app.State_machine.db.update_job_status", new_callable=AsyncMock) as mock_job_update, \
             patch("app.State_machine.rc.delete_progress", new_callable=AsyncMock):
            await handle_failure(message)
            mock_job_update.assert_called_with("j_123", "failed", error="FFmpeg crashed")

    async def test_failure_task_not_found(self):
        message = {
            "type": "transcribe",
            "status": "failed",
            "job_id": "j_123",
            "task_id": "t_999",
            "error": "something",
        }

        with patch("app.State_machine.db.get_tasks_by_job", new_callable=AsyncMock, return_value=[]):
            await handle_failure(message)  # should not crash


# =============================================================================
# State machine — handle_completion routing
# =============================================================================

@pytest.mark.asyncio
class TestHandleCompletion:
    async def test_routes_to_media_handler(self):
        message = {
            "type": "media",
            "status": "completed",
            "job_id": "j_123",
            "task_id": "t_001",
            "chunks": [],
        }

        with patch("app.State_machine.handle_media_completed", new_callable=AsyncMock) as mock_handler:
            await handle_completion(message)
            mock_handler.assert_called_once_with(message)

    async def test_routes_to_transcribe_handler(self):
        message = {
            "type": "transcribe",
            "status": "completed",
            "job_id": "j_123",
            "task_id": "t_042",
        }

        with patch("app.State_machine.handle_transcribe_completed", new_callable=AsyncMock) as mock_handler:
            await handle_completion(message)
            mock_handler.assert_called_once_with(message)

    async def test_routes_to_subtitle_handler(self):
        message = {
            "type": "subtitle",
            "status": "completed",
            "job_id": "j_123",
            "task_id": "t_200",
        }

        with patch("app.State_machine.handle_subtitle_completed", new_callable=AsyncMock) as mock_handler:
            await handle_completion(message)
            mock_handler.assert_called_once_with(message)

    async def test_routes_failure_to_failure_handler(self):
        message = {
            "type": "transcribe",
            "status": "failed",
            "job_id": "j_123",
            "task_id": "t_042",
            "error": "crash",
        }

        with patch("app.State_machine.handle_failure", new_callable=AsyncMock) as mock_handler:
            await handle_completion(message)
            mock_handler.assert_called_once_with(message)

    async def test_invalid_message_ignored(self):
        message = {"type": "media"}  # missing required fields

        # Should not crash
        await handle_completion(message)