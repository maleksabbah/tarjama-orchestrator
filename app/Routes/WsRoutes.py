# orchestrator: app/Routes/WsRoutes.py
"""
WebSocket routes — real-time transcription.

Auth is handled by nginx (auth_request → gateway /auth/me) before forwarding.
nginx sets X-User-ID on the WS upgrade request; we just trust it.

Endpoints:
  /ws/live        → live mic streaming
  /ws/transcribe  → upload audio file, then stream results
"""
import asyncio
import json

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.Configs.Redis import get_redis
from app.Repositories import LiveSessionRepository
from app.Services import LiveTranscriptionService


router = APIRouter(tags=["ws"])


def _service() -> LiveTranscriptionService:
    return LiveTranscriptionService(LiveSessionRepository(get_redis()))


def _user_id_from_headers(websocket: WebSocket) -> str | None:
    """nginx sets X-User-ID after auth_request succeeded."""
    return websocket.headers.get("x-user-id")


# ─── Live mic ───────────────────────────────────────────────────────────

@router.websocket("/ws/live")
async def live_transcription(websocket: WebSocket):
    await websocket.accept()

    user_id = _user_id_from_headers(websocket)
    if not user_id:
        await websocket.send_json({"type": "error", "message": "Missing user id"})
        await websocket.close(code=4001)
        return

    service = _service()
    session_id = service.make_live_session_id(user_id, id(websocket))

    try:
        await websocket.send_json({"type": "connected", "session_id": session_id})

        while True:
            try:
                message = await asyncio.wait_for(websocket.receive(), timeout=30.0)
            except asyncio.TimeoutError:
                await websocket.send_json({"type": "ping"})
                continue
            except WebSocketDisconnect:
                break

            if message.get("type") == "websocket.disconnect":
                break

            if message.get("bytes"):
                await service.push_live_audio(session_id, message["bytes"])

            elif message.get("text"):
                try:
                    data = json.loads(message["text"])
                    if data.get("type") == "end":
                        await service.signal_live_end(session_id)
                except json.JSONDecodeError:
                    pass

            for raw in await service.drain_live_results(session_id):
                await websocket.send_json(json.loads(raw))

    except WebSocketDisconnect:
        pass
    except Exception as e:
        try:
            await websocket.send_json({"type": "error", "message": str(e)})
        except Exception:
            pass
    finally:
        await service.cleanup_live(session_id)


# ─── File upload + stream ───────────────────────────────────────────────

@router.websocket("/ws/transcribe")
async def file_transcription(websocket: WebSocket):
    await websocket.accept()

    user_id = _user_id_from_headers(websocket)
    if not user_id:
        await websocket.send_json({"type": "error", "message": "Missing user id"})
        await websocket.close(code=4001)
        return

    service = _service()
    session_id = service.make_file_session_id(user_id, id(websocket))

    try:
        await websocket.send_json({"type": "connected", "session_id": session_id})

        upload_done = False
        while not upload_done:
            try:
                message = await asyncio.wait_for(websocket.receive(), timeout=60.0)
            except asyncio.TimeoutError:
                await websocket.send_json({"type": "error", "message": "Upload timeout"})
                break
            except WebSocketDisconnect:
                break

            if message.get("type") == "websocket.disconnect":
                break

            if message.get("bytes"):
                await service.append_file_chunk(session_id, message["bytes"])
                await websocket.send_json({"type": "uploading"})

            elif message.get("text"):
                try:
                    data = json.loads(message["text"])
                    if data.get("type") == "end":
                        upload_done = True
                        await service.submit_file(session_id, user_id)
                        await websocket.send_json({"type": "processing"})
                except json.JSONDecodeError:
                    pass

        if upload_done:
            idle = 0
            max_idle = 240
            while idle < max_idle:
                raw = await service.pop_file_result(session_id)
                if raw:
                    data = json.loads(raw)
                    await websocket.send_json(data)
                    idle = 0
                    if data.get("type") == "done":
                        break
                else:
                    await asyncio.sleep(0.5)
                    idle += 1

    except WebSocketDisconnect:
        pass
    except Exception as e:
        try:
            await websocket.send_json({"type": "error", "message": str(e)})
        except Exception:
            pass
    finally:
        await service.cleanup_file(session_id)