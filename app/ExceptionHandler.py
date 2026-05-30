"""
Domain exceptions + FastAPI handler.
Services raise DomainException subclasses; the handler translates them to HTTP.
"""
import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse


logger = logging.getLogger(__name__)


# ─── Base ──────────────────────────────────────────────────────────────

class DomainException(Exception):
    status_code: int = 500
    code: str = "internal_error"

    def __init__(self, message: str, **details):
        super().__init__(message)
        self.message = message
        self.details = details


# ─── Job ───────────────────────────────────────────────────────────────

class JobNotFound(DomainException):
    status_code = 404
    code = "job_not_found"

    def __init__(self, job_id: str):
        super().__init__(f"Job {job_id} not found", job_id=job_id)


class JobForbidden(DomainException):
    status_code = 403
    code = "job_forbidden"

    def __init__(self, job_id: str):
        super().__init__(f"Job {job_id} does not belong to this user", job_id=job_id)


class JobAlreadyTerminal(DomainException):
    status_code = 400
    code = "job_already_terminal"

    def __init__(self, job_id: str, status: str):
        super().__init__(
            f"Job {job_id} already {status}",
            job_id=job_id,
            current_status=status,
        )


# ─── Auth / quota / validation ─────────────────────────────────────────

class Unauthorized(DomainException):
    status_code = 401
    code = "unauthorized"


class Forbidden(DomainException):
    status_code = 403
    code = "forbidden"


class QuotaExceeded(DomainException):
    status_code = 403
    code = "quota_exceeded"


class RateLimited(DomainException):
    status_code = 429
    code = "rate_limited"


class ValidationError(DomainException):
    status_code = 400
    code = "validation_error"


# ─── FastAPI handlers ──────────────────────────────────────────────────

async def domain_exception_handler(
    request: Request, exc: DomainException
) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": {
                "code": exc.code,
                "message": exc.message,
                "details": exc.details,
            }
        },
    )


async def unhandled_exception_handler(
    request: Request, exc: Exception
) -> JSONResponse:
    logger.exception("Unhandled exception", exc_info=exc)
    return JSONResponse(
        status_code=500,
        content={
            "error": {
                "code": "internal_error",
                "message": "An unexpected error occurred.",
                "details": {},
            }
        },
    )


def register_exception_handlers(app: FastAPI) -> None:
    """Wire all exception handlers onto the app. Call once in main.py."""
    app.add_exception_handler(DomainException, domain_exception_handler)
    app.add_exception_handler(Exception, unhandled_exception_handler)