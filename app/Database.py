"""
Orchestrator Database
asyncpg connection pool + raw SQL for jobs and tasks tables.
"""
import uuid
import asyncpg
from app.Config import config


pool: asyncpg.Pool = None


async def init_db():
    """Create connection pool and ensure tables exist."""
    global pool
    pool = await asyncpg.create_pool(
        config.DATABASE_URL,
        min_size=config.DB_POOL_MIN,
        max_size=config.DB_POOL_MAX,
    )

    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id VARCHAR(36) PRIMARY KEY,
                user_id INTEGER NOT NULL,
                status VARCHAR(50) DEFAULT 'queued',
                input_file_path TEXT NOT NULL,
                input_duration FLOAT,
                dialect VARCHAR(50) DEFAULT 'auto',
                output_type VARCHAR(50) DEFAULT 'all',
                subtitle_format VARCHAR(10) DEFAULT 'srt',
                burn_subtitles BOOLEAN DEFAULT false,
                transcript_path TEXT,
                subtitle_path TEXT,
                video_output_path TEXT,
                error TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                id VARCHAR(36) PRIMARY KEY,
                job_id VARCHAR(36) NOT NULL REFERENCES jobs(id),
                type VARCHAR(50) NOT NULL,
                status VARCHAR(50) DEFAULT 'pending',
                input_path TEXT,
                output_path TEXT,
                chunk_index INTEGER,
                retries INTEGER DEFAULT 0,
                error TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                started_at TIMESTAMP,
                completed_at TIMESTAMP
            )
        """)

        # Index for fast lookups
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_jobs_user_id ON jobs(user_id)
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_tasks_job_id ON tasks(job_id)
        """)


async def close_db():
    global pool
    if pool:
        await pool.close()


def gen_id() -> str:
    return str(uuid.uuid4())


# =============================================================================
# Job queries
# =============================================================================

async def create_job(user_id: int, input_file_path: str, dialect: str = "auto",
                     output_type: str = "all", subtitle_format: str = "srt",
                     burn_subtitles: bool = False) -> dict:
    job_id = gen_id()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO jobs (id, user_id, input_file_path, dialect, output_type,
                              subtitle_format, burn_subtitles)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING *
            """,
            job_id, user_id, input_file_path, dialect, output_type,
            subtitle_format, burn_subtitles,
        )
        return dict(row)


async def get_job(job_id: str) -> dict | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM jobs WHERE id = $1", job_id)
        return dict(row) if row else None


async def get_jobs_by_user(user_id: int, limit: int = 50, offset: int = 0) -> list:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT * FROM jobs WHERE user_id = $1
            ORDER BY created_at DESC LIMIT $2 OFFSET $3
            """,
            user_id, limit, offset,
        )
        return [dict(r) for r in rows]


async def update_job_status(job_id: str, status: str, **kwargs) -> dict | None:
    """Update job status and any additional fields."""
    set_clauses = ["status = $2", "updated_at = NOW()"]
    values = [job_id, status]
    idx = 3

    # Add optional timestamp fields based on status
    if status == "extracting":
        set_clauses.append("started_at = NOW()")
    elif status in ("completed", "failed"):
        set_clauses.append("completed_at = NOW()")

    # Add any extra fields
    for key, value in kwargs.items():
        set_clauses.append(f"{key} = ${idx}")
        values.append(value)
        idx += 1

    query = f"UPDATE jobs SET {', '.join(set_clauses)} WHERE id = $1 RETURNING *"
    async with pool.acquire() as conn:
        row = await conn.fetchrow(query, *values)
        return dict(row) if row else None


async def count_user_jobs(user_id: int) -> int:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT COUNT(*) as count FROM jobs WHERE user_id = $1", user_id
        )
        return row["count"]


# =============================================================================
# Task queries
# =============================================================================

async def create_task(job_id: str, task_type: str, input_path: str = None,
                      chunk_index: int = None) -> dict:
    task_id = gen_id()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO tasks (id, job_id, type, input_path, chunk_index)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING *
            """,
            task_id, job_id, task_type, input_path, chunk_index,
        )
        return dict(row)


async def create_tasks_batch(job_id: str, task_type: str,
                             items: list[dict]) -> list[dict]:
    """Create multiple tasks at once (e.g., 120 transcription tasks)."""
    tasks = []
    async with pool.acquire() as conn:
        for item in items:
            task_id = gen_id()
            row = await conn.fetchrow(
                """
                INSERT INTO tasks (id, job_id, type, input_path, chunk_index)
                VALUES ($1, $2, $3, $4, $5)
                RETURNING *
                """,
                task_id, job_id, task_type,
                item.get("input_path"), item.get("chunk_index"),
            )
            tasks.append(dict(row))
    return tasks


async def update_task_status(task_id: str, status: str, **kwargs) -> dict | None:
    set_clauses = ["status = $2"]
    values = [task_id, status]
    idx = 3

    if status == "running":
        set_clauses.append("started_at = NOW()")
    elif status in ("completed", "failed"):
        set_clauses.append("completed_at = NOW()")

    for key, value in kwargs.items():
        set_clauses.append(f"{key} = ${idx}")
        values.append(value)
        idx += 1

    query = f"UPDATE tasks SET {', '.join(set_clauses)} WHERE id = $1 RETURNING *"
    async with pool.acquire() as conn:
        row = await conn.fetchrow(query, *values)
        return dict(row) if row else None


async def get_tasks_by_job(job_id: str) -> list:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM tasks WHERE job_id = $1 ORDER BY chunk_index NULLS LAST",
            job_id,
        )
        return [dict(r) for r in rows]


async def count_tasks_by_status(job_id: str) -> dict:
    """Count tasks by status for a job. Used to check if all chunks are done."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT status, COUNT(*) as count
            FROM tasks WHERE job_id = $1
            GROUP BY status
            """,
            job_id,
        )
        return {r["status"]: r["count"] for r in rows}


async def get_pending_tasks(job_id: str, task_type: str) -> list:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT * FROM tasks
            WHERE job_id = $1 AND type = $2 AND status = 'pending'
            ORDER BY chunk_index NULLS LAST
            """,
            job_id, task_type,
        )
        return [dict(r) for r in rows]