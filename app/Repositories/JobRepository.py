"""
Job repository — async DB access for the Job entity.
All SQL queries on `jobs` table go through here.
"""
import uuid
from typing import Optional, Sequence

from sqlalchemy import select, func, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.Entities import Job


class JobRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create(
        self,
        user_id: int,
        input_file_path: str,
        dialect: str = "auto",
        output_type: str = "all",
        subtitle_format: str = "srt",
        burn_subtitles: bool = False,
    ) -> Job:
        job = Job(
            id=str(uuid.uuid4()),
            user_id=user_id,
            input_file_path=input_file_path,
            dialect=dialect,
            output_type=output_type,
            subtitle_format=subtitle_format,
            burn_subtitles=burn_subtitles,
        )
        self.session.add(job)
        await self.session.flush()
        return job

    async def get(self, job_id: str) -> Optional[Job]:
        result = await self.session.execute(
            select(Job).where(Job.id == job_id)
        )
        return result.scalar_one_or_none()

    async def list_by_user(
        self, user_id: int, limit: int = 50, offset: int = 0
    ) -> Sequence[Job]:
        result = await self.session.execute(
            select(Job)
            .where(Job.user_id == user_id)
            .order_by(Job.created_at.desc())
            .limit(limit)
            .offset(offset)
        )
        return result.scalars().all()

    async def count_by_user(self, user_id: int) -> int:
        result = await self.session.execute(
            select(func.count()).select_from(Job).where(Job.user_id == user_id)
        )
        return result.scalar_one()

    async def update_status(
        self, job_id: str, status: str, **fields
    ) -> Optional[Job]:
        values = {"status": status, **fields}

        # Stamp lifecycle timestamps automatically
        if status == "extracting":
            values["started_at"] = func.now()
        elif status in ("completed", "failed"):
            values["completed_at"] = func.now()

        await self.session.execute(
            update(Job).where(Job.id == job_id).values(**values)
        )
        return await self.get(job_id)