"""
Task repository — async DB access for the Task entity.
"""
import uuid
from typing import Optional, Sequence

from sqlalchemy import select, func, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.Entities import Task


class TaskRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create(
        self,
        job_id: str,
        task_type: str,
        input_path: Optional[str] = None,
        chunk_index: Optional[int] = None,
    ) -> Task:
        task = Task(
            id=str(uuid.uuid4()),
            job_id=job_id,
            type=task_type,
            input_path=input_path,
            chunk_index=chunk_index,
        )
        self.session.add(task)
        await self.session.flush()
        return task

    async def create_batch(
        self, job_id: str, task_type: str, items: list[dict]
    ) -> list[Task]:
        tasks = [
            Task(
                id=str(uuid.uuid4()),
                job_id=job_id,
                type=task_type,
                input_path=item.get("input_path"),
                chunk_index=item.get("chunk_index"),
            )
            for item in items
        ]
        self.session.add_all(tasks)
        await self.session.flush()
        return tasks

    async def get(self, task_id: str) -> Optional[Task]:
        result = await self.session.execute(
            select(Task).where(Task.id == task_id)
        )
        return result.scalar_one_or_none()

    async def list_by_job(self, job_id: str) -> Sequence[Task]:
        result = await self.session.execute(
            select(Task)
            .where(Task.job_id == job_id)
            .order_by(Task.chunk_index.asc().nulls_last())
        )
        return result.scalars().all()

    async def list_pending(self, job_id: str, task_type: str) -> Sequence[Task]:
        result = await self.session.execute(
            select(Task)
            .where(
                Task.job_id == job_id,
                Task.type == task_type,
                Task.status == "pending",
            )
            .order_by(Task.chunk_index.asc().nulls_last())
        )
        return result.scalars().all()

    async def update_status(
        self, task_id: str, status: str, **fields
    ) -> Optional[Task]:
        values = {"status": status, **fields}
        if status == "running":
            values["started_at"] = func.now()
        elif status in ("completed", "failed"):
            values["completed_at"] = func.now()

        await self.session.execute(
            update(Task).where(Task.id == task_id).values(**values)
        )
        return await self.get(task_id)

    async def count_by_status(self, job_id: str) -> dict[str, int]:
        result = await self.session.execute(
            select(Task.status, func.count())
            .where(Task.job_id == job_id)
            .group_by(Task.status)
        )
        return {row[0]: row[1] for row in result.all()}