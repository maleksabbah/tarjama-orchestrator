from datetime import datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict


class TaskResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    job_id: str
    type: str
    status: str
    input_path: Optional[str]
    output_path: Optional[str]
    chunk_index: Optional[int]
    retries: int
    error: Optional[str]
    created_at: datetime
    started_at: Optional[datetime]
    completed_at: Optional[datetime]


class TaskListResponse(BaseModel):
    tasks: list[TaskResponse]
    total: int