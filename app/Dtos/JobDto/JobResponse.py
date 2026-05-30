from datetime import datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict


class JobResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    user_id: int
    status: str
    input_file_path: str
    dialect: Optional[str]
    output_type: str
    subtitle_format: str
    burn_subtitles: bool
    transcript_path: Optional[str]
    subtitle_path: Optional[str]
    video_output_path: Optional[str]
    error: Optional[str]
    created_at: datetime
    started_at: Optional[datetime]
    completed_at: Optional[datetime]


class JobListResponse(BaseModel):
    jobs: list[JobResponse]
    total: int


class JobProgressResponse(BaseModel):
    job_id: str
    status: str
    total_chunks: int
    completed_chunks: int
    failed_chunks: int
    started_at: Optional[str] = None