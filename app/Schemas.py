"""
Orchestrator Schemas
"""
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime

class CreateJobRequest(BaseModel):
    user_id: int
    file_path: str
    dialect: Optional[str] = "auto"
    output_type: Optional[str] = "all"
    subtitle_format: Optional[str] = "srt"
    burn_subtitles: Optional[bool] = False

class JobResponse(BaseModel):
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
    jobs: List[JobResponse]
    total: int

class ProgressResponse(BaseModel):
    job_id: str
    status: str
    total_chunks: int
    completed_chunks: int
    failed_chunks: int
    started_at: Optional[str]
class TaskResponse(BaseModel):
    id: str
    job_id: int
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
