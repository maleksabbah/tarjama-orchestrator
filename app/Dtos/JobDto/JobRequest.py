from typing import Optional
from pydantic import BaseModel


class CreateJobRequest(BaseModel):
    user_id: int
    file_path: str
    dialect: Optional[str] = "auto"
    output_type: Optional[str] = "all"
    subtitle_format: Optional[str] = "srt"
    burn_subtitles: Optional[bool] = False