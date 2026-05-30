from datetime import datetime
from sqlalchemy import Boolean, Float, Integer, String, Text, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.Entities.Base import Base


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(50), default="queued", index=True)
    input_file_path: Mapped[str] = mapped_column(Text, nullable=False)
    input_duration: Mapped[float | None] = mapped_column(Float)
    dialect: Mapped[str] = mapped_column(String(50), default="auto")
    output_type: Mapped[str] = mapped_column(String(50), default="all")
    subtitle_format: Mapped[str] = mapped_column(String(10), default="srt")
    burn_subtitles: Mapped[bool] = mapped_column(Boolean, default=False)
    transcript_path: Mapped[str | None] = mapped_column(Text)
    subtitle_path: Mapped[str | None] = mapped_column(Text)
    video_output_path: Mapped[str | None] = mapped_column(Text)
    error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    started_at: Mapped[datetime | None] = mapped_column(DateTime)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )

    tasks: Mapped[list["Task"]] = relationship(back_populates="job", cascade="all, delete-orphan")