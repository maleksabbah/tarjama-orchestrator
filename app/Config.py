"""
Orchestrator Configuration
"""
import os


class Config:
    # Database
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:Saras2345%40@localhost:5432/orchestrator_db")
    DB_POOL_MIN = int(os.getenv("DB_POOL_MIN", "2"))
    DB_POOL_MAX = int(os.getenv("DB_POOL_MAX", "10"))

    # Redis
    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    # Queues
    QUEUE_MEDIA = "queue:media"
    QUEUE_TRANSCRIBE = "queue:transcribe"
    QUEUE_SUBTITLE = "queue:subtitle"
    QUEUE_COMPLETED = "queue:completed"

    # Retry
    MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

    # Server
    HOST = os.getenv("HOST", "0.0.0.0")
    PORT = int(os.getenv("PORT", "8001"))


config = Config()