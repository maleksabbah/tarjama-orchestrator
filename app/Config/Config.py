"""
Orchestrator app-wide config.
Reads from environment variables.
"""
import os


class Config:
    # Postgres
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:postgres@postgres:5432/orchestrator_db",
    )
    DB_POOL_MIN: int = int(os.getenv("DB_POOL_MIN", "2"))
    DB_POOL_MAX: int = int(os.getenv("DB_POOL_MAX", "10"))

    # Redis
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://redis:6379/0")

    # Service URLs (for talking to other microservices)
    STORAGE_URL: str = os.getenv("STORAGE_URL", "http://storage:8002")

    # Queue names
    QUEUE_MEDIA: str = os.getenv("QUEUE_MEDIA", "queue:media")
    QUEUE_TRANSCRIBE: str = os.getenv("QUEUE_TRANSCRIBE", "queue:transcribe")
    QUEUE_SUBTITLE: str = os.getenv("QUEUE_SUBTITLE", "queue:subtitle")
    QUEUE_COMPLETED: str = os.getenv("QUEUE_COMPLETED", "queue:completed")

    # Retry policy
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

    # Topic names
    TOPIC_MEDIA_TASKS: str = os.getenv("TOPIC_MEDIA_TASKS", "tarjama.media.tasks")
    TOPIC_TRANSCRIBE_TASKS: str = os.getenv("TOPIC_TRANSCRIBE_TASKS", "tarjama.transcribe.tasks")
    TOPIC_SUBTITLE_TASKS: str = os.getenv("TOPIC_SUBTITLE_TASKS", "tarjama.subtitle.tasks")
    TOPIC_COMPLETED: str = os.getenv("TOPIC_COMPLETED", "tarjama.completed")

    # Consumer group IDs
    GROUP_ORCHESTRATOR: str = os.getenv("GROUP_ORCHESTRATOR", "tarjama.orchestrator")
    GROUP_MEDIA_WORKER: str = os.getenv("GROUP_MEDIA_WORKER", "tarjama.media")
    GROUP_TRANSCRIBE_WORKER: str = os.getenv("GROUP_TRANSCRIBE_WORKER", "tarjama.transcribe")
    GROUP_SUBTITLE_WORKER: str = os.getenv("GROUP_SUBTITLE_WORKER", "tarjama.subtitle")


config = Config()