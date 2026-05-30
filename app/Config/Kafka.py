"""
Kafka producer + consumer factories.
Uses aiokafka for async support, matching the rest of the codebase.
"""
from typing import Optional

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

from app.Config.Config import config


_producer: Optional[AIOKafkaProducer] = None


async def get_producer() -> AIOKafkaProducer:
    """
    Lazy-init shared producer. One per process.
    Producers are thread-safe and high-throughput; reuse the same one.
    """
    global _producer
    if _producer is None:
        _producer = AIOKafkaProducer(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
            acks="all",                  # wait for full replication
            enable_idempotence=True,     # exactly-once semantics within session
            compression_type="gzip",
        )
        await _producer.start()
    return _producer


async def close_producer() -> None:
    global _producer
    if _producer is not None:
        await _producer.stop()
        _producer = None


def make_consumer(
    topics: list[str],
    group_id: str,
    auto_offset_reset: str = "earliest",
) -> AIOKafkaConsumer:
    """
    Create a fresh consumer. Each consumer owns its own connection.
    Caller is responsible for .start() and .stop().
    """
    return AIOKafkaConsumer(
        *topics,
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=False,        # we commit manually after handling
        value_deserializer=lambda v: v.decode("utf-8"),
    )