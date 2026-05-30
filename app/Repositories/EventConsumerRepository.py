"""
Consumes events from Kafka topics.
Replaces the old QueueRepository.pop_completed (and worker queue pops).
"""
import json
from typing import AsyncIterator

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord


class EventConsumer:
    """
    Wraps a single AIOKafkaConsumer.
    The caller owns lifecycle — start/stop is managed externally.
    """

    def __init__(self, consumer: AIOKafkaConsumer):
        self.consumer = consumer

    async def start(self) -> None:
        await self.consumer.start()

    async def stop(self) -> None:
        await self.consumer.stop()

    async def messages(self) -> AsyncIterator[dict]:
        """
        Yields decoded message dicts in arrival order.
        Caller is responsible for calling commit() after a message is fully handled.
        """
        async for record in self.consumer:
            yield self._decode(record)

    async def commit(self) -> None:
        """Commit current offsets after a message has been handled."""
        await self.consumer.commit()

    @staticmethod
    def _decode(record: ConsumerRecord) -> dict:
        """Parse the JSON payload from a Kafka record."""
        raw = record.value
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return json.loads(raw)