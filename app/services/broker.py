from __future__ import annotations

import json
import logging
import uuid
from typing import Optional

import aio_pika
from aio_pika.abc import AbstractRobustChannel, AbstractRobustConnection, AbstractRobustQueue

from app.config import Settings
from app.core.enums import PRIORITY_RANK, Channel, Priority

logger = logging.getLogger(__name__)


class Broker:
    """Thin wrapper around a robust RabbitMQ connection with a priority queue."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._connection: Optional[AbstractRobustConnection] = None
        self._channel: Optional[AbstractRobustChannel] = None
        self._queue: Optional[AbstractRobustQueue] = None

    async def connect(self) -> None:
        if self._connection is not None and not self._connection.is_closed:
            return
        self._connection = await aio_pika.connect_robust(self.settings.rabbitmq_dsn)
        self._channel = await self._connection.channel()
        await self._channel.set_qos(prefetch_count=self.settings.worker_prefetch)
        self._queue = await self._channel.declare_queue(
            self.settings.notifications_queue,
            durable=True,
            arguments={"x-max-priority": self.settings.notifications_max_priority},
        )
        logger.info(
            "broker connected",
            extra={"queue": self.settings.notifications_queue},
        )

    async def close(self) -> None:
        if self._connection is not None and not self._connection.is_closed:
            await self._connection.close()
        self._connection = None
        self._channel = None
        self._queue = None

    @property
    def channel(self) -> AbstractRobustChannel:
        if self._channel is None:
            raise RuntimeError("Broker is not connected")
        return self._channel

    @property
    def queue(self) -> AbstractRobustQueue:
        if self._queue is None:
            raise RuntimeError("Broker is not connected")
        return self._queue

    async def publish_notification(
        self,
        *,
        notification_id: uuid.UUID,
        batch_id: uuid.UUID,
        recipient_id: str,
        channel: Channel,
        priority: Priority,
        message: str,
    ) -> None:
        body = {
            "notification_id": str(notification_id),
            "batch_id": str(batch_id),
            "recipient_id": recipient_id,
            "channel": channel.value,
            "priority": priority.value,
            "message": message,
        }
        msg = aio_pika.Message(
            body=json.dumps(body).encode("utf-8"),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            priority=PRIORITY_RANK[priority],
            message_id=str(notification_id),
            headers={"x-batch-id": str(batch_id)},
        )
        await self.channel.default_exchange.publish(
            msg, routing_key=self.settings.notifications_queue
        )
