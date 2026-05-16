from __future__ import annotations

import asyncio
import json
import logging
import signal
import uuid
from datetime import datetime
from typing import Optional

from aio_pika.abc import AbstractIncomingMessage

from app.config import Settings, get_settings
from app.core.enums import Channel, NotificationStatus
from app.core.logging import configure_logging
from app.database import dispose_engine, init_engine, session_scope
from app.models import Notification
from app.providers.base import (
    ProviderOutcomeStatus,
    ProviderResult,
    TransientProviderError,
)
from app.providers.registry import ProviderRegistry
from app.services.broker import Broker

logger = logging.getLogger(__name__)


class Worker:
    """Consumes notification messages and drives them through the provider."""

    def __init__(
        self,
        settings: Settings,
        broker: Broker,
        registry: ProviderRegistry,
    ) -> None:
        self.settings = settings
        self.broker = broker
        self.registry = registry
        self._stop = asyncio.Event()
        self._pending: set[asyncio.Task] = set()

    async def run(self) -> None:
        await self.broker.connect()
        queue = self.broker.queue
        logger.info(
            "worker consuming",
            extra={
                "queue": self.settings.notifications_queue,
                "prefetch": self.settings.worker_prefetch,
            },
        )
        async with queue.iterator() as it:
            async for message in it:
                if self._stop.is_set():
                    await message.nack(requeue=True)
                    break
                await self._handle(message)

        # Wait for in-flight delivery confirmation tasks to settle.
        if self._pending:
            await asyncio.gather(*self._pending, return_exceptions=True)

    def request_stop(self) -> None:
        logger.info("worker stop requested")
        self._stop.set()

    async def _handle(self, message: AbstractIncomingMessage) -> None:
        try:
            body = json.loads(message.body)
            notification_id = uuid.UUID(body["notification_id"])
        except Exception:
            logger.exception("worker dropping malformed message")
            await message.reject(requeue=False)
            return

        try:
            await self._process(notification_id, body)
            await message.ack()
        except TransientProviderError as exc:
            requeue, attempts = await self._record_transient_failure(
                notification_id, str(exc)
            )
            if requeue:
                await self._backoff(attempts)
            await message.nack(requeue=requeue)
        except Exception:
            logger.exception(
                "worker unexpected error processing message",
                extra={"notification_id": str(notification_id)},
            )
            _, attempts = await self._record_transient_failure(
                notification_id, "internal worker error"
            )
            await self._backoff(attempts)
            await message.nack(requeue=True)

    async def _process(self, notification_id: uuid.UUID, body: dict) -> None:
        async with session_scope() as session:
            row = await session.get(
                Notification, notification_id, with_for_update=True
            )
            if row is None:
                logger.warning(
                    "worker received message for unknown notification",
                    extra={"notification_id": str(notification_id)},
                )
                return

            if row.status in (
                NotificationStatus.SENT,
                NotificationStatus.DELIVERED,
                NotificationStatus.FAILED,
            ):
                logger.info(
                    "worker skipping already-processed notification",
                    extra={
                        "notification_id": str(notification_id),
                        "status": row.status,
                    },
                )
                return

            row.attempts += 1
            channel = Channel(row.channel)
            provider = self.registry.get(channel)
            recipient = row.recipient_id
            message_text = row.message
            current_attempts = row.attempts

        # Call the provider outside the transaction to avoid holding the DB
        # row lock during slow gateway calls.
        try:
            result: ProviderResult = await provider.send(
                notification_id, recipient, message_text
            )
        except TransientProviderError:
            raise

        async with session_scope() as session:
            row = await session.get(
                Notification, notification_id, with_for_update=True
            )
            if row is None:
                return
            row.provider_message_id = result.provider_message_id
            if result.status == ProviderOutcomeStatus.PERMANENT_FAILURE:
                row.status = NotificationStatus.FAILED
                row.last_error = result.error
                logger.info(
                    "notification permanently failed",
                    extra={
                        "notification_id": str(notification_id),
                        "attempts": current_attempts,
                    },
                )
            elif result.status == ProviderOutcomeStatus.DELIVERED:
                row.status = NotificationStatus.DELIVERED
                row.sent_at = row.sent_at or datetime.utcnow()
                row.delivered_at = datetime.utcnow()
                row.last_error = None
            else:  # ACCEPTED
                row.status = NotificationStatus.SENT
                row.sent_at = datetime.utcnow()
                row.last_error = None
                self._schedule_async_delivery(notification_id)

    async def _record_transient_failure(
        self, notification_id: uuid.UUID, error: str
    ) -> tuple[bool, int]:
        """Update DB with the error; return (should_requeue, attempts).

        Once the persisted attempt count reaches the configured maximum we mark
        the notification as failed and stop requeuing.
        """
        async with session_scope() as session:
            row = await session.get(
                Notification, notification_id, with_for_update=True
            )
            if row is None:
                return False, 0
            row.last_error = error
            if row.attempts >= self.settings.worker_max_attempts:
                row.status = NotificationStatus.FAILED
                logger.warning(
                    "notification giving up after retries",
                    extra={
                        "notification_id": str(notification_id),
                        "attempts": row.attempts,
                    },
                )
                return False, row.attempts
            logger.info(
                "notification transient failure, requeueing",
                extra={
                    "notification_id": str(notification_id),
                    "attempts": row.attempts,
                    "error": error,
                },
            )
            return True, row.attempts

    async def _backoff(self, attempts: int) -> None:
        delay_ms = min(
            self.settings.worker_retry_base_delay_ms * (2 ** max(attempts - 1, 0)),
            self.settings.worker_retry_max_delay_ms,
        )
        await asyncio.sleep(delay_ms / 1000.0)

    def _schedule_async_delivery(self, notification_id: uuid.UUID) -> None:
        task = asyncio.create_task(self._async_deliver(notification_id))
        self._pending.add(task)
        task.add_done_callback(self._pending.discard)

    async def _async_deliver(self, notification_id: uuid.UUID) -> None:
        await asyncio.sleep(self.settings.provider_delivery_delay_ms / 1000.0)
        async with session_scope() as session:
            row = await session.get(
                Notification, notification_id, with_for_update=True
            )
            if row is None or row.status != NotificationStatus.SENT:
                return
            row.status = NotificationStatus.DELIVERED
            row.delivered_at = datetime.utcnow()


async def run(worker: Optional[Worker] = None) -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    init_engine(settings)

    broker = Broker(settings)
    registry = ProviderRegistry.from_settings(settings)
    worker = worker or Worker(settings, broker, registry)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, worker.request_stop)
        except NotImplementedError:
            # Signals aren't supported on Windows event loops.
            pass

    try:
        await worker.run()
    finally:
        await broker.close()
        await dispose_engine()
