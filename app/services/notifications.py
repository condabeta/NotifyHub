from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import desc, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import Channel, NotificationStatus, Priority
from app.models import Notification, NotificationBatch
from app.schemas import BatchAccepted, NotificationCreate, NotificationRead
from app.services.broker import Broker
from app.services.idempotency import IdempotencyConflict, IdempotencyStore

logger = logging.getLogger(__name__)


class NotificationService:
    """Coordinates batch creation, queue dispatch, status updates and queries."""

    def __init__(self, broker: Broker, idempotency: IdempotencyStore) -> None:
        self.broker = broker
        self.idempotency = idempotency

    async def create_batch(
        self,
        session: AsyncSession,
        payload: NotificationCreate,
        idempotency_key: Optional[str],
    ) -> BatchAccepted:
        payload.validate_recipients_for_channel()

        cache_payload = payload.model_dump(mode="json")

        if idempotency_key:
            cached = await self.idempotency.get(idempotency_key, cache_payload)
            if cached is not None:
                cached["duplicate"] = True
                return BatchAccepted.model_validate(cached)

        batch = await self._upsert_batch(session, payload, idempotency_key)
        notifications = await self._upsert_notifications(session, batch, payload)
        await session.flush()
        await session.commit()

        published_ids: list[uuid.UUID] = []
        for n in notifications:
            if n.status == NotificationStatus.QUEUED:
                await self.broker.publish_notification(
                    notification_id=n.id,
                    batch_id=n.batch_id,
                    recipient_id=n.recipient_id,
                    channel=Channel(n.channel),
                    priority=Priority(n.priority),
                    message=n.message,
                )
            published_ids.append(n.id)

        response = BatchAccepted(
            batch_id=batch.id,
            accepted=len(notifications),
            duplicate=False,
            notification_ids=published_ids,
        )

        if idempotency_key:
            await self.idempotency.save(
                idempotency_key, cache_payload, response.model_dump(mode="json")
            )

        return response

    async def _upsert_batch(
        self,
        session: AsyncSession,
        payload: NotificationCreate,
        idempotency_key: Optional[str],
    ) -> NotificationBatch:
        if idempotency_key:
            existing = (
                await session.execute(
                    select(NotificationBatch).where(
                        NotificationBatch.idempotency_key == idempotency_key
                    )
                )
            ).scalar_one_or_none()
            if existing is not None:
                return existing

        batch = NotificationBatch(
            id=uuid.uuid4(),
            idempotency_key=idempotency_key,
            channel=payload.channel,
            priority=payload.priority,
            message=payload.message,
            total=len(payload.recipients),
        )
        session.add(batch)
        try:
            await session.flush()
            return batch
        except IntegrityError:
            await session.rollback()
            # Another caller inserted with the same idempotency_key concurrently;
            # the second-write SELECT must succeed by uniqueness invariant.
            if not idempotency_key:
                raise
            existing = (
                await session.execute(
                    select(NotificationBatch).where(
                        NotificationBatch.idempotency_key == idempotency_key
                    )
                )
            ).scalar_one()
            return existing

    async def _upsert_notifications(
        self,
        session: AsyncSession,
        batch: NotificationBatch,
        payload: NotificationCreate,
    ) -> list[Notification]:
        rows = [
            {
                "id": uuid.uuid4(),
                "batch_id": batch.id,
                "recipient_id": r,
                "channel": payload.channel.value,
                "priority": payload.priority.value,
                "message": payload.message,
                "status": NotificationStatus.QUEUED.value,
                "attempts": 0,
            }
            for r in payload.recipients
        ]
        stmt = pg_insert(Notification).values(rows)
        stmt = stmt.on_conflict_do_nothing(
            constraint="uq_notifications_batch_recipient"
        )
        await session.execute(stmt)

        fetched_q = await session.execute(
            select(Notification)
            .where(Notification.batch_id == batch.id)
            .order_by(Notification.created_at)
        )
        return list(fetched_q.scalars().all())

    async def get_by_id(
        self, session: AsyncSession, notification_id: uuid.UUID
    ) -> Optional[NotificationRead]:
        row = await session.get(Notification, notification_id)
        if row is None:
            return None
        return NotificationRead.model_validate(row)

    async def list_by_recipient(
        self,
        session: AsyncSession,
        recipient_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> list[NotificationRead]:
        q = (
            select(Notification)
            .where(Notification.recipient_id == recipient_id)
            .order_by(desc(Notification.created_at))
            .limit(limit)
            .offset(offset)
        )
        result = await session.execute(q)
        return [NotificationRead.model_validate(r) for r in result.scalars().all()]

    async def count_by_recipient(self, session: AsyncSession, recipient_id: str) -> int:
        from sqlalchemy import func

        q = select(func.count(Notification.id)).where(
            Notification.recipient_id == recipient_id
        )
        return int((await session.execute(q)).scalar_one())

    async def apply_callback(
        self,
        session: AsyncSession,
        notification_id: uuid.UUID,
        new_status: NotificationStatus,
        provider_message_id: Optional[str],
        error: Optional[str],
    ) -> Optional[NotificationRead]:
        row = await session.get(Notification, notification_id, with_for_update=True)
        if row is None:
            return None

        if new_status == NotificationStatus.DELIVERED:
            if row.status in (
                NotificationStatus.SENT,
                NotificationStatus.DELIVERED,
            ):
                row.status = NotificationStatus.DELIVERED
                row.delivered_at = row.delivered_at or datetime.utcnow()
        elif new_status == NotificationStatus.FAILED:
            row.status = NotificationStatus.FAILED
            row.last_error = error

        if provider_message_id:
            row.provider_message_id = provider_message_id

        await session.flush()
        return NotificationRead.model_validate(row)


__all__ = ["NotificationService", "IdempotencyConflict"]
