from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Annotated, Optional

from fastapi import Depends, Header, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_session_factory
from app.services.broker import Broker
from app.services.idempotency import IdempotencyStore
from app.services.notifications import NotificationService


async def db_session() -> AsyncIterator[AsyncSession]:
    factory = get_session_factory()
    async with factory() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise


def get_broker(request: Request) -> Broker:
    return request.app.state.broker


def get_idempotency(request: Request) -> IdempotencyStore:
    return request.app.state.idempotency


def get_notification_service(request: Request) -> NotificationService:
    return request.app.state.notification_service


def idempotency_key_header(
    idempotency_key: Annotated[
        Optional[str],
        Header(
            alias="Idempotency-Key",
            description="Client-supplied key to deduplicate retried batch creation calls.",
            max_length=255,
        ),
    ] = None,
) -> Optional[str]:
    if idempotency_key is None:
        return None
    cleaned = idempotency_key.strip()
    return cleaned or None


DbSession = Annotated[AsyncSession, Depends(db_session)]
NotificationServiceDep = Annotated[NotificationService, Depends(get_notification_service)]
IdempotencyKeyDep = Annotated[Optional[str], Depends(idempotency_key_header)]
