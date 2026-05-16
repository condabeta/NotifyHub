from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException, Query, Response, status

from app.api.deps import DbSession, IdempotencyKeyDep, NotificationServiceDep
from app.schemas import (
    BatchAccepted,
    DeliveryCallback,
    NotificationCreate,
    NotificationRead,
    RecipientHistory,
)
from app.services.idempotency import IdempotencyConflict

router = APIRouter(prefix="/api/v1", tags=["notifications"])


@router.post(
    "/notifications",
    response_model=BatchAccepted,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit a batch of notifications",
    responses={
        409: {"description": "Idempotency-Key reuse with a different payload"},
        422: {"description": "Invalid request payload"},
    },
)
async def create_batch(
    payload: NotificationCreate,
    session: DbSession,
    service: NotificationServiceDep,
    idempotency_key: IdempotencyKeyDep,
    response: Response,
) -> BatchAccepted:
    try:
        result = await service.create_batch(session, payload, idempotency_key)
    except IdempotencyConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    if result.duplicate:
        response.status_code = status.HTTP_200_OK
    return result


@router.get(
    "/notifications/{notification_id}",
    response_model=NotificationRead,
    summary="Fetch a single notification by ID",
)
async def get_notification(
    notification_id: uuid.UUID,
    session: DbSession,
    service: NotificationServiceDep,
) -> NotificationRead:
    found = await service.get_by_id(session, notification_id)
    if found is None:
        raise HTTPException(status_code=404, detail="notification not found")
    return found


@router.get(
    "/recipients/{recipient_id}/notifications",
    response_model=RecipientHistory,
    summary="List notification history for a recipient",
)
async def list_recipient_notifications(
    recipient_id: str,
    session: DbSession,
    service: NotificationServiceDep,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> RecipientHistory:
    items = await service.list_by_recipient(session, recipient_id, limit, offset)
    total = await service.count_by_recipient(session, recipient_id)
    return RecipientHistory(recipient_id=recipient_id, total=total, items=items)


@router.post(
    "/notifications/callbacks/delivery",
    response_model=NotificationRead,
    summary="Provider callback to record delivered/failed status",
)
async def delivery_callback(
    payload: DeliveryCallback,
    session: DbSession,
    service: NotificationServiceDep,
) -> NotificationRead:
    updated = await service.apply_callback(
        session,
        notification_id=payload.notification_id,
        new_status=payload.status,
        provider_message_id=payload.provider_message_id,
        error=payload.error,
    )
    await session.commit()
    if updated is None:
        raise HTTPException(status_code=404, detail="notification not found")
    return updated


@router.get("/health", tags=["health"], summary="Liveness probe")
async def health() -> dict[str, str]:
    return {"status": "ok"}
