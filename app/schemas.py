from __future__ import annotations

import re
import uuid
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.core.enums import Channel, NotificationStatus, Priority

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
PHONE_RE = re.compile(r"^\+?[0-9]{6,20}$")


class NotificationCreate(BaseModel):
    channel: Channel
    priority: Priority = Priority.MARKETING
    message: str = Field(min_length=1, max_length=2000)
    recipients: list[str] = Field(min_length=1, max_length=10_000)

    @field_validator("recipients")
    @classmethod
    def _validate_recipients(cls, v: list[str]) -> list[str]:
        cleaned = [r.strip() for r in v if r and r.strip()]
        if not cleaned:
            raise ValueError("recipients must contain at least one non-empty value")
        if len(cleaned) != len(set(cleaned)):
            raise ValueError("recipients must be unique within a request")
        return cleaned

    def validate_recipients_for_channel(self) -> None:
        pattern = EMAIL_RE if self.channel == Channel.EMAIL else PHONE_RE
        bad = [r for r in self.recipients if not pattern.match(r)]
        if bad:
            sample = ", ".join(bad[:3])
            raise ValueError(
                f"invalid recipients for channel {self.channel.value}: {sample}"
                + (f" (+{len(bad) - 3} more)" if len(bad) > 3 else "")
            )


class NotificationRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    batch_id: uuid.UUID
    recipient_id: str
    channel: Channel
    priority: Priority
    message: str
    status: NotificationStatus
    attempts: int
    last_error: Optional[str] = None
    provider_message_id: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    sent_at: Optional[datetime] = None
    delivered_at: Optional[datetime] = None


class BatchAccepted(BaseModel):
    batch_id: uuid.UUID
    accepted: int
    duplicate: bool = False
    notification_ids: list[uuid.UUID]


class RecipientHistory(BaseModel):
    recipient_id: str
    total: int
    items: list[NotificationRead]


class DeliveryCallback(BaseModel):
    notification_id: uuid.UUID
    status: NotificationStatus
    provider_message_id: Optional[str] = None
    error: Optional[str] = None

    @field_validator("status")
    @classmethod
    def _allowed(cls, v: NotificationStatus) -> NotificationStatus:
        if v not in (NotificationStatus.DELIVERED, NotificationStatus.FAILED):
            raise ValueError("callback status must be 'delivered' or 'failed'")
        return v


class ErrorResponse(BaseModel):
    detail: str
