from __future__ import annotations

import abc
import uuid
from dataclasses import dataclass
from enum import StrEnum


class ProviderOutcomeStatus(StrEnum):
    ACCEPTED = "accepted"
    DELIVERED = "delivered"
    PERMANENT_FAILURE = "permanent_failure"


@dataclass(frozen=True)
class ProviderResult:
    status: ProviderOutcomeStatus
    provider_message_id: str
    error: str | None = None


class TransientProviderError(Exception):
    """Raised when the provider is temporarily unavailable; the caller should retry."""


class Provider(abc.ABC):
    name: str

    @abc.abstractmethod
    async def send(
        self,
        notification_id: uuid.UUID,
        recipient: str,
        message: str,
    ) -> ProviderResult: ...
