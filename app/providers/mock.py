from __future__ import annotations

import asyncio
import logging
import random
import re
import uuid
from typing import Optional

from app.config import Settings
from app.core.enums import Channel
from app.providers.base import (
    Provider,
    ProviderOutcomeStatus,
    ProviderResult,
    TransientProviderError,
)

logger = logging.getLogger(__name__)

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
PHONE_RE = re.compile(r"^\+?[0-9]{6,20}$")


class MockProvider(Provider):
    """Deterministic-ish mock provider used in place of a real SMS/Email gateway.

    Behaviour is driven by the recipient identifier, so tests can pin outcomes:
      - recipient ending in "@fail.test" or "+0000000" → permanent failure
      - recipient ending in "@retry.test" or "+1111111" → transient error
      - recipient ending in "@async.test" → accepted (delivery confirmed async)
      - everything else → delivered immediately (with small random jitter)
    Otherwise the provider injects configured background fail rates.
    """

    def __init__(
        self,
        name: str,
        channel: Channel,
        *,
        transient_fail_rate: float = 0.0,
        permanent_fail_rate: float = 0.0,
        delivery_delay_ms: int = 0,
        rng: Optional[random.Random] = None,
    ) -> None:
        self.name = name
        self.channel = channel
        self.transient_fail_rate = transient_fail_rate
        self.permanent_fail_rate = permanent_fail_rate
        self.delivery_delay_ms = delivery_delay_ms
        self._rng = rng or random.Random()

    @classmethod
    def for_channel(cls, channel: Channel, settings: Settings) -> "MockProvider":
        name = f"mock-{channel.value}"
        return cls(
            name=name,
            channel=channel,
            transient_fail_rate=settings.provider_transient_fail_rate,
            permanent_fail_rate=settings.provider_permanent_fail_rate,
            delivery_delay_ms=settings.provider_delivery_delay_ms,
        )

    def _validate_recipient(self, recipient: str) -> None:
        pattern = EMAIL_RE if self.channel == Channel.EMAIL else PHONE_RE
        if not pattern.match(recipient):
            raise PermanentRecipientError(recipient)

    async def send(
        self,
        notification_id: uuid.UUID,
        recipient: str,
        message: str,
    ) -> ProviderResult:
        if self.delivery_delay_ms:
            jitter = self._rng.uniform(0.5, 1.5)
            await asyncio.sleep((self.delivery_delay_ms / 1000.0) * jitter)

        try:
            self._validate_recipient(recipient)
        except PermanentRecipientError:
            return ProviderResult(
                status=ProviderOutcomeStatus.PERMANENT_FAILURE,
                provider_message_id=f"{self.name}:invalid",
                error=f"invalid recipient: {recipient}",
            )

        # Recipient-driven deterministic behaviour for tests.
        if recipient.endswith("@fail.test") or recipient == "+00000000000":
            return ProviderResult(
                status=ProviderOutcomeStatus.PERMANENT_FAILURE,
                provider_message_id=f"{self.name}:rejected",
                error="recipient rejected by provider",
            )
        if recipient.endswith("@retry.test") or recipient == "+11111111111":
            raise TransientProviderError("upstream gateway 503")
        if recipient.endswith("@async.test"):
            return ProviderResult(
                status=ProviderOutcomeStatus.ACCEPTED,
                provider_message_id=f"{self.name}:{uuid.uuid4()}",
            )

        # Background random outcomes.
        roll = self._rng.random()
        if roll < self.transient_fail_rate:
            raise TransientProviderError("upstream gateway 503")
        if roll < self.transient_fail_rate + self.permanent_fail_rate:
            return ProviderResult(
                status=ProviderOutcomeStatus.PERMANENT_FAILURE,
                provider_message_id=f"{self.name}:rejected",
                error="recipient rejected by provider",
            )

        return ProviderResult(
            status=ProviderOutcomeStatus.DELIVERED,
            provider_message_id=f"{self.name}:{uuid.uuid4()}",
        )


class PermanentRecipientError(Exception):
    def __init__(self, recipient: str) -> None:
        super().__init__(f"invalid recipient: {recipient}")
        self.recipient = recipient
