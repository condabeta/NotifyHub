from __future__ import annotations

import hashlib
import json
import logging
from typing import Optional

import redis.asyncio as redis

from app.config import Settings

logger = logging.getLogger(__name__)


def _fingerprint(payload: dict) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


class IdempotencyStore:
    """Redis-backed cache mapping Idempotency-Key + request-fingerprint to a stored response.

    Behaviour:
      - First write with a key reserves it and stores the response payload + fingerprint.
      - Subsequent reads with the same key + matching fingerprint return the stored response.
      - Mismatched fingerprint for the same key raises IdempotencyConflict.
    """

    def __init__(self, client: redis.Redis, ttl_seconds: int) -> None:
        self.client = client
        self.ttl = ttl_seconds

    @classmethod
    def from_settings(cls, settings: Settings) -> "IdempotencyStore":
        client = redis.from_url(settings.redis_dsn, decode_responses=True)
        return cls(client=client, ttl_seconds=settings.idempotency_ttl_seconds)

    async def close(self) -> None:
        await self.client.aclose()

    def _key(self, idempotency_key: str) -> str:
        return f"idem:batch:{idempotency_key}"

    async def get(self, idempotency_key: str, payload: dict) -> Optional[dict]:
        raw = await self.client.get(self._key(idempotency_key))
        if not raw:
            return None
        record = json.loads(raw)
        if record.get("fingerprint") != _fingerprint(payload):
            raise IdempotencyConflict(idempotency_key)
        return record.get("response")

    async def save(self, idempotency_key: str, payload: dict, response: dict) -> None:
        record = {
            "fingerprint": _fingerprint(payload),
            "response": response,
        }
        await self.client.set(
            self._key(idempotency_key),
            json.dumps(record, default=str),
            ex=self.ttl,
        )


class IdempotencyConflict(Exception):
    """Raised when an Idempotency-Key is reused with a different request body."""

    def __init__(self, key: str) -> None:
        super().__init__(f"Idempotency-Key {key} conflicts with a prior request")
        self.key = key
