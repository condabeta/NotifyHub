from __future__ import annotations

import asyncio
import contextlib
import os
import uuid
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

import app.database as db_module
from app.api.routes import router
from app.config import Settings, get_settings
from app.core.enums import Channel
from app.models import Base
from app.providers.base import (
    Provider,
    ProviderOutcomeStatus,
    ProviderResult,
)
from app.providers.registry import ProviderRegistry
from app.services.broker import Broker
from app.services.idempotency import IdempotencyStore
from app.services.notifications import NotificationService
from app.worker.consumer import Worker

# Tests target the docker-compose services by default; override via env.
os.environ.setdefault("POSTGRES_HOST", "localhost")
os.environ.setdefault("POSTGRES_PORT", "5432")
os.environ.setdefault("RABBITMQ_HOST", "localhost")
os.environ.setdefault("RABBITMQ_PORT", "5672")
os.environ.setdefault("REDIS_HOST", "localhost")
os.environ.setdefault("REDIS_PORT", "6379")
os.environ.setdefault("APP_ENV", "test")


@pytest.fixture(scope="session")
def settings() -> Settings:
    get_settings.cache_clear()
    s = get_settings()
    # Each test session uses a fresh queue so reruns do not clash.
    s.notifications_queue = f"notifyhub.test.{uuid.uuid4().hex[:8]}"
    s.worker_max_attempts = 3
    s.provider_delivery_delay_ms = 10
    s.idempotency_ttl_seconds = 60
    # Prefetch=1 makes ordering tests deterministic; retry backoff kept short.
    s.worker_prefetch = 1
    s.worker_retry_base_delay_ms = 10
    s.worker_retry_max_delay_ms = 100
    return s


@pytest_asyncio.fixture(scope="session")
async def _engine(settings: Settings):
    db_module.init_engine(settings)
    engine = db_module._engine
    assert engine is not None
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await db_module.dispose_engine()


@pytest_asyncio.fixture
async def db_session(_engine) -> AsyncIterator:
    factory = async_sessionmaker(_engine, expire_on_commit=False)
    async with factory() as session:
        yield session
    async with _engine.begin() as conn:
        await conn.execute(
            text("TRUNCATE notifications, notification_batches RESTART IDENTITY CASCADE")
        )


@pytest_asyncio.fixture
async def idempotency_store(settings: Settings) -> AsyncIterator[IdempotencyStore]:
    store = IdempotencyStore.from_settings(settings)
    # Flush only keys we control so we don't blow away other tenants in shared Redis.
    yield store
    await store.client.flushdb()
    await store.close()


@pytest_asyncio.fixture
async def broker(settings: Settings) -> AsyncIterator[Broker]:
    b = Broker(settings)
    await b.connect()
    await b.queue.purge()
    yield b
    await b.close()


class ProgrammableProvider(Provider):
    """Deterministic test provider: records every call, replays scripted outcomes."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.calls: list[tuple[uuid.UUID, str, str]] = []
        self._script: list[ProviderResult | Exception] = []
        self.default: ProviderResult = ProviderResult(
            status=ProviderOutcomeStatus.DELIVERED,
            provider_message_id="prog-default",
        )
        self._lock = asyncio.Lock()

    def script(self, *entries: ProviderResult | Exception) -> "ProgrammableProvider":
        self._script.extend(entries)
        return self

    async def send(self, notification_id, recipient, message):
        async with self._lock:
            self.calls.append((notification_id, recipient, message))
            if not self._script:
                return self.default
            entry = self._script.pop(0)
        if isinstance(entry, Exception):
            raise entry
        return entry


@pytest.fixture
def programmable_providers() -> tuple[ProviderRegistry, ProgrammableProvider, ProgrammableProvider]:
    sms = ProgrammableProvider("test-sms")
    email = ProgrammableProvider("test-email")
    registry = ProviderRegistry({Channel.SMS: sms, Channel.EMAIL: email})
    return registry, sms, email


@pytest_asyncio.fixture
async def app_client(
    broker: Broker, idempotency_store: IdempotencyStore, settings: Settings
) -> AsyncIterator[AsyncClient]:
    app = FastAPI(title="NotifyHub-Test")
    app.include_router(router)
    app.state.settings = settings
    app.state.broker = broker
    app.state.idempotency = idempotency_store
    app.state.notification_service = NotificationService(broker, idempotency_store)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client


@pytest_asyncio.fixture
async def worker(
    settings: Settings,
    programmable_providers,
) -> AsyncIterator[Worker]:
    registry, _, _ = programmable_providers
    worker_broker = Broker(settings)
    await worker_broker.connect()
    w = Worker(settings, worker_broker, registry)
    task = asyncio.create_task(w.run())
    try:
        # Allow the consumer iterator to start before tests publish messages.
        await asyncio.sleep(0.1)
        yield w
    finally:
        w.request_stop()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, asyncio.TimeoutError):
            await asyncio.wait_for(task, timeout=5)
        await worker_broker.close()
