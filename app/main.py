from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routes import router
from app.config import get_settings
from app.core.logging import configure_logging
from app.database import dispose_engine, init_engine
from app.services.broker import Broker
from app.services.idempotency import IdempotencyStore
from app.services.notifications import NotificationService

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    configure_logging(settings.log_level)
    init_engine(settings)

    broker = Broker(settings)
    await broker.connect()
    idempotency = IdempotencyStore.from_settings(settings)
    notification_service = NotificationService(broker, idempotency)

    app.state.settings = settings
    app.state.broker = broker
    app.state.idempotency = idempotency
    app.state.notification_service = notification_service

    logger.info("notifyhub api started")
    try:
        yield
    finally:
        await broker.close()
        await idempotency.close()
        await dispose_engine()
        logger.info("notifyhub api stopped")


def create_app() -> FastAPI:
    app = FastAPI(
        title="NotifyHub",
        description=(
            "Notification microservice: priority queues, retries, "
            "idempotency, delivery status tracking."
        ),
        version="1.0.0",
        lifespan=lifespan,
    )
    app.include_router(router)
    return app


app = create_app()
