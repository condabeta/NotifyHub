from __future__ import annotations

import asyncio
import uuid

import pytest
from httpx import AsyncClient

from app.providers.base import (
    ProviderOutcomeStatus,
    ProviderResult,
    TransientProviderError,
)


async def _poll(coro_factory, *, timeout: float = 8.0, interval: float = 0.05):
    """Repeat ``coro_factory()`` until truthy or timeout."""
    deadline = asyncio.get_event_loop().time() + timeout
    last = None
    while asyncio.get_event_loop().time() < deadline:
        last = await coro_factory()
        if last:
            return last
        await asyncio.sleep(interval)
    raise AssertionError(f"condition not met within {timeout}s, last={last!r}")


async def _get_status(client: AsyncClient, notification_id: uuid.UUID) -> dict | None:
    resp = await client.get(f"/api/v1/notifications/{notification_id}")
    if resp.status_code == 404:
        return None
    assert resp.status_code == 200, resp.text
    return resp.json()


async def _wait_status(
    client: AsyncClient,
    notification_id: uuid.UUID,
    expected: str,
    *,
    timeout: float = 8.0,
) -> dict:
    async def _check():
        body = await _get_status(client, notification_id)
        return body if body and body["status"] == expected else None

    return await _poll(_check, timeout=timeout)


@pytest.mark.asyncio
async def test_full_pipeline_delivered(
    app_client: AsyncClient,
    worker,
    programmable_providers,
):
    """Happy path covering API → queue → worker → provider → DB status update."""
    _, sms_provider, _ = programmable_providers
    sms_provider.script(
        ProviderResult(
            status=ProviderOutcomeStatus.DELIVERED,
            provider_message_id="prov-1",
        )
    )

    payload = {
        "channel": "sms",
        "priority": "transactional",
        "message": "Your code is 4242",
        "recipients": ["+12025550101"],
    }
    resp = await app_client.post("/api/v1/notifications", json=payload)
    assert resp.status_code == 202
    notification_id = uuid.UUID(resp.json()["notification_ids"][0])

    body = await _wait_status(app_client, notification_id, "delivered")
    assert body["provider_message_id"] == "prov-1"
    assert body["attempts"] == 1
    assert len(sms_provider.calls) == 1
    assert sms_provider.calls[0][1] == "+12025550101"


@pytest.mark.asyncio
async def test_transient_failure_retries_then_succeeds(
    app_client: AsyncClient,
    worker,
    programmable_providers,
):
    _, _, email_provider = programmable_providers
    email_provider.script(
        TransientProviderError("503 from gateway"),
        TransientProviderError("503 from gateway"),
        ProviderResult(
            status=ProviderOutcomeStatus.DELIVERED,
            provider_message_id="prov-retry-success",
        ),
    )

    payload = {
        "channel": "email",
        "priority": "transactional",
        "message": "Welcome aboard",
        "recipients": ["bob@example.com"],
    }
    resp = await app_client.post("/api/v1/notifications", json=payload)
    notification_id = uuid.UUID(resp.json()["notification_ids"][0])

    body = await _wait_status(app_client, notification_id, "delivered", timeout=15.0)
    assert body["attempts"] == 3
    assert body["provider_message_id"] == "prov-retry-success"
    assert len(email_provider.calls) == 3


@pytest.mark.asyncio
async def test_permanent_failure_marks_failed(
    app_client: AsyncClient,
    worker,
    programmable_providers,
):
    _, sms_provider, _ = programmable_providers
    sms_provider.script(
        ProviderResult(
            status=ProviderOutcomeStatus.PERMANENT_FAILURE,
            provider_message_id="prov-rejected",
            error="invalid recipient",
        )
    )

    payload = {
        "channel": "sms",
        "priority": "marketing",
        "message": "promo",
        "recipients": ["+12025550199"],
    }
    resp = await app_client.post("/api/v1/notifications", json=payload)
    notification_id = uuid.UUID(resp.json()["notification_ids"][0])

    body = await _wait_status(app_client, notification_id, "failed")
    assert body["last_error"] == "invalid recipient"
    assert body["attempts"] == 1


@pytest.mark.asyncio
async def test_transient_failure_exhausts_attempts(
    app_client: AsyncClient,
    worker,
    programmable_providers,
    settings,
):
    """When attempts hit max_attempts the worker must stop requeueing and mark failed."""
    _, sms_provider, _ = programmable_providers
    sms_provider.script(
        *[TransientProviderError("perma-503")]
        * (settings.worker_max_attempts + 5)
    )

    payload = {
        "channel": "sms",
        "priority": "marketing",
        "message": "ping",
        "recipients": ["+12025550400"],
    }
    resp = await app_client.post("/api/v1/notifications", json=payload)
    notification_id = uuid.UUID(resp.json()["notification_ids"][0])

    body = await _wait_status(app_client, notification_id, "failed", timeout=15.0)
    assert body["attempts"] == settings.worker_max_attempts
    assert "perma-503" in (body["last_error"] or "")


@pytest.mark.asyncio
async def test_async_acceptance_transitions_sent_then_delivered(
    app_client: AsyncClient,
    worker,
    programmable_providers,
):
    """Provider returning ACCEPTED should leave status=sent, then become delivered."""
    _, sms_provider, _ = programmable_providers
    sms_provider.script(
        ProviderResult(
            status=ProviderOutcomeStatus.ACCEPTED,
            provider_message_id="prov-async-1",
        )
    )

    payload = {
        "channel": "sms",
        "priority": "transactional",
        "message": "Code",
        "recipients": ["+12025550500"],
    }
    resp = await app_client.post("/api/v1/notifications", json=payload)
    notification_id = uuid.UUID(resp.json()["notification_ids"][0])

    body = await _wait_status(app_client, notification_id, "delivered", timeout=5.0)
    assert body["sent_at"] is not None
    assert body["delivered_at"] is not None


@pytest.mark.asyncio
async def test_high_priority_jumps_ahead_of_marketing(
    app_client: AsyncClient,
    settings,
    programmable_providers,
    broker,  # for purging shared state
):
    """Transactional messages must be consumed before marketing ones already queued.

    To make ordering deterministic we hold the first consumed message in the
    provider until after both batches have landed in the queue.
    """
    from app.services.broker import Broker
    from app.worker.consumer import Worker as PipelineWorker

    registry, sms_provider, _ = programmable_providers

    block = asyncio.Event()
    release = asyncio.Event()

    original_send = sms_provider.send

    async def blocking_send(notification_id, recipient, message):
        block.set()
        await release.wait()
        return await original_send(notification_id, recipient, message)

    sms_provider.send = blocking_send  # type: ignore[assignment]

    # Publish marketing batch first.
    marketing_payload = {
        "channel": "sms",
        "priority": "marketing",
        "message": "promo",
        "recipients": [f"+1202555{i:04d}" for i in range(10)],
    }
    txn_payload = {
        "channel": "sms",
        "priority": "transactional",
        "message": "urgent",
        "recipients": ["+19998887777"],
    }
    marketing_resp = await app_client.post(
        "/api/v1/notifications", json=marketing_payload
    )
    assert marketing_resp.status_code == 202

    # Start the worker now; first message will block until we release.
    worker_broker = Broker(settings)
    await worker_broker.connect()
    w = PipelineWorker(settings, worker_broker, registry)
    worker_task = asyncio.create_task(w.run())

    try:
        await asyncio.wait_for(block.wait(), timeout=5.0)
        # First message is now held by the provider — publish transactional.
        txn_resp = await app_client.post("/api/v1/notifications", json=txn_payload)
        assert txn_resp.status_code == 202
        txn_id = uuid.UUID(txn_resp.json()["notification_ids"][0])

        # Give RabbitMQ a moment to register the new high-priority message.
        await asyncio.sleep(0.5)
        release.set()

        await _wait_status(app_client, txn_id, "delivered", timeout=10.0)

        # The transactional call must be among the first few processed,
        # ahead of most marketing recipients.
        txn_index = next(
            i for i, c in enumerate(sms_provider.calls) if c[0] == txn_id
        )
        marketing_recipients = set(marketing_payload["recipients"])
        marketing_before_txn = sum(
            1 for c in sms_provider.calls[:txn_index] if c[1] in marketing_recipients
        )
        # Only the one already-in-flight marketing message should precede the
        # transactional one; the rest must come after.
        assert marketing_before_txn <= 1, (
            f"{marketing_before_txn} marketing messages preceded the urgent one"
        )
    finally:
        w.request_stop()
        try:
            await asyncio.wait_for(worker_task, timeout=5.0)
        except asyncio.TimeoutError:
            worker_task.cancel()
        await worker_broker.close()


@pytest.mark.asyncio
async def test_idempotent_batch_does_not_double_send(
    app_client: AsyncClient,
    worker,
    programmable_providers,
):
    _, sms_provider, _ = programmable_providers

    payload = {
        "channel": "sms",
        "priority": "transactional",
        "message": "Code 1",
        "recipients": ["+12025550111"],
    }
    headers = {"Idempotency-Key": "dedup-key"}
    r1 = await app_client.post("/api/v1/notifications", json=payload, headers=headers)
    r2 = await app_client.post("/api/v1/notifications", json=payload, headers=headers)
    assert r1.status_code == 202
    assert r2.status_code == 200
    assert r1.json()["batch_id"] == r2.json()["batch_id"]
    assert r2.json()["duplicate"] is True

    notification_id = uuid.UUID(r1.json()["notification_ids"][0])
    await _wait_status(app_client, notification_id, "delivered", timeout=5.0)

    # Even after a second submission, the provider must be called exactly once.
    assert len(sms_provider.calls) == 1


@pytest.mark.asyncio
async def test_recipient_history_endpoint(
    app_client: AsyncClient,
    worker,
    programmable_providers,
):
    _, _, email_provider = programmable_providers
    email_provider.script(
        ProviderResult(
            status=ProviderOutcomeStatus.DELIVERED,
            provider_message_id="hist-1",
        ),
        ProviderResult(
            status=ProviderOutcomeStatus.DELIVERED,
            provider_message_id="hist-2",
        ),
    )

    recipient = "history@example.com"
    payload = {
        "channel": "email",
        "priority": "transactional",
        "message": "msg",
        "recipients": [recipient],
    }
    r1 = await app_client.post("/api/v1/notifications", json=payload)
    r2 = await app_client.post("/api/v1/notifications", json=payload)
    n1 = uuid.UUID(r1.json()["notification_ids"][0])
    n2 = uuid.UUID(r2.json()["notification_ids"][0])
    await _wait_status(app_client, n1, "delivered")
    await _wait_status(app_client, n2, "delivered")

    hist_resp = await app_client.get(f"/api/v1/recipients/{recipient}/notifications")
    assert hist_resp.status_code == 200
    body = hist_resp.json()
    assert body["recipient_id"] == recipient
    assert body["total"] == 2
    statuses = {item["status"] for item in body["items"]}
    assert statuses == {"delivered"}


@pytest.mark.asyncio
async def test_delivery_callback_transitions_to_delivered(
    app_client: AsyncClient,
    worker,
    programmable_providers,
):
    _, sms_provider, _ = programmable_providers
    sms_provider.script(
        ProviderResult(
            status=ProviderOutcomeStatus.ACCEPTED,
            provider_message_id="cb-msg",
        )
    )

    payload = {
        "channel": "sms",
        "priority": "transactional",
        "message": "cb test",
        "recipients": ["+12025550600"],
    }
    resp = await app_client.post("/api/v1/notifications", json=payload)
    notification_id = uuid.UUID(resp.json()["notification_ids"][0])

    # Wait for ACCEPTED → sent (the async delivery hook will move it forward,
    # but the explicit callback should still apply correctly).
    async def reach_sent_or_delivered():
        body = await _get_status(app_client, notification_id)
        if body and body["status"] in ("sent", "delivered"):
            return body
        return None

    await _poll(reach_sent_or_delivered, timeout=5.0)

    cb_payload = {
        "notification_id": str(notification_id),
        "status": "delivered",
        "provider_message_id": "cb-msg",
    }
    cb_resp = await app_client.post(
        "/api/v1/notifications/callbacks/delivery", json=cb_payload
    )
    assert cb_resp.status_code == 200
    assert cb_resp.json()["status"] == "delivered"
