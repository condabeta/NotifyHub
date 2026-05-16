from __future__ import annotations

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_create_batch_returns_202_with_notification_ids(
    app_client: AsyncClient, broker
):
    payload = {
        "channel": "sms",
        "priority": "marketing",
        "message": "Hello there",
        "recipients": ["+12025550101", "+12025550102"],
    }
    resp = await app_client.post("/api/v1/notifications", json=payload)
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert len(body["notification_ids"]) == 2
    assert body["accepted"] == 2
    assert body["duplicate"] is False


@pytest.mark.asyncio
async def test_idempotency_key_returns_cached_response(
    app_client: AsyncClient, broker
):
    payload = {
        "channel": "email",
        "priority": "transactional",
        "message": "Code 1234",
        "recipients": ["alice@example.com"],
    }
    headers = {"Idempotency-Key": "test-key-1"}
    first = await app_client.post(
        "/api/v1/notifications", json=payload, headers=headers
    )
    assert first.status_code == 202
    second = await app_client.post(
        "/api/v1/notifications", json=payload, headers=headers
    )
    assert second.status_code == 200
    assert second.json()["batch_id"] == first.json()["batch_id"]
    assert second.json()["duplicate"] is True


@pytest.mark.asyncio
async def test_idempotency_key_conflict_returns_409(
    app_client: AsyncClient, broker
):
    base = {
        "channel": "email",
        "priority": "transactional",
        "message": "Code 1234",
        "recipients": ["alice@example.com"],
    }
    headers = {"Idempotency-Key": "test-key-conflict"}
    r1 = await app_client.post("/api/v1/notifications", json=base, headers=headers)
    assert r1.status_code == 202
    mutated = {**base, "message": "Code 9999"}
    r2 = await app_client.post("/api/v1/notifications", json=mutated, headers=headers)
    assert r2.status_code == 409


@pytest.mark.asyncio
async def test_rejects_invalid_recipients_for_channel(
    app_client: AsyncClient, broker
):
    payload = {
        "channel": "email",
        "priority": "marketing",
        "message": "hi",
        "recipients": ["not-an-email"],
    }
    resp = await app_client.post("/api/v1/notifications", json=payload)
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_get_notification_returns_404_for_unknown(
    app_client: AsyncClient, broker
):
    resp = await app_client.get(
        "/api/v1/notifications/00000000-0000-0000-0000-000000000000"
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_list_history_returns_items_for_recipient(
    app_client: AsyncClient, broker
):
    payload = {
        "channel": "sms",
        "priority": "marketing",
        "message": "ping",
        "recipients": ["+12025550199"],
    }
    create_resp = await app_client.post("/api/v1/notifications", json=payload)
    assert create_resp.status_code == 202

    list_resp = await app_client.get(
        "/api/v1/recipients/+12025550199/notifications"
    )
    assert list_resp.status_code == 200
    body = list_resp.json()
    assert body["total"] == 1
    assert body["items"][0]["recipient_id"] == "+12025550199"
    assert body["items"][0]["status"] in ("queued", "sent", "delivered")
