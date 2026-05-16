# NotifyHub — Notification Microservice

A Python/FastAPI microservice for mass SMS / Email notifications with priority
queues, retries, idempotency, and full delivery-status tracking.

Stack:

- **Python 3.12** + **FastAPI** + **SQLAlchemy 2 (async)** + **Alembic**
- **PostgreSQL 16** as the system of record
- **RabbitMQ 3.13** with priority queues (transactional > marketing)
- **Redis 7** for request-level idempotency
- **aio-pika** for async AMQP, **httpx** / **pytest-asyncio** for tests
- Mock SMS & Email providers (pluggable via `ProviderRegistry`)

---

## Quick start

The entire stack — Postgres, RabbitMQ, Redis, API, worker — boots with one
command:

```bash
docker-compose up --build
```

Once the API logs `notifyhub api started`, useful endpoints are:

- API base: <http://localhost:8000>
- Swagger UI: <http://localhost:8000/docs>
- OpenAPI JSON: <http://localhost:8000/openapi.json>
- RabbitMQ management UI: <http://localhost:15672> (`notifyhub` / `notifyhub`)

Stop everything with `docker-compose down` (add `-v` to also drop the Postgres
volume).

---

## Architecture

```
              ┌────────────┐    POST /api/v1/notifications
              │   Client   │ ──────────────────────────────────┐
              └────────────┘                                   │
                                                               ▼
   ┌─────────┐    idempotency-key   ┌────────────────────────────────┐
   │  Redis  │ ◀──────────────────▶ │           FastAPI API          │
   └─────────┘                      │                                │
                                    │  · validates payload           │
   ┌─────────────┐  insert batch +  │  · upserts batch+notifications │
   │ PostgreSQL  │ ◀── notifications│  · publishes to RabbitMQ       │
   └─────────────┘                  └────────────────┬───────────────┘
         ▲                                           │ publish (priority)
         │ update status                             ▼
         │                              ┌────────────────────────┐
         │                              │       RabbitMQ         │
         │                              │  notifyhub.notifications│
         │                              │  (x-max-priority=10)   │
         │                              └────────────┬───────────┘
         │                                           │ consume
         │            ┌──────────────────────────────▼───────────────┐
         │            │                   Worker                      │
         └────────────│  · row-lock notification (FOR UPDATE)         │
                      │  · call channel provider                      │
                      │  · status: queued → sent / delivered / failed │
                      │  · retry transient errors with backoff        │
                      └──────────────────────────────────────────────┘
```

### Why this shape

| Requirement | How it's met |
|---|---|
| **Mass send** | `POST /api/v1/notifications` accepts a channel, message and recipient list; each recipient becomes one DB row and one RabbitMQ message. |
| **Priority** | A single RabbitMQ queue declared with `x-max-priority=10`. Transactional messages publish at priority `10`, marketing at `1`; RabbitMQ pops higher priorities first. |
| **Status detail** | `notifications.status` ∈ {`queued`,`sent`,`delivered`,`failed`}. Worker updates atomically inside row-locked transactions. |
| **Persistence** | Notifications are committed to Postgres *before* being published. Messages are `delivery_mode=PERSISTENT`; the queue is `durable=True`. |
| **At-least-once delivery** | Manual ACK only after the worker has updated the DB. Transient provider errors NACK with requeue. |
| **Retries with backoff** | Per-notification `attempts` counter, exponential backoff inside the worker before requeue, hard cap (`WORKER_MAX_ATTEMPTS`). |
| **Idempotency** | `Idempotency-Key` header → cached `BatchAccepted` response in Redis. Notification rows use `UNIQUE(batch_id, recipient_id)` + `INSERT … ON CONFLICT DO NOTHING`. |
| **Exactly-once (business)** | Worker checks status under `FOR UPDATE` before sending: a redelivered message for an already-`sent`/`delivered`/`failed` row is skipped. Real providers can be passed `notification_id` as their dedup key. |
| **Cloud-native** | Single `docker-compose up`; API and worker are independent Docker services that scale horizontally. |
| **Integration tests** | End-to-end coverage of the full chain — see `tests/test_integration_pipeline.py`. |

---

## API

All endpoints are under `/api/v1`. Full schemas live in Swagger UI at `/docs`.

### `POST /api/v1/notifications`

Submit a batch of notifications.

**Headers**

- `Idempotency-Key` *(optional)* — re-submitting the same key with the same body
  returns the original `batch_id` (HTTP 200). Different body with the same key
  → HTTP 409.

**Body**

```json
{
  "channel": "sms" | "email",
  "priority": "transactional" | "marketing",
  "message": "string (1–2000)",
  "recipients": ["+12025550101", "+12025550102"]
}
```

**Response — `202 Accepted`** (or `200 OK` when returned from idempotency cache)

```json
{
  "batch_id": "9b3f...uuid...",
  "accepted": 2,
  "duplicate": false,
  "notification_ids": ["8f1a...", "c204..."]
}
```

**Example**

```bash
curl -s -X POST http://localhost:8000/api/v1/notifications \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: signup-2026-05-15-abc123' \
  -d '{
        "channel": "sms",
        "priority": "transactional",
        "message": "Your verification code is 4242",
        "recipients": ["+12025550101"]
      }'
```

### `GET /api/v1/notifications/{notification_id}`

Return a single notification with current status, attempts, error and
timestamps.

### `GET /api/v1/recipients/{recipient_id}/notifications`

List the full history of notifications sent to a recipient.

**Query parameters:** `limit` (1–500, default 100), `offset` (default 0)

```json
{
  "recipient_id": "+12025550101",
  "total": 12,
  "items": [
    {
      "id": "...",
      "batch_id": "...",
      "recipient_id": "+12025550101",
      "channel": "sms",
      "priority": "transactional",
      "message": "Your verification code is 4242",
      "status": "delivered",
      "attempts": 1,
      "provider_message_id": "mock-sms:...",
      "created_at": "2026-05-15T12:34:56Z",
      "sent_at": "2026-05-15T12:34:56.250Z",
      "delivered_at": "2026-05-15T12:34:56.500Z"
    }
  ]
}
```

### `POST /api/v1/notifications/callbacks/delivery`

Webhook endpoint a provider would call to mark a `sent` notification as
`delivered` or `failed`. (For the mock provider this is exercised by the
integration tests.)

```json
{
  "notification_id": "...uuid...",
  "status": "delivered",
  "provider_message_id": "real-provider-msg-id",
  "error": null
}
```

### `GET /api/v1/health`

Liveness probe.

---

## Delivery-status state machine

```
queued ──► sent ──► delivered     (provider accepted, then confirmed)
   │         │
   │         └──► failed          (callback reported delivery failure)
   │
   └────────► delivered           (provider confirmed inline)
   │
   └────────► failed              (permanent provider rejection,
                                   or attempts == WORKER_MAX_ATTEMPTS)
```

Transitions are guarded with `SELECT … FOR UPDATE` so a redelivered message
cannot drive the row backwards.

---

## Mock provider behaviour

`MockProvider` (`app/providers/mock.py`) imitates a real gateway and lets you
pin outcomes via the recipient identifier — useful for ad-hoc testing:

| Recipient pattern        | Outcome                          |
|--------------------------|----------------------------------|
| `*@fail.test` / `+00000000000` | permanent failure (`failed`)     |
| `*@retry.test` / `+11111111111` | transient error (worker retries) |
| `*@async.test`           | accepted (`sent` → `delivered`)  |
| anything else (valid)    | `delivered` (with configured random failure mix) |

Background random rates are tuned by env vars:

```
PROVIDER_TRANSIENT_FAIL_RATE=0.10
PROVIDER_PERMANENT_FAIL_RATE=0.02
PROVIDER_DELIVERY_DELAY_MS=200
```

To plug in a real provider, implement `app.providers.base.Provider` and
register it in `ProviderRegistry.from_settings`.

---

## Running the tests

Tests are real integration tests — they require live Postgres, RabbitMQ and
Redis. Boot the infrastructure and run them either on the host or inside a
container.

**Option A — host:**

```bash
docker-compose up -d postgres rabbitmq redis
pip install -r requirements-dev.txt
pytest
```

**Option B — inside the API container:**

```bash
docker-compose up -d postgres rabbitmq redis
docker-compose run --rm api test
```

The integration test file (`tests/test_integration_pipeline.py`) covers:

- happy path: API → queue → consumer → provider call → status `delivered`
- transient failures with retry until success
- transient failures hitting the `WORKER_MAX_ATTEMPTS` cap → status `failed`
- permanent failures → status `failed` after a single call
- `accepted` outcomes that transition `sent` → `delivered` asynchronously
- transactional priority overtaking already-queued marketing messages
- duplicate batch submission with the same `Idempotency-Key` (single provider call)
- recipient history endpoint
- provider delivery-callback endpoint

`tests/test_api.py` covers request validation, idempotency-conflict semantics
and 404 handling on the API surface.

---

## Configuration

All knobs read from environment variables (see `.env.example`). The defaults
match `docker-compose.yml`.

| Variable | Default | Meaning |
|---|---|---|
| `POSTGRES_*` | `notifyhub` | Postgres connection |
| `RABBITMQ_*` | `notifyhub` | RabbitMQ connection |
| `REDIS_*` | `localhost:6379/0` | Redis connection |
| `IDEMPOTENCY_TTL_SECONDS` | `86400` | How long a cached idempotent response is honoured |
| `WORKER_PREFETCH` | `16` | RabbitMQ prefetch per worker |
| `WORKER_MAX_ATTEMPTS` | `5` | Hard cap on retries before `failed` |
| `WORKER_RETRY_BASE_DELAY_MS` | `500` | Exponential-backoff base |
| `WORKER_RETRY_MAX_DELAY_MS` | `30000` | Exponential-backoff cap |
| `PROVIDER_TRANSIENT_FAIL_RATE` | `0.1` | Mock provider transient-error rate |
| `PROVIDER_PERMANENT_FAIL_RATE` | `0.02` | Mock provider permanent-failure rate |
| `PROVIDER_DELIVERY_DELAY_MS` | `200` | Simulated `sent → delivered` latency |

To scale the worker pool:

```bash
docker-compose up -d --scale worker=4
```

---

## Project layout

```
app/
├── api/                 # FastAPI routes + dependency wiring
├── core/                # enums, logging
├── providers/           # provider abstraction + mock implementation
├── services/            # batch creation, broker, idempotency
├── worker/              # AMQP consumer + retry / backoff loop
├── config.py            # pydantic-settings
├── database.py          # async engine + session lifecycle
├── models.py            # SQLAlchemy 2 declarative models
├── schemas.py           # Pydantic v2 request/response schemas
└── main.py              # FastAPI app factory + lifespan
migrations/              # Alembic
tests/                   # integration + API tests
docker/entrypoint.sh     # waits for deps, then runs `api`, `worker` or `migrate`
```

---

## Notes on production-readiness

This is a take-home implementation; what would harden it further:

- Transactional outbox + dispatcher instead of "commit then publish" so a
  process crash between commit and publish cannot orphan a `queued` row.
- Provider-side idempotency keys (passing `notification_id` as the provider's
  request token) for true exactly-once at the gateway boundary.
- Dead-letter queue + delayed-retry exchange instead of in-worker backoff.
- Metrics (Prometheus), tracing (OTel), structured request IDs in logs.
- Rate-limit / per-recipient throttling using Redis token buckets.

The codebase is laid out so each of these slots in without touching the API
contract.
