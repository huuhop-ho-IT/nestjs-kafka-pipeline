# NestJS Kafka Pipeline

> Production-grade asynchronous order processing pipeline built with **NestJS**, **Apache Kafka**, and **Docker Compose**.

This project demonstrates how to design a **fault-tolerant event-driven backend** — the kind of architecture that powers systems that need to handle failures gracefully without losing data or processing the same message twice.

The core idea: instead of processing an order synchronously inside the HTTP request, we emit an event to Kafka and let a consumer handle it in the background. This decouples the API from the processing logic, allows for independent scaling, and makes the system resilient to partial failures.

---

## What it does

A client creates an order via REST API. The order is immediately saved and an event is fired to Kafka. A consumer picks it up asynchronously, processes it (think: payment validation, inventory check), and updates the order status. If something goes wrong, the system retries with backoff. If it keeps failing, the message is moved to a Dead Letter Queue so no order is silently lost.

```
Client → POST /api/orders
             ↓
        HTTP responds 201 immediately
             ↓
        Kafka topic: orders.created
             ↓
        Consumer picks it up
             ↓
        PENDING → PROCESSING → COMPLETED
                                   ↓ (on failure after retries)
                             FAILED + DLQ
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        NestJS Application                        │
│                                                                  │
│  ┌───────────────────────┐     ┌────────────────────────────┐   │
│  │     HTTP Server        │     │   Kafka Microservice       │   │
│  │                       │     │   (Consumer Group)         │   │
│  │  POST /api/orders     │────►│   @EventPattern            │   │
│  │  GET  /api/orders     │     │   orders.created           │   │
│  │  GET  /api/orders/dlq │     │                            │   │
│  │  GET  /health         │     │   Retry + Backoff          │   │
│  │  GET  /health/ready   │     │   Idempotency guard        │   │
│  └───────────────────────┘     │   Dead Letter Queue        │   │
│          │ emit()              └────────────────────────────┘   │
│          │                              │ updateStatus()         │
└──────────┼──────────────────────────────┼────────────────────────┘
           ▼                              ▼
┌──────────────────────────────────────────────────────────────┐
│                       Apache Kafka                            │
│                                                               │
│   orders.created      ──► consumer processes orders          │
│   orders.created.dlq  ──► dead-lettered orders (failures)    │
│   orders.processed    ──► downstream services                 │
└──────────────────────────────────────────────────────────────┘
```

Every inbound HTTP request gets a `x-correlation-id` header (generated if absent), which flows through Kafka headers to the consumer. Every log line includes this ID, so you can trace a single order's journey end-to-end across the entire pipeline.

---

## Production patterns implemented

| Pattern | Where | Why it matters |
|---|---|---|
| **Dead Letter Queue (DLQ)** | `DlqService`, `orders.consumer.ts` | Failed messages are not silently dropped — moved to `orders.created.dlq` and exposed via API for inspection and replay |
| **Retry with exponential backoff** | `orders.consumer.ts` | Transient failures (network hiccup, slow DB) are retried up to 3 times with increasing delays (100ms → 200ms → 400ms) before declaring failure |
| **Idempotency guard** | `ProcessedEventsRepository` | Kafka delivers at-least-once. The guard ensures the same order is never processed twice, even if Kafka redelivers it after a consumer crash |
| **Correlation ID propagation** | `CorrelationIdMiddleware`, Kafka headers | Every request gets an ID that travels from HTTP → Kafka headers → consumer logs, making distributed tracing possible without OpenTelemetry |
| **Health checks** | `HealthController` | Liveness (`GET /health`) and readiness (`GET /health/ready`) probes, ready for Kubernetes — readiness reflects Kafka connection state |
| **Graceful shutdown** | `main.ts` | `enableShutdownHooks()` ensures in-flight Kafka messages are drained before the process exits (critical for zero-downtime deploys) |
| **Structured logging** | All services | Every log is a JSON object with `orderId`, `correlationId`, and `event` fields — easy to query in any log aggregator |

---

## Tech stack

| Layer | Technology |
|---|---|
| Framework | NestJS 10, TypeScript |
| Messaging | Apache Kafka (via KafkaJS) |
| Transport | `@nestjs/microservices` Kafka transport |
| API Docs | Swagger / OpenAPI (`/api/docs`) |
| Containerization | Docker, Docker Compose |
| Monitoring | Kafka UI (Provectus) |
| Testing | Jest, Supertest (55 tests) |

---

## Getting started

### Option A — Local development (recommended for coding)

Run only the infrastructure in Docker, and the NestJS app directly on your machine with hot reload:

```bash
# Start Kafka + Zookeeper + Kafka UI
docker-compose up -d zookeeper kafka kafka-ui

# Install dependencies
npm install

# Run with hot reload
npm run start:dev
```

Hot reload means any code change is reflected immediately — no need to rebuild anything.

### Option B — Full Docker (simulates production)

```bash
docker-compose up --build
```

> First startup takes 20–30 seconds while Kafka initializes. The app retries automatically.

### Services

| Service | URL |
|---|---|
| REST API | http://localhost:3000/api |
| Swagger UI | http://localhost:3000/api/docs |
| Kafka UI | http://localhost:8080 |
| Kafka broker | localhost:9092 |

---

## API reference

### Create an order

```bash
curl -X POST http://localhost:3000/api/orders \
  -H "Content-Type: application/json" \
  -H "x-correlation-id: my-trace-123" \
  -d '{
    "product": "MacBook Pro 16",
    "quantity": 1,
    "price": 2499.99,
    "customerId": "customer-abc-123"
  }'
```

Response:
```json
{
  "message": "Order created and sent to Kafka pipeline",
  "correlationId": "my-trace-123",
  "data": {
    "id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
    "product": "MacBook Pro 16",
    "quantity": 1,
    "price": 2499.99,
    "customerId": "customer-abc-123",
    "status": "PENDING",
    "createdAt": "2025-01-15T10:00:00.000Z"
  }
}
```

The order starts as `PENDING`. Within a second or two, the Kafka consumer will pick it up and update it to `PROCESSING` then `COMPLETED`.

### List all orders + stats

```bash
curl http://localhost:3000/api/orders
```

### Get a single order

```bash
curl http://localhost:3000/api/orders/{id}
```

### Order statistics

```bash
curl http://localhost:3000/api/orders/stats
```

### Dead Letter Queue — inspect failed orders

```bash
curl http://localhost:3000/api/orders/dlq
```

Response includes the original payload, the error message, retry count, correlation ID, and timestamp — everything you need to debug or replay the message.

### Health probes

```bash
# Liveness — is the process alive?
curl http://localhost:3000/api/health

# Readiness — is Kafka connected?
curl http://localhost:3000/api/health/ready
```

---

## How the pipeline flows

### 1. HTTP → Kafka (producer side)

1. `POST /api/orders` arrives at `OrdersController`
2. `CorrelationIdMiddleware` attaches `x-correlation-id` (or preserves the one the client sent)
3. `OrdersService.createOrder()` saves the order as `PENDING` and emits to `orders.created`
4. Kafka headers carry the correlation ID, source service name, and timestamp
5. HTTP responds **immediately** — the client doesn't wait for processing

### 2. Kafka → processing (consumer side)

The consumer runs in the same NestJS process but as a separate microservice transport. When a message arrives on `orders.created`:

1. **Parse** the payload (handles both raw objects and JSON strings)
2. **Idempotency check** — if this order ID was already processed, skip it
3. **Retry loop** — up to 4 attempts (attempt 0 + 3 retries) with exponential backoff:
   - attempt 0: immediate
   - attempt 1: wait 100ms
   - attempt 2: wait 200ms
   - attempt 3: wait 400ms
4. On success: order → `COMPLETED`, ID marked as processed
5. On all retries failed: order → `FAILED`, message sent to DLQ

### 3. Dead Letter Queue

When a message cannot be processed after all retries, `DlqService` does two things:
- Publishes the failed message to `orders.created.dlq` (visible in Kafka UI)
- Stores it in-memory so `GET /api/orders/dlq` can surface it

In production, the DLQ store would be backed by PostgreSQL or Redis, and a separate alerting pipeline would trigger on DLQ writes.

---

## Observing the pipeline

Create an order and watch the logs:

```
[OrdersService]  { event: 'order_created', orderId: 'abc-123', correlationId: 'my-trace-123', product: 'MacBook Pro 16' }
[OrdersConsumer] { event: 'processing_started', orderId: 'abc-123', correlationId: 'my-trace-123' }
[OrdersConsumer] { event: 'processing_completed', orderId: 'abc-123', correlationId: 'my-trace-123', attempt: 0, total: 2499.99 }
```

All log entries share the same `correlationId` — you can filter any log aggregator (Datadog, CloudWatch, Loki) by correlation ID to see the full journey of a single order.

Check Kafka UI at http://localhost:8080 to see:
- Messages in `orders.created` and `orders.created.dlq`
- Consumer group lag for `order-processor-group`
- Broker health and partition assignments

---

## Project structure

```
nestjs-kafka-pipeline/
├── src/
│   ├── main.ts                              # Bootstrap: HTTP server + Kafka microservice
│   ├── app.module.ts                        # Root module + CorrelationId middleware
│   ├── common/
│   │   └── correlation-id.middleware.ts     # Attach/generate x-correlation-id on every request
│   ├── health/
│   │   ├── health.controller.ts             # GET /health and GET /health/ready
│   │   └── health.module.ts
│   ├── kafka/
│   │   ├── kafka.constants.ts               # Topic names, retry config, header names
│   │   ├── kafka.module.ts                  # ClientsModule registration
│   │   └── dlq.service.ts                   # Dead Letter Queue: publish + store failed messages
│   └── orders/
│       ├── dto/
│       │   └── create-order.dto.ts          # Request validation (class-validator)
│       ├── entities/
│       │   └── order.entity.ts              # Order model + status enum
│       ├── orders.consumer.ts               # Kafka consumer: retry, idempotency, DLQ
│       ├── orders.controller.ts             # REST endpoints + DLQ endpoint
│       ├── orders.module.ts                 # Feature module
│       ├── orders.repository.ts             # In-memory store (swap with TypeORM/Prisma)
│       ├── orders.service.ts                # Business logic + Kafka producer with headers
│       └── processed-events.repository.ts  # Idempotency store (swap with Redis)
├── docker-compose.yml                       # Zookeeper + Kafka + Kafka UI + App
├── Dockerfile                               # Multi-stage production build
└── .env.example                             # Environment variable reference
```

---

## Running tests

```bash
# All tests (55 tests across 5 suites)
npm test

# With coverage report
npm run test:cov

# Integration tests only
npm run test:integration

# Watch mode
npm run test:watch
```

The test suite covers:
- Unit tests for every service, controller, and consumer
- Integration tests for the full HTTP → service → repository flow
- Consumer retry and DLQ behavior with mocked failures
- Idempotency: duplicate events are skipped
- Correlation ID propagation through Kafka headers

---

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `PORT` | `3000` | HTTP server port |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated broker addresses |
| `KAFKA_CONSUMER_GROUP_ID` | `order-processor-group` | Consumer group ID |

---

## What I'd add for a real production system

This project intentionally keeps things simple (in-memory store, no real payment processing) to focus on the Kafka patterns. In a real system I'd layer in:

- **PostgreSQL** with TypeORM/Prisma to replace the in-memory repository
- **Redis** for the idempotency store and DLQ, with a TTL to avoid unbounded growth
- **Prometheus metrics** — consumer lag, DLQ rate, processing duration histograms
- **OpenTelemetry** distributed tracing to replace the manual correlation ID approach
- **Schema Registry (Avro)** to enforce event contracts between producer and consumer
- **Transactional Outbox** (`pg-transactional-outbox`) to guarantee the DB write and Kafka emit are atomic — prevents the case where an order is saved but the Kafka emit silently fails

---

## License

MIT © [Ho Huu Hop](https://linkedin.com/in/ho-huu-hop-585856175)
