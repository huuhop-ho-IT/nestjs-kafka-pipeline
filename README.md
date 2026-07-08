# NestJS Kafka Pipeline

> Asynchronous order processing pipeline demo using **NestJS**, **Apache Kafka**, and **Docker Compose**.

This project demonstrates a production-ready pattern for event-driven backend architecture:
- HTTP API receives orders and **produces** events to Kafka
- A **consumer** group picks up the events and processes them asynchronously
- Orders transition through states: `PENDING → PROCESSING → COMPLETED`

---

## Architecture

```
┌────────────────────────────────────────────────────────────┐
│                    NestJS Application                       │
│                                                            │
│  ┌─────────────────────┐      ┌─────────────────────────┐  │
│  │    HTTP Server       │      │  Kafka Microservice     │  │
│  │                     │      │  (Consumer Group)       │  │
│  │  POST /api/orders   │─────►│  @EventPattern          │  │
│  │  GET  /api/orders   │      │  orders.created         │  │
│  │  GET  /api/orders/:id│◄────│                         │  │
│  └─────────────────────┘      └─────────────────────────┘  │
│         │  ClientKafka.emit()          │ updateStatus()     │
└─────────┼────────────────────────────-┼────────────────────┘
          │                             │
          ▼                             ▼
┌─────────────────────────────────────────────────────────┐
│                    Apache Kafka                          │
│                                                         │
│   Topic: orders.created    ──► Consumer processes       │
│   Topic: orders.processed  ──► Logged / forwarded       │
└─────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Framework | NestJS 10, TypeScript |
| Messaging | Apache Kafka (via KafkaJS) |
| Transport | `@nestjs/microservices` Kafka transport |
| Containerization | Docker, Docker Compose |
| API Docs | Swagger / OpenAPI |
| Monitoring | Kafka UI (provectuslabs) |

---

## Prerequisites

- [Docker](https://www.docker.com/) and Docker Compose
- Node.js 20+ (for local development only)

---

## Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/YOUR_USERNAME/nestjs-kafka-pipeline.git
cd nestjs-kafka-pipeline
```

### 2. Start with Docker Compose (recommended)

```bash
docker-compose up --build
```

All services will start automatically:

| Service | URL |
|---|---|
| NestJS API | http://localhost:3000/api |
| Swagger Docs | http://localhost:3000/api/docs |
| Kafka UI | http://localhost:8080 |
| Kafka Broker | localhost:9092 |

> **Note:** The app retries the Kafka connection automatically. First startup may take 20–30 seconds while Kafka initializes.

### 3. Local development (without Docker)

```bash
# Start only Kafka infrastructure
docker-compose up zookeeper kafka kafka-ui -d

# Copy and configure environment
cp .env.example .env

# Install dependencies
npm install

# Run in watch mode
npm run start:dev
```

---

## API Reference

### Create an Order

```bash
curl -X POST http://localhost:3000/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "product": "MacBook Pro 16",
    "quantity": 1,
    "price": 2499.99,
    "customerId": "customer-abc-123"
  }'
```

**Response:**
```json
{
  "message": "Order created and sent to Kafka pipeline",
  "data": {
    "id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
    "product": "MacBook Pro 16",
    "quantity": 1,
    "price": 2499.99,
    "customerId": "customer-abc-123",
    "status": "PENDING",
    "createdAt": "2025-01-15T10:00:00.000Z",
    "updatedAt": "2025-01-15T10:00:00.000Z"
  }
}
```

### List All Orders

```bash
curl http://localhost:3000/api/orders
```

### Get a Specific Order

```bash
curl http://localhost:3000/api/orders/{id}
```

### Get Statistics

```bash
curl http://localhost:3000/api/orders/stats
```

---

## How It Works

### Producer (HTTP → Kafka)

1. Client sends `POST /api/orders`
2. `OrdersController` validates the request body
3. `OrdersService.createOrder()` saves the order with status `PENDING`
4. `ClientKafka.emit('orders.created', order)` fires the event to Kafka
5. HTTP response returns immediately — **non-blocking**

### Consumer (Kafka → Processing)

1. `OrdersConsumer` listens to topic `orders.created` via `@EventPattern`
2. Updates order status to `PROCESSING`
3. Simulates async work (e.g., payment validation, inventory check)
4. Updates order status to `COMPLETED`

### Observing the Pipeline

Watch the logs while creating an order:

```
[OrdersService]  Order created: abc-123 — emitting to Kafka...
[OrdersConsumer] [CONSUMER] Received order: abc-123 | Product: "MacBook Pro 16"
[OrdersConsumer] [CONSUMER] Order abc-123 → PROCESSING
[OrdersConsumer] [CONSUMER] Order abc-123 → COMPLETED ✓ | Total: $2499.99
```

Use **Kafka UI** at http://localhost:8080 to inspect:
- Topic messages and payloads
- Consumer group lag
- Broker health

---

## Project Structure

```
nestjs-kafka-pipeline/
├── src/
│   ├── main.ts                      # Hybrid app bootstrap (HTTP + Kafka)
│   ├── app.module.ts                # Root module
│   ├── kafka/
│   │   ├── kafka.constants.ts       # Topic names & injection tokens
│   │   └── kafka.module.ts          # Kafka ClientsModule registration
│   └── orders/
│       ├── dto/
│       │   └── create-order.dto.ts  # Request validation
│       ├── entities/
│       │   └── order.entity.ts      # Order model & status enum
│       ├── orders.consumer.ts       # Kafka event handlers (@EventPattern)
│       ├── orders.controller.ts     # HTTP REST endpoints
│       ├── orders.module.ts         # Orders feature module
│       ├── orders.repository.ts     # In-memory data store
│       └── orders.service.ts        # Business logic + Kafka producer
├── docker-compose.yml               # Zookeeper + Kafka + Kafka UI + App
├── Dockerfile                       # Multi-stage production build
├── .env.example                     # Environment variable reference
└── README.md
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `PORT` | `3000` | HTTP server port |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated Kafka broker addresses |
| `KAFKA_CONSUMER_GROUP_ID` | `order-processor-group` | Consumer group ID |

---

## Extending This Demo

This demo uses an **in-memory repository**. For production, replace it with:

```typescript
// orders.repository.ts — swap with TypeORM/Prisma
@Injectable()
export class OrdersRepository {
  constructor(
    @InjectRepository(Order)
    private readonly repo: Repository<Order>,
  ) {}
  // ...
}
```

Other improvements to consider:
- Add dead-letter queue (DLQ) for failed messages
- Add Prometheus metrics for consumer lag monitoring
- Add multiple consumer group instances for parallel processing
- Replace in-memory store with PostgreSQL + TypeORM

---

## License

MIT © [Ho Huu Hop](https://linkedin.com/in/ho-huu-hop-585856175)
