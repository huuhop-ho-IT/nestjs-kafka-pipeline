import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { KAFKA_CLIENT } from './kafka.constants';

export interface DlqEntry {
  id: string;
  topic: string;
  orderId: string;
  originalPayload: unknown;
  error: string;
  retryCount: number;
  correlationId: string;
  failedAt: string;
}

/**
 * DlqService — Dead Letter Queue
 *
 * When a Kafka message cannot be processed after MAX_RETRIES:
 *   1. Publishes the failed message to a dedicated DLQ Kafka topic
 *      (visible in Kafka UI under `orders.created.dlq`)
 *   2. Stores it in-memory so `GET /api/orders/dlq` can display it
 *
 * Production upgrade: replace in-memory store with PostgreSQL or Redis.
 */
@Injectable()
export class DlqService implements OnModuleInit {
  private readonly logger = new Logger(DlqService.name);
  private readonly entries: DlqEntry[] = [];

  constructor(@Inject(KAFKA_CLIENT) private readonly kafkaClient: ClientKafka) {}

  async onModuleInit(): Promise<void> {
    await this.kafkaClient.connect();
  }

  async send(dlqTopic: string, entry: Omit<DlqEntry, 'id'>): Promise<void> {
    const id = `dlq-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;
    const full: DlqEntry = { id, ...entry };

    // 1. Publish to Kafka DLQ topic (visible in Kafka UI)
    this.kafkaClient.emit(dlqTopic, {
      key: full.orderId,
      value: JSON.stringify(full),
      headers: {
        'x-correlation-id': full.correlationId,
        'x-dlq-source-topic': full.topic,
        'x-dlq-failed-at': full.failedAt,
        'x-retry-count': String(full.retryCount),
      },
    });

    // 2. Store locally for REST API inspection
    this.entries.push(full);

    this.logger.warn(
      `[DLQ] Dead-lettered → topic=${dlqTopic} | orderId=${full.orderId} | ` +
      `retries=${full.retryCount} | error="${full.error}"`,
    );
  }

  findAll(): DlqEntry[] {
    return [...this.entries];
  }

  count(): number {
    return this.entries.length;
  }
}
