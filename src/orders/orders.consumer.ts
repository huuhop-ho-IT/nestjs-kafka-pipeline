import { Controller, Logger } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, Payload } from '@nestjs/microservices';
import { CORRELATION_ID_HEADER, KAFKA_TOPICS, RETRY_CONFIG } from '../kafka/kafka.constants';
import { DlqService } from '../kafka/dlq.service';
import { Order, OrderStatus } from './entities/order.entity';
import { OrdersRepository } from './orders.repository';
import { ProcessedEventsRepository } from './processed-events.repository';

@Controller()
export class OrdersConsumer {
  private readonly logger = new Logger(OrdersConsumer.name);

  constructor(
    private readonly ordersRepository: OrdersRepository,
    private readonly processedEvents: ProcessedEventsRepository,
    private readonly dlqService: DlqService,
  ) {}

  /**
   * Main order consumer with:
   *   Correlation ID propagation from Kafka headers
   *   Idempotency guard (skip duplicates)
   *   Retry with exponential backoff (up to MAX_RETRIES)
   *   Dead Letter Queue on permanent failure
   */
  @EventPattern(KAFKA_TOPICS.ORDERS_CREATED)
  async handleOrderCreated(
    @Payload() message: any,
    @Ctx() context: KafkaContext = null as any,
  ): Promise<void> {
    const order: Order = this.parsePayload(message);
    const headers = context?.getMessage?.()?.headers ?? {};
    const correlationId = (headers[CORRELATION_ID_HEADER] as Buffer | string)?.toString() ?? 'unknown';

    const logCtx = { orderId: order.id, correlationId };

    // Idempotency guard 
    if (this.processedEvents.isProcessed(order.id)) {
      this.logger.warn({ ...logCtx, event: 'duplicate_skipped' });
      return;
    }

    this.logger.log({ ...logCtx, event: 'processing_started', product: order.product });

    // Retry loop with exponential backoff 
    let lastError: Error | undefined;

    for (let attempt = 0; attempt <= RETRY_CONFIG.MAX_RETRIES; attempt++) {
      try {
        if (attempt > 0) {
          const delayMs = RETRY_CONFIG.BASE_DELAY_MS * Math.pow(2, attempt - 1);
          this.logger.warn({ ...logCtx, event: 'retrying', attempt, delayMs });
          await this.sleep(delayMs);
        }

        this.ordersRepository.updateStatus(order.id, OrderStatus.PROCESSING);
        await this.simulateProcessing(order);
        this.ordersRepository.updateStatus(order.id, OrderStatus.COMPLETED);

        // Mark as processed only on success — prevents re-processing on redelivery
        this.processedEvents.markProcessed(order.id);

        this.logger.log({
          ...logCtx,
          event: 'processing_completed',
          attempt,
          total: order.price * order.quantity,
        });
        return;
      } catch (err) {
        lastError = err instanceof Error ? err : new Error(String(err));
        this.logger.warn({ ...logCtx, event: 'attempt_failed', attempt, error: lastError.message });
      }
    }

    //  All retries exhausted → Dead Letter Queue 
    this.ordersRepository.updateStatus(order.id, OrderStatus.FAILED);

    await this.dlqService.send(KAFKA_TOPICS.ORDERS_CREATED_DLQ, {
      topic: KAFKA_TOPICS.ORDERS_CREATED,
      orderId: order.id,
      originalPayload: order,
      error: lastError?.message ?? 'unknown error',
      retryCount: RETRY_CONFIG.MAX_RETRIES,
      correlationId,
      failedAt: new Date().toISOString(),
    });

    this.logger.error({ ...logCtx, event: 'dead_lettered', error: lastError?.message });
  }

  @EventPattern(KAFKA_TOPICS.ORDERS_PROCESSED)
  async handleOrderProcessed(
    @Payload() message: any,
    @Ctx() context: KafkaContext = null as any,
  ): Promise<void> {
    const order: Order = this.parsePayload(message);
    const correlationId =
      (context?.getMessage?.()?.headers?.[CORRELATION_ID_HEADER] as Buffer | string)?.toString() ?? 'unknown';

    this.logger.log({
      orderId: order.id,
      correlationId,
      event: 'order_processed_received',
      status: order.status,
    });
  }

  private parsePayload(message: any): Order {
    const raw = message?.value ?? message;
    return typeof raw === 'string' ? JSON.parse(raw) : raw;
  }

  private simulateProcessing(order: Order): Promise<void> {
    // case 1: success 
    const delay = Math.min(500 + order.quantity * 200, 2000);
    return new Promise((resolve) => setTimeout(resolve, delay));
    // case 2: failure if you want to test DLQ
    // return Promise.reject(new Error('Simulated payment failure'));
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
