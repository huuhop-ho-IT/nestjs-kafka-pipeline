import { Controller, Logger } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { KAFKA_TOPICS } from '../kafka/kafka.constants';
import { Order, OrderStatus } from './entities/order.entity';
import { OrdersRepository } from './orders.repository';

@Controller()
export class OrdersConsumer {
  private readonly logger = new Logger(OrdersConsumer.name);

  constructor(private readonly ordersRepository: OrdersRepository) {}

  /**
   * Listens to the 'orders.created' Kafka topic.
   * Simulates async order processing: PENDING → PROCESSING → COMPLETED
   */
  @EventPattern(KAFKA_TOPICS.ORDERS_CREATED)
  async handleOrderCreated(@Payload() message: any) {
    try {
      const order: Order = this.parsePayload(message);
      this.logger.log(`[CONSUMER] Received order: ${order.id} | Product: "${order.product}"`);

      // Step 1: Mark as PROCESSING
      this.ordersRepository.updateStatus(order.id, OrderStatus.PROCESSING);
      this.logger.log(`[CONSUMER] Order ${order.id} → PROCESSING`);

      // Simulate async processing delay (e.g., payment gateway, inventory check)
      await this.simulateProcessing(order);

      // Step 2: Mark as COMPLETED
      this.ordersRepository.updateStatus(order.id, OrderStatus.COMPLETED);
      this.logger.log(
        `[CONSUMER] Order ${order.id} → COMPLETED ✓ | Total: $${(order.price * order.quantity).toFixed(2)}`,
      );
    } catch (err) {
      this.logger.error(`[CONSUMER] Failed to process order: ${err.message}`);
    }
  }

  @EventPattern(KAFKA_TOPICS.ORDERS_PROCESSED)
  async handleOrderProcessed(@Payload() message: any) {
    const order: Order = this.parsePayload(message);
    this.logger.log(
      `[CONSUMER] Order processed event received: ${order.id} | Status: ${order.status}`,
    );
  }

  private parsePayload(message: any): Order {
    // NestJS Kafka transport wraps the value; handle both raw and wrapped formats
    const raw = message?.value ?? message;
    return typeof raw === 'string' ? JSON.parse(raw) : raw;
  }

  private simulateProcessing(order: Order): Promise<void> {
    // Simulate variable processing time (500ms – 2s) based on quantity
    const delay = Math.min(500 + order.quantity * 200, 2000);
    return new Promise((resolve) => setTimeout(resolve, delay));
  }
}
