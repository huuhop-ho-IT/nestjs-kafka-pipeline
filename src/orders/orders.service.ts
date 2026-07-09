import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { v4 as uuidv4 } from 'uuid';
import { CORRELATION_ID_HEADER, KAFKA_CLIENT, KAFKA_TOPICS } from '../kafka/kafka.constants';
import { CreateOrderDto } from './dto/create-order.dto';
import { Order, OrderStatus } from './entities/order.entity';
import { OrdersRepository } from './orders.repository';

@Injectable()
export class OrdersService implements OnModuleInit {
  private readonly logger = new Logger(OrdersService.name);

  constructor(
    @Inject(KAFKA_CLIENT) private readonly kafkaClient: ClientKafka,
    private readonly ordersRepository: OrdersRepository,
  ) {}

  async onModuleInit() {
    await this.kafkaClient.connect();
    this.logger.log('Kafka producer connected');
  }

  async createOrder(dto: CreateOrderDto, correlationId?: string): Promise<Order> {
    const order: Order = {
      id: uuidv4(),
      product: dto.product,
      quantity: dto.quantity,
      price: dto.price,
      customerId: dto.customerId,
      status: OrderStatus.PENDING,
      createdAt: new Date(),
      updatedAt: new Date(),
    };

    this.ordersRepository.save(order);

    const cid = correlationId ?? uuidv4();
    this.logger.log({
      event: 'order_created',
      orderId: order.id,
      correlationId: cid,
      product: order.product,
    });

    this.kafkaClient.emit(KAFKA_TOPICS.ORDERS_CREATED, {
      key: order.id,
      value: JSON.stringify(order),
      headers: {
        [CORRELATION_ID_HEADER]: cid,
        'x-source-service': 'order-service',
        'x-event-timestamp': new Date().toISOString(),
        'x-retry-count': '0',
      },
    });

    return order;
  }

  findAll(): Order[] {
    return this.ordersRepository.findAll();
  }

  findOne(id: string): Order | undefined {
    return this.ordersRepository.findById(id);
  }

  updateStatus(id: string, status: OrderStatus): Order | undefined {
    return this.ordersRepository.updateStatus(id, status);
  }

  getStats() {
    const orders = this.ordersRepository.findAll();
    return {
      total: orders.length,
      byStatus: {
        [OrderStatus.PENDING]: orders.filter((o) => o.status === OrderStatus.PENDING).length,
        [OrderStatus.PROCESSING]: orders.filter((o) => o.status === OrderStatus.PROCESSING).length,
        [OrderStatus.COMPLETED]: orders.filter((o) => o.status === OrderStatus.COMPLETED).length,
        [OrderStatus.FAILED]: orders.filter((o) => o.status === OrderStatus.FAILED).length,
      },
    };
  }
}
