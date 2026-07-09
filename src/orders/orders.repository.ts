import { Injectable } from '@nestjs/common';
import { Order, OrderStatus } from './entities/order.entity';

// in-memory store for demo — swap with DB in prod
@Injectable()
export class OrdersRepository {
  private readonly orders = new Map<string, Order>();

  save(order: Order): Order {
    this.orders.set(order.id, order);
    return order;
  }

  findById(id: string): Order | undefined {
    return this.orders.get(id);
  }

  findAll(): Order[] {
    return Array.from(this.orders.values()).sort(
      (a, b) => b.createdAt.getTime() - a.createdAt.getTime(),
    );
  }

  updateStatus(id: string, status: OrderStatus): Order | undefined {
    const order = this.orders.get(id);
    if (!order) return undefined;

    order.status = status;
    order.updatedAt = new Date();
    this.orders.set(id, order);
    return order;
  }

  count(): number {
    return this.orders.size;
  }
}
