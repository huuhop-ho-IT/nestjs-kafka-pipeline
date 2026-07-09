import { Test, TestingModule } from '@nestjs/testing';
import { OrdersConsumer } from './orders.consumer';
import { OrdersRepository } from './orders.repository';
import { Order, OrderStatus } from './entities/order.entity';

const mockOrdersRepository = {
  updateStatus: jest.fn(),
};

const makeOrder = (overrides: Partial<Order> = {}): Order => ({
  id: 'order-1',
  product: 'MacBook Pro',
  quantity: 1,
  price: 2499.99,
  customerId: 'customer-abc',
  status: OrderStatus.PENDING,
  createdAt: new Date(),
  updatedAt: new Date(),
  ...overrides,
});

describe('OrdersConsumer', () => {
  let consumer: OrdersConsumer;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        OrdersConsumer,
        { provide: OrdersRepository, useValue: mockOrdersRepository },
      ],
    }).compile();

    consumer = module.get<OrdersConsumer>(OrdersConsumer);
    jest.clearAllMocks();

    jest
      .spyOn(consumer as any, 'simulateProcessing')
      .mockResolvedValue(undefined);
  });

  describe('handleOrderCreated', () => {
    it('should mark the order as PROCESSING then COMPLETED', async () => {
      const order = makeOrder();
      await consumer.handleOrderCreated(order);

      expect(mockOrdersRepository.updateStatus).toHaveBeenCalledTimes(2);
      expect(mockOrdersRepository.updateStatus).toHaveBeenNthCalledWith(
        1,
        order.id,
        OrderStatus.PROCESSING,
      );
      expect(mockOrdersRepository.updateStatus).toHaveBeenNthCalledWith(
        2,
        order.id,
        OrderStatus.COMPLETED,
      );
    });

    it('should accept a JSON string wrapped in a value field (NestJS Kafka transport format)', async () => {
      const order = makeOrder();
      const wrapped = { value: JSON.stringify(order) };
      await consumer.handleOrderCreated(wrapped);

      expect(mockOrdersRepository.updateStatus).toHaveBeenCalledWith(
        order.id,
        OrderStatus.PROCESSING,
      );
    });

    it('should accept a raw JSON string payload', async () => {
      const order = makeOrder();
      await consumer.handleOrderCreated(JSON.stringify(order));

      expect(mockOrdersRepository.updateStatus).toHaveBeenCalledWith(
        order.id,
        OrderStatus.PROCESSING,
      );
    });

    it('should not throw when processing raises an error', async () => {
      jest
        .spyOn(consumer as any, 'simulateProcessing')
        .mockRejectedValue(new Error('payment gateway timeout'));

      const order = makeOrder();
      await expect(consumer.handleOrderCreated(order)).resolves.not.toThrow();
    });

    it('should not call updateStatus(COMPLETED) when an error occurs', async () => {
      jest
        .spyOn(consumer as any, 'simulateProcessing')
        .mockRejectedValue(new Error('error'));

      await consumer.handleOrderCreated(makeOrder());

      const completedCalls = mockOrdersRepository.updateStatus.mock.calls.filter(
        ([, status]) => status === OrderStatus.COMPLETED,
      );
      expect(completedCalls).toHaveLength(0);
    });
  });

  describe('handleOrderProcessed', () => {
    it('should handle an order processed event without errors', async () => {
      const order = makeOrder({ status: OrderStatus.COMPLETED });
      await expect(consumer.handleOrderProcessed(order)).resolves.not.toThrow();
    });
  });

  describe('simulateProcessing (private)', () => {
    it('should resolve faster for small quantities and slower for large quantities', async () => {
      jest.restoreAllMocks();

      const small = makeOrder({ quantity: 1 });
      const large = makeOrder({ quantity: 10 });

      const start1 = Date.now();
      await (consumer as any).simulateProcessing(small);
      const elapsed1 = Date.now() - start1;

      const start2 = Date.now();
      await (consumer as any).simulateProcessing(large);
      const elapsed2 = Date.now() - start2;

      expect(elapsed2).toBeGreaterThanOrEqual(elapsed1);
    }, 10000);
  });
});
