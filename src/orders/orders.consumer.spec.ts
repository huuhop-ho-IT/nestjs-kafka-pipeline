import { Test, TestingModule } from '@nestjs/testing';
import { OrdersConsumer } from './orders.consumer';
import { OrdersRepository } from './orders.repository';
import { ProcessedEventsRepository } from './processed-events.repository';
import { DlqService } from '../kafka/dlq.service';
import { Order, OrderStatus } from './entities/order.entity';

const mockOrdersRepository = {
  updateStatus: jest.fn(),
};

const mockProcessedEventsRepository = {
  isProcessed: jest.fn().mockReturnValue(false),
  markProcessed: jest.fn(),
};

const mockDlqService = {
  send: jest.fn().mockResolvedValue(undefined),
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
        { provide: ProcessedEventsRepository, useValue: mockProcessedEventsRepository },
        { provide: DlqService, useValue: mockDlqService },
      ],
    }).compile();

    consumer = module.get<OrdersConsumer>(OrdersConsumer);
    jest.clearAllMocks();

    // Reset idempotency mock to default (not yet processed)
    mockProcessedEventsRepository.isProcessed.mockReturnValue(false);

    // Mock simulateProcessing to resolve immediately (avoid 500ms+ delays)
    jest.spyOn(consumer as any, 'simulateProcessing').mockResolvedValue(undefined);

    // Mock sleep to run instantly (avoid retry backoff delays in unit tests)
    jest.spyOn(consumer as any, 'sleep').mockResolvedValue(undefined);
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

    it('should not throw when processing raises an error (sends to DLQ instead)', async () => {
      jest
        .spyOn(consumer as any, 'simulateProcessing')
        .mockRejectedValue(new Error('payment gateway timeout'));

      const order = makeOrder();
      await expect(consumer.handleOrderCreated(order)).resolves.not.toThrow();
      expect(mockDlqService.send).toHaveBeenCalledTimes(1);
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

    it('should call updateStatus(FAILED) after exhausting all retries', async () => {
      jest
        .spyOn(consumer as any, 'simulateProcessing')
        .mockRejectedValue(new Error('permanent failure'));

      await consumer.handleOrderCreated(makeOrder());

      expect(mockOrdersRepository.updateStatus).toHaveBeenCalledWith(
        'order-1',
        OrderStatus.FAILED,
      );
    });

    it('should skip processing duplicate events (idempotency)', async () => {
      mockProcessedEventsRepository.isProcessed.mockReturnValue(true);

      await consumer.handleOrderCreated(makeOrder());

      expect(mockOrdersRepository.updateStatus).not.toHaveBeenCalled();
    });

    it('should send to DLQ after exhausting all retries', async () => {
      jest
        .spyOn(consumer as any, 'simulateProcessing')
        .mockRejectedValue(new Error('broker unavailable'));

      await consumer.handleOrderCreated(makeOrder());

      expect(mockDlqService.send).toHaveBeenCalledTimes(1);
      expect(mockDlqService.send).toHaveBeenCalledWith(
        'orders.created.dlq',
        expect.objectContaining({
          orderId: 'order-1',
          error: 'broker unavailable',
        }),
      );
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
