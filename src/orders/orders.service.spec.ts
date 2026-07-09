import { Test, TestingModule } from '@nestjs/testing';
import { OrdersService } from './orders.service';
import { OrdersRepository } from './orders.repository';
import { CORRELATION_ID_HEADER, KAFKA_CLIENT, KAFKA_TOPICS } from '../kafka/kafka.constants';
import { CreateOrderDto } from './dto/create-order.dto';
import { Order, OrderStatus } from './entities/order.entity';

const mockKafkaClient = {
  connect: jest.fn().mockResolvedValue(undefined),
  emit: jest.fn().mockReturnValue({ subscribe: jest.fn() }),
};

const mockOrdersRepository = {
  save: jest.fn(),
  findAll: jest.fn(),
  findById: jest.fn(),
  updateStatus: jest.fn(),
};

describe('OrdersService', () => {
  let service: OrdersService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        OrdersService,
        { provide: KAFKA_CLIENT, useValue: mockKafkaClient },
        { provide: OrdersRepository, useValue: mockOrdersRepository },
      ],
    }).compile();

    service = module.get<OrdersService>(OrdersService);
    jest.clearAllMocks();
  });

  describe('onModuleInit', () => {
    it('should connect to Kafka on init', async () => {
      await service.onModuleInit();
      expect(mockKafkaClient.connect).toHaveBeenCalledTimes(1);
    });
  });

  describe('createOrder', () => {
    const dto: CreateOrderDto = {
      product: 'MacBook Pro',
      quantity: 2,
      price: 2499.99,
      customerId: 'customer-abc',
    };

    beforeEach(() => {
      mockOrdersRepository.save.mockImplementation((order: Order) => order);
    });

    it('should create and return an order with PENDING status', async () => {
      const order = await service.createOrder(dto);

      expect(order.product).toBe(dto.product);
      expect(order.quantity).toBe(dto.quantity);
      expect(order.price).toBe(dto.price);
      expect(order.customerId).toBe(dto.customerId);
      expect(order.status).toBe(OrderStatus.PENDING);
      expect(order.id).toBeDefined();
      expect(order.createdAt).toBeInstanceOf(Date);
    });

    it('should save the order to the repository', async () => {
      await service.createOrder(dto);
      expect(mockOrdersRepository.save).toHaveBeenCalledTimes(1);
      expect(mockOrdersRepository.save).toHaveBeenCalledWith(
        expect.objectContaining({ product: dto.product }),
      );
    });

    it('should emit an event to the Kafka orders.created topic', async () => {
      const order = await service.createOrder(dto);
      expect(mockKafkaClient.emit).toHaveBeenCalledWith(
        KAFKA_TOPICS.ORDERS_CREATED,
        expect.objectContaining({ key: order.id }),
      );
    });

    it('should emit a JSON-serializable value in the Kafka message', async () => {
      await service.createOrder(dto);
      const emitCall = mockKafkaClient.emit.mock.calls[0];
      const message = emitCall[1];
      expect(() => JSON.parse(message.value)).not.toThrow();
    });

    it('should include correlation ID in Kafka message headers', async () => {
      await service.createOrder(dto, 'my-corr-id');
      const emitCall = mockKafkaClient.emit.mock.calls[0];
      const headers = emitCall[1].headers;
      expect(headers[CORRELATION_ID_HEADER]).toBe('my-corr-id');
    });

    it('should generate a correlation ID when none is provided', async () => {
      await service.createOrder(dto);
      const emitCall = mockKafkaClient.emit.mock.calls[0];
      const headers = emitCall[1].headers;
      expect(headers[CORRELATION_ID_HEADER]).toBeDefined();
      expect(typeof headers[CORRELATION_ID_HEADER]).toBe('string');
    });
  });

  describe('findAll', () => {
    it('should delegate to the repository', () => {
      const orders: Order[] = [
        {
          id: 'order-1',
          product: 'iPad',
          quantity: 1,
          price: 999,
          customerId: 'cust-1',
          status: OrderStatus.COMPLETED,
          createdAt: new Date(),
          updatedAt: new Date(),
        },
      ];
      mockOrdersRepository.findAll.mockReturnValue(orders);
      expect(service.findAll()).toEqual(orders);
    });
  });

  describe('findOne', () => {
    it('should return the order when found', () => {
      const order: Order = {
        id: 'order-1',
        product: 'iPhone',
        quantity: 1,
        price: 1199,
        customerId: 'cust-1',
        status: OrderStatus.PENDING,
        createdAt: new Date(),
        updatedAt: new Date(),
      };
      mockOrdersRepository.findById.mockReturnValue(order);
      expect(service.findOne('order-1')).toEqual(order);
    });

    it('should return undefined when order is not found', () => {
      mockOrdersRepository.findById.mockReturnValue(undefined);
      expect(service.findOne('ghost')).toBeUndefined();
    });
  });

  describe('updateStatus', () => {
    it('should delegate to the repository and return updated order', () => {
      const updated: Order = {
        id: 'order-1',
        product: 'AirPods',
        quantity: 1,
        price: 249,
        customerId: 'cust-2',
        status: OrderStatus.COMPLETED,
        createdAt: new Date(),
        updatedAt: new Date(),
      };
      mockOrdersRepository.updateStatus.mockReturnValue(updated);
      const result = service.updateStatus('order-1', OrderStatus.COMPLETED);
      expect(result).toEqual(updated);
    });
  });

  describe('getStats', () => {
    it('should return correct counts by status', () => {
      const orders: Order[] = [
        { id: '1', product: 'A', quantity: 1, price: 10, customerId: 'c', status: OrderStatus.PENDING, createdAt: new Date(), updatedAt: new Date() },
        { id: '2', product: 'B', quantity: 1, price: 10, customerId: 'c', status: OrderStatus.PROCESSING, createdAt: new Date(), updatedAt: new Date() },
        { id: '3', product: 'C', quantity: 1, price: 10, customerId: 'c', status: OrderStatus.COMPLETED, createdAt: new Date(), updatedAt: new Date() },
        { id: '4', product: 'D', quantity: 1, price: 10, customerId: 'c', status: OrderStatus.COMPLETED, createdAt: new Date(), updatedAt: new Date() },
        { id: '5', product: 'E', quantity: 1, price: 10, customerId: 'c', status: OrderStatus.FAILED, createdAt: new Date(), updatedAt: new Date() },
      ];
      mockOrdersRepository.findAll.mockReturnValue(orders);

      const stats = service.getStats();
      expect(stats.total).toBe(5);
      expect(stats.byStatus[OrderStatus.PENDING]).toBe(1);
      expect(stats.byStatus[OrderStatus.PROCESSING]).toBe(1);
      expect(stats.byStatus[OrderStatus.COMPLETED]).toBe(2);
      expect(stats.byStatus[OrderStatus.FAILED]).toBe(1);
    });

    it('should return all zeros when there are no orders', () => {
      mockOrdersRepository.findAll.mockReturnValue([]);
      const stats = service.getStats();
      expect(stats.total).toBe(0);
      Object.values(stats.byStatus).forEach((count) => expect(count).toBe(0));
    });
  });
});
