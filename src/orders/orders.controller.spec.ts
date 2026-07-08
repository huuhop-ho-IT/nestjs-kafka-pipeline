import { Test, TestingModule } from '@nestjs/testing';
import { NotFoundException } from '@nestjs/common';
import { OrdersController } from './orders.controller';
import { OrdersService } from './orders.service';
import { CreateOrderDto } from './dto/create-order.dto';
import { Order, OrderStatus } from './entities/order.entity';

const makeOrder = (overrides: Partial<Order> = {}): Order => ({
  id: 'order-1',
  product: 'MacBook Pro',
  quantity: 2,
  price: 2499.99,
  customerId: 'customer-abc',
  status: OrderStatus.PENDING,
  createdAt: new Date(),
  updatedAt: new Date(),
  ...overrides,
});

const mockOrdersService = {
  createOrder: jest.fn(),
  findAll: jest.fn(),
  findOne: jest.fn(),
  getStats: jest.fn(),
};

describe('OrdersController', () => {
  let controller: OrdersController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [OrdersController],
      providers: [{ provide: OrdersService, useValue: mockOrdersService }],
    }).compile();

    controller = module.get<OrdersController>(OrdersController);
    jest.clearAllMocks();
  });

  describe('POST /orders — create', () => {
    const dto: CreateOrderDto = {
      product: 'MacBook Pro',
      quantity: 2,
      price: 2499.99,
      customerId: 'customer-abc',
    };

    it('should return 201 with the created order wrapped in a message envelope', async () => {
      const order = makeOrder();
      mockOrdersService.createOrder.mockResolvedValue(order);

      const result = await controller.create(dto);

      expect(result.message).toBeDefined();
      expect(result.data).toEqual(order);
    });

    it('should call ordersService.createOrder with the DTO', async () => {
      mockOrdersService.createOrder.mockResolvedValue(makeOrder());
      await controller.create(dto);
      expect(mockOrdersService.createOrder).toHaveBeenCalledWith(dto);
    });
  });

  describe('GET /orders — findAll', () => {
    it('should return all orders with stats', () => {
      const orders = [makeOrder(), makeOrder({ id: 'order-2' })];
      const stats = {
        total: 2,
        byStatus: {
          PENDING: 2,
          PROCESSING: 0,
          COMPLETED: 0,
          FAILED: 0,
        },
      };
      mockOrdersService.findAll.mockReturnValue(orders);
      mockOrdersService.getStats.mockReturnValue(stats);

      const result = controller.findAll();

      expect(result.data).toEqual(orders);
      expect(result.stats).toEqual(stats);
    });
  });

  describe('GET /orders/stats — getStats', () => {
    it('should return stats from the service', () => {
      const stats = { total: 5, byStatus: { PENDING: 1, PROCESSING: 1, COMPLETED: 2, FAILED: 1 } };
      mockOrdersService.getStats.mockReturnValue(stats);
      expect(controller.getStats()).toEqual(stats);
    });
  });

  describe('GET /orders/:id — findOne', () => {
    it('should return the order wrapped in a data envelope', () => {
      const order = makeOrder();
      mockOrdersService.findOne.mockReturnValue(order);

      const result = controller.findOne('order-1');

      expect(result.data).toEqual(order);
    });

    it('should throw NotFoundException when order does not exist', () => {
      mockOrdersService.findOne.mockReturnValue(undefined);
      expect(() => controller.findOne('ghost-id')).toThrow(NotFoundException);
    });

    it('should include the order id in the NotFoundException message', () => {
      mockOrdersService.findOne.mockReturnValue(undefined);
      expect(() => controller.findOne('ghost-id')).toThrow('ghost-id');
    });
  });
});
