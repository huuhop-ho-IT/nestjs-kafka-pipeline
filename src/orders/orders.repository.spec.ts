import { OrdersRepository } from './orders.repository';
import { Order, OrderStatus } from './entities/order.entity';

const makeOrder = (overrides: Partial<Order> = {}): Order => ({
  id: 'order-1',
  product: 'MacBook Pro',
  quantity: 1,
  price: 2499.99,
  customerId: 'customer-abc',
  status: OrderStatus.PENDING,
  createdAt: new Date('2024-01-01T00:00:00Z'),
  updatedAt: new Date('2024-01-01T00:00:00Z'),
  ...overrides,
});

describe('OrdersRepository', () => {
  let repository: OrdersRepository;

  beforeEach(() => {
    repository = new OrdersRepository();
  });

  describe('save', () => {
    it('should save and return the order', () => {
      const order = makeOrder();
      const result = repository.save(order);
      expect(result).toEqual(order);
    });

    it('should overwrite an existing order with the same id', () => {
      const order = makeOrder();
      repository.save(order);

      const updated = { ...order, status: OrderStatus.COMPLETED };
      repository.save(updated);

      expect(repository.findById('order-1')?.status).toBe(OrderStatus.COMPLETED);
    });
  });

  describe('findById', () => {
    it('should return the order when it exists', () => {
      const order = makeOrder();
      repository.save(order);
      expect(repository.findById('order-1')).toEqual(order);
    });

    it('should return undefined for unknown id', () => {
      expect(repository.findById('non-existent')).toBeUndefined();
    });
  });

  describe('findAll', () => {
    it('should return an empty array when no orders exist', () => {
      expect(repository.findAll()).toEqual([]);
    });

    it('should return all saved orders sorted by createdAt descending', () => {
      const older = makeOrder({ id: 'order-1', createdAt: new Date('2024-01-01') });
      const newer = makeOrder({ id: 'order-2', createdAt: new Date('2024-06-01') });
      repository.save(older);
      repository.save(newer);

      const result = repository.findAll();
      expect(result[0].id).toBe('order-2');
      expect(result[1].id).toBe('order-1');
    });
  });

  describe('updateStatus', () => {
    it('should update the order status and updatedAt', () => {
      const order = makeOrder();
      repository.save(order);

      const before = new Date();
      const result = repository.updateStatus('order-1', OrderStatus.PROCESSING);
      const after = new Date();

      expect(result?.status).toBe(OrderStatus.PROCESSING);
      expect(result?.updatedAt.getTime()).toBeGreaterThanOrEqual(before.getTime());
      expect(result?.updatedAt.getTime()).toBeLessThanOrEqual(after.getTime());
    });

    it('should return undefined for unknown id', () => {
      expect(repository.updateStatus('ghost', OrderStatus.COMPLETED)).toBeUndefined();
    });

    it('should persist the status change via findById', () => {
      const order = makeOrder();
      repository.save(order);
      repository.updateStatus('order-1', OrderStatus.COMPLETED);
      expect(repository.findById('order-1')?.status).toBe(OrderStatus.COMPLETED);
    });
  });

  describe('count', () => {
    it('should return 0 initially', () => {
      expect(repository.count()).toBe(0);
    });

    it('should reflect the number of saved orders', () => {
      repository.save(makeOrder({ id: 'a' }));
      repository.save(makeOrder({ id: 'b' }));
      expect(repository.count()).toBe(2);
    });
  });
});
