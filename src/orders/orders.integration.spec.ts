import { Test, TestingModule } from '@nestjs/testing';
import { INestApplication, ValidationPipe } from '@nestjs/common';
import * as request from 'supertest';
import { OrdersController } from './orders.controller';
import { OrdersService } from './orders.service';
import { OrdersRepository } from './orders.repository';
import { ProcessedEventsRepository } from './processed-events.repository';
import { DlqService } from '../kafka/dlq.service';
import { KAFKA_CLIENT } from '../kafka/kafka.constants';
import { OrderStatus } from './entities/order.entity';

const mockKafkaClient = {
  connect: jest.fn().mockResolvedValue(undefined),
  emit: jest.fn().mockReturnValue({ subscribe: jest.fn() }),
};

const mockDlqService = {
  send: jest.fn().mockResolvedValue(undefined),
  findAll: jest.fn().mockReturnValue([]),
  count: jest.fn().mockReturnValue(0),
};

describe('Orders API (integration)', () => {
  let app: INestApplication;
  let repository: OrdersRepository;

  beforeAll(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [OrdersController],
      providers: [
        OrdersService,
        OrdersRepository,
        ProcessedEventsRepository,
        { provide: KAFKA_CLIENT, useValue: mockKafkaClient },
        { provide: DlqService, useValue: mockDlqService },
      ],
    }).compile();

    app = module.createNestApplication();
    app.useGlobalPipes(new ValidationPipe({ whitelist: true, forbidNonWhitelisted: true }));
    await app.init();

    repository = module.get<OrdersRepository>(OrdersRepository);
  });

  afterAll(async () => {
    await app.close();
  });

  afterEach(() => {
    jest.clearAllMocks();
    (repository as any).orders.clear();
  });

  describe('POST /orders', () => {
    const validBody = {
      product: 'MacBook Pro 16"',
      quantity: 2,
      price: 2499.99,
      customerId: 'customer-abc-123',
    };

    it('should return 201 and the created order', async () => {
      const res = await request(app.getHttpServer())
        .post('/orders')
        .send(validBody)
        .expect(201);

      expect(res.body.message).toBeDefined();
      expect(res.body.data).toMatchObject({
        product: validBody.product,
        quantity: validBody.quantity,
        price: validBody.price,
        customerId: validBody.customerId,
        status: OrderStatus.PENDING,
      });
      expect(res.body.data.id).toBeDefined();
    });

    it('should emit a Kafka event after creating the order', async () => {
      await request(app.getHttpServer()).post('/orders').send(validBody).expect(201);
      expect(mockKafkaClient.emit).toHaveBeenCalledTimes(1);
    });

    it('should return 400 when required fields are missing', async () => {
      await request(app.getHttpServer())
        .post('/orders')
        .send({ product: 'iPad' })
        .expect(400);
    });

    it('should return 400 when quantity is not a positive number', async () => {
      await request(app.getHttpServer())
        .post('/orders')
        .send({ ...validBody, quantity: -1 })
        .expect(400);
    });

    it('should return 400 when price is negative', async () => {
      await request(app.getHttpServer())
        .post('/orders')
        .send({ ...validBody, price: -50 })
        .expect(400);
    });

    it('should return 400 when extra unknown fields are sent', async () => {
      await request(app.getHttpServer())
        .post('/orders')
        .send({ ...validBody, hackerField: 'injection' })
        .expect(400);
    });
  });

  describe('GET /orders', () => {
    it('should return an empty list before any orders are created', async () => {
      const res = await request(app.getHttpServer()).get('/orders').expect(200);
      expect(res.body.data).toEqual([]);
      expect(res.body.stats.total).toBe(0);
    });

    it('should list all previously created orders', async () => {
      const body = { product: 'iPhone 15', quantity: 1, price: 1199.0, customerId: 'cust-1' };
      await request(app.getHttpServer()).post('/orders').send(body);

      const res = await request(app.getHttpServer()).get('/orders').expect(200);
      expect(res.body.data.length).toBeGreaterThanOrEqual(1);
      expect(res.body.stats.total).toBeGreaterThanOrEqual(1);
    });
  });

  describe('GET /orders/stats', () => {
    it('should return stats with correct shape', async () => {
      const res = await request(app.getHttpServer()).get('/orders/stats').expect(200);
      expect(res.body).toHaveProperty('total');
      expect(res.body).toHaveProperty('byStatus');
      expect(res.body.byStatus).toHaveProperty(OrderStatus.PENDING);
      expect(res.body.byStatus).toHaveProperty(OrderStatus.COMPLETED);
    });
  });

  describe('GET /orders/:id', () => {
    it('should return 200 and the order when it exists', async () => {
      const createRes = await request(app.getHttpServer())
        .post('/orders')
        .send({ product: 'AirPods Pro', quantity: 1, price: 249.0, customerId: 'cust-2' });

      const { id } = createRes.body.data;

      const res = await request(app.getHttpServer()).get(`/orders/${id}`).expect(200);
      expect(res.body.data.id).toBe(id);
    });

    it('should return 404 for a non-existent order id', async () => {
      await request(app.getHttpServer()).get('/orders/non-existent-id').expect(404);
    });
  });

  describe('Full order lifecycle', () => {
    it('should reflect a newly created order in both list and detail endpoints', async () => {
      const body = { product: 'Apple Watch Ultra', quantity: 1, price: 799.0, customerId: 'cust-lifecycle' };

      const createRes = await request(app.getHttpServer())
        .post('/orders')
        .send(body)
        .expect(201);

      const { id } = createRes.body.data;

      const listRes = await request(app.getHttpServer()).get('/orders').expect(200);
      const inList = listRes.body.data.find((o: any) => o.id === id);
      expect(inList).toBeDefined();

      const detailRes = await request(app.getHttpServer()).get(`/orders/${id}`).expect(200);
      expect(detailRes.body.data.product).toBe(body.product);
      expect(detailRes.body.data.status).toBe(OrderStatus.PENDING);
    });
  });
});
