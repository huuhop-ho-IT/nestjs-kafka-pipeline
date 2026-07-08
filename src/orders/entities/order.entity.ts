export enum OrderStatus {
  PENDING = 'PENDING',
  PROCESSING = 'PROCESSING',
  COMPLETED = 'COMPLETED',
  FAILED = 'FAILED',
}

export class Order {
  id: string;
  product: string;
  quantity: number;
  price: number;
  customerId: string;
  status: OrderStatus;
  createdAt: Date;
  updatedAt: Date;
}
