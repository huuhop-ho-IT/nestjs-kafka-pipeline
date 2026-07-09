export const KAFKA_CLIENT = 'KAFKA_CLIENT';

export const KAFKA_TOPICS = {
  ORDERS_CREATED: 'orders.created',
  ORDERS_PROCESSED: 'orders.processed',
  ORDERS_CREATED_DLQ: 'orders.created.dlq',
} as const;

export const RETRY_CONFIG = {
  /** Maximum number of retry attempts before dead-lettering the message */
  MAX_RETRIES: 3,
  /** Base delay in ms — doubles on each retry (100 → 200 → 400ms) */
  BASE_DELAY_MS: 100,
} as const;

export const CORRELATION_ID_HEADER = 'x-correlation-id' as const;
