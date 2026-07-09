import { Injectable } from '@nestjs/common';

/**
 * ProcessedEventsRepository — Idempotency guard
 *
 * Kafka guarantees at-least-once delivery, meaning the same message can arrive
 * more than once (e.g., after a broker restart or consumer rebalance).
 * This store tracks already-processed event IDs to prevent double-processing.
 *
 * Current implementation: in-memory Set (survives until process restart).
 * Production upgrade: Redis with TTL
 *   await redis.set(`processed:${eventId}`, '1', 'EX', 86400);
 */
@Injectable()
export class ProcessedEventsRepository {
  private readonly processedIds = new Set<string>();

  isProcessed(eventId: string): boolean {
    return this.processedIds.has(eventId);
  }

  markProcessed(eventId: string): void {
    this.processedIds.add(eventId);
  }

  count(): number {
    return this.processedIds.size;
  }
}
