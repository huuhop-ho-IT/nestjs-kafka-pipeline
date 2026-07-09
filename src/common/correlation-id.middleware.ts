import { Injectable, NestMiddleware } from '@nestjs/common';
import { Request, Response, NextFunction } from 'express';
import { v4 as uuidv4 } from 'uuid';
import { CORRELATION_ID_HEADER } from '../kafka/kafka.constants';

/**
 * Ensures every inbound HTTP request carries a correlation ID.
 * - If the client already sends `x-correlation-id`, it is preserved.
 * - Otherwise a new UUID v4 is generated.
 * The ID is echoed back in the response header so clients can trace their request.
 *
 * Usage: wire this middleware in AppModule.configure().
 */
@Injectable()
export class CorrelationIdMiddleware implements NestMiddleware {
  use(req: Request, res: Response, next: NextFunction): void {
    const existing = req.headers[CORRELATION_ID_HEADER] as string | undefined;
    const correlationId = existing?.trim() || uuidv4();

    req.headers[CORRELATION_ID_HEADER] = correlationId;
    res.setHeader(CORRELATION_ID_HEADER, correlationId);

    next();
  }
}
