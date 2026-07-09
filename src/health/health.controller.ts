import { Controller, Get } from '@nestjs/common';
import { ApiOperation, ApiTags } from '@nestjs/swagger';
import { OrdersService } from '../orders/orders.service';

/**
 * Health endpoints for Kubernetes / load-balancer probes:
 *
 *   GET /health         — liveness probe (is the process alive?)
 *   GET /health/ready   — readiness probe (is the service ready to accept traffic?)
 *
 * Kubernetes uses these to decide whether to restart a pod (liveness)
 * or route traffic to it (readiness).
 */
@ApiTags('Health')
@Controller('health')
export class HealthController {
  private readonly startedAt = new Date();
  private kafkaConnected = false;

  constructor(private readonly ordersService: OrdersService) {}

  /** Called by OrdersService.onModuleInit — marks Kafka as connected */
  setKafkaReady(): void {
    this.kafkaConnected = true;
  }

  /** Liveness probe — always OK while process is running */
  @Get()
  @ApiOperation({
    summary: 'Liveness probe',
    description: 'Returns 200 while the process is running. Used by Kubernetes to decide when to restart.',
  })
  liveness() {
    return {
      status: 'ok',
      uptime: Math.floor(process.uptime()),
      timestamp: new Date().toISOString(),
      startedAt: this.startedAt.toISOString(),
    };
  }

  /** Readiness probe — OK only when Kafka producer is connected */
  @Get('ready')
  @ApiOperation({
    summary: 'Readiness probe',
    description: 'Returns 200 only when Kafka is connected. Kubernetes stops routing traffic if this fails.',
  })
  readiness() {
    const stats = this.ordersService.getStats();
    return {
      status: this.kafkaConnected ? 'ready' : 'not_ready',
      kafka: this.kafkaConnected ? 'connected' : 'disconnected',
      orders: stats,
      timestamp: new Date().toISOString(),
    };
  }
}
