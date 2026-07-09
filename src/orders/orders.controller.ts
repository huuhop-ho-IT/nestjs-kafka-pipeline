import {
  Body,
  Controller,
  Get,
  NotFoundException,
  Param,
  Post,
  HttpCode,
  HttpStatus,
  Req,
} from '@nestjs/common';
import { ApiOperation, ApiParam, ApiResponse, ApiTags } from '@nestjs/swagger';
import { Request } from 'express';
import { CORRELATION_ID_HEADER } from '../kafka/kafka.constants';
import { CreateOrderDto } from './dto/create-order.dto';
import { OrdersService } from './orders.service';
import { DlqService } from '../kafka/dlq.service';

@ApiTags('Orders')
@Controller('orders')
export class OrdersController {
  constructor(
    private readonly ordersService: OrdersService,
    private readonly dlqService: DlqService,
  ) {}

  @Post()
  @HttpCode(HttpStatus.CREATED)
  @ApiOperation({
    summary: 'Create a new order',
    description:
      'Creates an order and emits an event to Kafka topic `orders.created`. ' +
      'The consumer will asynchronously process the order: PENDING → PROCESSING → COMPLETED.',
  })
  @ApiResponse({ status: HttpStatus.CREATED, description: 'Order created and emitted to Kafka.' })
  @ApiResponse({ status: HttpStatus.BAD_REQUEST, description: 'Validation error.' })
  async create(@Body() dto: CreateOrderDto, @Req() req?: Request) {
    const correlationId = req?.headers?.[CORRELATION_ID_HEADER] as string | undefined;
    const order = await this.ordersService.createOrder(dto, correlationId);
    return {
      message: 'Order created and sent to Kafka pipeline',
      data: order,
      correlationId: correlationId ?? order.id,
    };
  }

  @Get()
  @ApiOperation({ summary: 'List all orders' })
  findAll() {
    return {
      data: this.ordersService.findAll(),
      stats: this.ordersService.getStats(),
    };
  }

  @Get('stats')
  @ApiOperation({ summary: 'Get order statistics' })
  getStats() {
    return this.ordersService.getStats();
  }

  @Get('dlq')
  @ApiOperation({
    summary: 'List dead-lettered orders',
    description:
      'Returns orders that failed processing after all retries were exhausted. ' +
      'Includes the original payload, error message, and retry count for debugging.',
  })
  getDlq() {
    return {
      data: this.dlqService.findAll(),
      total: this.dlqService.count(),
    };
  }

  @Get(':id')
  @ApiOperation({ summary: 'Get a single order by ID' })
  @ApiParam({ name: 'id', description: 'Order UUID' })
  @ApiResponse({ status: HttpStatus.CREATED, description: 'Order found.' })
  @ApiResponse({ status: HttpStatus.BAD_REQUEST, description: 'Order not found.' })
  findOne(@Param('id') id: string) {
    const order = this.ordersService.findOne(id);
    if (!order) throw new NotFoundException(`Order ${id} not found`);
    return { data: order };
  }
}
