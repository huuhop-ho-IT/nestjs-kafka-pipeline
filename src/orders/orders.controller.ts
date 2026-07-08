import {
  Body,
  Controller,
  Get,
  NotFoundException,
  Param,
  Post,
  HttpCode,
  HttpStatus,
} from '@nestjs/common';
import { ApiOperation, ApiParam, ApiResponse, ApiTags } from '@nestjs/swagger';
import { CreateOrderDto } from './dto/create-order.dto';
import { OrdersService } from './orders.service';

@ApiTags('Orders')
@Controller('orders')
export class OrdersController {
  constructor(private readonly ordersService: OrdersService) {}

  @Post()
  @HttpCode(HttpStatus.CREATED)
  @ApiOperation({
    summary: 'Create a new order',
    description:
      'Creates an order and emits an event to Kafka topic `orders.created`. ' +
      'The consumer will asynchronously process the order: PENDING → PROCESSING → COMPLETED.',
  })
  @ApiResponse({ status: 201, description: 'Order created and emitted to Kafka.' })
  @ApiResponse({ status: 400, description: 'Validation error.' })
  async create(@Body() dto: CreateOrderDto) {
    const order = await this.ordersService.createOrder(dto);
    return {
      message: 'Order created and sent to Kafka pipeline',
      data: order,
    };
  }

  @Get()
  @ApiOperation({
    summary: 'List all orders',
    description: 'Returns all orders with their current processing status.',
  })
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

  @Get(':id')
  @ApiOperation({ summary: 'Get a single order by ID' })
  @ApiParam({ name: 'id', description: 'Order UUID' })
  @ApiResponse({ status: 200, description: 'Order found.' })
  @ApiResponse({ status: 404, description: 'Order not found.' })
  findOne(@Param('id') id: string) {
    const order = this.ordersService.findOne(id);
    if (!order) throw new NotFoundException(`Order ${id} not found`);
    return { data: order };
  }
}
