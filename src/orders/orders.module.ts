import { Module } from '@nestjs/common';
import { KafkaModule } from '../kafka/kafka.module';
import { OrdersConsumer } from './orders.consumer';
import { OrdersController } from './orders.controller';
import { OrdersRepository } from './orders.repository';
import { OrdersService } from './orders.service';

@Module({
  imports: [KafkaModule],
  controllers: [OrdersController, OrdersConsumer],
  providers: [OrdersService, OrdersRepository],
})
export class OrdersModule {}
