import { Module } from '@nestjs/common';
import { KafkaModule } from '../kafka/kafka.module';
import { DlqService } from '../kafka/dlq.service';
import { OrdersConsumer } from './orders.consumer';
import { OrdersController } from './orders.controller';
import { OrdersRepository } from './orders.repository';
import { OrdersService } from './orders.service';
import { ProcessedEventsRepository } from './processed-events.repository';

@Module({
  imports: [KafkaModule],
  controllers: [OrdersController, OrdersConsumer],
  providers: [
    OrdersService,
    OrdersRepository,
    ProcessedEventsRepository,
    DlqService,
  ],
  exports: [OrdersService],
})
export class OrdersModule {}
