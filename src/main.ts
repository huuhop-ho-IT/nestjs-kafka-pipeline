import { ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  // ─── Kafka Microservice (Consumer) ───────────────────────────────────────────
  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.KAFKA,
    options: {
      client: {
        clientId: 'order-consumer',
        brokers: [(process.env.KAFKA_BROKERS || 'localhost:9092')],
        retry: {
          initialRetryTime: 300,
          retries: 10,
        },
      },
      consumer: {
        groupId: process.env.KAFKA_CONSUMER_GROUP_ID || 'order-processor-group',
        allowAutoTopicCreation: true,
      },
    },
  });

  // ─── HTTP Server ──────────────────────────────────────────────────────────────
  app.setGlobalPrefix('api');
  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      transform: true,
      forbidNonWhitelisted: true,
    }),
  );

  // ─── Swagger Documentation ────────────────────────────────────────────────────
  const swaggerConfig = new DocumentBuilder()
    .setTitle('NestJS Kafka Pipeline')
    .setDescription(
      'Demo: Asynchronous order processing pipeline using NestJS + Apache Kafka.\n\n' +
      '**Flow:** POST /orders → Kafka topic `orders.created` → Consumer processes → COMPLETED',
    )
    .setVersion('1.0')
    .addTag('Orders', 'Order management and Kafka pipeline endpoints')
    .build();
  const document = SwaggerModule.createDocument(app, swaggerConfig);
  SwaggerModule.setup('api/docs', app, document);

  // ─── Start ────────────────────────────────────────────────────────────────────
  await app.startAllMicroservices();

  const port = process.env.PORT || 3000;
  await app.listen(port);

  console.log('\n╔══════════════════════════════════════════════╗');
  console.log('║       NestJS Kafka Pipeline — RUNNING        ║');
  console.log('╠══════════════════════════════════════════════╣');
  console.log(`║  HTTP API  → http://localhost:${port}/api       ║`);
  console.log(`║  Swagger   → http://localhost:${port}/api/docs  ║`);
  console.log(`║  Kafka UI  → http://localhost:8080            ║`);
  console.log('╚══════════════════════════════════════════════╝\n');
}
bootstrap();
