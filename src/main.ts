import { ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

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

  app.setGlobalPrefix('api');
  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      transform: true,
      forbidNonWhitelisted: true,
    }),
  );

  const swaggerConfig = new DocumentBuilder()
    .setTitle('NestJS Kafka Pipeline')
    .setDescription('Order processing pipeline with NestJS and Kafka.')
    .setVersion('1.0')
    .addTag('Orders', 'Order management and Kafka pipeline endpoints')
    .build();
  const document = SwaggerModule.createDocument(app, swaggerConfig);
  SwaggerModule.setup('api/docs', app, document);

  await app.startAllMicroservices();

  const port = process.env.PORT || 3000;
  await app.listen(port);

  console.log('\n╔══════════════════════════════════════════════╗');
  console.log('║       NestJS Kafka Pipeline — RUNNING        ║');
  console.log('╠══════════════════════════════════════════════╣');
  console.log(`║  API     → http://localhost:${port}/api       ║`);
  console.log(`║  Swagger → http://localhost:${port}/api/docs  ║`);
  console.log(`║  Kafka UI→ http://localhost:8080            ║`);
  console.log('╚══════════════════════════════════════════════╝\n');
}
bootstrap();
