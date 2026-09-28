import { Module } from "@nestjs/common";
import { TerminusModule } from "@nestjs/terminus";
import { DEAD_LETTER_PORT } from "@application/ports/dead-letter.port";
import {
  type IReplicaReader,
  type IReplicaRepository,
  REPLICA_READER,
  REPLICA_REPOSITORY,
  SYNC_CUSTOMER_REPLICA,
  SYNC_ITEM_REPLICA,
  SYNC_TRANSPORT_TYPE_REPLICA,
} from "@application/ports/replica.ports";
import {
  type ISalesOrderRepository,
  SALES_ORDER_REPOSITORY,
} from "@application/ports/sales-order.repository.port";
import {
  SyncCustomerReplicaUseCase,
  SyncItemReplicaUseCase,
  SyncTransportTypeReplicaUseCase,
} from "@application/use-cases/replica-sync.use-cases";
import {
  ChangeSalesOrderStatusUseCase,
  ChangeSalesOrderTransportUseCase,
  CreateSalesOrderUseCase,
  GetSalesOrderUseCase,
  ListSalesOrdersUseCase,
  RescheduleDeliveryUseCase,
  ScheduleDeliveryUseCase,
} from "@application/use-cases/sales-order.use-cases";
import { ReplicaRepositoryPrisma } from "@infrastructure/database/repositories/replica.repository";
import { SalesOrderRepositoryPrisma } from "@infrastructure/database/repositories/sales-order.repository";
import { ReplicaConsumersHealthIndicator } from "@infrastructure/health/consumers.health";
import { HealthController } from "@infrastructure/health/health.controller";
import { PrismaHealthIndicator } from "@infrastructure/health/prisma.health";
import { SalesOrderController } from "@infrastructure/http/sales-order.controller";
import { IdempotencyModule } from "@infrastructure/idempotency/idempotency.module";
import { DeadLetterPublisher } from "@infrastructure/messaging/dead-letter.publisher";
import {
  CustomerReplicaConsumer,
  ItemReplicaConsumer,
  TransportTypeReplicaConsumer,
} from "@infrastructure/messaging/inbound/replica.consumers";
import { MessagingModule } from "@infrastructure/messaging/messaging.module";
import { OutboxPublisherService } from "@infrastructure/outbox/outbox-publisher.service";
import { OutboxRepository } from "@infrastructure/outbox/outbox.repository";

const clock = (): Date => new Date();

// Composicao: use cases nao conhecem Nest (useFactory aqui).
@Module({
  imports: [MessagingModule, IdempotencyModule, TerminusModule],
  controllers: [SalesOrderController, HealthController],
  providers: [
    { provide: SALES_ORDER_REPOSITORY, useClass: SalesOrderRepositoryPrisma },
    ReplicaRepositoryPrisma,
    { provide: REPLICA_REPOSITORY, useExisting: ReplicaRepositoryPrisma },
    { provide: REPLICA_READER, useExisting: ReplicaRepositoryPrisma },
    {
      provide: CreateSalesOrderUseCase,
      useFactory: (orders: ISalesOrderRepository, replicas: IReplicaReader) =>
        new CreateSalesOrderUseCase(orders, replicas, clock),
      inject: [SALES_ORDER_REPOSITORY, REPLICA_READER],
    },
    {
      provide: ChangeSalesOrderTransportUseCase,
      useFactory: (orders: ISalesOrderRepository, replicas: IReplicaReader) =>
        new ChangeSalesOrderTransportUseCase(orders, replicas, clock),
      inject: [SALES_ORDER_REPOSITORY, REPLICA_READER],
    },
    {
      provide: GetSalesOrderUseCase,
      useFactory: (orders: ISalesOrderRepository) => new GetSalesOrderUseCase(orders),
      inject: [SALES_ORDER_REPOSITORY],
    },
    {
      provide: ListSalesOrdersUseCase,
      useFactory: (orders: ISalesOrderRepository) => new ListSalesOrdersUseCase(orders),
      inject: [SALES_ORDER_REPOSITORY],
    },
    ...[ChangeSalesOrderStatusUseCase, ScheduleDeliveryUseCase, RescheduleDeliveryUseCase].map((useCase) => ({
      provide: useCase,
      useFactory: (orders: ISalesOrderRepository) => new useCase(orders, clock),
      inject: [SALES_ORDER_REPOSITORY],
    })),
    {
      provide: SYNC_ITEM_REPLICA,
      useFactory: (replicas: IReplicaRepository) => new SyncItemReplicaUseCase(replicas),
      inject: [REPLICA_REPOSITORY],
    },
    {
      provide: SYNC_CUSTOMER_REPLICA,
      useFactory: (replicas: IReplicaRepository) => new SyncCustomerReplicaUseCase(replicas),
      inject: [REPLICA_REPOSITORY],
    },
    {
      provide: SYNC_TRANSPORT_TYPE_REPLICA,
      useFactory: (replicas: IReplicaRepository) => new SyncTransportTypeReplicaUseCase(replicas),
      inject: [REPLICA_REPOSITORY],
    },
    { provide: DEAD_LETTER_PORT, useExisting: DeadLetterPublisher },
    ItemReplicaConsumer,
    CustomerReplicaConsumer,
    TransportTypeReplicaConsumer,
    OutboxRepository,
    OutboxPublisherService,
    PrismaHealthIndicator,
    ReplicaConsumersHealthIndicator,
  ],
})
export class SalesOrderModule {}
