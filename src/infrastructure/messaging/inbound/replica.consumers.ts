import { Inject, Injectable } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import type { Kafka } from "kafkajs";
import { type ILogger, LOGGER_TOKEN } from "@common/logger/logger.interface";
import { DEAD_LETTER_PORT, type DeadLetterPort } from "@application/ports/dead-letter.port";
import {
  type CustomerReplicaEvent,
  type ItemReplicaEvent,
  SYNC_CUSTOMER_REPLICA,
  SYNC_ITEM_REPLICA,
  SYNC_TRANSPORT_TYPE_REPLICA,
  type SyncReplicaPort,
  type TransportTypeReplicaEvent,
} from "@application/ports/replica.ports";
import { type Env, readEnv } from "@config/env";
import type { InboundMessage } from "@infrastructure/messaging/kafka-consumer.base";
import { KAFKA_CLIENT } from "@infrastructure/messaging/kafka.tokens";
import { MetricsService } from "@infrastructure/metrics/metrics.service";
import { CUSTOMER_TOPICS, decodeCustomerEvent } from "./customer.decoder";
import type { DecodeResult } from "./envelope.decoder";
import { decodeItemCreated, ITEM_CREATED_TOPIC } from "./item-created.decoder";
import { ReplicaEventConsumer, subscriptionFor } from "./replica-event.consumer";
import { decodeTransportTypeEvent, TRANSPORT_TYPE_TOPICS } from "./transport-type.decoder";

@Injectable()
export class ItemReplicaConsumer extends ReplicaEventConsumer<ItemReplicaEvent> {
  constructor(
    @Inject(KAFKA_CLIENT) kafka: Kafka,
    @Inject(LOGGER_TOKEN) logger: ILogger,
    @Inject(SYNC_ITEM_REPLICA) sync: SyncReplicaPort<ItemReplicaEvent>,
    @Inject(DEAD_LETTER_PORT) deadLetter: DeadLetterPort,
    metrics: MetricsService,
    config: ConfigService<Env, true>,
  ) {
    super(
      kafka,
      logger,
      subscriptionFor(config, readEnv(config, "CATALOG_ITEM_SYNC_GROUP_ID"), [ITEM_CREATED_TOPIC]),
      sync,
      deadLetter,
      metrics,
    );
  }

  protected decode(message: InboundMessage): DecodeResult<ItemReplicaEvent> {
    return decodeItemCreated(message.value, message);
  }
}

@Injectable()
export class CustomerReplicaConsumer extends ReplicaEventConsumer<CustomerReplicaEvent> {
  constructor(
    @Inject(KAFKA_CLIENT) kafka: Kafka,
    @Inject(LOGGER_TOKEN) logger: ILogger,
    @Inject(SYNC_CUSTOMER_REPLICA) sync: SyncReplicaPort<CustomerReplicaEvent>,
    @Inject(DEAD_LETTER_PORT) deadLetter: DeadLetterPort,
    metrics: MetricsService,
    config: ConfigService<Env, true>,
  ) {
    super(
      kafka,
      logger,
      subscriptionFor(config, readEnv(config, "CUSTOMER_SYNC_GROUP_ID"), CUSTOMER_TOPICS),
      sync,
      deadLetter,
      metrics,
    );
  }

  protected decode(message: InboundMessage): DecodeResult<CustomerReplicaEvent> {
    return decodeCustomerEvent(message.value);
  }
}

@Injectable()
export class TransportTypeReplicaConsumer extends ReplicaEventConsumer<TransportTypeReplicaEvent> {
  constructor(
    @Inject(KAFKA_CLIENT) kafka: Kafka,
    @Inject(LOGGER_TOKEN) logger: ILogger,
    @Inject(SYNC_TRANSPORT_TYPE_REPLICA) sync: SyncReplicaPort<TransportTypeReplicaEvent>,
    @Inject(DEAD_LETTER_PORT) deadLetter: DeadLetterPort,
    metrics: MetricsService,
    config: ConfigService<Env, true>,
  ) {
    super(
      kafka,
      logger,
      subscriptionFor(config, readEnv(config, "TRANSPORT_TYPE_SYNC_GROUP_ID"), TRANSPORT_TYPE_TOPICS),
      sync,
      deadLetter,
      metrics,
    );
  }

  protected decode(message: InboundMessage): DecodeResult<TransportTypeReplicaEvent> {
    return decodeTransportTypeEvent(message.value);
  }
}
