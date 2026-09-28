import { Injectable } from "@nestjs/common";
import { type HealthIndicatorResult, HealthIndicatorService } from "@nestjs/terminus";
import type { ConsumerHealth } from "@infrastructure/messaging/kafka-consumer.base";
import {
  CustomerReplicaConsumer,
  ItemReplicaConsumer,
  TransportTypeReplicaConsumer,
} from "@infrastructure/messaging/inbound/replica.consumers";

// Pronto so com os tres consumers de replica consumindo: degradado (retry
// esgotado, banco ou broker fora) ou ainda entrando no grupo = down.
@Injectable()
export class ReplicaConsumersHealthIndicator {
  constructor(
    private readonly items: ItemReplicaConsumer,
    private readonly customers: CustomerReplicaConsumer,
    private readonly transportTypes: TransportTypeReplicaConsumer,
    private readonly healthIndicatorService: HealthIndicatorService,
  ) {}

  isHealthy<const K extends string>(key: K): HealthIndicatorResult<K> {
    const indicator = this.healthIndicatorService.check(key);
    const consumers: Record<string, ConsumerHealth> = {
      items: this.items.getHealth(),
      customers: this.customers.getHealth(),
      transportTypes: this.transportTypes.getHealth(),
    };
    const data = Object.fromEntries(
      Object.entries(consumers).map(([name, health]) => [name, `${health.status} desde ${health.since.toISOString()}`]),
    );
    const allRunning = Object.values(consumers).every((health) => health.status === "running");
    return allRunning ? indicator.up(data) : indicator.down(data);
  }
}
