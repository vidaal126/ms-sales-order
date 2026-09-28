import type { ConfigService } from "@nestjs/config";
import type { Kafka } from "kafkajs";
import type { ILogger } from "@common/logger/logger.interface";
import { InvalidReplicaEventError } from "@domain/errors/sales-order.errors";
import type { DeadLetterPort, DeadLetterReason } from "@application/ports/dead-letter.port";
import type { SyncOutcome, SyncReplicaPort, TransportTypeReplicaEvent } from "@application/ports/replica.ports";
import type { Env } from "@config/env";
import type { InboundMessage } from "@infrastructure/messaging/kafka-consumer.base";
import { MetricsService } from "@infrastructure/metrics/metrics.service";
import { sampleValue } from "../../../test/metrics.helpers";
import { TransportTypeReplicaConsumer } from "./replica.consumers";

const silentLogger: ILogger = { log: () => undefined, warn: () => undefined, error: () => undefined, debug: () => undefined };
const envValues: Partial<Env> = {
  TRANSPORT_TYPE_SYNC_GROUP_ID: "ms-sales-order.transport-type-sync",
  CONSUMER_RETRY_RETRIES: 1,
  CONSUMER_RETRY_INITIAL_MS: 1,
  CONSUMER_RETRY_MAX_MS: 1,
  CONSUMER_PAUSE_MS: 10,
};
const config: Pick<ConfigService<Env, true>, "get"> = {
  get: ((key: keyof Env) => envValues[key]),
};

class Testable extends TransportTypeReplicaConsumer {
  run(message: InboundMessage): Promise<void> {
    return this.handle(message);
  }
}

function message(value: unknown): InboundMessage {
  return {
    topic: "transport.TransportTypeCreated",
    partition: 0,
    offset: "5",
    timestamp: "0",
    key: null,
    value: Buffer.from(typeof value === "string" ? value : JSON.stringify(value)),
    headers: {},
  };
}

const ID = "11111111-1111-4111-8111-111111111111";
const valid = {
  eventId: "9af6023e-9e78-4b77-afcd-cb61c1e5c068",
  eventType: "TransportTypeCreated",
  schemaVersion: 2,
  occurredAt: "2026-09-22T23:50:58.000Z",
  aggregateId: ID,
  correlationId: "c",
  payload: { id: ID, name: "Caminhao", description: null, active: true },
};

describe("ReplicaEventConsumer", () => {
  let deadLetters: Array<{ reason: DeadLetterReason; group: string }>;
  let metrics: MetricsService;
  let outcome: SyncOutcome | Error;

  const build = (): Testable => {
    const deadLetter: DeadLetterPort = {
      publish: async (_m, reason, _detail, group): Promise<void> => {
        deadLetters.push({ reason, group });
      },
      ensureTopic: async (): Promise<void> => undefined,
    };
    const sync: SyncReplicaPort<TransportTypeReplicaEvent> = {
      execute: async (): Promise<SyncOutcome> => {
        if (outcome instanceof Error) throw outcome;
        return outcome;
      },
    };
    return new Testable({} as Kafka, silentLogger, sync, deadLetter, metrics, config as ConfigService<Env, true>);
  };

  beforeEach(() => {
    deadLetters = [];
    metrics = new MetricsService();
    outcome = "applied";
  });

  it("evento valido: aplica e conta o outcome", async () => {
    await build().run(message(valid));
    const text = await metrics.render();
    expect(deadLetters).toEqual([]);
    expect(sampleValue(text, "kafka_messages_consumed_total", { topic: "transport.TransportTypeCreated", outcome: "applied" })).toBe(1);
  });

  it("mensagem invalida: DLT com o group do consumer", async () => {
    await build().run(message("t"));
    expect(deadLetters).toEqual([{ reason: "invalid_json", group: "ms-sales-order.transport-type-sync" }]);
  });

  it("invariante no sync: DLT domain_invariant_violation", async () => {
    outcome = new InvalidReplicaEventError("x");
    await build().run(message(valid));
    expect(deadLetters).toEqual([{ reason: "domain_invariant_violation", group: "ms-sales-order.transport-type-sync" }]);
  });

  it("erro recuperavel: propaga (sem DLT)", async () => {
    outcome = new Error("banco fora");
    await expect(build().run(message(valid))).rejects.toThrow("banco fora");
    expect(deadLetters).toEqual([]);
  });
});
