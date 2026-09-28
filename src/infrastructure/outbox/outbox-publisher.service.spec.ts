import { ConfigService } from "@nestjs/config";
import type { ILogger } from "@common/logger/logger.interface";
import type { Env } from "@config/env";
import type { OutboxEvent } from "@infrastructure/database/generated/client";
import type { OutboundMessage } from "@infrastructure/messaging/event-envelope";
import type { KafkaProducerService } from "@infrastructure/messaging/kafka-producer.service";
import { MetricsService } from "@infrastructure/metrics/metrics.service";
import { OutboxPublisherService } from "./outbox-publisher.service";
import type { OutboxRepository } from "./outbox.repository";

const POLL_INTERVAL_MS = 1_000;

function outboxEvent(sequence: number, aggregateId: string): OutboxEvent {
  return {
    id: `event-${sequence}`,
    sequence: BigInt(sequence),
    aggregateId,
    eventType: "OrderStatusChanged",
    schemaVersion: 2,
    correlationId: "corr-1",
    payload: { id: aggregateId },
    createdAt: new Date("2026-09-28T12:00:00.000Z"),
    publishedAt: null,
  };
}

function eventIdOf(message: OutboundMessage): string {
  if (typeof message.value !== "string") throw new Error("envelope sem valor");
  const parsed: unknown = JSON.parse(message.value);
  if (typeof parsed === "object" && parsed !== null && "eventId" in parsed && typeof parsed.eventId === "string") {
    return parsed.eventId;
  }
  throw new Error("envelope sem eventId");
}

describe("OutboxPublisherService", () => {
  let pending: OutboxEvent[];
  let failingIds: Set<string>;
  let sentIds: string[];
  let markedIds: string[];
  let logger: ILogger & { error: jest.Mock };
  let createPublisher: () => OutboxPublisherService;

  beforeEach(() => {
    jest.useFakeTimers();
    pending = [];
    failingIds = new Set();
    sentIds = [];
    markedIds = [];
    logger = { log: jest.fn(), warn: jest.fn(), debug: jest.fn(), error: jest.fn() };

    const outbox: Pick<OutboxRepository, "findPending" | "countPending" | "markPublished"> = {
      findPending: async (limit) => pending.slice(0, limit),
      countPending: async () => pending.length,
      markPublished: async (ids) => {
        markedIds.push(...ids);
      },
    };
    const producer: Pick<KafkaProducerService, "send"> = {
      send: async (message) => {
        const eventId = eventIdOf(message);
        if (failingIds.has(eventId)) throw new Error("broker indisponivel");
        sentIds.push(eventId);
      },
    };
    const config = new ConfigService<Env, true>({ OUTBOX_POLL_INTERVAL_MS: POLL_INTERVAL_MS, OUTBOX_BATCH_SIZE: 20 });

    createPublisher = (): OutboxPublisherService => new OutboxPublisherService(
      outbox as OutboxRepository,
      producer as KafkaProducerService,
      logger,
      new MetricsService(),
      config,
    );
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  // Um tick do polling; onModuleDestroy aguarda o ciclo em andamento. Cada
  // ciclo usa uma instancia nova, pois onModuleDestroy encerra o polling.
  const runOneCycle = async (): Promise<void> => {
    const publisher = createPublisher();
    publisher.onModuleInit();
    await jest.advanceTimersByTimeAsync(POLL_INTERVAL_MS);
    await publisher.onModuleDestroy();
  };

  it("publica em ordem de sequence e marca todos", async () => {
    pending = [outboxEvent(1, "order-a"), outboxEvent(2, "order-a"), outboxEvent(3, "order-b")];

    await runOneCycle();

    expect(sentIds).toEqual(["event-1", "event-2", "event-3"]);
    expect(markedIds).toEqual(["event-1", "event-2", "event-3"]);
  });

  it("falha num evento bloqueia os seguintes do mesmo aggregate, mas nao os de outros", async () => {
    pending = [outboxEvent(1, "order-a"), outboxEvent(2, "order-b"), outboxEvent(3, "order-a"), outboxEvent(4, "order-b")];
    failingIds.add("event-1");

    await runOneCycle();

    expect(sentIds).toEqual(["event-2", "event-4"]);
    expect(markedIds).toEqual(["event-2", "event-4"]);
    expect(logger.error).toHaveBeenCalledTimes(1);
  });

  it("o proximo ciclo retoma o aggregate bloqueado a partir do evento que falhou", async () => {
    pending = [outboxEvent(1, "order-a"), outboxEvent(2, "order-a")];
    failingIds.add("event-1");
    await runOneCycle();
    expect(sentIds).toEqual([]);

    failingIds.clear();
    await runOneCycle();

    expect(sentIds).toEqual(["event-1", "event-2"]);
  });
});
