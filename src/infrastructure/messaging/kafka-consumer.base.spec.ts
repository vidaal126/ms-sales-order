import type { EachMessagePayload, Kafka, KafkaMessage } from "kafkajs";
import type { ILogger } from "@common/logger/logger.interface";
import {
  type ConsumerRetryPolicy,
  type ConsumerSubscription,
  type InboundMessage,
  KafkaConsumerBase,
  nextOffset,
  processWithRetry,
  retryDelayMs,
  toInboundMessage,
} from "./kafka-consumer.base";

describe("toInboundMessage", () => {
  it("decodifica key e headers e preserva o valor bruto", () => {
    const value = Buffer.from("{isto nao e json");
    const message: KafkaMessage = {
      key: Buffer.from("agg-1"),
      value,
      timestamp: "1700000000000",
      attributes: 0,
      offset: "42",
      headers: {
        correlationId: Buffer.from("corr-1"),
        schemaVersion: "2",
        multi: [Buffer.from("first"), Buffer.from("second")],
        missing: undefined,
      },
    };

    const inbound = toInboundMessage("catalog.ItemCreated", 0, message);

    expect(inbound).toEqual({
      topic: "catalog.ItemCreated",
      partition: 0,
      offset: "42",
      timestamp: "1700000000000",
      key: "agg-1",
      value,
      headers: { correlationId: "corr-1", schemaVersion: "2", multi: "first" },
    });
  });
});

describe("retryDelayMs", () => {
  const policy = { initialDelayMs: 100, maxDelayMs: 1_000 };

  it("cresce exponencialmente com metade fixa e metade aleatoria", () => {
    expect(retryDelayMs(1, policy, () => 0)).toBe(50);
    expect(retryDelayMs(1, policy, () => 1)).toBe(100);
    expect(retryDelayMs(3, policy, () => 0)).toBe(200);
    expect(retryDelayMs(3, policy, () => 1)).toBe(400);
  });

  it("respeita o teto", () => {
    expect(retryDelayMs(20, policy, () => 1)).toBe(1_000);
  });
});

describe("nextOffset", () => {
  it("soma 1 sem perder precisao em offsets grandes", () => {
    expect(nextOffset("41")).toBe("42");
    expect(nextOffset("9007199254740993")).toBe("9007199254740994");
  });
});

describe("processWithRetry", () => {
  const policy = { retries: 2, initialDelayMs: 1, maxDelayMs: 1 };
  const noWait = {
    policy,
    heartbeat: async (): Promise<void> => undefined,
    sleep: async (): Promise<boolean> => true,
    onRetry: (): void => undefined,
  };

  it("sucesso depois de falhas transitorias", async () => {
    let calls = 0;
    const result = await processWithRetry(async () => {
      calls += 1;
      if (calls < 3) throw new Error("banco fora");
    }, noWait);

    expect(result).toEqual({ outcome: "handled" });
    expect(calls).toBe(3);
  });

  it("esgota 1 + retries tentativas e devolve o ultimo erro sem lancar", async () => {
    let calls = 0;
    const result = await processWithRetry(async () => {
      calls += 1;
      throw new Error(`falha ${calls}`);
    }, noWait);

    expect(result).toMatchObject({ outcome: "exhausted", error: { message: "falha 3" } });
    expect(calls).toBe(3);
  });

  it("para quando o sleep e interrompido (shutdown)", async () => {
    let calls = 0;
    const result = await processWithRetry(
      async () => {
        calls += 1;
        throw new Error("banco fora");
      },
      { ...noWait, sleep: async (): Promise<boolean> => false },
    );

    expect(result).toEqual({ outcome: "interrupted" });
    expect(calls).toBe(1);
  });

  it("falha de heartbeat (rebalance) propaga para o KafkaJS nao avancar a mensagem", async () => {
    const rebalance = new Error("REBALANCE_IN_PROGRESS");
    const result = processWithRetry(
      async () => {
        throw new Error("banco fora");
      },
      {
        ...noWait,
        heartbeat: async (): Promise<void> => {
          throw rebalance;
        },
      },
    );

    await expect(result).rejects.toBe(rebalance);
  });
});

describe("KafkaConsumerBase: commit, retry e pausa", () => {
  const silentLogger: ILogger = {
    log: () => undefined,
    warn: () => undefined,
    error: () => undefined,
    debug: () => undefined,
  };

  class FakeConsumer {
    readonly events = { GROUP_JOIN: "group_join", CRASH: "crash" };
    readonly commits: Array<{ topic: string; partition: number; offset: string }> = [];
    readonly seeks: Array<{ topic: string; partition: number; offset: string }> = [];
    runConfig: { autoCommit?: boolean; eachMessage: (p: EachMessagePayload) => Promise<void> } | undefined;
    private onRun: (() => void) | undefined;
    readonly running = new Promise<void>((resolve) => {
      this.onRun = resolve;
    });

    on(): void {}
    async connect(): Promise<void> {}
    async subscribe(): Promise<void> {}
    async disconnect(): Promise<void> {}
    async run(config: NonNullable<FakeConsumer["runConfig"]>): Promise<void> {
      this.runConfig = config;
      this.onRun?.();
    }
    commitError: Error | undefined;
    async commitOffsets(offsets: FakeConsumer["commits"]): Promise<void> {
      if (this.commitError) throw this.commitError;
      this.commits.push(...offsets);
    }
    seek(position: { topic: string; partition: number; offset: string }): void {
      this.seeks.push(position);
    }
  }

  class TestConsumer extends KafkaConsumerBase {
    protected readonly subscription: ConsumerSubscription;

    constructor(
      kafka: Kafka,
      retry: ConsumerRetryPolicy,
      private readonly handler: (message: InboundMessage) => Promise<void>,
    ) {
      super(kafka, silentLogger);
      this.subscription = { groupId: "g", topics: ["t"], fromBeginning: true, retry };
    }

    protected handle(message: InboundMessage): Promise<void> {
      return this.handler(message);
    }
  }

  const kafkaMessage: KafkaMessage = {
    key: null,
    value: Buffer.from("{}"),
    timestamp: "0",
    attributes: 0,
    offset: "7",
    headers: {},
  };

  async function startWith(
    retry: ConsumerRetryPolicy,
    handler: (message: InboundMessage) => Promise<void>,
  ): Promise<{ consumer: TestConsumer; fake: FakeConsumer; deliver: () => Promise<() => number> }> {
    const fake = new FakeConsumer();
    const kafka = { consumer: () => fake } as unknown as Kafka;
    const consumer = new TestConsumer(kafka, retry, handler);
    consumer.onApplicationBootstrap();
    await fake.running;

    // Entrega a mensagem; devolve um leitor de quantas vezes a particao foi
    // retomada (o resume acontece depois, pelo timer).
    const deliver = async (): Promise<() => number> => {
      let resumed = 0;
      const eachMessage = fake.runConfig?.eachMessage;
      if (!eachMessage) throw new Error("run nao chamado");
      await eachMessage({
        topic: "t",
        partition: 0,
        message: kafkaMessage,
        heartbeat: async () => undefined,
        pause: () => () => {
          resumed += 1;
        },
      });
      return () => resumed;
    };
    return { consumer, fake, deliver };
  }

  const fastRetry: ConsumerRetryPolicy = {
    retries: 2,
    initialDelayMs: 1,
    maxDelayMs: 1,
    pauseMs: 10,
  };

  it("roda com autoCommit desligado e commita offset + 1 depois do handler", async () => {
    const { consumer, fake, deliver } = await startWith(fastRetry, async () => undefined);

    await deliver();

    expect(fake.runConfig?.autoCommit).toBe(false);
    expect(fake.commits).toEqual([{ topic: "t", partition: 0, offset: "8" }]);
    expect(fake.seeks).toEqual([]);
    await consumer.onModuleDestroy();
  });

  it("falha no commit nao propaga para o KafkaJS nem repete o handler", async () => {
    let calls = 0;
    const { consumer, fake, deliver } = await startWith(fastRetry, async () => {
      calls += 1;
    });
    fake.commitError = new Error("REBALANCE_IN_PROGRESS");

    await expect(deliver()).resolves.toBeDefined();

    expect(calls).toBe(1);
    expect(fake.commits).toEqual([]);
    expect(fake.seeks).toEqual([]);
    await consumer.onModuleDestroy();
  });

  it("falha transitoria seguida de sucesso: um unico commit", async () => {
    let calls = 0;
    const { consumer, fake, deliver } = await startWith(fastRetry, async () => {
      calls += 1;
      if (calls === 1) throw new Error("banco fora");
    });

    await deliver();

    expect(calls).toBe(2);
    expect(fake.commits).toEqual([{ topic: "t", partition: 0, offset: "8" }]);
    await consumer.onModuleDestroy();
  });

  it("retry esgotado: sem commit, seek na mesma mensagem, pausa e retoma", async () => {
    const { consumer, fake, deliver } = await startWith(fastRetry, async () => {
      throw new Error("banco fora");
    });

    const resumed = await deliver();

    expect(fake.commits).toEqual([]);
    expect(fake.seeks).toEqual([{ topic: "t", partition: 0, offset: "7" }]);
    expect(resumed()).toBe(0);
    expect(consumer.getHealth()).toMatchObject({ status: "degraded", lastError: "banco fora" });

    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(resumed()).toBe(1);
    await consumer.onModuleDestroy();
  });

  it("shutdown durante o backoff: retorna sem commit e sem seek", async () => {
    const slowRetry: ConsumerRetryPolicy = { ...fastRetry, initialDelayMs: 60_000, maxDelayMs: 60_000 };
    const { consumer, fake, deliver } = await startWith(slowRetry, async () => {
      throw new Error("banco fora");
    });

    const delivering = deliver();
    await new Promise((resolve) => setImmediate(resolve));
    await consumer.onModuleDestroy();
    await delivering;

    expect(fake.commits).toEqual([]);
    expect(fake.seeks).toEqual([]);
  });
});
