import { randomUUID } from "node:crypto";
import { Kafka, logLevel, Partitioners } from "kafkajs";

export interface ProducedMessage {
  readonly key: string;
  readonly value: string;
  readonly headers?: Record<string, string>;
}

export interface ConsumedMessage {
  readonly key: string | null;
  readonly value: string;
  readonly headers: Record<string, string>;
}

// Cliente Kafka dos testes: publica mensagens e le topicos do inicio.
export class KafkaTestClient {
  private readonly kafka: Kafka;

  constructor(broker: string) {
    this.kafka = new Kafka({ clientId: "integration-test", brokers: [broker], logLevel: logLevel.NOTHING });
  }

  // Cria o topico antes do uso, como o kafka-init do docker compose.
  async createTopic(topic: string): Promise<void> {
    const admin = this.kafka.admin();
    await admin.connect();
    try {
      await admin.createTopics({ waitForLeaders: true, topics: [{ topic, numPartitions: 1 }] });
    } finally {
      await admin.disconnect();
    }
  }

  // Varios topicos de uma vez, sem waitForLeaders (que so repete em
  // LEADER_NOT_AVAILABLE): espera pelo metadata ate todos terem lider.
  async createTopics(topics: readonly string[], timeoutMs = 60_000): Promise<void> {
    const admin = this.kafka.admin();
    await admin.connect();
    try {
      await admin.createTopics({ waitForLeaders: false, topics: topics.map((topic) => ({ topic, numPartitions: 1 })) });
      const deadline = Date.now() + timeoutMs;
      for (;;) {
        try {
          const { topics: metadata } = await admin.fetchTopicMetadata({ topics: [...topics] });
          if (metadata.every((t) => t.partitions.every((p) => p.leader >= 0))) return;
        } catch {
          // metadata ainda nao propagado
        }
        if (Date.now() > deadline) throw new Error("topicos sem lider dentro do prazo");
        await new Promise((resolve) => setTimeout(resolve, 300));
      }
    } finally {
      await admin.disconnect();
    }
  }

  async produce(topic: string, messages: readonly ProducedMessage[]): Promise<void> {
    const producer = this.kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
    await producer.connect();
    try {
      // Uma mensagem por send para fixar a ordem dos offsets.
      for (const message of messages) {
        await sendWithRetry(() => producer.send({ topic, messages: [message] }).then(() => undefined));
      }
    } finally {
      await producer.disconnect();
    }
  }

  // Le do inicio ate juntar `count` mensagens ou estourar o tempo.
  async readFromBeginning(topic: string, count: number, timeoutMs: number): Promise<ConsumedMessage[]> {
    const consumer = this.kafka.consumer({ groupId: `integration-reader-${randomUUID()}` });
    const received: ConsumedMessage[] = [];
    await consumer.connect();
    try {
      await consumer.subscribe({ topics: [topic], fromBeginning: true });
      await new Promise<void>((resolve) => {
        const timer = setTimeout(resolve, timeoutMs);
        void consumer.run({
          eachMessage: async ({ message }): Promise<void> => {
            const headers: Record<string, string> = {};
            for (const [name, raw] of Object.entries(message.headers ?? {})) {
              const first = Array.isArray(raw) ? raw[0] : raw;
              if (first !== undefined) headers[name] = first.toString();
            }
            received.push({
              key: message.key?.toString() ?? null,
              value: message.value?.toString() ?? "",
              headers,
            });
            if (received.length >= count) {
              clearTimeout(timer);
              resolve();
            }
          },
        });
      });
    } finally {
      await consumer.disconnect();
    }
    return received;
  }
}

export async function waitFor(
  description: string,
  condition: () => Promise<boolean>,
  timeoutMs = 60_000,
  intervalMs = 500,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await condition()) return;
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  throw new Error(`Tempo esgotado esperando: ${description}`);
}

// Logo apos criar o topico (auto-create desligado) o metadata do broker pode
// ainda nao listar a particao: UNKNOWN_TOPIC_OR_PARTITION e transitorio aqui.
async function sendWithRetry(send: () => Promise<void>, attempts = 20): Promise<void> {
  for (let attempt = 1; ; attempt += 1) {
    try {
      await send();
      return;
    } catch (err) {
      const transient = err instanceof Error && /does not host this topic-partition|leader/i.test(err.message);
      if (!transient || attempt >= attempts) throw err;
      await new Promise((resolve) => setTimeout(resolve, 500));
    }
  }
}
