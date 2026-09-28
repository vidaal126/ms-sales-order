import { Inject, Injectable } from "@nestjs/common";
import type { Kafka } from "kafkajs";
import type {
  DeadLetterPort,
  DeadLetterReason,
  DeadLetterSource,
} from "@application/ports/dead-letter.port";
import { type ILogger, LOGGER_TOKEN } from "@common/logger/logger.interface";
import { KafkaProducerService } from "./kafka-producer.service";
import { KAFKA_CLIENT } from "./kafka.tokens";

// DLT por servico consumidor: catalog.ItemCreated e transport.* tambem sao
// consumidos por outros servicos, e cada um precisa da sua propria DLT.
export const DLT_SUFFIX = ".ms-sales-order.DLT";

export const DLT_HEADERS = {
  reason: "dlt-reason",
  detail: "dlt-detail",
  sourceTopic: "dlt-source-topic",
  sourcePartition: "dlt-source-partition",
  sourceOffset: "dlt-source-offset",
  sourceTimestamp: "dlt-source-timestamp",
  failedAt: "dlt-failed-at",
  consumerGroup: "dlt-consumer-group",
} as const;

const MAX_DETAIL_LENGTH = 500;
// A DLT guarda o que precisa de investigacao manual: nao pode expirar como a
// retencao padrao de 7 dias.
const RETENTION_FOREVER = "-1";

export function deadLetterTopicFor(topic: string): string {
  return `${topic}${DLT_SUFFIX}`;
}

export interface DeadLetterRecord {
  readonly topic: string;
  readonly key: string | null;
  readonly value: Buffer | null;
  readonly headers: Record<string, string>;
}

// Payload e headers originais intactos; headers dlt-* descrevem o motivo e a
// posicao de origem para reprocessamento/inspecao.
export function toDeadLetterRecord(
  message: DeadLetterSource,
  reason: DeadLetterReason,
  detail: string,
  failedAt: Date,
  consumerGroup: string,
): DeadLetterRecord {
  return {
    topic: deadLetterTopicFor(message.topic),
    key: message.key,
    value: message.value,
    headers: {
      ...message.headers,
      [DLT_HEADERS.reason]: reason,
      [DLT_HEADERS.detail]: detail.slice(0, MAX_DETAIL_LENGTH),
      [DLT_HEADERS.sourceTopic]: message.topic,
      [DLT_HEADERS.sourcePartition]: String(message.partition),
      [DLT_HEADERS.sourceOffset]: message.offset,
      [DLT_HEADERS.sourceTimestamp]: message.timestamp,
      [DLT_HEADERS.failedAt]: failedAt.toISOString(),
      [DLT_HEADERS.consumerGroup]: consumerGroup,
    },
  };
}

@Injectable()
export class DeadLetterPublisher implements DeadLetterPort {
  constructor(
    private readonly producer: KafkaProducerService,
    @Inject(KAFKA_CLIENT) private readonly kafka: Kafka,
    @Inject(LOGGER_TOKEN) private readonly logger: ILogger,
  ) {}

  // Falha aqui propaga: sem DLT gravada o offset nao pode ser commitado.
  async publish(
    message: DeadLetterSource,
    reason: DeadLetterReason,
    detail: string,
    consumerGroup: string,
  ): Promise<void> {
    const record = toDeadLetterRecord(message, reason, detail, new Date(), consumerGroup);
    await this.producer.send(record);
    this.logger.warn("Mensagem enviada para a DLT", {
      reason,
      detail: record.headers[DLT_HEADERS.detail],
      sourceTopic: message.topic,
      sourcePartition: message.partition,
      sourceOffset: message.offset,
      consumerGroup,
      deadLetterTopic: record.topic,
    });
  }

  // Cria a DLT com retencao infinita se ainda nao existir. Idempotente.
  async ensureTopic(sourceTopic: string): Promise<void> {
    const topic = deadLetterTopicFor(sourceTopic);
    const admin = this.kafka.admin();
    await admin.connect();
    try {
      const existing = await admin.listTopics();
      if (existing.includes(topic)) return;
      await admin.createTopics({
        waitForLeaders: true,
        topics: [
          {
            topic,
            numPartitions: 1,
            configEntries: [{ name: "retention.ms", value: RETENTION_FOREVER }],
          },
        ],
      });
      this.logger.log("DLT criada", { topic });
    } finally {
      await admin.disconnect();
    }
  }
}
