import type { Kafka } from "kafkajs";
import type { ConfigService } from "@nestjs/config";
import { runWithCorrelationId } from "@common/correlation/correlation-context";
import type { ILogger } from "@common/logger/logger.interface";
import { InvariantViolationError } from "@domain/errors/domain.error";
import type { DeadLetterPort } from "@application/ports/dead-letter.port";
import type { SourceEvent, SyncReplicaPort } from "@application/ports/replica.ports";
import { type Env, readEnv } from "@config/env";
import {
  type ConsumerSubscription,
  type InboundMessage,
  KafkaConsumerBase,
} from "@infrastructure/messaging/kafka-consumer.base";
import type { MetricsService } from "@infrastructure/metrics/metrics.service";
import type { DecodeResult } from "./envelope.decoder";

export function subscriptionFor(
  config: ConfigService<Env, true>,
  groupId: string,
  topics: readonly string[],
): ConsumerSubscription {
  return {
    groupId,
    topics,
    fromBeginning: true,
    retry: {
      retries: readEnv(config, "CONSUMER_RETRY_RETRIES"),
      initialDelayMs: readEnv(config, "CONSUMER_RETRY_INITIAL_MS"),
      maxDelayMs: readEnv(config, "CONSUMER_RETRY_MAX_MS"),
      pauseMs: readEnv(config, "CONSUMER_PAUSE_MS"),
    },
  };
}

// Adapter de entrada comum das replicas. Classificacao:
// - nao recuperavel (JSON invalido, schema, versao, invariante, dado
//   rejeitado pelo banco): DLT do servico com os bytes originais; resolve =>
//   offset commitado depois do ack da DLT;
// - recuperavel (banco, timeout, qualquer erro nao classificado): lanca =>
//   sem commit; a base faz retry com backoff e pausa a particao.
// Group fixo com fromBeginning: sem offset commitado, le o historico inteiro.
export abstract class ReplicaEventConsumer<TEvent extends SourceEvent> extends KafkaConsumerBase {
  protected constructor(
    kafka: Kafka,
    logger: ILogger,
    protected readonly subscription: ConsumerSubscription,
    private readonly sync: SyncReplicaPort<TEvent>,
    private readonly deadLetter: DeadLetterPort,
    private readonly metrics: MetricsService,
  ) {
    super(kafka, logger);
  }

  protected abstract decode(message: InboundMessage): DecodeResult<TEvent>;

  protected override async beforeStart(): Promise<void> {
    for (const topic of this.subscription.topics) await this.deadLetter.ensureTopic(topic);
  }

  protected async handle(message: InboundMessage): Promise<void> {
    const decoded = this.decode(message);
    if (!decoded.ok) {
      await this.toDeadLetter(message, decoded.reason, decoded.detail);
      return;
    }
    const { event, correlationId } = decoded;
    if (correlationId !== undefined) {
      await runWithCorrelationId(correlationId, () => this.apply(message, event));
      return;
    }
    await this.apply(message, event);
  }

  protected override onRetryExhausted(message: InboundMessage): void {
    this.metrics.recordConsumed(message.topic, "retry_exhausted");
  }

  private async apply(message: InboundMessage, event: TEvent): Promise<void> {
    try {
      const outcome = await this.sync.execute(event);
      this.metrics.recordConsumed(message.topic, outcome);
      const context = { topic: message.topic, partition: message.partition, offset: message.offset, eventId: event.eventId, outcome };
      if (outcome === "applied") this.logger.log("Evento aplicado na replica", context);
      else this.logger.debug("Evento ignorado pela replica (duplicado ou mais antigo)", context);
    } catch (err) {
      if (err instanceof InvariantViolationError) {
        await this.toDeadLetter(message, "domain_invariant_violation", err.message);
        return;
      }
      throw err;
    }
  }

  private async toDeadLetter(
    message: InboundMessage,
    reason: Parameters<DeadLetterPort["publish"]>[1],
    detail: string,
  ): Promise<void> {
    await this.deadLetter.publish(message, reason, detail, this.subscription.groupId);
    this.metrics.recordConsumed(message.topic, "dead_letter");
  }
}
