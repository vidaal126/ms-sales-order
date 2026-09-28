import {
  Inject,
  Injectable,
  type OnModuleDestroy,
  type OnModuleInit,
} from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { type ILogger, LOGGER_TOKEN } from "@common/logger/logger.interface";
import { type Env, readEnv } from "@config/env";
import { KafkaProducerService } from "@infrastructure/messaging/kafka-producer.service";
import { MetricsService } from "@infrastructure/metrics/metrics.service";
import { toOutboundMessage } from "./outbox-message.mapper";
import { OutboxRepository } from "./outbox.repository";

// Este é o componente que fecha o padrão Outbox. Sem ele, gravar o evento
// na tabela outbox_events não serve pra nada - é só um log morto.
//
// O que ele faz, a cada tick:
// 1. Busca até BATCH_SIZE eventos com publishedAt = null (não publicados)
// 2. Envia cada um pro Kafka
// 3. Marca publishedAt = now() SÓ depois de confirmar o envio
//
// Dor proposital: se o processo morrer entre o envio ao Kafka e o UPDATE
// do publishedAt, o evento será reenviado no próximo poll (at-least-once,
// não exactly-once). O idempotent:true do producer protege contra
// duplicação por retry de rede, mas não contra o processo caindo de
// verdade no meio - por isso consumidores desse evento PRECISAM ser
// idempotentes (o eventId do envelope é estável entre reenvios).
//
// Ordem por pedido: os eventos saem em ordem de sequence. Se o envio de um
// evento falha, o aggregateId dele fica bloqueado ate o fim do ciclo e os
// eventos seguintes do mesmo pedido nao saem antes dele; os de outros
// pedidos seguem normalmente. O proximo tick tenta de novo a partir do que
// falhou.
//
// Shutdown: onModuleDestroy para o polling, interrompe o lote entre um
// evento e outro e aguarda o ciclo em andamento marcar o que já foi enviado.
// Producer e Prisma só desconectam depois, em onApplicationShutdown.
@Injectable()
export class OutboxPublisherService implements OnModuleInit, OnModuleDestroy {
  private readonly pollIntervalMs: number;
  private readonly batchSize: number;
  private intervalHandle: NodeJS.Timeout | null = null;
  private currentCycle: Promise<void> | null = null;
  private stopping = false;

  constructor(
    private readonly outbox: OutboxRepository,
    private readonly kafkaProducer: KafkaProducerService,
    @Inject(LOGGER_TOKEN) private readonly logger: ILogger,
    private readonly metrics: MetricsService,
    config: ConfigService<Env, true>,
  ) {
    this.pollIntervalMs = readEnv(config, "OUTBOX_POLL_INTERVAL_MS");
    this.batchSize = readEnv(config, "OUTBOX_BATCH_SIZE");
  }

  onModuleInit(): void {
    this.metrics.registerAsyncGauge(
      "outbox_pending_events",
      "Eventos do outbox ainda nao publicados",
      () => this.outbox.countPending(),
    );
    this.intervalHandle = setInterval((): void => {
      if (this.currentCycle || this.stopping) return;
      // pollAndPublish nunca rejeita (erros sao logados dentro dele).
      this.currentCycle = this.pollAndPublish().finally((): void => {
        this.currentCycle = null;
      });
    }, this.pollIntervalMs);
  }

  async onModuleDestroy(): Promise<void> {
    this.stopping = true;
    if (this.intervalHandle) clearInterval(this.intervalHandle);
    if (this.currentCycle) {
      this.logger.log("Aguardando ciclo do outbox em andamento para encerrar");
      await this.currentCycle;
    }
  }

  private async pollAndPublish(): Promise<void> {
    try {
      const pending = await this.outbox.findPending(this.batchSize);

      const publishedIds: string[] = [];
      const blockedAggregates = new Set<string>();

      for (const event of pending) {
        if (this.stopping) break;
        if (blockedAggregates.has(event.aggregateId)) continue;

        try {
          await this.kafkaProducer.send(toOutboundMessage(event));

          publishedIds.push(event.id);
          this.metrics.recordOutboxPublished(event.eventType);

          this.logger.log(
            `Evento publicado: ${event.eventType} (aggregateId=${event.aggregateId})`,
            { eventId: event.id, correlationId: event.correlationId },
          );
        } catch (err) {
          blockedAggregates.add(event.aggregateId);
          this.logger.error(
            `Falha ao publicar evento ${event.id}; demais eventos do aggregateId=${event.aggregateId} ficam para o proximo ciclo`,
            toError(err),
          );
        }
      }

      if (publishedIds.length > 0) {
        await this.outbox.markPublished(publishedIds, new Date());
      }
    } catch (err) {
      // Banco indisponivel etc.: o proximo tick tenta de novo. Eventos ja
      // enviados e nao marcados serao reenviados (at-least-once).
      this.logger.error("Falha no ciclo do outbox", toError(err));
    }
  }
}

function toError(err: unknown): Error {
  return err instanceof Error ? err : new Error(String(err));
}
