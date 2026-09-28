import { Injectable } from "@nestjs/common";
import { collectDefaultMetrics, Counter, Gauge, Histogram, Registry } from "prom-client";
import { SERVICE_NAME } from "@config/service";

// Resultado do processamento de uma mensagem consumida.
export type ConsumeOutcome = "applied" | "duplicate" | "stale" | "dead_letter" | "retry_exhausted";

// Registry proprio (nao o global do prom-client): testes sobem varias
// instancias do app no mesmo processo sem colisao de metricas.
@Injectable()
export class MetricsService {
  readonly registry = new Registry();
  private readonly httpDuration: Histogram<"method" | "route" | "status_code">;
  private readonly messagesConsumed: Counter<"topic" | "outcome">;
  private readonly outboxPublished: Counter<"event_type">;

  constructor() {
    this.registry.setDefaultLabels({ service: SERVICE_NAME });
    collectDefaultMetrics({ register: this.registry });

    this.httpDuration = new Histogram({
      name: "http_request_duration_seconds",
      help: "Duracao das requisicoes HTTP por rota (template) e status",
      labelNames: ["method", "route", "status_code"],
      buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
      registers: [this.registry],
    });
    this.messagesConsumed = new Counter({
      name: "kafka_messages_consumed_total",
      help: "Mensagens Kafka consumidas por topico e resultado",
      labelNames: ["topic", "outcome"],
      registers: [this.registry],
    });
    this.outboxPublished = new Counter({
      name: "outbox_events_published_total",
      help: "Eventos do outbox publicados no Kafka",
      labelNames: ["event_type"],
      registers: [this.registry],
    });
  }

  observeHttp(method: string, route: string, statusCode: number, seconds: number): void {
    this.httpDuration.observe({ method, route, status_code: String(statusCode) }, seconds);
  }

  recordConsumed(topic: string, outcome: ConsumeOutcome): void {
    this.messagesConsumed.inc({ topic, outcome });
  }

  recordOutboxPublished(eventType: string): void {
    this.outboxPublished.inc({ event_type: eventType });
  }

  // Gauge calculado no scrape (ex.: contagem de pendentes no banco). Falha na
  // coleta nao derruba o /metrics: o gauge fica sem valor nesse scrape.
  registerAsyncGauge(name: string, help: string, read: () => Promise<number>): void {
    new Gauge({
      name,
      help,
      registers: [this.registry],
      async collect(): Promise<void> {
        try {
          this.set(await read());
        } catch {
          this.reset();
        }
      },
    });
  }

  render(): Promise<string> {
    return this.registry.metrics();
  }
}
