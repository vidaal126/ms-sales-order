import { setTimeout as sleep } from "node:timers/promises";
import type { OnApplicationBootstrap, OnModuleDestroy } from "@nestjs/common";
import type { Consumer, IHeaders, Kafka, KafkaMessage } from "kafkajs";
import { runWithCorrelationId } from "@common/correlation/correlation-context";
import { resolveCorrelationId } from "@common/correlation/correlation-id";
import type { ILogger } from "@common/logger/logger.interface";
import { EVENT_HEADERS } from "./event-envelope";

export interface ConsumerRetryPolicy {
  // Novas tentativas por mensagem com falha recuperavel, em processo, antes de
  // pausar a particao.
  readonly retries: number;
  readonly initialDelayMs: number;
  readonly maxDelayMs: number;
  // Quanto tempo a particao fica pausada antes de retomar da mesma mensagem.
  readonly pauseMs: number;
}

export interface ConsumerSubscription {
  readonly groupId: string;
  readonly topics: readonly string[];
  readonly fromBeginning: boolean;
  readonly retry: ConsumerRetryPolicy;
}

export interface InboundMessage {
  readonly topic: string;
  readonly partition: number;
  readonly offset: string;
  readonly timestamp: string;
  readonly key: string | null;
  // Bruto e intacto: parse e validacao sao do handler (e a DLT precisa do
  // payload original).
  readonly value: Buffer | null;
  readonly headers: Readonly<Record<string, string>>;
}

// starting: ainda nao entrou no grupo.
// running: consumindo normalmente.
// degraded: particao pausada por falha recuperavel persistente, ou consumer
//   crashou (broker fora) e esta tentando voltar.
// stopped: desligado no shutdown.
export type ConsumerStatus = "starting" | "running" | "degraded" | "stopped";

export interface ConsumerHealth {
  readonly status: ConsumerStatus;
  readonly since: Date;
  readonly lastError?: string;
}

const MAX_START_BACKOFF_MS = 30_000;
const START_BACKOFF_BASE_MS = 1_000;

// Base para consumers: conexao, subscribe, contexto de log com correlationId,
// commit manual, retry/pausa, estado para health check, recuperacao
// automatica e desligamento.
//
// Contrato do handler concreto:
// - resolver = mensagem tratada (persistida, enviada para a DLT com ack, ou
//   duplicata) => commit de offset + 1;
// - lancar = falha recuperavel => nada e commitado; retry em processo com
//   backoff exponencial e jitter; esgotado, seek de volta para a mensagem e
//   pausa da particao por pauseMs. Nunca pula mensagem e nunca deixa o erro
//   do handler chegar ao KafkaJS (que crasharia e reiniciaria o consumer).
// Excecao deliberada: falha de heartbeat (rebalance em andamento) sobe para o
// KafkaJS, que refaz o join sem avancar o offset local da mensagem. Engolir
// esse erro faria o KafkaJS seguir para a proxima mensagem da particao, e um
// commit posterior passaria por cima da mensagem nao processada.
export abstract class KafkaConsumerBase
  implements OnApplicationBootstrap, OnModuleDestroy
{
  private consumer: Consumer | null = null;
  private health: ConsumerHealth = { status: "starting", since: new Date() };
  private isStopping = false;
  private startTimer: NodeJS.Timeout | null = null;
  private startAttempt: Promise<void> | null = null;
  private readonly shutdown = new AbortController();
  private readonly resumeTimers = new Set<NodeJS.Timeout>();

  protected constructor(
    private readonly kafka: Kafka,
    protected readonly logger: ILogger,
  ) {}

  protected abstract readonly subscription: ConsumerSubscription;

  protected abstract handle(message: InboundMessage): Promise<void>;

  // Preparacao antes de conectar (ex.: garantir topicos). Falha aqui segue o
  // mesmo caminho de falha de conexao: consumer degradado e nova tentativa.
  protected async beforeStart(): Promise<void> {}

  // Nao bloqueia o bootstrap: broker fora na subida deixa o consumer
  // degradado (visivel no health) e tentando de novo, sem derrubar a app.
  onApplicationBootstrap(): void {
    this.scheduleStart(0, 0);
  }

  getHealth(): ConsumerHealth {
    return this.health;
  }

  // Interrompe o backoff em andamento (a mensagem fica sem commit e volta no
  // proximo start); disconnect para o fetch e aguarda o eachMessage terminar.
  async onModuleDestroy(): Promise<void> {
    this.isStopping = true;
    this.shutdown.abort();
    this.clearResumeTimers();
    if (this.startTimer) clearTimeout(this.startTimer);
    await this.startAttempt;
    await this.consumer?.disconnect();
    this.consumer = null;
    this.setStatus("stopped");
  }

  private scheduleStart(delayMs: number, attempt: number): void {
    this.startTimer = setTimeout((): void => {
      this.startTimer = null;
      // start nunca rejeita (falhas viram novo agendamento).
      this.startAttempt = this.start(attempt).finally((): void => {
        this.startAttempt = null;
      });
    }, delayMs);
  }

  private async start(attempt: number): Promise<void> {
    if (this.isStopping) return;

    const { groupId, topics, fromBeginning, retry } = this.subscription;
    const consumer = this.kafka.consumer({
      groupId,
      retry: {
        // So erros do proprio KafkaJS (broker, rebalance) chegam aqui: falha
        // de processamento de mensagem e tratada em handleMessage.
        restartOnFailure: (): Promise<boolean> => Promise.resolve(true),
      },
    });
    this.consumer = consumer;

    consumer.on(consumer.events.GROUP_JOIN, (): void => {
      if (this.health.status === "starting") this.setStatus("running");
    });
    consumer.on(consumer.events.CRASH, ({ payload }): void => {
      this.logger.error("Consumer Kafka crashou", payload.error, {
        groupId,
        restart: payload.restart,
      });
      this.setStatus("degraded", payload.error.message);
      // Sem restart automatico (erro nao retentavel do KafkaJS): recria o
      // consumer por conta propria.
      if (!payload.restart && !this.isStopping) {
        void this.recreate(attempt + 1);
      }
    });

    try {
      await this.beforeStart();
      await consumer.connect();
      await consumer.subscribe({ topics: [...topics], fromBeginning });

      // autoCommit desligado: o unico commit e o explicito em handleMessage,
      // feito depois da persistencia (ou do ack da DLT, ou da duplicata).
      await consumer.run({
        autoCommit: false,
        eachMessage: async (payload): Promise<void> => {
          const inbound = toInboundMessage(payload.topic, payload.partition, payload.message);
          await runWithCorrelationId(
            resolveCorrelationId(inbound.headers[EVENT_HEADERS.correlationId]),
            () =>
              this.handleMessage(
                consumer,
                inbound,
                retry,
                () => payload.heartbeat(),
                () => payload.pause(),
              ),
          );
        },
      });
    } catch (err) {
      const error = err instanceof Error ? err : new Error(String(err));
      this.logger.error("Falha ao iniciar consumer Kafka", error, {
        groupId,
        attempt,
      });
      this.setStatus("degraded", error.message);
      await this.recreate(attempt + 1);
    }
  }

  private async handleMessage(
    consumer: Consumer,
    message: InboundMessage,
    retry: ConsumerRetryPolicy,
    heartbeat: () => Promise<void>,
    pause: () => () => void,
  ): Promise<void> {
    const position = {
      topic: message.topic,
      partition: message.partition,
      offset: message.offset,
    };

    const result = await processWithRetry(() => this.handle(message), {
      policy: retry,
      heartbeat,
      sleep: (ms) => this.interruptibleSleep(ms),
      onRetry: (attempt, delayMs, error) => {
        this.logger.warn("Falha recuperavel; nova tentativa sem commit", {
          ...position,
          attempt,
          delayMs,
          error: error.message,
        });
      },
    });

    switch (result.outcome) {
      case "handled":
        await this.commitQuietly(consumer, message);
        if (this.health.status !== "running") this.setStatus("running");
        return;
      case "exhausted": {
        this.onRetryExhausted(message);
        // seek antes de retornar: o KafkaJS resolve o offset localmente quando
        // eachMessage retorna, e o seek pendente sobrepoe isso no proximo
        // fetch (com autoCommit desligado, seek nao commita nada).
        consumer.seek({ topic: message.topic, partition: message.partition, offset: message.offset });
        const resume = pause();
        this.scheduleResume(resume, retry.pauseMs, position);
        this.logger.error("Retry esgotado; particao pausada sem commit", result.error, {
          ...position,
          pauseMs: retry.pauseMs,
        });
        this.setStatus("degraded", result.error.message);
        return;
      }
      case "interrupted":
        // Shutdown durante o backoff: sem commit, a mensagem volta no proximo start.
        return;
    }
  }

  // Gancho para metricas: retry esgotado e particao pausada.
  protected onRetryExhausted(_message: InboundMessage): void {
    // padrao: nada alem do log da base.
  }

  // A mensagem ja foi persistida (ou enviada para a DLT): se o commit falhar
  // (ex.: rebalance), ela volta a ser entregue e cai no caminho de duplicata.
  // Propagar faria o KafkaJS repetir o handler e, esgotado, reiniciar o consumer.
  private async commitQuietly(consumer: Consumer, message: InboundMessage): Promise<void> {
    const offset = nextOffset(message.offset);
    try {
      await consumer.commitOffsets([{ topic: message.topic, partition: message.partition, offset }]);
    } catch (err) {
      this.logger.warn("Falha ao commitar offset; a mensagem sera reentregue como duplicata", {
        topic: message.topic,
        partition: message.partition,
        offset,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  private scheduleResume(
    resume: () => void,
    delayMs: number,
    position: { topic: string; partition: number; offset: string },
  ): void {
    const timer = setTimeout((): void => {
      this.resumeTimers.delete(timer);
      if (this.isStopping) return;
      this.logger.log("Retomando particao pausada", position);
      resume();
    }, delayMs);
    this.resumeTimers.add(timer);
  }

  private clearResumeTimers(): void {
    for (const timer of this.resumeTimers) clearTimeout(timer);
    this.resumeTimers.clear();
  }

  // false = interrompido pelo shutdown.
  private async interruptibleSleep(ms: number): Promise<boolean> {
    try {
      await sleep(ms, undefined, { signal: this.shutdown.signal });
      return true;
    } catch {
      return false;
    }
  }

  private async recreate(attempt: number): Promise<void> {
    this.clearResumeTimers();
    const previous = this.consumer;
    this.consumer = null;
    await previous?.disconnect().catch((): void => undefined);
    if (this.isStopping) return;
    this.scheduleStart(startBackoffMs(attempt), attempt);
  }

  private setStatus(status: ConsumerStatus, lastError?: string): void {
    this.health = { status, since: new Date(), lastError };
  }
}

function startBackoffMs(attempt: number): number {
  return Math.min(START_BACKOFF_BASE_MS * 2 ** attempt, MAX_START_BACKOFF_MS);
}

export function nextOffset(offset: string): string {
  return (BigInt(offset) + 1n).toString();
}

// Backoff exponencial com "equal jitter": metade fixa garante espera minima,
// metade aleatoria espalha as retentativas de replicas diferentes.
export function retryDelayMs(
  attempt: number,
  policy: Pick<ConsumerRetryPolicy, "initialDelayMs" | "maxDelayMs">,
  random: () => number = Math.random,
): number {
  const exponential = Math.min(policy.initialDelayMs * 2 ** (attempt - 1), policy.maxDelayMs);
  const half = exponential / 2;
  return Math.round(half + random() * half);
}

export type RetryResult =
  | { readonly outcome: "handled" }
  | { readonly outcome: "exhausted"; readonly error: Error }
  | { readonly outcome: "interrupted" };

export interface RetryOptions {
  readonly policy: Pick<ConsumerRetryPolicy, "retries" | "initialDelayMs" | "maxDelayMs">;
  readonly heartbeat: () => Promise<void>;
  // false = interrompido (shutdown).
  readonly sleep: (ms: number) => Promise<boolean>;
  readonly onRetry: (attempt: number, delayMs: number, error: Error) => void;
  readonly random?: () => number;
}

// 1 tentativa + policy.retries novas tentativas. Erro do handler nunca sai
// daqui; so a falha de heartbeat propaga (ver contrato em KafkaConsumerBase).
export async function processWithRetry(
  handle: () => Promise<void>,
  options: RetryOptions,
): Promise<RetryResult> {
  const { policy } = options;
  for (let attempt = 0; ; attempt += 1) {
    try {
      await handle();
      return { outcome: "handled" };
    } catch (err) {
      const error = err instanceof Error ? err : new Error(String(err));
      if (attempt >= policy.retries) return { outcome: "exhausted", error };

      const delayMs = retryDelayMs(attempt + 1, policy, options.random);
      options.onRetry(attempt + 1, delayMs, error);
      // Heartbeat antes de esperar: o backoff nao pode estourar a sessao do
      // grupo e provocar rebalance. Se ele falhar, o erro propaga de proposito.
      await options.heartbeat();
      if (!(await options.sleep(delayMs))) return { outcome: "interrupted" };
    }
  }
}

export function toInboundMessage(
  topic: string,
  partition: number,
  message: KafkaMessage,
): InboundMessage {
  return {
    topic,
    partition,
    offset: message.offset,
    timestamp: message.timestamp,
    key: message.key?.toString() ?? null,
    value: message.value,
    headers: decodeHeaders(message.headers),
  };
}

function decodeHeaders(headers: IHeaders | undefined): Record<string, string> {
  const decoded: Record<string, string> = {};
  for (const [name, raw] of Object.entries(headers ?? {})) {
    const first = Array.isArray(raw) ? raw[0] : raw;
    if (first !== undefined) decoded[name] = first.toString();
  }
  return decoded;
}
