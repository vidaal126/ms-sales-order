import { Inject, Injectable, type OnApplicationShutdown } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { type HealthIndicatorResult, HealthIndicatorService } from "@nestjs/terminus";
import type { Admin, Kafka } from "kafkajs";
import { type ILogger, LOGGER_TOKEN } from "@common/logger/logger.interface";
import { withTimeout } from "@common/with-timeout";
import { KAFKA_CLIENT } from "./kafka.tokens";

interface HealthEnv {
  HEALTH_CHECK_TIMEOUT_MS: number;
}

// Pergunta ao broker (describeCluster) em vez de olhar o estado do producer:
// o producer so reconecta quando ha algo a enviar.
@Injectable()
export class KafkaHealthIndicator implements OnApplicationShutdown {
  private admin: Admin | null = null;
  private readonly timeoutMs: number;

  constructor(
    @Inject(KAFKA_CLIENT) private readonly kafka: Kafka,
    private readonly healthIndicatorService: HealthIndicatorService,
    @Inject(LOGGER_TOKEN) private readonly logger: ILogger,
    config: ConfigService<HealthEnv, true>,
  ) {
    const timeoutMs = config.get("HEALTH_CHECK_TIMEOUT_MS", { infer: true });
    this.timeoutMs = timeoutMs;
  }

  async isHealthy<const K extends string>(key: K): Promise<HealthIndicatorResult<K>> {
    const indicator = this.healthIndicatorService.check(key);
    try {
      await withTimeout(this.ping(), this.timeoutMs, "Kafka health check timeout");
      return indicator.up();
    } catch (err) {
      this.logger.warn("Kafka indisponivel no health check", {
        error: err instanceof Error ? err.message : String(err),
      });
      return indicator.down({ message: "indisponivel" });
    }
  }

  async onApplicationShutdown(): Promise<void> {
    await this.admin?.disconnect();
  }

  private async ping(): Promise<void> {
    // retries: 0 - quem repete e o orquestrador chamando o probe de novo.
    this.admin ??= this.kafka.admin({ retry: { retries: 0 } });
    await this.admin.connect();
    await this.admin.describeCluster();
  }
}
