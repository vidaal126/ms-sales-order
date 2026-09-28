import {
  Inject,
  Injectable,
  type OnModuleDestroy,
  type OnModuleInit,
} from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { type ILogger, LOGGER_TOKEN } from "@common/logger/logger.interface";
import { type Env, readEnv } from "@config/env";
import { IDEMPOTENCY_STORE, type IdempotencyStore } from "./idempotency.store";

// Remove chaves expiradas periodicamente. Rodar em varias replicas e seguro:
// o DELETE e idempotente.
@Injectable()
export class IdempotencyCleanupService implements OnModuleInit, OnModuleDestroy {
  private readonly intervalMs: number;
  private intervalHandle: NodeJS.Timeout | null = null;
  private currentRun: Promise<void> | null = null;

  constructor(
    @Inject(IDEMPOTENCY_STORE) private readonly store: IdempotencyStore,
    @Inject(LOGGER_TOKEN) private readonly logger: ILogger,
    config: ConfigService<Env, true>,
  ) {
    this.intervalMs = readEnv(config, "IDEMPOTENCY_CLEANUP_INTERVAL_MS");
  }

  onModuleInit(): void {
    this.intervalHandle = setInterval((): void => {
      if (this.currentRun) return;
      // run nunca rejeita (erros sao logados dentro dele).
      this.currentRun = this.run().finally((): void => {
        this.currentRun = null;
      });
    }, this.intervalMs);
  }

  async onModuleDestroy(): Promise<void> {
    if (this.intervalHandle) clearInterval(this.intervalHandle);
    await this.currentRun;
  }

  async run(): Promise<void> {
    try {
      const deleted = await this.store.deleteExpired(new Date());
      if (deleted > 0) {
        this.logger.log("Chaves de idempotencia expiradas removidas", { deleted });
      }
    } catch (err) {
      this.logger.error(
        "Falha na limpeza de chaves de idempotencia",
        err instanceof Error ? err : new Error(String(err)),
      );
    }
  }
}
