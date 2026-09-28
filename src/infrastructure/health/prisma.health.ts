import { Inject, Injectable } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { type HealthIndicatorResult, HealthIndicatorService } from "@nestjs/terminus";
import { type ILogger, LOGGER_TOKEN } from "@common/logger/logger.interface";
import { withTimeout } from "@common/with-timeout";
import { type Env, readEnv } from "@config/env";
import { PrismaService } from "@infrastructure/database/prisma/prisma.service";

// Indicador proprio: o PrismaHealthIndicator do terminus tenta $runCommandRaw
// (client Mongo) antes de SELECT 1.
@Injectable()
export class PrismaHealthIndicator {
  private readonly timeoutMs: number;

  constructor(
    private readonly prisma: PrismaService,
    private readonly healthIndicatorService: HealthIndicatorService,
    @Inject(LOGGER_TOKEN) private readonly logger: ILogger,
    config: ConfigService<Env, true>,
  ) {
    this.timeoutMs = readEnv(config, "HEALTH_CHECK_TIMEOUT_MS");
  }

  async isHealthy<const K extends string>(key: K): Promise<HealthIndicatorResult<K>> {
    const indicator = this.healthIndicatorService.check(key);
    try {
      await withTimeout(this.prisma.$queryRaw`SELECT 1`, this.timeoutMs, "Database health check timeout");
      return indicator.up();
    } catch (err) {
      // Detalhe so no log: a resposta do probe nao expoe infraestrutura.
      this.logger.warn("Banco indisponivel no health check", {
        error: err instanceof Error ? err.message : String(err),
      });
      return indicator.down({ message: "indisponivel" });
    }
  }
}
