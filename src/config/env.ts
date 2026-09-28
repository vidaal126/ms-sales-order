import type { ConfigService } from "@nestjs/config";
import { z } from "zod";

const positiveInt = z.coerce.number().int().positive();
const groupId = (fallback: string): z.ZodDefault<z.ZodString> =>
  z.string().regex(/^[A-Za-z0-9._-]{1,249}$/, "group id Kafka invalido").default(fallback);

export const envSchema = z.object({
  NODE_ENV: z.enum(["development", "test", "production"]).default("development"),
  PORT: z.coerce.number().int().min(1).max(65535).default(3004),
  LOG_LEVEL: z.enum(["fatal", "error", "warn", "info", "debug", "trace"]).default("info"),

  DATABASE_URL: z.string().regex(/^postgres(ql)?:\/\//, "deve ser uma URL postgresql://"),

  // Lista separada por virgula: "host1:9092,host2:9092".
  KAFKA_BROKER: z
    .string()
    .min(1)
    .transform((value) => value.split(",").map((broker) => broker.trim()))
    .pipe(z.array(z.string().regex(/^[^\s:]+:\d+$/, "formato host:porta")).min(1)),
  KAFKA_CLIENT_ID: z.string().min(1).default("ms-sales-order"),
  // Teto de cada envio do producer (conexao + ack). O producer idempotente
  // tem retries ilimitados; sem teto um envio travaria o outbox e a DLT.
  KAFKA_SEND_TIMEOUT_MS: positiveInt.default(5_000),

  // Groups fixos das replicas. Sobrescrever so para replay com um group
  // temporario (fixo por execucao, nunca aleatorio).
  CATALOG_ITEM_SYNC_GROUP_ID: groupId("ms-sales-order.catalog-item-sync"),
  CUSTOMER_SYNC_GROUP_ID: groupId("ms-sales-order.customer-sync"),
  TRANSPORT_TYPE_SYNC_GROUP_ID: groupId("ms-sales-order.transport-type-sync"),

  // Falha recuperavel: retry em processo com backoff exponencial e jitter;
  // esgotado, pausa a particao por CONSUMER_PAUSE_MS.
  CONSUMER_RETRY_RETRIES: z.coerce.number().int().min(0).default(5),
  CONSUMER_RETRY_INITIAL_MS: positiveInt.default(300),
  CONSUMER_RETRY_MAX_MS: positiveInt.default(30_000),
  CONSUMER_PAUSE_MS: positiveInt.default(30_000),

  OUTBOX_POLL_INTERVAL_MS: positiveInt.default(2_000),
  OUTBOX_BATCH_SIZE: positiveInt.max(1_000).default(20),

  IDEMPOTENCY_TTL_HOURS: positiveInt.default(24),
  IDEMPOTENCY_LOCK_TIMEOUT_MS: positiveInt.default(30_000),
  IDEMPOTENCY_CLEANUP_INTERVAL_MS: positiveInt.default(3_600_000),

  THROTTLE_DEFAULT_TTL_MS: positiveInt.default(60_000),
  THROTTLE_DEFAULT_LIMIT: positiveInt.default(100),

  HEALTH_CHECK_TIMEOUT_MS: positiveInt.default(1_500),
  SHUTDOWN_TIMEOUT_MS: positiveInt.default(10_000),
});

export type Env = z.infer<typeof envSchema>;

export class InvalidEnvironmentError extends Error {}

export function validateEnv(raw: Record<string, unknown>): Env {
  const result = envSchema.safeParse(raw);
  if (!result.success) {
    throw new InvalidEnvironmentError(
      `Variaveis de ambiente invalidas:\n${z.prettifyError(result.error)}`,
    );
  }
  return result.data;
}

export function readEnv<K extends keyof Env>(config: ConfigService<Env, true>, key: K): Env[K] {
  return config.get(key, { infer: true });
}
