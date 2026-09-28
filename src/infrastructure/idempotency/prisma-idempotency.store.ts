import { Injectable } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import type { JsonValue } from "@common/json";
import { type Env, readEnv } from "@config/env";
import { Prisma } from "@infrastructure/database/generated/client";
import { PrismaService } from "@infrastructure/database/prisma/prisma.service";
import {
  decideForExistingKey,
  IDEMPOTENCY_STATUS,
} from "./idempotency.policy";
import type { ClaimResult, IdempotencyStore } from "./idempotency.store";

const HOUR_MS = 3_600_000;
// Insert falhou + registro sumiu (cleanup) ou takeover perdido: tenta de novo
// poucas vezes; persistindo a disputa, trata como em andamento.
const MAX_CLAIM_ATTEMPTS = 3;

@Injectable()
export class PrismaIdempotencyStore implements IdempotencyStore {
  private readonly ttlMs: number;
  private readonly lockTimeoutMs: number;

  constructor(
    private readonly prisma: PrismaService,
    config: ConfigService<Env, true>,
  ) {
    this.ttlMs = readEnv(config, "IDEMPOTENCY_TTL_HOURS") * HOUR_MS;
    this.lockTimeoutMs = readEnv(config, "IDEMPOTENCY_LOCK_TIMEOUT_MS");
  }

  async claim(key: string, requestHash: string, now: Date): Promise<ClaimResult> {
    const expiresAt = new Date(now.getTime() + this.ttlMs);

    for (let attempt = 0; attempt < MAX_CLAIM_ATTEMPTS; attempt++) {
      // INSERT ... ON CONFLICT DO NOTHING: a PK decide atomicamente quem
      // reserva a chave, sem janela entre ler e gravar.
      const inserted = await this.prisma.idempotencyKey.createMany({
        data: [
          {
            key,
            requestHash,
            status: IDEMPOTENCY_STATUS.inProgress,
            lockedAt: now,
            expiresAt,
          },
        ],
        skipDuplicates: true,
      });
      if (inserted.count === 1) return { kind: "claimed", lockedAt: now };

      const existing = await this.prisma.idempotencyKey.findUnique({
        where: { key },
      });
      if (!existing) continue;

      const decision = decideForExistingKey(
        existing,
        requestHash,
        now,
        this.lockTimeoutMs,
      );
      if (decision.kind !== "takeover") return decision;

      // Update condicional ao lockedAt lido: se outro processo assumiu antes,
      // count = 0 e reavaliamos.
      const taken = await this.prisma.idempotencyKey.updateMany({
        where: { key, lockedAt: existing.lockedAt },
        data: {
          requestHash,
          status: IDEMPOTENCY_STATUS.inProgress,
          lockedAt: now,
          expiresAt,
          responseStatus: null,
          responseBody: Prisma.DbNull,
        },
      });
      if (taken.count === 1) return { kind: "claimed", lockedAt: now };
    }

    return { kind: "in_progress" };
  }

  async complete(
    key: string,
    lockedAt: Date,
    responseStatus: number,
    responseBody: JsonValue,
  ): Promise<boolean> {
    const updated = await this.prisma.idempotencyKey.updateMany({
      where: { key, lockedAt, status: IDEMPOTENCY_STATUS.inProgress },
      data: {
        status: IDEMPOTENCY_STATUS.completed,
        responseStatus,
        responseBody: responseBody === null ? Prisma.JsonNull : responseBody,
      },
    });
    return updated.count === 1;
  }

  async release(key: string, lockedAt: Date): Promise<void> {
    await this.prisma.idempotencyKey.deleteMany({
      where: { key, lockedAt, status: IDEMPOTENCY_STATUS.inProgress },
    });
  }

  async deleteExpired(now: Date): Promise<number> {
    const deleted = await this.prisma.idempotencyKey.deleteMany({
      where: { expiresAt: { lt: now } },
    });
    return deleted.count;
  }
}
