import type { JsonValue } from "@common/json";
import type { ClaimResult } from "./idempotency.store";

export const IDEMPOTENCY_STATUS = {
  inProgress: "in_progress",
  completed: "completed",
} as const;

export interface IdempotencyRecord {
  readonly requestHash: string;
  readonly status: string;
  readonly responseStatus: number | null;
  readonly responseBody: JsonValue | null;
  readonly lockedAt: Date;
  readonly expiresAt: Date;
}

export type ExistingKeyDecision =
  | Exclude<ClaimResult, { kind: "claimed" }>
  | { readonly kind: "takeover" };

// Decide o que fazer quando a chave ja existe. Ordem importa:
// 1. expirada (TTL): chave livre para qualquer corpo;
// 2. corpo diferente: 422, mesmo que a primeira ainda esteja rodando;
// 3. concluida: devolve a resposta original;
// 4. em andamento com lock vencido (processo morreu no meio): assume;
// 5. em andamento: 409.
export function decideForExistingKey(
  record: IdempotencyRecord,
  requestHash: string,
  now: Date,
  lockTimeoutMs: number,
): ExistingKeyDecision {
  if (record.expiresAt.getTime() <= now.getTime()) {
    return { kind: "takeover" };
  }

  if (record.requestHash !== requestHash) {
    return { kind: "mismatch" };
  }

  if (record.status === IDEMPOTENCY_STATUS.completed) {
    if (record.responseStatus === null || record.responseBody === null) {
      throw new Error("Registro de idempotencia concluido sem resposta");
    }
    return {
      kind: "completed",
      responseStatus: record.responseStatus,
      responseBody: record.responseBody,
    };
  }

  if (now.getTime() - record.lockedAt.getTime() >= lockTimeoutMs) {
    return { kind: "takeover" };
  }

  return { kind: "in_progress" };
}
