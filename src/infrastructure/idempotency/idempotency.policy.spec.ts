import {
  decideForExistingKey,
  type ExistingKeyDecision,
  IDEMPOTENCY_STATUS,
  type IdempotencyRecord,
} from "./idempotency.policy";

const NOW = new Date("2026-09-22T12:00:00.000Z");
const LOCK_TIMEOUT_MS = 30_000;

function record(overrides: Partial<IdempotencyRecord> = {}): IdempotencyRecord {
  return {
    requestHash: "hash-a",
    status: IDEMPOTENCY_STATUS.inProgress,
    responseStatus: null,
    responseBody: null,
    lockedAt: new Date(NOW.getTime() - 1_000),
    expiresAt: new Date(NOW.getTime() + 60_000),
    ...overrides,
  };
}

const completed = (overrides: Partial<IdempotencyRecord> = {}): IdempotencyRecord =>
  record({
    status: IDEMPOTENCY_STATUS.completed,
    responseStatus: 201,
    responseBody: { id: "x" },
    ...overrides,
  });

describe("decideForExistingKey", () => {
  const decide = (r: IdempotencyRecord, hash = "hash-a"): ExistingKeyDecision =>
    decideForExistingKey(r, hash, NOW, LOCK_TIMEOUT_MS);

  it("mesmo corpo e concluida: devolve a resposta original", () => {
    expect(decide(completed())).toEqual({
      kind: "completed",
      responseStatus: 201,
      responseBody: { id: "x" },
    });
  });

  it("corpo diferente: mismatch, concluida ou em andamento", () => {
    expect(decide(completed(), "hash-b")).toEqual({ kind: "mismatch" });
    expect(decide(record(), "hash-b")).toEqual({ kind: "mismatch" });
  });

  it("em andamento dentro do lock: in_progress", () => {
    expect(decide(record())).toEqual({ kind: "in_progress" });
  });

  it("em andamento com lock vencido: takeover", () => {
    const stale = record({ lockedAt: new Date(NOW.getTime() - LOCK_TIMEOUT_MS) });

    expect(decide(stale)).toEqual({ kind: "takeover" });
  });

  it("expirada: takeover mesmo com corpo diferente", () => {
    const expired = completed({ expiresAt: NOW });

    expect(decide(expired, "hash-b")).toEqual({ kind: "takeover" });
  });
});
