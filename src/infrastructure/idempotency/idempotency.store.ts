import type { JsonValue } from "@common/json";

export const IDEMPOTENCY_STORE = Symbol("IDEMPOTENCY_STORE");

export type ClaimResult =
  // lockedAt identifica esta posse da chave: complete/release so atuam se a
  // chave ainda estiver com o mesmo lockedAt (ninguem assumiu no meio).
  | { readonly kind: "claimed"; readonly lockedAt: Date }
  | { readonly kind: "mismatch" }
  | { readonly kind: "in_progress" }
  | {
      readonly kind: "completed";
      readonly responseStatus: number;
      readonly responseBody: JsonValue;
    };

export interface IdempotencyStore {
  claim(key: string, requestHash: string, now: Date): Promise<ClaimResult>;
  complete(
    key: string,
    lockedAt: Date,
    responseStatus: number,
    responseBody: JsonValue,
  ): Promise<boolean>;
  release(key: string, lockedAt: Date): Promise<void>;
  deleteExpired(now: Date): Promise<number>;
}
