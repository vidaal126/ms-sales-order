import { randomUUID } from "node:crypto";

export const CORRELATION_ID_HEADER = "x-correlation-id";

// Valor vindo de fora segue para eventos e logs: restringe charset e tamanho
// em vez de confiar no que chegou.
const CORRELATION_ID_PATTERN = /^[A-Za-z0-9._:-]{1,128}$/;

export function isValidCorrelationId(value: unknown): value is string {
  return typeof value === "string" && CORRELATION_ID_PATTERN.test(value);
}

export function resolveCorrelationId(candidate: unknown): string {
  return isValidCorrelationId(candidate) ? candidate : randomUUID();
}
