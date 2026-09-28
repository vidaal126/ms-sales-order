import { z } from "zod";
import type { DeadLetterReason } from "@application/ports/dead-letter.port";

export const SUPPORTED_ENVELOPE_VERSION = 2;

export type DecodeResult<TEvent> =
  | { readonly ok: true; readonly event: TEvent; readonly correlationId?: string }
  | { readonly ok: false; readonly reason: DeadLetterReason; readonly detail: string };

export interface EnvelopeMeta {
  readonly eventId: string;
  readonly eventType: string;
  readonly occurredAt: Date;
  readonly aggregateId: string;
  readonly correlationId: string;
}

const envelopeV2Schema = z.object({
  eventId: z.uuid(),
  eventType: z.string().min(1),
  schemaVersion: z.number().int(),
  occurredAt: z.iso.datetime(),
  aggregateId: z.string().min(1),
  correlationId: z.string().min(1),
  payload: z.unknown(),
});

export const hasTopLevelVersionSchema = z.object({ schemaVersion: z.number() });

export function parseJson(raw: Buffer | null): { ok: true; value: unknown } | DecodeFailure {
  if (raw === null || raw.length === 0) return fail("invalid_json", "mensagem vazia");
  try {
    const value: unknown = JSON.parse(raw.toString("utf8"));
    return { ok: true, value };
  } catch (err) {
    return fail("invalid_json", err instanceof Error ? err.message : "JSON invalido");
  }
}

export type DecodeFailure = { readonly ok: false; readonly reason: DeadLetterReason; readonly detail: string };

// Envelope v2 com eventType esperado e payload validado; payload.id tem que
// ser o aggregateId (key da mensagem e ordem por agregado dependem disso).
export function decodeEnvelopeV2<TPayload extends { readonly id: string }>(
  parsed: unknown,
  eventTypes: readonly string[],
  payloadSchema: z.ZodType<TPayload>,
): { ok: true; meta: EnvelopeMeta; payload: TPayload } | DecodeFailure {
  const envelope = envelopeV2Schema.safeParse(parsed);
  if (!envelope.success) return schemaFailure(envelope.error);
  const { schemaVersion, eventType } = envelope.data;
  if (schemaVersion !== SUPPORTED_ENVELOPE_VERSION) {
    return fail("unsupported_schema_version", `schemaVersion ${schemaVersion} no envelope`);
  }
  if (!eventTypes.includes(eventType)) {
    return fail("schema_validation_failed", `eventType ${eventType} inesperado`);
  }
  const payload = payloadSchema.safeParse(envelope.data.payload);
  if (!payload.success) return schemaFailure(payload.error);
  if (payload.data.id !== envelope.data.aggregateId) {
    return fail(
      "schema_validation_failed",
      `aggregateId ${envelope.data.aggregateId} difere de payload.id ${payload.data.id}`,
    );
  }
  return {
    ok: true,
    meta: {
      eventId: envelope.data.eventId,
      eventType,
      occurredAt: new Date(envelope.data.occurredAt),
      aggregateId: envelope.data.aggregateId,
      correlationId: envelope.data.correlationId,
    },
    payload: payload.data,
  };
}

export function schemaFailure(error: z.ZodError): DecodeFailure {
  return fail("schema_validation_failed", z.prettifyError(error));
}

export function fail(reason: DeadLetterReason, detail: string): DecodeFailure {
  return { ok: false, reason, detail };
}
