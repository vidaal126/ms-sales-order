import { v5 as uuidv5 } from "uuid";
import { z } from "zod";
import type { ItemReplicaEvent } from "@application/ports/replica.ports";
import {
  type DecodeResult,
  decodeEnvelopeV2,
  fail,
  hasTopLevelVersionSchema,
  parseJson,
  schemaFailure,
} from "./envelope.decoder";

export const ITEM_CREATED_TOPIC = "catalog.ItemCreated";
export const ITEM_CREATED_EVENT_TYPE = "ItemCreated";
const SUPPORTED_LEGACY_VERSION = 1;

// Namespace fixo do ms-sales-order para derivar eventIds de eventos v1 (que
// nao tem eventId). Nunca mudar: mudaria o id dos legados ja processados.
export const LEGACY_EVENT_ID_NAMESPACE = "5d2a8c1e-7b64-4f3a-9e0d-1c8b6a4f2e97";

export interface MessagePosition {
  readonly topic: string;
  readonly partition: number;
  readonly offset: string;
}

// So o que a ordem de venda usa. unitPrice e validado no use case (preco
// invalido = invariante violada = DLT).
const payloadSchema = z.object({
  id: z.uuid(),
  sku: z.string().min(1),
  name: z.string().min(1),
  unitPrice: z.number(),
});

const legacyEnvelopeSchema = z.object({
  eventType: z.literal(ITEM_CREATED_EVENT_TYPE),
  aggregateId: z.string().min(1),
  occurredAt: z.iso.datetime(),
  payload: z.record(z.string(), z.unknown()),
});

const legacyPayloadVersionSchema = z.object({ schemaVersion: z.number().int() });

// Aceita o envelope v2 e o v1 legado (schemaVersion 1 dentro do payload).
// Legado sem schemaVersion (anterior ao contrato) vai para a DLT.
export function decodeItemCreated(raw: Buffer | null, position: MessagePosition): DecodeResult<ItemReplicaEvent> {
  const json = parseJson(raw);
  if (!json.ok) return json;

  if (hasTopLevelVersionSchema.safeParse(json.value).success) {
    const decoded = decodeEnvelopeV2(json.value, [ITEM_CREATED_EVENT_TYPE], payloadSchema);
    if (!decoded.ok) return decoded;
    return {
      ok: true,
      correlationId: decoded.meta.correlationId,
      event: toEvent(decoded.meta.eventId, decoded.meta.occurredAt, decoded.payload),
    };
  }

  const legacy = legacyEnvelopeSchema.safeParse(json.value);
  if (!legacy.success) return schemaFailure(legacy.error);
  const version = legacyPayloadVersionSchema.safeParse(legacy.data.payload);
  if (!version.success) return fail("unsupported_schema_version", "evento legado sem schemaVersion");
  if (version.data.schemaVersion !== SUPPORTED_LEGACY_VERSION) {
    return fail("unsupported_schema_version", `schemaVersion ${version.data.schemaVersion} no payload`);
  }
  const payload = payloadSchema.safeParse(legacy.data.payload);
  if (!payload.success) return schemaFailure(payload.error);
  if (payload.data.id !== legacy.data.aggregateId) {
    return fail(
      "schema_validation_failed",
      `aggregateId ${legacy.data.aggregateId} difere de payload.id ${payload.data.id}`,
    );
  }
  return { ok: true, event: toEvent(legacyEventId(position), new Date(legacy.data.occurredAt), payload.data) };
}

// UUID v5 de topico:particao:offset: deterministico para a mesma mensagem
// (replay seguro); o mesmo evento republicado em outro offset ganha outro id,
// e ai a protecao e o guard de sourceOccurredAt (limitacao documentada).
export function legacyEventId(position: MessagePosition): string {
  return uuidv5(`${position.topic}:${position.partition}:${position.offset}`, LEGACY_EVENT_ID_NAMESPACE);
}

function toEvent(eventId: string, occurredAt: Date, payload: z.infer<typeof payloadSchema>): ItemReplicaEvent {
  return {
    eventId,
    eventType: ITEM_CREATED_EVENT_TYPE,
    occurredAt,
    item: { itemId: payload.id, sku: payload.sku, name: payload.name, unitPrice: payload.unitPrice },
  };
}
