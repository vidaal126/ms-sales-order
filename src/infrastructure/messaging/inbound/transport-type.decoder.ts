import { z } from "zod";
import type { TransportTypeReplicaEvent } from "@application/ports/replica.ports";
import { type DecodeResult, decodeEnvelopeV2, parseJson } from "./envelope.decoder";

export const TRANSPORT_TYPE_EVENT_TYPES = ["TransportTypeCreated", "TransportTypeUpdated"] as const;
export const TRANSPORT_TYPE_TOPICS = TRANSPORT_TYPE_EVENT_TYPES.map((type) => `transport.${type}`);

const payloadSchema = z.object({
  id: z.uuid(),
  name: z.string().min(1),
  description: z.string().nullable(),
  active: z.boolean(),
});

export function decodeTransportTypeEvent(raw: Buffer | null): DecodeResult<TransportTypeReplicaEvent> {
  const json = parseJson(raw);
  if (!json.ok) return json;
  const decoded = decodeEnvelopeV2(json.value, TRANSPORT_TYPE_EVENT_TYPES, payloadSchema);
  if (!decoded.ok) return decoded;
  return {
    ok: true,
    correlationId: decoded.meta.correlationId,
    event: {
      eventId: decoded.meta.eventId,
      eventType: decoded.meta.eventType,
      occurredAt: decoded.meta.occurredAt,
      transportType: {
        transportTypeId: decoded.payload.id,
        name: decoded.payload.name,
        active: decoded.payload.active,
      },
    },
  };
}
