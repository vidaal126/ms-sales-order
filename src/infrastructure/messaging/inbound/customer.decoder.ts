import { z } from "zod";
import type { CustomerReplicaEvent } from "@application/ports/replica.ports";
import { type DecodeResult, decodeEnvelopeV2, parseJson } from "./envelope.decoder";

export const CUSTOMER_EVENT_TYPES = ["CustomerCreated", "CustomerUpdated"] as const;
export const CUSTOMER_TOPICS = CUSTOMER_EVENT_TYPES.map((type) => `customer.${type}`);

const payloadSchema = z.object({
  id: z.uuid(),
  name: z.string().min(1),
  document: z.string(),
  authorizedTransportTypeIds: z.array(z.uuid()),
});

export function decodeCustomerEvent(raw: Buffer | null): DecodeResult<CustomerReplicaEvent> {
  const json = parseJson(raw);
  if (!json.ok) return json;
  const decoded = decodeEnvelopeV2(json.value, CUSTOMER_EVENT_TYPES, payloadSchema);
  if (!decoded.ok) return decoded;
  return {
    ok: true,
    correlationId: decoded.meta.correlationId,
    event: {
      eventId: decoded.meta.eventId,
      eventType: decoded.meta.eventType,
      occurredAt: decoded.meta.occurredAt,
      customer: {
        customerId: decoded.payload.id,
        name: decoded.payload.name,
        authorizedTransportTypeIds: decoded.payload.authorizedTransportTypeIds,
      },
    },
  };
}
