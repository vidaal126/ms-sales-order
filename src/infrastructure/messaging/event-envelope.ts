import type { JsonValue } from "@common/json";

export interface EventEnvelope {
  readonly eventId: string;
  readonly eventType: string;
  readonly schemaVersion: number;
  readonly occurredAt: string;
  readonly aggregateId: string;
  readonly correlationId: string;
  readonly payload: JsonValue;
}

export const EVENT_HEADERS = {
  eventType: "eventType",
  schemaVersion: "schemaVersion",
  correlationId: "correlationId",
} as const;

export interface OutboundMessage {
  readonly topic: string;
  readonly key: string | null;
  // Buffer permite republicar bytes originais sem reserializar (DLT).
  readonly value: string | Buffer | null;
  readonly headers: Readonly<Record<string, string>>;
}

// Key = aggregateId garante ordem por agregado dentro da particao.
export function serializeEnvelope(
  topic: string,
  envelope: EventEnvelope,
): OutboundMessage {
  return {
    topic,
    key: envelope.aggregateId,
    value: JSON.stringify(envelope),
    headers: {
      [EVENT_HEADERS.eventType]: envelope.eventType,
      [EVENT_HEADERS.schemaVersion]: String(envelope.schemaVersion),
      [EVENT_HEADERS.correlationId]: envelope.correlationId,
    },
  };
}
