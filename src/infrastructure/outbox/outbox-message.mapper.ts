import type { OutboxEvent } from "@infrastructure/database/generated/client";
import {
  type OutboundMessage,
  serializeEnvelope,
} from "@infrastructure/messaging/event-envelope";

export function topicFor(eventType: string): string {
  return `sales-order.${eventType}`;
}

// O id da linha do outbox e o eventId: estavel entre republicacoes, o que
// permite ao consumer deduplicar quando o publisher reenvia (at-least-once).
export function toOutboundMessage(event: OutboxEvent): OutboundMessage {
  return serializeEnvelope(topicFor(event.eventType), {
    eventId: event.id,
    eventType: event.eventType,
    schemaVersion: event.schemaVersion,
    occurredAt: event.createdAt.toISOString(),
    aggregateId: event.aggregateId,
    correlationId: event.correlationId,
    payload: event.payload,
  });
}
