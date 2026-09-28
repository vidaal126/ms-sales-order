import type { Prisma } from "@infrastructure/database/generated/client";
import type { SalesOrderEvent } from "@domain/events/sales-order.events";
import type { PersistenceContext } from "@application/ports/sales-order.repository.port";

// Contrato publicado (envelope v2: schemaVersion no envelope).
export const SALES_ORDER_EVENTS_SCHEMA_VERSION = 2;

export function toOutboxEventData(
  event: SalesOrderEvent,
  context: PersistenceContext,
): Prisma.OutboxEventCreateManyInput {
  return {
    aggregateId: event.aggregateId,
    eventType: event.eventType,
    schemaVersion: SALES_ORDER_EVENTS_SCHEMA_VERSION,
    correlationId: context.correlationId,
    payload: toPayload(event),
    createdAt: event.occurredAt,
  };
}

function toPayload(event: SalesOrderEvent): Prisma.InputJsonObject {
  switch (event.eventType) {
    case "OrderCreated":
      return {
        id: event.aggregateId,
        customerId: event.customerId,
        transportTypeId: event.transportTypeId,
        status: event.status,
        items: event.items.map((line) => ({
          itemId: line.itemId,
          quantity: line.quantity,
          unitPrice: line.unitPrice,
        })),
        total: event.total,
      };
    case "OrderStatusChanged":
      return { id: event.aggregateId, previousStatus: event.previousStatus, currentStatus: event.currentStatus };
    case "DeliveryScheduled":
      return {
        id: event.aggregateId,
        deliveryDate: event.deliveryDate,
        windowStart: event.windowStart.toISOString(),
        windowEnd: event.windowEnd.toISOString(),
      };
    case "DeliveryRescheduled":
      return {
        id: event.aggregateId,
        previousDeliveryDate: event.previousDeliveryDate,
        deliveryDate: event.deliveryDate,
        windowStart: event.windowStart.toISOString(),
        windowEnd: event.windowEnd.toISOString(),
      };
    case "TransportChanged":
      return {
        id: event.aggregateId,
        previousTransportTypeId: event.previousTransportTypeId,
        transportTypeId: event.transportTypeId,
      };
  }
}
