import type { OrderStatus } from "@domain/value-objects/order-status";
import type { DomainEvent } from "./domain-event";

export interface OrderLineSnapshot {
  readonly itemId: string;
  readonly quantity: number;
  readonly unitPrice: number;
}

export class OrderCreatedEvent implements DomainEvent {
  static readonly EVENT_TYPE = "OrderCreated";
  readonly eventType = OrderCreatedEvent.EVENT_TYPE;

  constructor(
    readonly aggregateId: string,
    readonly occurredAt: Date,
    readonly customerId: string,
    readonly transportTypeId: string,
    readonly status: OrderStatus,
    readonly items: readonly OrderLineSnapshot[],
    readonly total: number,
  ) {}
}

export class OrderStatusChangedEvent implements DomainEvent {
  static readonly EVENT_TYPE = "OrderStatusChanged";
  readonly eventType = OrderStatusChangedEvent.EVENT_TYPE;

  constructor(
    readonly aggregateId: string,
    readonly occurredAt: Date,
    readonly previousStatus: OrderStatus,
    readonly currentStatus: OrderStatus,
  ) {}
}

export class DeliveryScheduledEvent implements DomainEvent {
  static readonly EVENT_TYPE = "DeliveryScheduled";
  readonly eventType = DeliveryScheduledEvent.EVENT_TYPE;

  constructor(
    readonly aggregateId: string,
    readonly occurredAt: Date,
    readonly deliveryDate: string,
    readonly windowStart: Date,
    readonly windowEnd: Date,
  ) {}
}

export class DeliveryRescheduledEvent implements DomainEvent {
  static readonly EVENT_TYPE = "DeliveryRescheduled";
  readonly eventType = DeliveryRescheduledEvent.EVENT_TYPE;

  constructor(
    readonly aggregateId: string,
    readonly occurredAt: Date,
    readonly previousDeliveryDate: string,
    readonly deliveryDate: string,
    readonly windowStart: Date,
    readonly windowEnd: Date,
  ) {}
}

export class TransportChangedEvent implements DomainEvent {
  static readonly EVENT_TYPE = "TransportChanged";
  readonly eventType = TransportChangedEvent.EVENT_TYPE;

  constructor(
    readonly aggregateId: string,
    readonly occurredAt: Date,
    readonly previousTransportTypeId: string,
    readonly transportTypeId: string,
  ) {}
}

export type SalesOrderEvent =
  | OrderCreatedEvent
  | OrderStatusChangedEvent
  | DeliveryScheduledEvent
  | DeliveryRescheduledEvent
  | TransportChangedEvent;
