import { type OrderLine, SalesOrder } from "@domain/entities/sales-order.entity";
import {
  SalesOrderNotFoundError,
  TransportNotAuthorizedError,
  UnknownCustomerError,
  UnknownItemsError,
} from "@domain/errors/sales-order.errors";
import type { DeliveryWindow } from "@domain/value-objects/delivery-window";
import type { OrderStatus } from "@domain/value-objects/order-status";
import type { CustomerReplica, IReplicaReader } from "@application/ports/replica.ports";
import type {
  ISalesOrderRepository,
  PersistenceContext,
  SalesOrderFilter,
} from "@application/ports/sales-order.repository.port";

export type Clock = () => Date;

export const DEFAULT_PAGE_SIZE = 20;
export const MAX_PAGE_SIZE = 100;

export function resolvePageSize(limit?: number): number {
  if (!limit || limit <= 0) return DEFAULT_PAGE_SIZE;
  return Math.min(limit, MAX_PAGE_SIZE);
}

// Transporte aceito para o cliente: existe na replica, ativo e autorizado.
async function assertTransportAllowed(
  replicas: IReplicaReader,
  customer: CustomerReplica,
  transportTypeId: string,
): Promise<void> {
  const transportType = await replicas.findTransportType(transportTypeId);
  if (!transportType) {
    throw new TransportNotAuthorizedError(transportTypeId, "tipo de transporte desconhecido");
  }
  if (!transportType.active) throw new TransportNotAuthorizedError(transportTypeId, "tipo de transporte inativo");
  if (!customer.authorizedTransportTypeIds.includes(transportTypeId)) {
    throw new TransportNotAuthorizedError(transportTypeId, `nao autorizado para o cliente ${customer.customerId}`);
  }
}

async function loadOrder(orders: ISalesOrderRepository, id: string): Promise<SalesOrder> {
  const order = await orders.findById(id);
  if (!order) throw new SalesOrderNotFoundError(id);
  return order;
}

export interface CreateSalesOrderInput {
  readonly customerId: string;
  readonly transportTypeId: string;
  readonly notes?: string | undefined;
  readonly items: ReadonlyArray<{ readonly itemId: string; readonly quantity: number }>;
}

// Valida contra as replicas locais (sem chamada sincrona a outros servicos):
// consistencia eventual, um cadastro recem-feito pode ainda nao ter chegado.
export class CreateSalesOrderUseCase {
  constructor(
    private readonly orders: ISalesOrderRepository,
    private readonly replicas: IReplicaReader,
    private readonly clock: Clock,
  ) {}

  async execute(input: CreateSalesOrderInput, context: PersistenceContext): Promise<SalesOrder> {
    const customer = await this.replicas.findCustomer(input.customerId);
    if (!customer) throw new UnknownCustomerError(input.customerId);
    await assertTransportAllowed(this.replicas, customer, input.transportTypeId);

    const requestedIds = [...new Set(input.items.map((line) => line.itemId))];
    const found = new Map((await this.replicas.findItems(requestedIds)).map((item) => [item.itemId, item]));
    const missing = requestedIds.filter((id) => !found.has(id));
    if (missing.length > 0) throw new UnknownItemsError(missing);

    const lines = input.items.map((line): OrderLine => {
      const item = found.get(line.itemId);
      if (!item) throw new UnknownItemsError([line.itemId]);
      return { itemId: line.itemId, quantity: line.quantity, unitPrice: item.unitPrice };
    });

    const order = SalesOrder.create({
      customerId: input.customerId,
      transportTypeId: input.transportTypeId,
      notes: input.notes,
      lines,
      now: this.clock(),
    });
    await this.orders.create(order, context);
    return order;
  }
}

export class GetSalesOrderUseCase {
  constructor(private readonly orders: ISalesOrderRepository) {}

  execute(id: string): Promise<SalesOrder> {
    return loadOrder(this.orders, id);
  }
}

export interface ListSalesOrdersInput extends SalesOrderFilter {
  readonly page?: number | undefined;
  readonly limit?: number | undefined;
}

export interface ListSalesOrdersOutput {
  readonly items: SalesOrder[];
  readonly total: number;
  readonly page: number;
  readonly pageSize: number;
}

export class ListSalesOrdersUseCase {
  constructor(private readonly orders: ISalesOrderRepository) {}

  async execute(input: ListSalesOrdersInput): Promise<ListSalesOrdersOutput> {
    const page = input.page && input.page > 0 ? input.page : 1;
    const pageSize = resolvePageSize(input.limit);
    const { page: _page, limit: _limit, ...filter } = input;
    const { items, total } = await this.orders.findAll(filter, { page, pageSize });
    return { items, total, page, pageSize };
  }
}

export class ChangeSalesOrderStatusUseCase {
  constructor(private readonly orders: ISalesOrderRepository, private readonly clock: Clock) {}

  async execute(id: string, status: OrderStatus, context: PersistenceContext): Promise<SalesOrder> {
    const order = await loadOrder(this.orders, id);
    order.changeStatus(status, this.clock());
    await this.orders.save(order, context);
    return order;
  }
}

export class ScheduleDeliveryUseCase {
  constructor(private readonly orders: ISalesOrderRepository, private readonly clock: Clock) {}

  async execute(id: string, window: DeliveryWindow, context: PersistenceContext): Promise<SalesOrder> {
    const order = await loadOrder(this.orders, id);
    order.schedule(window, this.clock());
    await this.orders.save(order, context);
    return order;
  }
}

export class RescheduleDeliveryUseCase {
  constructor(private readonly orders: ISalesOrderRepository, private readonly clock: Clock) {}

  async execute(id: string, window: DeliveryWindow, context: PersistenceContext): Promise<SalesOrder> {
    const order = await loadOrder(this.orders, id);
    order.reschedule(window, this.clock());
    await this.orders.save(order, context);
    return order;
  }
}

export class ChangeSalesOrderTransportUseCase {
  constructor(
    private readonly orders: ISalesOrderRepository,
    private readonly replicas: IReplicaReader,
    private readonly clock: Clock,
  ) {}

  async execute(id: string, transportTypeId: string, context: PersistenceContext): Promise<SalesOrder> {
    const order = await loadOrder(this.orders, id);
    if (transportTypeId === order.transportTypeId) {
      // Mesmo transporte: no-op, mas o bloqueio por status continua valendo.
      order.changeTransport(transportTypeId, this.clock());
      return order;
    }
    const customer = await this.replicas.findCustomer(order.customerId);
    if (!customer) throw new UnknownCustomerError(order.customerId);
    await assertTransportAllowed(this.replicas, customer, transportTypeId);

    if (order.changeTransport(transportTypeId, this.clock())) await this.orders.save(order, context);
    return order;
  }
}
