import { SalesOrder } from "@domain/entities/sales-order.entity";
import { SalesOrderConcurrentModificationError } from "@domain/errors/sales-order.errors";
import type { SalesOrderEvent } from "@domain/events/sales-order.events";
import type {
  CustomerReplica,
  IReplicaReader,
  ItemReplica,
  TransportTypeReplica,
} from "@application/ports/replica.ports";
import type {
  ISalesOrderRepository,
  Page,
  PageRequest,
  PersistenceContext,
  SalesOrderFilter,
} from "@application/ports/sales-order.repository.port";

export const CUSTOMER_ID = "11111111-1111-4111-8111-111111111111";
export const TRUCK_ID = "22222222-2222-4222-8222-222222222222";
export const BIKE_ID = "33333333-3333-4333-8333-333333333333";
export const INACTIVE_ID = "44444444-4444-4444-8444-444444444444";
export const ITEM_A = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
export const ITEM_B = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";

export class InMemoryReplicaReader implements IReplicaReader {
  readonly items = new Map<string, ItemReplica>([
    [ITEM_A, { itemId: ITEM_A, sku: "A", name: "Item A", unitPrice: 0.1 }],
    [ITEM_B, { itemId: ITEM_B, sku: "B", name: "Item B", unitPrice: 19.9 }],
  ]);
  readonly customers = new Map<string, CustomerReplica>([
    [CUSTOMER_ID, { customerId: CUSTOMER_ID, name: "Cliente", authorizedTransportTypeIds: [TRUCK_ID, BIKE_ID, INACTIVE_ID] }],
  ]);
  readonly transportTypes = new Map<string, TransportTypeReplica>([
    [TRUCK_ID, { transportTypeId: TRUCK_ID, name: "Caminhao", active: true }],
    [BIKE_ID, { transportTypeId: BIKE_ID, name: "Moto", active: true }],
    [INACTIVE_ID, { transportTypeId: INACTIVE_ID, name: "Carroca", active: false }],
  ]);

  async findItems(itemIds: readonly string[]): Promise<ItemReplica[]> {
    return itemIds.flatMap((id) => {
      const item = this.items.get(id);
      return item ? [item] : [];
    });
  }

  async findCustomer(customerId: string): Promise<CustomerReplica | undefined> {
    return this.customers.get(customerId);
  }

  async findTransportType(transportTypeId: string): Promise<TransportTypeReplica | undefined> {
    return this.transportTypes.get(transportTypeId);
  }
}

// Guarda uma copia reidratada (como o banco), com versao e eventos "no outbox".
export class InMemorySalesOrderRepository implements ISalesOrderRepository {
  readonly rows = new Map<string, SalesOrder>();
  readonly outbox: Array<{ event: SalesOrderEvent; context: PersistenceContext }> = [];
  saves = 0;

  async findById(id: string): Promise<SalesOrder | undefined> {
    return this.rows.get(id);
  }

  async findAll(_filter: SalesOrderFilter, page: PageRequest): Promise<Page<SalesOrder>> {
    const all = [...this.rows.values()];
    return { items: all.slice((page.page - 1) * page.pageSize, page.page * page.pageSize), total: all.length };
  }

  async create(order: SalesOrder, context: PersistenceContext): Promise<void> {
    this.pushEvents(order, context);
    this.rows.set(order.id, this.snapshot(order, order.version));
  }

  async save(order: SalesOrder, context: PersistenceContext): Promise<void> {
    const stored = this.rows.get(order.id);
    if (stored?.version !== order.version) throw new SalesOrderConcurrentModificationError(order.id);
    this.saves += 1;
    this.pushEvents(order, context);
    this.rows.set(order.id, this.snapshot(order, order.version + 1));
  }

  private pushEvents(order: SalesOrder, context: PersistenceContext): void {
    for (const event of order.pullDomainEvents()) this.outbox.push({ event, context });
  }

  private snapshot(order: SalesOrder, version: number): SalesOrder {
    return SalesOrder.restore({
      id: order.id,
      customerId: order.customerId,
      transportTypeId: order.transportTypeId,
      status: order.status,
      notes: order.notes,
      lines: order.lines,
      schedule: order.deliverySchedule,
      version,
      createdAt: order.createdAt,
      updatedAt: order.updatedAt,
    });
  }
}
