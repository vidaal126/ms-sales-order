import type { SalesOrder } from "@domain/entities/sales-order.entity";
import {
  SalesOrderConcurrentModificationError,
  SalesOrderNotFoundError,
  TransportNotAuthorizedError,
  UnknownCustomerError,
  UnknownItemsError,
} from "@domain/errors/sales-order.errors";
import {
  BIKE_ID,
  CUSTOMER_ID,
  INACTIVE_ID,
  InMemoryReplicaReader,
  InMemorySalesOrderRepository,
  ITEM_A,
  ITEM_B,
  TRUCK_ID,
} from "../../test/sales-order.fakes";
import {
  ChangeSalesOrderStatusUseCase,
  ChangeSalesOrderTransportUseCase,
  CreateSalesOrderUseCase,
  GetSalesOrderUseCase,
  ListSalesOrdersUseCase,
  RescheduleDeliveryUseCase,
  ScheduleDeliveryUseCase,
} from "./sales-order.use-cases";

const NOW = new Date("2026-10-01T12:00:00.000Z");
const clock = (): Date => NOW;
const ctx = { correlationId: "corr-1" };
const WINDOW = {
  deliveryDate: new Date("2026-10-05T00:00:00.000Z"),
  windowStart: new Date("2026-10-05T09:00:00.000Z"),
  windowEnd: new Date("2026-10-05T12:00:00.000Z"),
};

describe("casos de uso de ordem de venda", () => {
  let orders: InMemorySalesOrderRepository;
  let replicas: InMemoryReplicaReader;
  let create: CreateSalesOrderUseCase;

  beforeEach(() => {
    orders = new InMemorySalesOrderRepository();
    replicas = new InMemoryReplicaReader();
    create = new CreateSalesOrderUseCase(orders, replicas, clock);
  });

  const createDefault = (): Promise<SalesOrder> =>
    create.execute({ customerId: CUSTOMER_ID, transportTypeId: TRUCK_ID, items: [{ itemId: ITEM_A, quantity: 3 }, { itemId: ITEM_B, quantity: 2 }] }, ctx);

  it("cria com preco congelado da replica, total e evento com correlationId", async () => {
    const order = await createDefault();

    expect(order.lines).toEqual([
      { itemId: ITEM_A, quantity: 3, unitPrice: 0.1 },
      { itemId: ITEM_B, quantity: 2, unitPrice: 19.9 },
    ]);
    expect(order.total).toBe(40.1);
    expect(orders.outbox).toHaveLength(1);
    expect(orders.outbox[0]?.event.eventType).toBe("OrderCreated");
    expect(orders.outbox[0]?.context).toEqual(ctx);
  });

  it("preco da replica mudar depois nao altera o pedido", async () => {
    const order = await createDefault();
    replicas.items.set(ITEM_A, { itemId: ITEM_A, sku: "A", name: "Item A", unitPrice: 999 });

    const reloaded = await new GetSalesOrderUseCase(orders).execute(order.id);
    expect(reloaded.lines[0]?.unitPrice).toBe(0.1);
  });

  it("cliente desconhecido: UnknownCustomerError", async () => {
    await expect(
      create.execute({ customerId: "99999999-9999-4999-8999-999999999999", transportTypeId: TRUCK_ID, items: [{ itemId: ITEM_A, quantity: 1 }] }, ctx),
    ).rejects.toBeInstanceOf(UnknownCustomerError);
  });

  it("itens desconhecidos: lista todos os faltantes", async () => {
    const missing = ["cccccccc-cccc-4ccc-8ccc-cccccccccccc", "dddddddd-dddd-4ddd-8ddd-dddddddddddd"];
    await expect(
      create.execute({ customerId: CUSTOMER_ID, transportTypeId: TRUCK_ID, items: [{ itemId: ITEM_A, quantity: 1 }, ...missing.map((itemId) => ({ itemId, quantity: 1 }))] }, ctx),
    ).rejects.toEqual(new UnknownItemsError(missing));
  });

  it.each([
    ["desconhecido", "99999999-9999-4999-8999-999999999999"],
    ["inativo", INACTIVE_ID],
  ])("transporte %s: TransportNotAuthorizedError", async (_label, transportTypeId) => {
    await expect(
      create.execute({ customerId: CUSTOMER_ID, transportTypeId, items: [{ itemId: ITEM_A, quantity: 1 }] }, ctx),
    ).rejects.toBeInstanceOf(TransportNotAuthorizedError);
  });

  it("transporte nao autorizado para o cliente: TransportNotAuthorizedError", async () => {
    replicas.customers.set(CUSTOMER_ID, { customerId: CUSTOMER_ID, name: "Cliente", authorizedTransportTypeIds: [BIKE_ID] });
    await expect(createDefault()).rejects.toBeInstanceOf(TransportNotAuthorizedError);
  });

  it("fluxo completo: planejar, agendar, reagendar, transporte, entrega", async () => {
    const { id } = await createDefault();
    const status = new ChangeSalesOrderStatusUseCase(orders, clock);

    await status.execute(id, "PLANEJADA", ctx);
    await new ScheduleDeliveryUseCase(orders, clock).execute(id, WINDOW, ctx);
    await new RescheduleDeliveryUseCase(orders, clock).execute(id, { ...WINDOW }, ctx);
    await status.execute(id, "EM_TRANSPORTE", ctx);
    const delivered = await status.execute(id, "ENTREGUE", ctx);

    expect(delivered.status).toBe("ENTREGUE");
    expect(orders.outbox.map((entry) => entry.event.eventType)).toEqual([
      "OrderCreated", "OrderStatusChanged", "DeliveryScheduled", "OrderStatusChanged",
      "DeliveryRescheduled", "OrderStatusChanged", "OrderStatusChanged",
    ]);
  });

  it("pedido inexistente: SalesOrderNotFoundError", async () => {
    await expect(new ChangeSalesOrderStatusUseCase(orders, clock).execute("x", "PLANEJADA", ctx)).rejects.toBeInstanceOf(SalesOrderNotFoundError);
  });

  it("gravacao concorrente com versao antiga: SalesOrderConcurrentModificationError", async () => {
    const { id } = await createDefault();
    const stale = await orders.findById(id);
    await new ChangeSalesOrderStatusUseCase(orders, clock).execute(id, "PLANEJADA", ctx);

    if (!stale) throw new Error("pedido nao encontrado");
    stale.changeTransport(BIKE_ID, NOW);
    await expect(orders.save(stale, ctx)).rejects.toBeInstanceOf(SalesOrderConcurrentModificationError);
  });

  it("troca de transporte: autorizado grava; mesmo transporte nao grava; inativo recusa", async () => {
    const { id } = await createDefault();
    const change = new ChangeSalesOrderTransportUseCase(orders, replicas, clock);

    await change.execute(id, TRUCK_ID, ctx);
    expect(orders.saves).toBe(0);
    const changed = await change.execute(id, BIKE_ID, ctx);
    expect(changed.transportTypeId).toBe(BIKE_ID);
    expect(orders.saves).toBe(1);
    await expect(change.execute(id, INACTIVE_ID, ctx)).rejects.toBeInstanceOf(TransportNotAuthorizedError);
  });

  it("listar aplica pagina padrao e limite maximo", async () => {
    await createDefault();
    expect(await new ListSalesOrdersUseCase(orders).execute({ limit: 500 })).toMatchObject({ page: 1, pageSize: 100, total: 1 });
  });
});
