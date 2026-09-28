import {
  InvalidDeliveryWindowError,
  InvalidSalesOrderError,
  InvalidStatusTransitionError,
} from "@domain/errors/sales-order.errors";
import { type DeliveryWindow, validateWindow } from "@domain/value-objects/delivery-window";
import { canTransition, ORDER_STATUSES, type OrderStatus } from "@domain/value-objects/order-status";
import { type OrderLine, SalesOrder } from "./sales-order.entity";

const NOW = new Date("2026-10-01T12:00:00.000Z");
const LINES: OrderLine[] = [
  { itemId: "a", quantity: 3, unitPrice: 0.1 },
  { itemId: "b", quantity: 1, unitPrice: 0.2 },
];
const WINDOW: DeliveryWindow = {
  deliveryDate: new Date("2026-10-05T00:00:00.000Z"),
  windowStart: new Date("2026-10-05T09:00:00.000Z"),
  windowEnd: new Date("2026-10-05T12:00:00.000Z"),
};

function newOrder(lines: OrderLine[] = LINES): SalesOrder {
  return SalesOrder.create({ customerId: "c", transportTypeId: "t", lines, now: NOW });
}

function orderIn(status: OrderStatus): SalesOrder {
  const order = newOrder();
  if (status === "CRIADA") return order;
  order.changeStatus("PLANEJADA", NOW);
  if (status === "PLANEJADA") return order;
  order.schedule(WINDOW, NOW);
  if (status === "AGENDADA") return order;
  order.changeStatus("EM_TRANSPORTE", NOW);
  if (status === "EM_TRANSPORTE") return order;
  order.changeStatus("ENTREGUE", NOW);
  return order;
}

describe("maquina de estados", () => {
  const valid = new Set(["CRIADA>PLANEJADA", "PLANEJADA>AGENDADA", "AGENDADA>EM_TRANSPORTE", "EM_TRANSPORTE>ENTREGUE"]);
  const pairs = ORDER_STATUSES.flatMap((from) => ORDER_STATUSES.map((to) => [from, to] as const));

  it.each(pairs)("canTransition %s -> %s", (from, to) => {
    expect(canTransition(from, to)).toBe(valid.has(`${from}>${to}`));
  });

  it.each(pairs)("changeStatus a partir de %s para %s", (from, to) => {
    const order = orderIn(from);
    const allowedViaPut = valid.has(`${from}>${to}`) && to !== "AGENDADA";
    if (allowedViaPut) {
      order.changeStatus(to, NOW);
      expect(order.status).toBe(to);
    } else {
      expect(() => { order.changeStatus(to, NOW); }).toThrow(InvalidStatusTransitionError);
      expect(order.status).toBe(from);
    }
  });

  it("AGENDADA via PUT e recusada com mensagem apontando o agendamento", () => {
    expect(() => { orderIn("PLANEJADA").changeStatus("AGENDADA", NOW); }).toThrow(/agendamento/);
  });

  it("EM_TRANSPORTE exige agendamento (pedido restaurado AGENDADA sem schedule)", () => {
    const order = orderIn("AGENDADA");
    const inconsistent = SalesOrder.restore({
      id: order.id, customerId: "c", transportTypeId: "t", status: "AGENDADA", notes: null,
      lines: LINES, schedule: null, version: 1, createdAt: NOW, updatedAt: NOW,
    });
    expect(() => { inconsistent.changeStatus("EM_TRANSPORTE", NOW); }).toThrow(InvalidStatusTransitionError);
  });
});

describe("criacao e eventos", () => {
  it("total em centavos inteiros (0.1*3 + 0.2 = 0.5, sem erro de float)", () => {
    expect(newOrder().total).toBe(0.5);
    expect(newOrder([{ itemId: "x", quantity: 10_000, unitPrice: 99_999_999.99 }]).total).toBe(999_999_999_900);
  });

  it("registra OrderCreated com linhas, total e status CRIADA", () => {
    const [event, ...rest] = newOrder().pullDomainEvents();
    expect(rest).toHaveLength(0);
    expect(event).toMatchObject({ eventType: "OrderCreated", status: "CRIADA", total: 0.5, items: LINES });
  });

  it.each([
    ["sem itens", []],
    ["item repetido", [LINES[0], LINES[0]]],
    ["quantidade zero", [{ itemId: "a", quantity: 0, unitPrice: 1 }]],
    ["quantidade acima de 10000", [{ itemId: "a", quantity: 10_001, unitPrice: 1 }]],
    ["quantidade fracionada", [{ itemId: "a", quantity: 1.5, unitPrice: 1 }]],
    ["preco com 3 casas", [{ itemId: "a", quantity: 1, unitPrice: 1.005 }]],
    ["preco zero", [{ itemId: "a", quantity: 1, unitPrice: 0 }]],
  ])("rejeita %s", (_label, lines) => {
    expect(() => newOrder(lines.filter((l): l is OrderLine => l !== undefined))).toThrow(InvalidSalesOrderError);
  });

  it("notes vazio vira null e acima de 1000 e rejeitado", () => {
    expect(SalesOrder.create({ customerId: "c", transportTypeId: "t", notes: "  ", lines: LINES, now: NOW }).notes).toBeNull();
    expect(() => SalesOrder.create({ customerId: "c", transportTypeId: "t", notes: "x".repeat(1001), lines: LINES, now: NOW })).toThrow(InvalidSalesOrderError);
  });

  it("schedule registra DeliveryScheduled e OrderStatusChanged PLANEJADA -> AGENDADA", () => {
    const order = orderIn("PLANEJADA");
    order.pullDomainEvents();

    order.schedule(WINDOW, NOW);

    expect(order.pullDomainEvents().map((e) => e.eventType)).toEqual(["DeliveryScheduled", "OrderStatusChanged"]);
    expect(order.deliverySchedule).toMatchObject({ confirmedAt: NOW, rescheduledAt: null });
  });

  it("schedule fora de PLANEJADA e recusado", () => {
    expect(() => { orderIn("CRIADA").schedule(WINDOW, NOW); }).toThrow(InvalidStatusTransitionError);
    expect(() => { orderIn("AGENDADA").schedule(WINDOW, NOW); }).toThrow(InvalidStatusTransitionError);
  });

  it("reschedule mantem status e confirmedAt, marca rescheduledAt e registra o evento", () => {
    const order = orderIn("AGENDADA");
    order.pullDomainEvents();
    const later = new Date("2026-10-02T00:00:00.000Z");

    order.reschedule({
      deliveryDate: new Date("2026-10-06T00:00:00.000Z"),
      windowStart: new Date("2026-10-06T14:00:00.000Z"),
      windowEnd: new Date("2026-10-06T16:00:00.000Z"),
    }, later);

    expect(order.status).toBe("AGENDADA");
    expect(order.deliverySchedule).toMatchObject({ confirmedAt: NOW, rescheduledAt: later });
    expect(order.pullDomainEvents()).toEqual([
      expect.objectContaining({ eventType: "DeliveryRescheduled", previousDeliveryDate: "2026-10-05", deliveryDate: "2026-10-06" }),
    ]);
  });

  it("reschedule fora de AGENDADA e recusado", () => {
    expect(() => { orderIn("PLANEJADA").reschedule(WINDOW, NOW); }).toThrow(InvalidStatusTransitionError);
    expect(() => { orderIn("EM_TRANSPORTE").reschedule(WINDOW, NOW); }).toThrow(InvalidStatusTransitionError);
  });
});

describe("troca de transporte", () => {
  it.each(["CRIADA", "PLANEJADA", "AGENDADA"] as const)("permitida em %s", (status) => {
    const order = orderIn(status);
    order.pullDomainEvents();
    expect(order.changeTransport("novo", NOW)).toBe(true);
    expect(order.transportTypeId).toBe("novo");
    expect(order.pullDomainEvents()).toEqual([
      expect.objectContaining({ eventType: "TransportChanged", previousTransportTypeId: "t", transportTypeId: "novo" }),
    ]);
  });

  it.each(["EM_TRANSPORTE", "ENTREGUE"] as const)("bloqueada em %s", (status) => {
    expect(() => orderIn(status).changeTransport("novo", NOW)).toThrow(InvalidStatusTransitionError);
  });

  it("mesmo transporte e no-op sem evento", () => {
    const order = orderIn("CRIADA");
    order.pullDomainEvents();
    expect(order.changeTransport("t", NOW)).toBe(false);
    expect(order.pullDomainEvents()).toHaveLength(0);
  });
});

describe("validateWindow", () => {
  const at = (iso: string): Date => new Date(iso);
  it.each([
    ["data invalida", { ...WINDOW, windowStart: at("x") }, /invalida/],
    ["inicio igual ao fim", { ...WINDOW, windowEnd: WINDOW.windowStart }, /anterior/],
    ["inicio depois do fim", { ...WINDOW, windowStart: at("2026-10-05T13:00:00.000Z") }, /anterior/],
    ["janela no passado", { deliveryDate: at("2026-10-01T00:00:00.000Z"), windowStart: at("2026-10-01T11:00:00.000Z"), windowEnd: at("2026-10-01T13:00:00.000Z") }, /passado/],
    ["alem de 365 dias", { deliveryDate: at("2027-10-02T00:00:00.000Z"), windowStart: at("2027-10-02T09:00:00.000Z"), windowEnd: at("2027-10-02T10:00:00.000Z") }, /horizonte/],
    ["janela em outro dia", { ...WINDOW, windowEnd: at("2026-10-06T01:00:00.000Z") }, /mesmo dia/],
  ])("rejeita %s", (_label, window, message) => {
    expect(() => validateWindow(window, NOW)).toThrow(InvalidDeliveryWindowError);
    expect(() => validateWindow(window, NOW)).toThrow(message);
  });

  it("aceita janela valida e normaliza deliveryDate para meia-noite UTC", () => {
    const result = validateWindow({ ...WINDOW, deliveryDate: at("2026-10-05T18:30:00.000Z") }, NOW);
    expect(result.deliveryDate.toISOString()).toBe("2026-10-05T00:00:00.000Z");
  });
});
