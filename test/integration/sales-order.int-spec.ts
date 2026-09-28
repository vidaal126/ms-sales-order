import { execFileSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import type { INestApplication } from "@nestjs/common";
import { KafkaContainer, type StartedKafkaContainer } from "@testcontainers/kafka";
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import { z } from "zod";
import type { PrismaService } from "@infrastructure/database/prisma/prisma.service";
import { sampleValue } from "../../src/test/metrics.helpers";
import { KafkaTestClient, type ProducedMessage, waitFor } from "./kafka-test-client";

const CONSUMED_TOPICS = [
  "catalog.ItemCreated",
  "customer.CustomerCreated",
  "customer.CustomerUpdated",
  "transport.TransportTypeCreated",
  "transport.TransportTypeUpdated",
];
const PUBLISHED_TOPICS = ["OrderCreated", "OrderStatusChanged", "DeliveryScheduled", "DeliveryRescheduled", "TransportChanged"].map(
  (type) => `sales-order.${type}`,
);
const dlt = (topic: string): string => `${topic}.ms-sales-order.DLT`;

const CUSTOMER = randomUUID();
const TRUCK = randomUUID();
const BIKE = randomUUID();
const REORDERED = randomUUID();
const UNAUTHORIZED = randomUUID();
const ITEM_A = randomUUID();
const ITEM_B = randomUUID();
const LEGACY_ITEM = randomUUID();

function envelope(eventType: string, aggregateId: string, payload: Record<string, unknown>, occurredAt: string): ProducedMessage {
  return {
    key: aggregateId,
    value: JSON.stringify({ eventId: randomUUID(), eventType, schemaVersion: 2, occurredAt, aggregateId, correlationId: `seed-${eventType}`, payload }),
    headers: { eventType, schemaVersion: "2" },
  };
}

const transport = (type: "Created" | "Updated", id: string, active: boolean, occurredAt: string): ProducedMessage =>
  envelope(`TransportType${type}`, id, { id, name: `T-${id.slice(0, 4)}`, description: null, active }, occurredAt);

const item = (id: string, sku: string, unitPrice: number): ProducedMessage =>
  envelope("ItemCreated", id, { id, sku, name: sku, unitPrice, weightKg: 1, dimensions: { lengthCm: 1, widthCm: 1, heightCm: 1 } }, "2026-09-01T00:00:00.000Z");

const orderSchema = z.object({
  id: z.uuid(),
  status: z.string(),
  transportTypeId: z.uuid(),
  total: z.number(),
  items: z.array(z.object({ itemId: z.uuid(), quantity: z.number(), unitPrice: z.number(), lineTotal: z.number() })),
  scheduling: z.object({ deliveryDate: z.string(), rescheduledAt: z.string().nullable() }).nullable(),
});

// Dia UTC daqui a `dayOffset` dias: janelas sempre no futuro.
function futureDay(dayOffset = 1): { deliveryDate: string } {
  return { deliveryDate: new Date(Date.now() + dayOffset * 24 * 3600 * 1000).toISOString().slice(0, 10) };
}

describe("ms-sales-order: replicas, pedidos e eventos (integracao)", () => {
  let postgres: StartedPostgreSqlContainer;
  let kafkaContainer: StartedKafkaContainer;
  let kafka: KafkaTestClient;
  let app: INestApplication;
  let prisma: PrismaService;
  let baseUrl: string;

  const call = async (method: string, path: string, body?: unknown, headers: Record<string, string> = {}): Promise<{ status: number; body: unknown; headers: Headers }> => {
    const response = await fetch(`${baseUrl}${path}`, {
      method,
      headers: { "content-type": "application/json", ...headers },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    const text = await response.text();
    return { status: response.status, body: text ? JSON.parse(text) : null, headers: response.headers };
  };

  beforeAll(async () => {
    [postgres, kafkaContainer] = await Promise.all([
      new PostgreSqlContainer("postgres:16-alpine").start(),
      new KafkaContainer("confluentinc/cp-kafka:7.6.1").withKraft().withEnvironment({ KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false" }).start(),
    ]);
    const broker = `${kafkaContainer.getHost()}:${kafkaContainer.getMappedPort(9093)}`;
    kafka = new KafkaTestClient(broker);
    await kafka.createTopics([...CONSUMED_TOPICS, ...CONSUMED_TOPICS.map(dlt), ...PUBLISHED_TOPICS]);

    const databaseUrl = postgres.getConnectionUri();
    execFileSync("npx", ["prisma", "migrate", "deploy"], { env: { ...process.env, DATABASE_URL: databaseUrl }, stdio: "pipe" });

    // Historico antes da subida: os groups novos (fromBeginning) constroem as
    // replicas. REORDERED: Updated (mais novo, inativo) chega antes do Created.
    await kafka.produce("transport.TransportTypeCreated", [
      transport("Created", TRUCK, true, "2026-09-01T00:00:00.000Z"),
      transport("Created", BIKE, true, "2026-09-01T00:00:00.000Z"),
      transport("Created", UNAUTHORIZED, true, "2026-09-01T00:00:00.000Z"),
      { key: "poison", value: "t" },
    ]);
    await kafka.produce("transport.TransportTypeUpdated", [transport("Updated", REORDERED, false, "2026-09-02T00:00:00.000Z"), { key: "poison", value: "t" }]);
    await kafka.produce("transport.TransportTypeCreated", [transport("Created", REORDERED, true, "2026-09-01T00:00:00.000Z")]);
    const customerPayload = { id: CUSTOMER, name: "Cliente", document: "529.982.247-25" };
    await kafka.produce("customer.CustomerCreated", [
      envelope("CustomerCreated", CUSTOMER, { ...customerPayload, authorizedTransportTypeIds: [TRUCK] }, "2026-09-01T00:00:00.000Z"),
      { key: "poison", value: "t" },
    ]);
    await kafka.produce("customer.CustomerUpdated", [
      envelope("CustomerUpdated", CUSTOMER, { ...customerPayload, authorizedTransportTypeIds: [TRUCK, BIKE, REORDERED] }, "2026-09-03T00:00:00.000Z"),
      { key: "poison", value: "t" },
    ]);
    await kafka.produce("catalog.ItemCreated", [
      item(ITEM_A, "A", 0.1),
      item(ITEM_B, "B", 19.9),
      {
        key: LEGACY_ITEM,
        value: JSON.stringify({ eventType: "ItemCreated", aggregateId: LEGACY_ITEM, occurredAt: "2026-09-01T00:00:00.000Z", payload: { schemaVersion: 1, id: LEGACY_ITEM, sku: "LEG", name: "Legado v1", unitPrice: 5 } }),
      },
      item(randomUUID(), "ZERO", 0),
      { key: "poison", value: "t" },
    ]);

    Object.assign(process.env, {
      NODE_ENV: "production",
      LOG_LEVEL: "error",
      DATABASE_URL: databaseUrl,
      KAFKA_BROKER: broker,
      OUTBOX_POLL_INTERVAL_MS: "200",
      CONSUMER_RETRY_RETRIES: "2",
      CONSUMER_RETRY_INITIAL_MS: "100",
    });
    const { NestFactory } = await import("@nestjs/core");
    const { AppModule } = await import("../../src/app.module");
    const { configureApp } = await import("../../src/app.setup");
    const { PrismaService: PrismaToken } = await import("@infrastructure/database/prisma/prisma.service");
    app = await NestFactory.create(AppModule, { logger: false, abortOnError: false });
    configureApp(app);
    await app.listen(0);
    baseUrl = (await app.getUrl()).replace("[::1]", "localhost");
    prisma = app.get(PrismaToken);
  });

  afterAll(async () => {
    await app?.close();
    await Promise.all([postgres?.stop(), kafkaContainer?.stop()]);
  });

  it("replicas construidas a partir do historico; guard de ordem entre Created e Updated", async () => {
    await waitFor("replicas", async () =>
      (await prisma.itemReplica.count()) === 3 &&
      (await prisma.transportTypeReplica.count()) === 4 &&
      (await prisma.customerReplica.findUnique({ where: { customerId: CUSTOMER } }))?.authorizedTransportTypeIds.length === 3,
    );
    const reordered = await prisma.transportTypeReplica.findUnique({ where: { transportTypeId: REORDERED } });
    expect(reordered?.active).toBe(false);
    const legacy = await prisma.itemReplica.findUnique({ where: { itemId: LEGACY_ITEM } });
    expect(legacy?.unitPrice.toNumber()).toBe(5);
  });

  it("poison message e preco zero vao para a DLT do servico com o group no header", async () => {
    const itemDlt = await kafka.readFromBeginning(dlt("catalog.ItemCreated"), 2, 30_000);
    expect(itemDlt.map((m) => m.headers["dlt-reason"]).sort()).toEqual(["domain_invariant_violation", "invalid_json"]);
    expect(itemDlt[0]?.headers["dlt-consumer-group"]).toBe("ms-sales-order.catalog-item-sync");
    for (const topic of CONSUMED_TOPICS.filter((t) => t !== "catalog.ItemCreated")) {
      const [message] = await kafka.readFromBeginning(dlt(topic), 1, 30_000);
      expect(message?.value).toBe("t");
      expect(message?.headers["dlt-source-topic"]).toBe(topic);
    }
  });

  let orderId = "";

  it("POST cria o pedido: preco congelado, total em centavos e OrderCreated com envelope v2", async () => {
    const created = await call(
      "POST",
      "/sales-orders",
      { customerId: CUSTOMER, transportTypeId: TRUCK, notes: "Portao 2", items: [{ itemId: ITEM_A, quantity: 3 }, { itemId: ITEM_B, quantity: 2 }] },
      { "x-correlation-id": "so-corr-1" },
    );

    expect(created.status).toBe(201);
    const order = orderSchema.parse(created.body);
    orderId = order.id;
    expect(order).toMatchObject({ status: "CRIADA", total: 40.1 });
    expect(order.items.find((i) => i.itemId === ITEM_A)).toMatchObject({ unitPrice: 0.1, lineTotal: 0.3 });

    const [message] = await kafka.readFromBeginning("sales-order.OrderCreated", 1, 30_000);
    expect(message?.key).toBe(orderId);
    expect(message?.headers).toEqual({ eventType: "OrderCreated", schemaVersion: "2", correlationId: "so-corr-1" });
    expect(JSON.parse(message?.value ?? "")).toMatchObject({ aggregateId: orderId, schemaVersion: 2, payload: { id: orderId, total: 40.1, status: "CRIADA" } });
  });

  it.each([
    ["item desconhecido", () => ({ customerId: CUSTOMER, transportTypeId: TRUCK, items: [{ itemId: randomUUID(), quantity: 1 }] }), "UnknownItemsError"],
    ["cliente desconhecido", () => ({ customerId: randomUUID(), transportTypeId: TRUCK, items: [{ itemId: ITEM_A, quantity: 1 }] }), "UnknownCustomerError"],
    ["transporte nao autorizado", () => ({ customerId: CUSTOMER, transportTypeId: UNAUTHORIZED, items: [{ itemId: ITEM_A, quantity: 1 }] }), "TransportNotAuthorizedError"],
    ["transporte inativo", () => ({ customerId: CUSTOMER, transportTypeId: REORDERED, items: [{ itemId: ITEM_A, quantity: 1 }] }), "TransportNotAuthorizedError"],
  ])("%s: 422", async (_label, body, error) => {
    const response = await call("POST", "/sales-orders", body());
    expect(response.status).toBe(422);
    expect(response.body).toMatchObject({ error });
  });

  it("corpo invalido (quantidade 0, item repetido no DTO aninhado): 400/422", async () => {
    const zero = await call("POST", "/sales-orders", { customerId: CUSTOMER, transportTypeId: TRUCK, items: [{ itemId: ITEM_A, quantity: 0 }] });
    const duplicated = await call("POST", "/sales-orders", { customerId: CUSTOMER, transportTypeId: TRUCK, items: [{ itemId: ITEM_A, quantity: 1 }, { itemId: ITEM_A, quantity: 2 }] });
    expect(zero.status).toBe(400);
    expect(duplicated.status).toBe(422);
  });

  it("ciclo: PLANEJADA, AGENDADA so via schedule, reagendar, EM_TRANSPORTE, ENTREGUE", async () => {
    const day = futureDay();
    const window = { deliveryDate: day.deliveryDate, windowStart: `${day.deliveryDate}T09:00:00.000Z`, windowEnd: `${day.deliveryDate}T12:00:00.000Z` };
    const later = futureDay(2);

    expect((await call("PUT", `/sales-orders/${orderId}/status`, { status: "PLANEJADA" })).status).toBe(200);
    const viaPut = await call("PUT", `/sales-orders/${orderId}/status`, { status: "AGENDADA" });
    expect(viaPut.status).toBe(422);
    const scheduled = await call("POST", `/sales-orders/${orderId}/schedule`, window);
    expect(scheduled.status).toBe(201);
    expect(orderSchema.parse(scheduled.body)).toMatchObject({ status: "AGENDADA", scheduling: { deliveryDate: day.deliveryDate, rescheduledAt: null } });
    const rescheduled = await call("PUT", `/sales-orders/${orderId}/schedule`, {
      deliveryDate: later.deliveryDate, windowStart: `${later.deliveryDate}T14:00:00.000Z`, windowEnd: `${later.deliveryDate}T16:00:00.000Z`,
    });
    expect(orderSchema.parse(rescheduled.body).scheduling?.deliveryDate).toBe(later.deliveryDate);
    expect((await call("PUT", `/sales-orders/${orderId}/transport`, { transportTypeId: BIKE })).status).toBe(200);
    expect((await call("PUT", `/sales-orders/${orderId}/status`, { status: "EM_TRANSPORTE" })).status).toBe(200);
    expect((await call("PUT", `/sales-orders/${orderId}/transport`, { transportTypeId: TRUCK })).status).toBe(422);
    const delivered = await call("PUT", `/sales-orders/${orderId}/status`, { status: "ENTREGUE" });
    expect(orderSchema.parse(delivered.body)).toMatchObject({ status: "ENTREGUE", transportTypeId: BIKE });

    const statusEvents = await kafka.readFromBeginning("sales-order.OrderStatusChanged", 4, 30_000);
    expect(statusEvents.map((m) => z.object({ payload: z.object({ currentStatus: z.string() }) }).parse(JSON.parse(m.value)).payload.currentStatus)).toEqual([
      "PLANEJADA", "AGENDADA", "EM_TRANSPORTE", "ENTREGUE",
    ]);
    expect(await kafka.readFromBeginning("sales-order.DeliveryScheduled", 1, 10_000)).toHaveLength(1);
    expect(await kafka.readFromBeginning("sales-order.DeliveryRescheduled", 1, 10_000)).toHaveLength(1);
    expect(await kafka.readFromBeginning("sales-order.TransportChanged", 1, 10_000)).toHaveLength(1);
  });

  it("janela invalida (fim antes do inicio): 422", async () => {
    const created = orderSchema.parse((await call("POST", "/sales-orders", { customerId: CUSTOMER, transportTypeId: TRUCK, items: [{ itemId: ITEM_A, quantity: 1 }] })).body);
    await call("PUT", `/sales-orders/${created.id}/status`, { status: "PLANEJADA" });
    const day = futureDay().deliveryDate;
    const response = await call("POST", `/sales-orders/${created.id}/schedule`, { deliveryDate: day, windowStart: `${day}T12:00:00.000Z`, windowEnd: `${day}T09:00:00.000Z` });
    expect(response.status).toBe(422);
    expect(response.body).toMatchObject({ error: "InvalidDeliveryWindowError" });
  });

  it("Idempotency-Key: mesma chave e corpo devolvem a resposta original sem criar outro pedido", async () => {
    const body = { customerId: CUSTOMER, transportTypeId: TRUCK, items: [{ itemId: ITEM_B, quantity: 7 }] };
    const headers = { "idempotency-key": "so-key-1" };
    const before = await prisma.salesOrder.count();

    const first = await call("POST", "/sales-orders", body, headers);
    const replay = await call("POST", "/sales-orders", body, headers);
    const mismatch = await call("POST", "/sales-orders", { ...body, items: [{ itemId: ITEM_B, quantity: 8 }] }, headers);

    expect(first.status).toBe(201);
    expect(replay.status).toBe(201);
    expect(replay.headers.get("idempotent-replayed")).toBe("true");
    expect(replay.body).toEqual(first.body);
    expect(mismatch.status).toBe(422);
    expect(await prisma.salesOrder.count()).toBe(before + 1);
  });

  it("GET por id, 404, listagem com filtros e total, dateTo < dateFrom = 400", async () => {
    expect((await call("GET", `/sales-orders/${orderId}`)).status).toBe(200);
    expect((await call("GET", `/sales-orders/${randomUUID()}`)).status).toBe(404);
    const delivered = await call("GET", `/sales-orders?status=ENTREGUE&customerId=${CUSTOMER}&itemId=${ITEM_A}&limit=10`);
    expect(delivered.body).toMatchObject({ total: 1, page: 1, pageSize: 10 });
    const all = await call("GET", `/sales-orders?customerId=${CUSTOMER}`);
    expect(all.body).toMatchObject({ total: 3 });
    expect((await call("GET", "/sales-orders?dateFrom=2026-10-02T00:00:00Z&dateTo=2026-10-01T00:00:00Z")).status).toBe(400);
  });

  it("GET /metrics: HTTP por template, consumo por outcome e outbox", async () => {
    await waitFor("outbox drenado", async () => (await prisma.outboxEvent.count({ where: { publishedAt: null } })) === 0);
    const text = await (await fetch(`${baseUrl}/metrics`)).text();
    expect(sampleValue(text, "http_request_duration_seconds_count", { method: "POST", route: "/sales-orders", status_code: "201" })).toBeGreaterThanOrEqual(1);
    expect(sampleValue(text, "kafka_messages_consumed_total", { topic: "catalog.ItemCreated", outcome: "dead_letter" })).toBe(2);
    expect(sampleValue(text, "outbox_pending_events", {})).toBe(0);
    expect(sampleValue(text, "outbox_events_published_total", { event_type: "OrderCreated" })).toBe(3);
  });

  it("health ready com os tres consumers rodando", async () => {
    expect((await call("GET", "/health/ready")).status).toBe(200);
  });
});
