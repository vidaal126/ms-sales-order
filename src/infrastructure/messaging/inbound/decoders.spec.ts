import { decodeCustomerEvent } from "./customer.decoder";
import { decodeItemCreated, legacyEventId } from "./item-created.decoder";
import { decodeTransportTypeEvent } from "./transport-type.decoder";

const ID = "b8a91f43-8755-4815-bd61-bb3b15760af0";
const EVENT_ID = "9af6023e-9e78-4b77-afcd-cb61c1e5c068";
const T = "11111111-1111-4111-8111-111111111111";
const position = { topic: "catalog.ItemCreated", partition: 0, offset: "3" };
const buf = (value: unknown): Buffer => Buffer.from(typeof value === "string" ? value : JSON.stringify(value));

function envelope(eventType: string, payload: Record<string, unknown>, overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    eventId: EVENT_ID,
    eventType,
    schemaVersion: 2,
    occurredAt: "2026-09-22T23:50:58.000Z",
    aggregateId: ID,
    correlationId: "corr-1",
    payload,
    ...overrides,
  };
}

const itemPayload = { id: ID, sku: "BOX-001", name: "Caixa", unitPrice: 24.9, weightKg: 0.75, dimensions: { lengthCm: 1, widthCm: 1, heightCm: 1 } };

describe("decodeItemCreated", () => {
  it("envelope v2", () => {
    expect(decodeItemCreated(buf(envelope("ItemCreated", itemPayload)), position)).toEqual({
      ok: true,
      correlationId: "corr-1",
      event: {
        eventId: EVENT_ID,
        eventType: "ItemCreated",
        occurredAt: new Date("2026-09-22T23:50:58.000Z"),
        item: { itemId: ID, sku: "BOX-001", name: "Caixa", unitPrice: 24.9 },
      },
    });
  });

  it("v1 legado com schemaVersion no payload: eventId UUID v5 deterministico da posicao", () => {
    const legacy = { eventType: "ItemCreated", aggregateId: ID, occurredAt: "2026-09-22T23:50:58.000Z", payload: { schemaVersion: 1, ...itemPayload } };
    const decoded = decodeItemCreated(buf(legacy), position);

    expect(decoded).toMatchObject({ ok: true, event: { eventId: legacyEventId(position), item: { unitPrice: 24.9 } } });
    expect(legacyEventId(position)).toBe(legacyEventId({ ...position }));
    expect(legacyEventId(position)).not.toBe(legacyEventId({ ...position, offset: "4" }));
  });

  it.each([
    ["texto nao JSON", "t", "invalid_json"],
    ["vazio", "", "invalid_json"],
    ["legado sem schemaVersion", { eventType: "ItemCreated", aggregateId: ID, occurredAt: "2026-09-22T23:50:58.000Z", payload: { id: ID, sku: "L", name: "L", unitPrice: "19.90" } }, "unsupported_schema_version"],
    ["envelope v3", envelope("ItemCreated", itemPayload, { schemaVersion: 3 }), "unsupported_schema_version"],
    ["unitPrice string", envelope("ItemCreated", { ...itemPayload, unitPrice: "24.90" }), "schema_validation_failed"],
    ["aggregateId diferente de payload.id", envelope("ItemCreated", { ...itemPayload, id: T }), "schema_validation_failed"],
    ["eventType inesperado", envelope("ItemDeleted", itemPayload), "schema_validation_failed"],
  ])("%s -> %s", (_label, raw, reason) => {
    expect(decodeItemCreated(buf(raw), position)).toMatchObject({ ok: false, reason });
  });
});

describe("decodeCustomerEvent", () => {
  const payload = { id: ID, name: "Ana", document: "529.982.247-25", authorizedTransportTypeIds: [T] };

  it.each(["CustomerCreated", "CustomerUpdated"])("%s v2", (eventType) => {
    expect(decodeCustomerEvent(buf(envelope(eventType, payload)))).toMatchObject({
      ok: true,
      event: { eventType, customer: { customerId: ID, name: "Ana", authorizedTransportTypeIds: [T] } },
    });
  });

  it.each([
    ["id de transporte nao uuid", { ...payload, authorizedTransportTypeIds: ["x"] }],
    ["sem lista de transportes", { id: ID, name: "Ana", document: "d" }],
  ])("%s -> schema_validation_failed", (_label, bad) => {
    expect(decodeCustomerEvent(buf(envelope("CustomerCreated", bad)))).toMatchObject({ ok: false, reason: "schema_validation_failed" });
  });
});

describe("decodeTransportTypeEvent", () => {
  const payload = { id: ID, name: "Caminhao", description: null, active: false };

  it("TransportTypeUpdated v2", () => {
    expect(decodeTransportTypeEvent(buf(envelope("TransportTypeUpdated", payload)))).toMatchObject({
      ok: true,
      event: { transportType: { transportTypeId: ID, name: "Caminhao", active: false } },
    });
  });

  it("active ausente -> schema_validation_failed; JSON invalido -> invalid_json", () => {
    expect(decodeTransportTypeEvent(buf(envelope("TransportTypeCreated", { id: ID, name: "x", description: null })))).toMatchObject({ ok: false, reason: "schema_validation_failed" });
    expect(decodeTransportTypeEvent(buf("{"))).toMatchObject({ ok: false, reason: "invalid_json" });
  });
});
