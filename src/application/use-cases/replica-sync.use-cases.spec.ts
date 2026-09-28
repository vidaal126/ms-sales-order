import { InvalidReplicaEventError } from "@domain/errors/sales-order.errors";
import type {
  CustomerReplicaEvent,
  IReplicaRepository,
  ItemReplicaEvent,
  SyncOutcome,
  TransportTypeReplicaEvent,
} from "@application/ports/replica.ports";
import { SyncCustomerReplicaUseCase, SyncItemReplicaUseCase, SyncTransportTypeReplicaUseCase } from "./replica-sync.use-cases";

class RecordingReplicas implements IReplicaRepository {
  readonly calls: unknown[] = [];
  async syncItem(event: ItemReplicaEvent): Promise<SyncOutcome> {
    this.calls.push(event);
    return "applied";
  }
  async syncCustomer(event: CustomerReplicaEvent): Promise<SyncOutcome> {
    this.calls.push(event);
    return "applied";
  }
  async syncTransportType(event: TransportTypeReplicaEvent): Promise<SyncOutcome> {
    this.calls.push(event);
    return "applied";
  }
}

const meta = { eventId: "e", eventType: "X", occurredAt: new Date(0) };

describe("sync das replicas", () => {
  it.each([0, -1, 1.005, Number.NaN, 100_000_000])("item com unitPrice %p: invariante (DLT)", async (unitPrice) => {
    const replicas = new RecordingReplicas();
    await expect(
      new SyncItemReplicaUseCase(replicas).execute({ ...meta, item: { itemId: "i", sku: "s", name: "n", unitPrice } }),
    ).rejects.toBeInstanceOf(InvalidReplicaEventError);
    expect(replicas.calls).toHaveLength(0);
  });

  it("item valido e repassado", async () => {
    const replicas = new RecordingReplicas();
    await expect(
      new SyncItemReplicaUseCase(replicas).execute({ ...meta, item: { itemId: "i", sku: "s", name: "n", unitPrice: 19.9 } }),
    ).resolves.toBe("applied");
  });

  it("cliente: deduplica e ordena os transportes autorizados", async () => {
    const replicas = new RecordingReplicas();
    await new SyncCustomerReplicaUseCase(replicas).execute({ ...meta, customer: { customerId: "c", name: "n", authorizedTransportTypeIds: ["b", "a", "b"] } });
    expect(replicas.calls[0]).toMatchObject({ customer: { authorizedTransportTypeIds: ["a", "b"] } });
  });

  it("transporte sem nome: invariante", async () => {
    await expect(
      new SyncTransportTypeReplicaUseCase(new RecordingReplicas()).execute({ ...meta, transportType: { transportTypeId: "t", name: " ", active: true } }),
    ).rejects.toBeInstanceOf(InvalidReplicaEventError);
  });
});
