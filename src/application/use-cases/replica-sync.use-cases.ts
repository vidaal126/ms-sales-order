import { InvalidReplicaEventError } from "@domain/errors/sales-order.errors";
import { isValidUnitPrice } from "@domain/value-objects/money";
import type {
  CustomerReplicaEvent,
  IReplicaRepository,
  ItemReplicaEvent,
  SyncOutcome,
  SyncReplicaPort,
  TransportTypeReplicaEvent,
} from "@application/ports/replica.ports";

export class SyncItemReplicaUseCase implements SyncReplicaPort<ItemReplicaEvent> {
  constructor(private readonly replicas: IReplicaRepository) {}

  // Preco e congelado no pedido a partir daqui: preco invalido nunca entra.
  async execute(event: ItemReplicaEvent): Promise<SyncOutcome> {
    if (!isValidUnitPrice(event.item.unitPrice)) {
      throw new InvalidReplicaEventError(
        `unitPrice ${event.item.unitPrice} invalido para o item ${event.item.itemId}`,
      );
    }
    if (event.item.sku.trim().length === 0 || event.item.name.trim().length === 0) {
      throw new InvalidReplicaEventError(`sku e name obrigatorios para o item ${event.item.itemId}`);
    }
    return this.replicas.syncItem(event);
  }
}

export class SyncCustomerReplicaUseCase implements SyncReplicaPort<CustomerReplicaEvent> {
  constructor(private readonly replicas: IReplicaRepository) {}

  async execute(event: CustomerReplicaEvent): Promise<SyncOutcome> {
    const ids = event.customer.authorizedTransportTypeIds;
    return this.replicas.syncCustomer({
      ...event,
      customer: { ...event.customer, authorizedTransportTypeIds: [...new Set(ids)].sort() },
    });
  }
}

export class SyncTransportTypeReplicaUseCase implements SyncReplicaPort<TransportTypeReplicaEvent> {
  constructor(private readonly replicas: IReplicaRepository) {}

  async execute(event: TransportTypeReplicaEvent): Promise<SyncOutcome> {
    if (event.transportType.name.trim().length === 0) {
      throw new InvalidReplicaEventError(
        `name obrigatorio para o tipo de transporte ${event.transportType.transportTypeId}`,
      );
    }
    return this.replicas.syncTransportType(event);
  }
}
