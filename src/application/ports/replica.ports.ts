export const REPLICA_REPOSITORY = Symbol("REPLICA_REPOSITORY");
export const REPLICA_READER = Symbol("REPLICA_READER");
export const SYNC_ITEM_REPLICA = Symbol("SYNC_ITEM_REPLICA");
export const SYNC_CUSTOMER_REPLICA = Symbol("SYNC_CUSTOMER_REPLICA");
export const SYNC_TRANSPORT_TYPE_REPLICA = Symbol("SYNC_TRANSPORT_TYPE_REPLICA");

// applied: replica gravada/atualizada. duplicate: eventId ja processado.
// stale: evento mais antigo (ou igual) ao que gerou a versao gravada.
export type SyncOutcome = "applied" | "duplicate" | "stale";

// Metadados comuns de todo evento consumido (ja decodificado).
export interface SourceEvent {
  readonly eventId: string;
  readonly eventType: string;
  readonly occurredAt: Date;
}

export interface ItemReplica {
  readonly itemId: string;
  readonly sku: string;
  readonly name: string;
  readonly unitPrice: number;
}

export interface CustomerReplica {
  readonly customerId: string;
  readonly name: string;
  readonly authorizedTransportTypeIds: readonly string[];
}

export interface TransportTypeReplica {
  readonly transportTypeId: string;
  readonly name: string;
  readonly active: boolean;
}

export interface ItemReplicaEvent extends SourceEvent {
  readonly item: ItemReplica;
}

export interface CustomerReplicaEvent extends SourceEvent {
  readonly customer: CustomerReplica;
}

export interface TransportTypeReplicaEvent extends SourceEvent {
  readonly transportType: TransportTypeReplica;
}

// Escrita das replicas: registra o eventId em processed_events e aplica o
// upsert condicional (sourceOccurredAt) na MESMA transacao.
export interface IReplicaRepository {
  syncItem(event: ItemReplicaEvent): Promise<SyncOutcome>;
  syncCustomer(event: CustomerReplicaEvent): Promise<SyncOutcome>;
  syncTransportType(event: TransportTypeReplicaEvent): Promise<SyncOutcome>;
}

// Leitura das replicas pelos casos de uso de pedido.
export interface IReplicaReader {
  findItems(itemIds: readonly string[]): Promise<ItemReplica[]>;
  findCustomer(customerId: string): Promise<CustomerReplica | undefined>;
  findTransportType(transportTypeId: string): Promise<TransportTypeReplica | undefined>;
}

// Ports de entrada dos consumers. Lancam InvariantViolationError para evento
// que nunca sera aceito; qualquer outro erro e recuperavel.
export interface SyncReplicaPort<TEvent> {
  execute(event: TEvent): Promise<SyncOutcome>;
}
