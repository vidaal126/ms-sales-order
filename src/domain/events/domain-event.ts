// Fato de dominio registrado pelo agregado. Versao de schema, eventId e
// correlationId sao preocupacoes de transporte e ficam fora daqui.
export interface DomainEvent {
  readonly eventType: string;
  readonly aggregateId: string;
  readonly occurredAt: Date;
}
