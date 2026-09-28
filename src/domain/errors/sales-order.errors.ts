import {
  EntityConflictError,
  EntityNotFoundError,
  InvariantViolationError,
} from "./domain.error";

export class SalesOrderNotFoundError extends EntityNotFoundError {
  constructor(readonly salesOrderId: string) {
    super(`Ordem de venda ${salesOrderId} nao encontrada`);
  }
}

// Outra requisicao alterou o pedido entre a leitura e a gravacao.
export class SalesOrderConcurrentModificationError extends EntityConflictError {
  constructor(readonly salesOrderId: string) {
    super(`Ordem de venda ${salesOrderId} foi alterada por outra requisicao; tente de novo`);
  }
}

export class InvalidSalesOrderError extends InvariantViolationError {}

export class InvalidStatusTransitionError extends InvariantViolationError {}

export class InvalidDeliveryWindowError extends InvariantViolationError {}

export class UnknownCustomerError extends InvariantViolationError {
  constructor(readonly customerId: string) {
    super(`Cliente ${customerId} desconhecido (ainda nao replicado ou inexistente)`);
  }
}

export class UnknownItemsError extends InvariantViolationError {
  constructor(readonly itemIds: readonly string[]) {
    super(`Itens desconhecidos (ainda nao replicados ou inexistentes): ${itemIds.join(", ")}`);
  }
}

export class TransportNotAuthorizedError extends InvariantViolationError {
  constructor(readonly transportTypeId: string, reason: string) {
    super(`Tipo de transporte ${transportTypeId} nao permitido: ${reason}`);
  }
}

// Evento de replica que viola as regras (ex.: preco invalido): nunca sera
// aceito, vai para a DLT.
export class InvalidReplicaEventError extends InvariantViolationError {}
