// Hierarquia de erros de dominio. A camada HTTP traduz cada categoria para um
// status (ver GlobalExceptionFilter); o dominio nao conhece HTTP.
export abstract class DomainError extends Error {
  constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

// Regra de negocio violada com entrada sintaticamente valida.
export abstract class InvariantViolationError extends DomainError {}

export abstract class EntityNotFoundError extends DomainError {}

export abstract class EntityConflictError extends DomainError {}
