import type { SalesOrder } from "@domain/entities/sales-order.entity";
import type { OrderStatus } from "@domain/value-objects/order-status";

export const SALES_ORDER_REPOSITORY = Symbol("SALES_ORDER_REPOSITORY");

export interface SalesOrderFilter {
  readonly status?: OrderStatus | undefined;
  readonly customerId?: string | undefined;
  readonly transportTypeId?: string | undefined;
  readonly itemId?: string | undefined;
  // Filtram createdAt (inclusivo).
  readonly dateFrom?: Date | undefined;
  readonly dateTo?: Date | undefined;
}

export interface PageRequest {
  readonly page: number;
  readonly pageSize: number;
}

export interface Page<T> {
  readonly items: T[];
  readonly total: number;
}

export interface PersistenceContext {
  readonly correlationId: string;
}

export interface ISalesOrderRepository {
  findById(id: string): Promise<SalesOrder | undefined>;
  findAll(filter: SalesOrderFilter, page: PageRequest): Promise<Page<SalesOrder>>;
  // Grava pedido, linhas e eventos (outbox) na mesma transacao.
  create(order: SalesOrder, context: PersistenceContext): Promise<void>;
  // Grava status/transporte/agendamento e eventos na mesma transacao, com
  // controle otimista pela versao lida: SalesOrderConcurrentModificationError
  // se outra requisicao gravou antes.
  save(order: SalesOrder, context: PersistenceContext): Promise<void>;
}
