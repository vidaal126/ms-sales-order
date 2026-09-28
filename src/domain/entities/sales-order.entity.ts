import { randomUUID } from "node:crypto";
import {
  InvalidSalesOrderError,
  InvalidStatusTransitionError,
} from "@domain/errors/sales-order.errors";
import {
  DeliveryRescheduledEvent,
  DeliveryScheduledEvent,
  OrderCreatedEvent,
  type OrderLineSnapshot,
  OrderStatusChangedEvent,
  type SalesOrderEvent,
  TransportChangedEvent,
} from "@domain/events/sales-order.events";
import { type DeliveryWindow, utcDay, validateWindow } from "@domain/value-objects/delivery-window";
import { fromCents, isValidUnitPrice, toCents } from "@domain/value-objects/money";
import {
  allowsTransportChange,
  canTransition,
  nextStatus,
  type OrderStatus,
} from "@domain/value-objects/order-status";
import { AggregateRoot } from "./aggregate-root";

export const MIN_QUANTITY = 1;
export const MAX_QUANTITY = 10_000;
export const MAX_LINES = 100;
export const NOTES_MAX_LENGTH = 1_000;

export interface OrderLine {
  readonly itemId: string;
  readonly quantity: number;
  // Congelado na criacao a partir da replica do catalogo.
  readonly unitPrice: number;
}

export interface DeliverySchedule extends DeliveryWindow {
  readonly confirmedAt: Date;
  readonly rescheduledAt: Date | null;
}

export interface CreateSalesOrderProps {
  readonly customerId: string;
  readonly transportTypeId: string;
  readonly notes?: string | null | undefined;
  readonly lines: readonly OrderLine[];
  readonly now: Date;
}

export interface RestoreSalesOrderProps {
  readonly id: string;
  readonly customerId: string;
  readonly transportTypeId: string;
  readonly status: OrderStatus;
  readonly notes: string | null;
  readonly lines: readonly OrderLine[];
  readonly schedule: DeliverySchedule | null;
  readonly version: number;
  readonly createdAt: Date;
  readonly updatedAt: Date;
}

// Agregado: pedido, linhas e agendamento mudam juntos na mesma transacao
// (status + agendamento e uma invariante real do agregado).
export class SalesOrder extends AggregateRoot<SalesOrderEvent> {
  private constructor(
    readonly id: string,
    readonly customerId: string,
    private _transportTypeId: string,
    private _status: OrderStatus,
    readonly notes: string | null,
    readonly lines: readonly OrderLine[],
    private _schedule: DeliverySchedule | null,
    // Versao lida do banco: controle otimista de concorrencia no save.
    readonly version: number,
    readonly createdAt: Date,
    private _updatedAt: Date,
  ) {
    super();
  }

  static create(props: CreateSalesOrderProps): SalesOrder {
    const lines = validateLines(props.lines);
    const order = new SalesOrder(
      randomUUID(),
      props.customerId,
      props.transportTypeId,
      "CRIADA",
      normalizeNotes(props.notes),
      lines,
      null,
      0,
      props.now,
      props.now,
    );
    order.record(
      new OrderCreatedEvent(
        order.id,
        props.now,
        order.customerId,
        order.transportTypeId,
        order.status,
        lines.map((line): OrderLineSnapshot => ({ ...line })),
        order.total,
      ),
    );
    return order;
  }

  static restore(props: RestoreSalesOrderProps): SalesOrder {
    return new SalesOrder(
      props.id,
      props.customerId,
      props.transportTypeId,
      props.status,
      props.notes,
      props.lines,
      props.schedule,
      props.version,
      props.createdAt,
      props.updatedAt,
    );
  }

  get status(): OrderStatus {
    return this._status;
  }

  get transportTypeId(): string {
    return this._transportTypeId;
  }

  get deliverySchedule(): DeliverySchedule | null {
    return this._schedule;
  }

  get updatedAt(): Date {
    return this._updatedAt;
  }

  // Soma em centavos inteiros, devolvida em reais com 2 casas.
  get total(): number {
    return fromCents(this.lines.reduce((sum, line) => sum + toCents(line.unitPrice) * line.quantity, 0));
  }

  // PUT /status: so os passos que nao dependem de agendamento. AGENDADA so
  // acontece via schedule() (o monolito deixava agendar sem Scheduling).
  changeStatus(target: OrderStatus, now: Date): void {
    if (target === "AGENDADA") {
      throw new InvalidStatusTransitionError("AGENDADA exige agendamento: use o agendamento de entrega");
    }
    if (target === "EM_TRANSPORTE" && this._schedule === null) {
      throw new InvalidStatusTransitionError("EM_TRANSPORTE exige agendamento de entrega");
    }
    this.transitionTo(target, now);
  }

  schedule(window: DeliveryWindow, now: Date): void {
    if (this._status !== "PLANEJADA") {
      throw new InvalidStatusTransitionError(
        `Agendamento exige status PLANEJADA (atual: ${this._status})`,
      );
    }
    if (this._schedule !== null) {
      throw new InvalidStatusTransitionError("Ordem de venda ja possui agendamento");
    }
    const valid = validateWindow(window, now);
    this._schedule = { ...valid, confirmedAt: now, rescheduledAt: null };
    this.record(
      new DeliveryScheduledEvent(this.id, now, utcDay(valid.deliveryDate), valid.windowStart, valid.windowEnd),
    );
    this.transitionTo("AGENDADA", now);
  }

  reschedule(window: DeliveryWindow, now: Date): void {
    if (this._status !== "AGENDADA" || this._schedule === null) {
      throw new InvalidStatusTransitionError(
        `Reagendamento exige status AGENDADA (atual: ${this._status})`,
      );
    }
    const previous = this._schedule;
    const valid = validateWindow(window, now);
    this._schedule = { ...valid, confirmedAt: previous.confirmedAt, rescheduledAt: now };
    this._updatedAt = now;
    this.record(
      new DeliveryRescheduledEvent(
        this.id,
        now,
        utcDay(previous.deliveryDate),
        utcDay(valid.deliveryDate),
        valid.windowStart,
        valid.windowEnd,
      ),
    );
  }

  // Autorizacao do transporte para o cliente e checada no use case (depende
  // das replicas). false = mesmo transporte, nada muda.
  changeTransport(transportTypeId: string, now: Date): boolean {
    if (!allowsTransportChange(this._status)) {
      throw new InvalidStatusTransitionError(
        `Troca de transporte bloqueada no status ${this._status}`,
      );
    }
    if (transportTypeId === this._transportTypeId) return false;
    const previous = this._transportTypeId;
    this._transportTypeId = transportTypeId;
    this._updatedAt = now;
    this.record(new TransportChangedEvent(this.id, now, previous, transportTypeId));
    return true;
  }

  private transitionTo(target: OrderStatus, now: Date): void {
    if (!canTransition(this._status, target)) {
      const allowed = nextStatus(this._status);
      throw new InvalidStatusTransitionError(
        `Transicao invalida: ${this._status} -> ${target}. Permitida: ${allowed ?? "nenhuma"}`,
      );
    }
    const previous = this._status;
    this._status = target;
    this._updatedAt = now;
    this.record(new OrderStatusChangedEvent(this.id, now, previous, target));
  }
}

function validateLines(lines: readonly OrderLine[]): OrderLine[] {
  if (lines.length === 0) {
    throw new InvalidSalesOrderError("Ordem de venda deve conter ao menos um item");
  }
  if (lines.length > MAX_LINES) {
    throw new InvalidSalesOrderError(`Ordem de venda aceita no maximo ${MAX_LINES} itens`);
  }
  if (new Set(lines.map((line) => line.itemId)).size !== lines.length) {
    throw new InvalidSalesOrderError("Ordem de venda nao pode conter o mesmo item em mais de uma linha");
  }
  for (const line of lines) {
    if (!Number.isInteger(line.quantity) || line.quantity < MIN_QUANTITY || line.quantity > MAX_QUANTITY) {
      throw new InvalidSalesOrderError(
        `Quantidade do item ${line.itemId} deve ser inteira entre ${MIN_QUANTITY} e ${MAX_QUANTITY}`,
      );
    }
    if (!isValidUnitPrice(line.unitPrice)) {
      throw new InvalidSalesOrderError(`Preco do item ${line.itemId} invalido`);
    }
  }
  return lines.map((line) => ({ ...line }));
}

function normalizeNotes(raw: string | null | undefined): string | null {
  if (raw === undefined || raw === null) return null;
  const notes = raw.trim();
  if (notes.length === 0) return null;
  if (notes.length > NOTES_MAX_LENGTH) {
    throw new InvalidSalesOrderError(`Observacoes excedem ${NOTES_MAX_LENGTH} caracteres`);
  }
  return notes;
}
