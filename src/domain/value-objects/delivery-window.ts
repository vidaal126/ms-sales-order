import { InvalidDeliveryWindowError } from "@domain/errors/sales-order.errors";

export const MAX_SCHEDULING_HORIZON_DAYS = 365;
const DAY_MS = 24 * 60 * 60 * 1000;

// deliveryDate e so data (meia-noite UTC); a janela e um intervalo absoluto
// dentro desse dia UTC.
export interface DeliveryWindow {
  readonly deliveryDate: Date;
  readonly windowStart: Date;
  readonly windowEnd: Date;
}

// Regras do monolito (Scheduling.validateWindow), na mesma ordem.
export function validateWindow(window: DeliveryWindow, now: Date): DeliveryWindow {
  const { deliveryDate, windowStart, windowEnd } = window;
  if ([deliveryDate, windowStart, windowEnd].some((d) => Number.isNaN(d.getTime()))) {
    throw new InvalidDeliveryWindowError("Data de entrega ou janela de atendimento invalida");
  }
  if (windowStart.getTime() >= windowEnd.getTime()) {
    throw new InvalidDeliveryWindowError("Inicio da janela deve ser anterior ao fim");
  }
  if (windowStart.getTime() < now.getTime()) {
    throw new InvalidDeliveryWindowError("Janela de atendimento no passado");
  }
  if (deliveryDate.getTime() > now.getTime() + MAX_SCHEDULING_HORIZON_DAYS * DAY_MS) {
    throw new InvalidDeliveryWindowError(
      `Entrega alem do horizonte de ${MAX_SCHEDULING_HORIZON_DAYS} dias`,
    );
  }
  const day = utcDay(deliveryDate);
  if (utcDay(windowStart) !== day || utcDay(windowEnd) !== day) {
    throw new InvalidDeliveryWindowError("Janela deve estar no mesmo dia (UTC) da entrega");
  }
  return { deliveryDate: new Date(`${day}T00:00:00.000Z`), windowStart, windowEnd };
}

export function utcDay(date: Date): string {
  return date.toISOString().slice(0, 10);
}
