import { InvalidSalesOrderError } from "@domain/errors/sales-order.errors";

// Maior valor de DECIMAL(10,2).
export const MAX_UNIT_PRICE = 99_999_999.99;

// Dinheiro trafega como numero com ate 2 casas, mas toda conta e feita em
// centavos inteiros: 0.1 + 0.2 nao pode virar 0.30000000000000004.
export function toCents(amount: number): number {
  if (!Number.isFinite(amount)) throw new InvalidSalesOrderError("Valor monetario invalido");
  const cents = Math.round(amount * 100);
  if (Math.abs(amount * 100 - cents) > 1e-6) {
    throw new InvalidSalesOrderError(`Valor ${amount} tem mais de 2 casas decimais`);
  }
  return cents;
}

export function fromCents(cents: number): number {
  return cents / 100;
}

export function isValidUnitPrice(amount: number): boolean {
  if (!Number.isFinite(amount) || amount <= 0 || amount > MAX_UNIT_PRICE) return false;
  return Math.abs(amount * 100 - Math.round(amount * 100)) <= 1e-6;
}
