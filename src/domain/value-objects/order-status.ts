export const ORDER_STATUSES = ["CRIADA", "PLANEJADA", "AGENDADA", "EM_TRANSPORTE", "ENTREGUE"] as const;
export type OrderStatus = (typeof ORDER_STATUSES)[number];

export function isOrderStatus(value: string): value is OrderStatus {
  return (ORDER_STATUSES as readonly string[]).includes(value);
}

// So avanca um passo por vez; ENTREGUE e final. Sem pulo e sem volta.
const NEXT_STATUS: Readonly<Record<OrderStatus, OrderStatus | undefined>> = {
  CRIADA: "PLANEJADA",
  PLANEJADA: "AGENDADA",
  AGENDADA: "EM_TRANSPORTE",
  EM_TRANSPORTE: "ENTREGUE",
  ENTREGUE: undefined,
};

export function nextStatus(from: OrderStatus): OrderStatus | undefined {
  return NEXT_STATUS[from];
}

export function canTransition(from: OrderStatus, to: OrderStatus): boolean {
  return NEXT_STATUS[from] === to;
}

// Troca de transporte proibida depois que a carga saiu.
export function allowsTransportChange(status: OrderStatus): boolean {
  return status !== "EM_TRANSPORTE" && status !== "ENTREGUE";
}
