import type { SalesOrder } from "@domain/entities/sales-order.entity";
import { utcDay } from "@domain/value-objects/delivery-window";
import { fromCents, toCents } from "@domain/value-objects/money";
import type { ListSalesOrdersOutput } from "@application/use-cases/sales-order.use-cases";
import type {
  PaginatedSalesOrdersResponseDto,
  SalesOrderResponseDto,
} from "@infrastructure/http/dto/sales-order.dto";

export function toSalesOrderResponse(order: SalesOrder): SalesOrderResponseDto {
  const schedule = order.deliverySchedule;
  return {
    id: order.id,
    customerId: order.customerId,
    transportTypeId: order.transportTypeId,
    status: order.status,
    notes: order.notes,
    items: order.lines.map((line) => ({
      itemId: line.itemId,
      quantity: line.quantity,
      unitPrice: line.unitPrice,
      lineTotal: fromCents(toCents(line.unitPrice) * line.quantity),
    })),
    total: order.total,
    scheduling: schedule
      ? {
          deliveryDate: utcDay(schedule.deliveryDate),
          windowStart: schedule.windowStart.toISOString(),
          windowEnd: schedule.windowEnd.toISOString(),
          confirmedAt: schedule.confirmedAt.toISOString(),
          rescheduledAt: schedule.rescheduledAt?.toISOString() ?? null,
        }
      : null,
    createdAt: order.createdAt.toISOString(),
    updatedAt: order.updatedAt.toISOString(),
  };
}

export function toPaginatedSalesOrdersResponse(output: ListSalesOrdersOutput): PaginatedSalesOrdersResponseDto {
  return {
    items: output.items.map(toSalesOrderResponse),
    total: output.total,
    page: output.page,
    pageSize: output.pageSize,
  };
}
