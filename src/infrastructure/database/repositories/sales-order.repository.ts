import { randomUUID } from "node:crypto";
import { Injectable } from "@nestjs/common";
import { Prisma } from "@infrastructure/database/generated/client";
import { SalesOrder } from "@domain/entities/sales-order.entity";
import { SalesOrderConcurrentModificationError } from "@domain/errors/sales-order.errors";
import { isOrderStatus } from "@domain/value-objects/order-status";
import type {
  ISalesOrderRepository,
  Page,
  PageRequest,
  PersistenceContext,
  SalesOrderFilter,
} from "@application/ports/sales-order.repository.port";
import { toOutboxEventData } from "@infrastructure/database/mappers/outbox-event.mapper";
import { PrismaService } from "@infrastructure/database/prisma/prisma.service";

const withRelations = { items: { orderBy: { itemId: "asc" } }, scheduling: true } as const;

type SalesOrderRow = Prisma.SalesOrderGetPayload<{ include: typeof withRelations }>;

@Injectable()
export class SalesOrderRepositoryPrisma implements ISalesOrderRepository {
  constructor(private readonly prisma: PrismaService) {}

  async findById(id: string): Promise<SalesOrder | undefined> {
    const row = await this.prisma.salesOrder.findUnique({ where: { id }, include: withRelations });
    return row ? toDomain(row) : undefined;
  }

  async findAll(filter: SalesOrderFilter, page: PageRequest): Promise<Page<SalesOrder>> {
    const where: Prisma.SalesOrderWhereInput = {
      status: filter.status,
      customerId: filter.customerId,
      transportTypeId: filter.transportTypeId,
      items: filter.itemId === undefined ? undefined : { some: { itemId: filter.itemId } },
      createdAt:
        filter.dateFrom === undefined && filter.dateTo === undefined
          ? undefined
          : { gte: filter.dateFrom, lte: filter.dateTo },
    };
    const [rows, total] = await this.prisma.$transaction([
      this.prisma.salesOrder.findMany({
        where,
        include: withRelations,
        take: page.pageSize,
        skip: (page.page - 1) * page.pageSize,
        orderBy: [{ createdAt: "desc" }, { id: "desc" }],
      }),
      this.prisma.salesOrder.count({ where }),
    ]);
    return { items: rows.map(toDomain), total };
  }

  async create(order: SalesOrder, context: PersistenceContext): Promise<void> {
    const events = order.pullDomainEvents();
    await this.prisma.$transaction(async (tx) => {
      await tx.salesOrder.create({
        data: {
          id: order.id,
          customerId: order.customerId,
          transportTypeId: order.transportTypeId,
          status: order.status,
          notes: order.notes,
          version: order.version,
          createdAt: order.createdAt,
          updatedAt: order.updatedAt,
          items: {
            create: order.lines.map((line) => ({
              id: randomUUID(),
              itemId: line.itemId,
              quantity: line.quantity,
              unitPrice: new Prisma.Decimal(line.unitPrice.toFixed(2)),
            })),
          },
        },
      });
      if (events.length > 0) {
        await tx.outboxEvent.createMany({ data: events.map((event) => toOutboxEventData(event, context)) });
      }
    });
  }

  async save(order: SalesOrder, context: PersistenceContext): Promise<void> {
    const events = order.pullDomainEvents();
    await this.prisma.$transaction(async (tx) => {
      // Compare-and-set pela versao lida: duas alteracoes concorrentes no
      // mesmo pedido, so uma grava; a outra recebe 409.
      const updated = await tx.salesOrder.updateMany({
        where: { id: order.id, version: order.version },
        data: {
          status: order.status,
          transportTypeId: order.transportTypeId,
          updatedAt: order.updatedAt,
          version: { increment: 1 },
        },
      });
      if (updated.count !== 1) throw new SalesOrderConcurrentModificationError(order.id);

      const schedule = order.deliverySchedule;
      if (schedule) {
        const data = {
          deliveryDate: schedule.deliveryDate,
          windowStart: schedule.windowStart,
          windowEnd: schedule.windowEnd,
          confirmedAt: schedule.confirmedAt,
          rescheduledAt: schedule.rescheduledAt,
          updatedAt: order.updatedAt,
        };
        await tx.scheduling.upsert({
          where: { salesOrderId: order.id },
          create: { id: randomUUID(), salesOrderId: order.id, createdAt: order.updatedAt, ...data },
          update: data,
        });
      }
      if (events.length > 0) {
        await tx.outboxEvent.createMany({ data: events.map((event) => toOutboxEventData(event, context)) });
      }
    });
  }
}

function toDomain(row: SalesOrderRow): SalesOrder {
  if (!isOrderStatus(row.status)) {
    // O CHECK do banco impede; se acontecer, e corrupcao, nao entrada do cliente.
    throw new Error(`Status desconhecido no banco para o pedido ${row.id}: ${row.status}`);
  }
  return SalesOrder.restore({
    id: row.id,
    customerId: row.customerId,
    transportTypeId: row.transportTypeId,
    status: row.status,
    notes: row.notes,
    lines: row.items.map((item) => ({
      itemId: item.itemId,
      quantity: item.quantity,
      unitPrice: item.unitPrice.toNumber(),
    })),
    schedule: row.scheduling
      ? {
          deliveryDate: row.scheduling.deliveryDate,
          windowStart: row.scheduling.windowStart,
          windowEnd: row.scheduling.windowEnd,
          confirmedAt: row.scheduling.confirmedAt,
          rescheduledAt: row.scheduling.rescheduledAt,
        }
      : null,
    version: row.version,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  });
}
