import { Injectable } from "@nestjs/common";
import { InvalidReplicaEventError } from "@domain/errors/sales-order.errors";
import type {
  CustomerReplica,
  CustomerReplicaEvent,
  IReplicaReader,
  IReplicaRepository,
  ItemReplica,
  ItemReplicaEvent,
  SourceEvent,
  SyncOutcome,
  TransportTypeReplica,
  TransportTypeReplicaEvent,
} from "@application/ports/replica.ports";
import type { Prisma } from "@infrastructure/database/generated/client";
import { isIntegrityViolation } from "@infrastructure/database/prisma/integrity-violation";
import { PrismaService } from "@infrastructure/database/prisma/prisma.service";

type Tx = Prisma.TransactionClient;

// Cada sync: processed_events (ON CONFLICT DO NOTHING) + upsert condicional
// na MESMA transacao. O upsert so sobrescreve se o evento for mais recente que
// o que gerou a versao gravada: Created e Updated chegam por topicos
// diferentes, sem ordem garantida entre si, e o guard resolve.
@Injectable()
export class ReplicaRepositoryPrisma implements IReplicaRepository, IReplicaReader {
  constructor(private readonly prisma: PrismaService) {}

  syncItem(event: ItemReplicaEvent): Promise<SyncOutcome> {
    const { item } = event;
    return this.sync(event, `item ${item.itemId}`, (tx) => tx.$executeRaw`
      INSERT INTO "items_replica" ("itemId", "sku", "name", "unitPrice", "sourceEventId", "sourceOccurredAt", "createdAt", "updatedAt")
      VALUES (${item.itemId}, ${item.sku}, ${item.name}, ${item.unitPrice.toFixed(2)}::numeric(10,2), ${event.eventId}, ${event.occurredAt}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
      ON CONFLICT ("itemId") DO UPDATE SET
        "sku" = EXCLUDED."sku",
        "name" = EXCLUDED."name",
        "unitPrice" = EXCLUDED."unitPrice",
        "sourceEventId" = EXCLUDED."sourceEventId",
        "sourceOccurredAt" = EXCLUDED."sourceOccurredAt",
        "updatedAt" = CURRENT_TIMESTAMP
      WHERE "items_replica"."sourceOccurredAt" < EXCLUDED."sourceOccurredAt"
    `);
  }

  syncCustomer(event: CustomerReplicaEvent): Promise<SyncOutcome> {
    const { customer } = event;
    const ids = [...customer.authorizedTransportTypeIds];
    return this.sync(event, `cliente ${customer.customerId}`, (tx) => tx.$executeRaw`
      INSERT INTO "customers_replica" ("customerId", "name", "authorizedTransportTypeIds", "sourceEventId", "sourceOccurredAt", "createdAt", "updatedAt")
      VALUES (${customer.customerId}, ${customer.name}, ${ids}::text[], ${event.eventId}, ${event.occurredAt}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
      ON CONFLICT ("customerId") DO UPDATE SET
        "name" = EXCLUDED."name",
        "authorizedTransportTypeIds" = EXCLUDED."authorizedTransportTypeIds",
        "sourceEventId" = EXCLUDED."sourceEventId",
        "sourceOccurredAt" = EXCLUDED."sourceOccurredAt",
        "updatedAt" = CURRENT_TIMESTAMP
      WHERE "customers_replica"."sourceOccurredAt" < EXCLUDED."sourceOccurredAt"
    `);
  }

  syncTransportType(event: TransportTypeReplicaEvent): Promise<SyncOutcome> {
    const { transportType } = event;
    return this.sync(event, `tipo de transporte ${transportType.transportTypeId}`, (tx) => tx.$executeRaw`
      INSERT INTO "transport_types_replica" ("transportTypeId", "name", "active", "sourceEventId", "sourceOccurredAt", "createdAt", "updatedAt")
      VALUES (${transportType.transportTypeId}, ${transportType.name}, ${transportType.active}, ${event.eventId}, ${event.occurredAt}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
      ON CONFLICT ("transportTypeId") DO UPDATE SET
        "name" = EXCLUDED."name",
        "active" = EXCLUDED."active",
        "sourceEventId" = EXCLUDED."sourceEventId",
        "sourceOccurredAt" = EXCLUDED."sourceOccurredAt",
        "updatedAt" = CURRENT_TIMESTAMP
      WHERE "transport_types_replica"."sourceOccurredAt" < EXCLUDED."sourceOccurredAt"
    `);
  }

  async findItems(itemIds: readonly string[]): Promise<ItemReplica[]> {
    if (itemIds.length === 0) return [];
    const rows = await this.prisma.itemReplica.findMany({ where: { itemId: { in: [...itemIds] } } });
    return rows.map((row) => ({ itemId: row.itemId, sku: row.sku, name: row.name, unitPrice: row.unitPrice.toNumber() }));
  }

  async findCustomer(customerId: string): Promise<CustomerReplica | undefined> {
    const row = await this.prisma.customerReplica.findUnique({ where: { customerId } });
    return row
      ? { customerId: row.customerId, name: row.name, authorizedTransportTypeIds: row.authorizedTransportTypeIds }
      : undefined;
  }

  async findTransportType(transportTypeId: string): Promise<TransportTypeReplica | undefined> {
    const row = await this.prisma.transportTypeReplica.findUnique({ where: { transportTypeId } });
    return row ? { transportTypeId: row.transportTypeId, name: row.name, active: row.active } : undefined;
  }

  // Dado rejeitado pelo banco (CHECK, range) vira erro de dominio: o evento
  // nunca sera aceito (DLT). Demais erros propagam (recuperaveis).
  private async sync(event: SourceEvent, label: string, upsert: (tx: Tx) => Promise<number>): Promise<SyncOutcome> {
    try {
      return await this.prisma.$transaction(async (tx) => {
        const registered = await tx.processedEvent.createMany({
          data: [{ eventId: event.eventId, eventType: event.eventType }],
          skipDuplicates: true,
        });
        if (registered.count === 0) return "duplicate";
        return (await upsert(tx)) === 1 ? "applied" : "stale";
      });
    } catch (err) {
      if (isIntegrityViolation(err)) {
        throw new InvalidReplicaEventError(
          `replica rejeitou ${label}: ${err instanceof Error ? err.message : String(err)}`,
        );
      }
      throw err;
    }
  }
}
