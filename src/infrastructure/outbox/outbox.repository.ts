import { Injectable } from "@nestjs/common";
import type { OutboxEvent } from "@infrastructure/database/generated/client";
import { PrismaService } from "@infrastructure/database/prisma/prisma.service";

@Injectable()
export class OutboxRepository {
  constructor(private readonly prisma: PrismaService) {}

  async findPending(limit: number): Promise<OutboxEvent[]> {
    return this.prisma.outboxEvent.findMany({
      where: { publishedAt: null },
      orderBy: { createdAt: "asc" },
      take: limit,
    });
  }

  async countPending(): Promise<number> {
    return this.prisma.outboxEvent.count({ where: { publishedAt: null } });
  }

  async markPublished(ids: readonly string[], publishedAt: Date): Promise<void> {
    await this.prisma.outboxEvent.updateMany({
      where: { id: { in: [...ids] } },
      data: { publishedAt },
    });
  }
}
