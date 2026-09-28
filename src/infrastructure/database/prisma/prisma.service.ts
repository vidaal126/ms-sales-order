import {
  Injectable,
  type OnApplicationShutdown,
  type OnModuleInit,
} from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { PrismaPg } from "@prisma/adapter-pg";
import { PrismaClient } from "@infrastructure/database/generated/client";

@Injectable()
export class PrismaService
  extends PrismaClient
  implements OnModuleInit, OnApplicationShutdown
{
  constructor(config: ConfigService) {
    const databaseUrl = config.getOrThrow<string>("DATABASE_URL");
    const adapter = new PrismaPg({ connectionString: databaseUrl });
    super({ adapter });
  }

  async onModuleInit(): Promise<void> {
    await this.$connect();
  }

  // onApplicationShutdown roda depois de todos os onModuleDestroy (consumer
  // Kafka ja parou) e depois do servidor HTTP fechar: nenhuma query em
  // andamento perde a conexao.
  async onApplicationShutdown(): Promise<void> {
    await this.$disconnect();
  }
}
