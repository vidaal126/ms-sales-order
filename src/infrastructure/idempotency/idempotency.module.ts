import { Module } from "@nestjs/common";
import { IdempotencyInterceptor } from "@infrastructure/http/interceptors/idempotency.interceptor";
import { IdempotencyCleanupService } from "./idempotency-cleanup.service";
import { IDEMPOTENCY_STORE } from "./idempotency.store";
import { PrismaIdempotencyStore } from "./prisma-idempotency.store";

@Module({
  providers: [
    { provide: IDEMPOTENCY_STORE, useClass: PrismaIdempotencyStore },
    IdempotencyCleanupService,
    IdempotencyInterceptor,
  ],
  exports: [IDEMPOTENCY_STORE, IdempotencyInterceptor],
})
export class IdempotencyModule {}
