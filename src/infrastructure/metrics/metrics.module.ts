import { Global, type MiddlewareConsumer, Module, type NestModule } from "@nestjs/common";
import { HttpMetricsMiddleware } from "./http-metrics.middleware";
import { MetricsController } from "./metrics.controller";
import { MetricsService } from "./metrics.service";

@Global()
@Module({
  controllers: [MetricsController],
  providers: [MetricsService],
  exports: [MetricsService],
})
export class MetricsModule implements NestModule {
  configure(consumer: MiddlewareConsumer): void {
    consumer.apply(HttpMetricsMiddleware).forRoutes("*");
  }
}
