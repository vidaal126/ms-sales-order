import { Controller, Get, Header } from "@nestjs/common";
import { SkipThrottle } from "@nestjs/throttler";
import { Registry } from "prom-client";
import { MetricsService } from "./metrics.service";

// Scrape do Prometheus pela rede interna; o gateway nao roteia /metrics.
@Controller("metrics")
@SkipThrottle()
export class MetricsController {
  constructor(private readonly metrics: MetricsService) {}

  @Get()
  @Header("Content-Type", Registry.PROMETHEUS_CONTENT_TYPE)
  scrape(): Promise<string> {
    return this.metrics.render();
  }
}
