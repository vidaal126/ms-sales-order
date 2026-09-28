import { sampleValue } from "../../test/metrics.helpers";
import { MetricsService } from "./metrics.service";

describe("MetricsService", () => {
  it("expoe metricas HTTP, de consumo e de outbox com o label service", async () => {
    const metrics = new MetricsService();
    metrics.observeHttp("GET", "/sales-orders/:id", 200, 0.012);
    metrics.recordConsumed("catalog.ItemCreated", "applied");
    metrics.recordConsumed("catalog.ItemCreated", "dead_letter");
    metrics.recordOutboxPublished("OrderCreated");
    metrics.registerAsyncGauge("outbox_pending_events", "pendentes", () => Promise.resolve(3));

    const text = await metrics.render();
    const service = "ms-sales-order";

    expect(
      sampleValue(text, "http_request_duration_seconds_count", {
        method: "GET",
        route: "/sales-orders/:id",
        status_code: "200",
        service,
      }),
    ).toBe(1);
    expect(
      sampleValue(text, "kafka_messages_consumed_total", { topic: "catalog.ItemCreated", outcome: "dead_letter", service }),
    ).toBe(1);
    expect(sampleValue(text, "outbox_events_published_total", { event_type: "OrderCreated", service })).toBe(1);
    expect(sampleValue(text, "outbox_pending_events", { service })).toBe(3);
    expect(text).toContain("process_cpu_user_seconds_total");
  });

  it("falha ao ler um gauge nao derruba o scrape", async () => {
    const metrics = new MetricsService();
    metrics.registerAsyncGauge("outbox_pending_events", "pendentes", () => Promise.reject(new Error("db")));

    await expect(metrics.render()).resolves.toContain("outbox_pending_events");
  });
});
