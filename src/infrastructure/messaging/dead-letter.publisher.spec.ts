import { DLT_HEADERS, toDeadLetterRecord } from "./dead-letter.publisher";
import type { InboundMessage } from "./kafka-consumer.base";

describe("toDeadLetterRecord", () => {
  it("mantem payload, key e headers originais e adiciona origem e motivo", () => {
    const value = Buffer.from("{isto nao e json");
    const message: InboundMessage = {
      topic: "catalog.ItemCreated",
      partition: 0,
      offset: "2",
      timestamp: "1700000000000",
      key: "invalid-message",
      value,
      headers: { correlationId: "corr-1" },
    };

    const record = toDeadLetterRecord(
      message,
      "invalid_json",
      "Unexpected token",
      new Date("2026-09-22T12:00:00.000Z"),
      "ms-sales-order.catalog-item-sync",
    );

    expect(record.topic).toBe("catalog.ItemCreated.ms-sales-order.DLT");
    expect(record.key).toBe("invalid-message");
    expect(record.value).toBe(value);
    expect(record.headers).toEqual({
      correlationId: "corr-1",
      [DLT_HEADERS.reason]: "invalid_json",
      [DLT_HEADERS.detail]: "Unexpected token",
      [DLT_HEADERS.sourceTopic]: "catalog.ItemCreated",
      [DLT_HEADERS.sourcePartition]: "0",
      [DLT_HEADERS.sourceOffset]: "2",
      [DLT_HEADERS.sourceTimestamp]: "1700000000000",
      [DLT_HEADERS.failedAt]: "2026-09-22T12:00:00.000Z",
      [DLT_HEADERS.consumerGroup]: "ms-sales-order.catalog-item-sync",
    });
  });

  it("trunca o detalhe do erro", () => {
    const message: InboundMessage = {
      topic: "t",
      partition: 0,
      offset: "0",
      timestamp: "0",
      key: null,
      value: null,
      headers: {},
    };

    const record = toDeadLetterRecord(message, "schema_validation_failed", "x".repeat(2000), new Date(), "g");

    expect(record.headers[DLT_HEADERS.detail]).toHaveLength(500);
  });
});
