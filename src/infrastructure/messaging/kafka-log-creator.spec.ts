import { logLevel } from "kafkajs";
import type { PinoLogger } from "nestjs-pino";
import { createKafkaLogCreator } from "./kafka-log-creator";

describe("createKafkaLogCreator", () => {
  const calls: Array<[string, unknown, string]> = [];
  const record =
    (level: string) =>
    (fields: unknown, message: string): void => {
      calls.push([level, fields, message]);
    };
  const logger: Pick<PinoLogger, "error" | "warn" | "info" | "debug"> = {
    error: record("error"),
    warn: record("warn"),
    info: record("info"),
    debug: record("debug"),
  };
  const log = createKafkaLogCreator(logger as PinoLogger)(logLevel.DEBUG);

  beforeEach(() => {
    calls.length = 0;
  });

  it.each([
    [logLevel.ERROR, "error"],
    [logLevel.WARN, "warn"],
    [logLevel.INFO, "info"],
    [logLevel.DEBUG, "debug"],
  ])("nivel KafkaJS %p vai para pino.%s com campos do KafkaJS aninhados em kafka", (level, method) => {
    log({
      namespace: "Connection",
      level,
      label: "X",
      log: { timestamp: "t", message: "msg", broker: "localhost:9092", correlationId: 3 },
    });

    expect(calls).toEqual([
      [
        method,
        { kafka: { namespace: "Connection", broker: "localhost:9092", correlationId: 3 } },
        "msg",
      ],
    ]);
  });
});
