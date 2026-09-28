import { ConfigService } from "@nestjs/config";
import type { Kafka, Producer } from "kafkajs";
import type { ILogger } from "@common/logger/logger.interface";
import { TimeoutError } from "@common/with-timeout";
import { KafkaProducerService } from "./kafka-producer.service";

const SEND_TIMEOUT_MS = 5_000;
const MESSAGE = { topic: "sales-order.OrderCreated", key: "order-1", value: "{}", headers: {} };

const silentLogger: ILogger = { log: jest.fn(), warn: jest.fn(), debug: jest.fn(), error: jest.fn() };

function createService(send: Producer["send"]): KafkaProducerService {
  const producer: Pick<Producer, "on" | "events" | "connect" | "send" | "disconnect"> = {
    on: jest.fn(),
    events: { DISCONNECT: "producer.disconnect" } as Producer["events"],
    connect: async () => undefined,
    send,
    disconnect: async () => undefined,
  };
  const kafka: Pick<Kafka, "producer"> = { producer: () => producer as Producer };
  const config = new ConfigService<{ KAFKA_SEND_TIMEOUT_MS: number }, true>({ KAFKA_SEND_TIMEOUT_MS: SEND_TIMEOUT_MS });
  return new KafkaProducerService(kafka as Kafka, silentLogger, config);
}

describe("KafkaProducerService", () => {
  afterEach(() => {
    jest.useRealTimers();
  });

  it("envia normalmente dentro do teto", async () => {
    const send = jest.fn<ReturnType<Producer["send"]>, Parameters<Producer["send"]>>(async () => []);

    await createService(send).send(MESSAGE);

    expect(send).toHaveBeenCalledWith({
      topic: MESSAGE.topic,
      messages: [{ key: MESSAGE.key, value: MESSAGE.value, headers: {} }],
    });
  });

  it("envio que nao termina rejeita com TimeoutError apos KAFKA_SEND_TIMEOUT_MS", async () => {
    jest.useFakeTimers();
    // Broker que nunca responde: com retries ilimitados o send ficaria preso.
    const service = createService(() => new Promise(() => undefined));

    const result = service.send(MESSAGE);
    const assertion = expect(result).rejects.toBeInstanceOf(TimeoutError);
    await jest.advanceTimersByTimeAsync(SEND_TIMEOUT_MS);

    await assertion;
  });
});
