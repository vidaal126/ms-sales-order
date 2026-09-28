import { type LogEntry, type logCreator, logLevel } from "kafkajs";
import type { PinoLogger } from "nestjs-pino";

// Roteia os logs internos do KafkaJS para o Pino. PinoLogger resolve o logger
// do contexto atual (requisicao HTTP ou runWithCorrelationId), entao logs do
// KafkaJS disparados dentro de um handler saem com o correlationId.
// Os campos do KafkaJS vao aninhados em "kafka": ele loga um correlationId
// proprio (id numerico de requisicao do protocolo) que colidiria com o nosso.
export function createKafkaLogCreator(logger: PinoLogger): logCreator {
  return () =>
    ({ namespace, level, log }: LogEntry): void => {
      const { message, timestamp: _timestamp, ...extra } = log;
      const fields: Record<string, unknown> = { kafka: { namespace, ...extra } };
      const text = message;

      switch (level) {
        case logLevel.ERROR:
          logger.error(fields, text);
          return;
        case logLevel.WARN:
          logger.warn(fields, text);
          return;
        case logLevel.INFO:
          logger.info(fields, text);
          return;
        case logLevel.DEBUG:
          logger.debug(fields, text);
          return;
        case logLevel.NOTHING:
          return;
      }
    };
}
