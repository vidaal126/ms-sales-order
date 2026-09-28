import { Injectable } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { Kafka, logLevel } from "kafkajs";
import { PinoLogger } from "nestjs-pino";
import { createKafkaLogCreator } from "./kafka-log-creator";

const CONNECTION_TIMEOUT_MS = 3_000;
const REQUEST_TIMEOUT_MS = 30_000;

// Contrato de configuracao do modulo: KAFKA_BROKER (lista) e KAFKA_CLIENT_ID,
// ja validados pelo schema de env do servico.
interface MessagingEnv {
  KAFKA_BROKER: string[];
  KAFKA_CLIENT_ID: string;
}

@Injectable()
export class KafkaClientFactory {
  constructor(
    private readonly config: ConfigService<MessagingEnv, true>,
    // PinoLogger e transient: cada injecao tem instancia propria. Nao usa
    // @InjectPinoLogger porque o nestjs-pino so registra contextos ja
    // decorados quando o LoggerModule e avaliado (antes deste modulo).
    private readonly logger: PinoLogger,
  ) {
    this.logger.setContext("kafkajs");
  }

  create(): Kafka {
    // Leituras sem anotacao de tipo: dentro do objeto de config do KafkaJS o
    // ConfigService.get inferiria o retorno pelo contexto, sem checagem.
    const clientId = this.config.get("KAFKA_CLIENT_ID", { infer: true });
    const brokers = this.config.get("KAFKA_BROKER", { infer: true });
    return new Kafka({
      clientId,
      brokers,
      // Timeout explícito por tentativa: sem ele o socket fica pendurado
      // esperando a rede responder eventualmente.
      connectionTimeout: CONNECTION_TIMEOUT_MS,
      requestTimeout: REQUEST_TIMEOUT_MS,
      retry: {
        initialRetryTime: 300,
        retries: 8,
      },
      // O nivel efetivo e filtrado pelo Pino (LOG_LEVEL).
      logLevel: logLevel.DEBUG,
      logCreator: createKafkaLogCreator(this.logger),
    });
  }
}
