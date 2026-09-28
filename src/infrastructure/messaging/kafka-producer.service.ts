import {
  Inject,
  Injectable,
  type OnApplicationShutdown,
  type OnModuleInit,
} from "@nestjs/common";
import { type Kafka, Partitioners, type Producer } from "kafkajs";
import { type ILogger, LOGGER_TOKEN } from "@common/logger/logger.interface";
import type { OutboundMessage } from "./event-envelope";
import { KAFKA_CLIENT } from "./kafka.tokens";

@Injectable()
export class KafkaProducerService
  implements OnModuleInit, OnApplicationShutdown
{
  private readonly producer: Producer;
  private isConnected = false;
  private connecting: Promise<void> | null = null;

  constructor(
    @Inject(KAFKA_CLIENT) kafka: Kafka,
    @Inject(LOGGER_TOKEN) private readonly logger: ILogger,
  ) {
    this.producer = kafka.producer({
      idempotent: true,
      // O producer idempotente exige retries ilimitados - qualquer teto invalida
      // a garantia de não-duplicação do broker, e o kafkajs avisa disso se ele
      // herdar o retries: 8 do client (KafkaClientFactory).
      retry: { retries: Number.MAX_SAFE_INTEGER },
      // Explícito para fixar o particionador da v2 e silenciar o warning de
      // migração do kafkajs. É o mesmo comportamento padrão desde a v2.0.0.
      createPartitioner: Partitioners.DefaultPartitioner,
    });

    this.producer.on(this.producer.events.DISCONNECT, (): void => {
      this.isConnected = false;
      this.logger.warn("Kafka producer desconectado", {
        service: KafkaProducerService.name,
      });
    });
  }

  onModuleInit(): void {
    // Degradação prevista: broker indisponível não pode derrubar a aplicação.
    // A conexão fica tentando em segundo plano (retries ilimitados exigidos pelo
    // producer idempotente) e a API continua respondendo. Quem publica (a DLT)
    // so commita o offset depois do ack, entao nada se perde: a mensagem de
    // origem e reprocessada quando o broker volta.
    void this.connect().catch((): void => undefined);
  }

  // onApplicationShutdown roda depois de todos os onModuleDestroy: o consumer
  // ja parou e terminou a mensagem em andamento quando chegamos aqui.
  async onApplicationShutdown(): Promise<void> {
    await this.producer.disconnect();
  }

  async send(message: OutboundMessage): Promise<void> {
    await this.connect();

    this.logger.log(
      `Sending message to topic ${message.topic} with key ${message.key}`,
    );
    await this.producer.send({
      topic: message.topic,
      messages: [
        {
          key: message.key,
          value: message.value,
          headers: { ...message.headers },
        },
      ],
    });
    this.logger.log(
      `Message sent to topic ${message.topic} with key ${message.key}`,
    );
  }

  private async connect(): Promise<void> {
    if (this.isConnected) return;

    // Uma única tentativa em voo por vez: senão cada publicação pendente
    // dispara o seu próprio connect() em paralelo contra o mesmo broker.
    this.connecting ??= this.producer
      .connect()
      .then((): void => {
        this.isConnected = true;
        this.logger.log("Kafka producer conectado");
      })
      .catch((err: unknown): never => {
        const error = err instanceof Error ? err : new Error(String(err));
        this.logger.error("Falha ao conectar o Kafka producer", error, {
          service: KafkaProducerService.name,
          method: "connect",
        });
        throw err;
      })
      .finally((): void => {
        this.connecting = null;
      });

    await this.connecting;
  }
}
