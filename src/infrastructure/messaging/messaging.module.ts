import { Module } from "@nestjs/common";
import { TerminusModule } from "@nestjs/terminus";
import type { Kafka } from "kafkajs";
import { DeadLetterPublisher } from "./dead-letter.publisher";
import { KafkaClientFactory } from "./kafka-client.factory";
import { KafkaHealthIndicator } from "./kafka.health";
import { KafkaProducerService } from "./kafka-producer.service";
import { KAFKA_CLIENT } from "./kafka.tokens";

// Modulo autocontido (config Kafka, client, producer, base de consumer,
// DLT, envelope e health), copia do mesmo modulo do ms-catalog. Depende apenas de @common
// e das variaveis KAFKA_BROKER, KAFKA_CLIENT_ID e HEALTH_CHECK_TIMEOUT_MS do
// servico.
@Module({
  imports: [TerminusModule],
  providers: [
    KafkaClientFactory,
    {
      provide: KAFKA_CLIENT,
      useFactory: (factory: KafkaClientFactory): Kafka => factory.create(),
      inject: [KafkaClientFactory],
    },
    KafkaProducerService,
    DeadLetterPublisher,
    KafkaHealthIndicator,
  ],
  exports: [KAFKA_CLIENT, KafkaProducerService, DeadLetterPublisher, KafkaHealthIndicator],
})
export class MessagingModule {}
