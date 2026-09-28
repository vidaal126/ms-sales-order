export const DEAD_LETTER_PORT = Symbol("DEAD_LETTER_PORT");

// Motivos de erro nao recuperavel: a mensagem nunca vai dar certo, entao vai
// para a DLT e o offset e commitado.
export type DeadLetterReason =
  | "invalid_json"
  | "schema_validation_failed"
  | "unsupported_schema_version"
  | "domain_invariant_violation";

// Mensagem de origem como chegou: bytes e headers intactos mais a posicao.
export interface DeadLetterSource {
  readonly topic: string;
  readonly partition: number;
  readonly offset: string;
  readonly timestamp: string;
  readonly key: string | null;
  readonly value: Buffer | null;
  readonly headers: Readonly<Record<string, string>>;
}

export interface DeadLetterPort {
  // Garante que a DLT do topico de origem existe. Idempotente.
  ensureTopic(sourceTopic: string): Promise<void>;
  // Resolve somente depois do ack do broker; falha propaga (sem DLT gravada
  // o offset nao pode ser commitado).
  // consumerGroup: group que falhou (header dlt-consumer-group).
  publish(
    source: DeadLetterSource,
    reason: DeadLetterReason,
    detail: string,
    consumerGroup: string,
  ): Promise<void>;
}
