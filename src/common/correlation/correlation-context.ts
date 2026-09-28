import { PinoLogger } from "nestjs-pino";
import { Store, storage } from "nestjs-pino/storage";
import type { Logger } from "pino";

// Executa fn com um logger filho carregando correlationId no mesmo
// AsyncLocalStorage que o nestjs-pino usa por requisicao HTTP: todo log feito
// via PinoLogger/ILogger dentro de fn sai com o campo, sem passar parametro.
// Usado fora do ciclo HTTP (consumers Kafka).
export async function runWithCorrelationId<T>(
  correlationId: string,
  fn: () => Promise<T>,
): Promise<T> {
  // Parte do logger raiz (nao do contexto atual): chamadas aninhadas trocam o
  // correlationId em vez de repetir a chave. PinoLogger.root so existe depois
  // que o LoggerModule registra o middleware (o tipo declarado nao reflete
  // isso); sem ele, usa o contexto atual ou executa sem contexto.
  const holder: { readonly root?: Logger } = PinoLogger;
  const base = holder.root ?? storage.getStore()?.logger;
  if (!base) return fn();
  return storage.run(new Store(base.child({ correlationId })), fn);
}
