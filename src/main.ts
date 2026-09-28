import type { INestApplication } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { NestFactory } from "@nestjs/core";
import type { NestExpressApplication } from "@nestjs/platform-express";
import { Logger } from "nestjs-pino";
import { type Env, readEnv } from "@config/env";
import { AppModule } from "./app.module";
import { configureApp } from "./app.setup";

const SHUTDOWN_SIGNALS: readonly NodeJS.Signals[] = ["SIGTERM", "SIGINT"];

// HTTP (pedidos e health checks); os consumers de replica e o publisher do
// outbox sobem com o ciclo de vida dos modulos.
async function bootstrap(): Promise<void> {
  // abortOnError: false - erro de inicializacao (ex.: env invalida) sobe para
  // o catch de bootstrap, que imprime a mensagem e sai com codigo 1.
  const app = await NestFactory.create<NestExpressApplication>(AppModule, {
    bufferLogs: true,
    abortOnError: false,
  });

  app.useLogger(app.get(Logger));
  configureApp(app);

  const config = app.get<ConfigService<Env, true>>(ConfigService);
  const port = readEnv(config, "PORT");

  registerGracefulShutdown(app, readEnv(config, "SHUTDOWN_TIMEOUT_MS"));

  await app.listen(port);

  app.get(Logger).log(`ms-sales-order: HTTP na porta ${port}`);
}

// Substitui app.enableShutdownHooks() para impor um teto de tempo: app.close()
// dispara onModuleDestroy (consumers param e aguardam a mensagem em
// andamento; o outbox termina o ciclo), fecha o servidor HTTP e so entao
// onApplicationShutdown (desconecta producer e Prisma). Estourado o teto, o
// processo sai com erro.
function registerGracefulShutdown(app: INestApplication, timeoutMs: number): void {
  const logger = app.get(Logger);
  let isShuttingDown = false;

  const shutdown = async (signal: NodeJS.Signals): Promise<void> => {
    if (isShuttingDown) return;
    isShuttingDown = true;
    logger.log(`${signal} recebido, encerrando (timeout ${timeoutMs}ms)`);

    const forceExit = setTimeout((): void => {
      logger.error(`Shutdown excedeu ${timeoutMs}ms, encerrando a forca`);
      process.exit(1);
    }, timeoutMs);
    forceExit.unref();

    let exitCode = 0;
    try {
      await app.close();
      logger.log("Shutdown concluido");
    } catch (err) {
      logger.error("Falha no shutdown", err instanceof Error ? err.stack : String(err));
      exitCode = 1;
    } finally {
      clearTimeout(forceExit);
    }
    // Saida explicita: um handle esquecido (socket, timer) nao pode deixar o
    // processo vivo depois do shutdown.
    process.exit(exitCode);
  };

  for (const signal of SHUTDOWN_SIGNALS) {
    process.once(signal, (): void => {
      void shutdown(signal);
    });
  }
}

bootstrap().catch((err: unknown): void => {
  // Env invalida cai aqui antes do logger existir: mensagem direta no stderr.
  process.stderr.write(
    `Falha ao iniciar ms-sales-order: ${err instanceof Error ? err.message : String(err)}\n`,
  );
  process.exit(1);
});
