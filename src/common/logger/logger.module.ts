import type { IncomingMessage, ServerResponse } from "node:http";
import { Global, Module, RequestMethod } from "@nestjs/common";
import { ConfigModule, ConfigService } from "@nestjs/config";
import { LoggerModule as PinoLoggerModule } from "nestjs-pino";
import {
  CORRELATION_ID_HEADER,
  resolveCorrelationId,
} from "@common/correlation/correlation-id";
import { type Env, readEnv } from "@config/env";
import { LOGGER_TOKEN } from "./logger.interface";
import { PinoLoggerService } from "./pino-logger.service";

type ExpressLike = IncomingMessage & { route?: { path?: string } };

@Global()
@Module({
  imports: [
    PinoLoggerModule.forRootAsync({
      imports: [ConfigModule],
      inject: [ConfigService],
      useFactory: (config: ConfigService<Env, true>) => ({
        forRoutes: [{ path: "*", method: RequestMethod.ALL }],
        pinoHttp: {
          level: readEnv(config, "LOG_LEVEL"),
          transport:
            readEnv(config, "NODE_ENV") !== "production"
              ? { target: "pino-pretty", options: { singleLine: true } }
              : undefined,
          // O id da requisicao e o correlationId: vem do header (se valido)
          // ou e gerado, e volta no header da resposta.
          genReqId: (req: IncomingMessage, res: ServerResponse): string => {
            const correlationId = resolveCorrelationId(
              req.headers[CORRELATION_ID_HEADER],
            );
            res.setHeader(CORRELATION_ID_HEADER, correlationId);
            return correlationId;
          },
          autoLogging: {
            ignore: (req: IncomingMessage): boolean =>
              (req.url?.startsWith("/health") ?? false) || (req.url?.startsWith("/metrics") ?? false),
          },
          // quietReqLogger + reqId renomeado: o logger da requisicao (usado
          // pelo nestjs-pino em todo log do request) carrega so correlationId,
          // uma vez. Via customProps ele sairia duplicado na linha final.
          quietReqLogger: true,
          customAttributeKeys: {
            responseTime: "duration",
            reqId: "correlationId",
          },
          customLogLevel: (
            _req: IncomingMessage,
            res: ServerResponse,
            err: Error | undefined,
          ): "error" | "warn" | "info" => {
            if (res.statusCode >= 500 || err) return "error";
            if (res.statusCode >= 400) return "warn";
            return "info";
          },
          serializers: {
            req: (): Record<string, never> => ({}),
            res: (): Record<string, never> => ({}),
          },
          customProps: (req: IncomingMessage): Record<string, unknown> => {
            const r = req as ExpressLike;
            return { route: r.route?.path ?? req.url };
          },
          redact: {
            paths: ["req", "res"],
            remove: true,
          },
        },
      }),
    }),
  ],
  providers: [{ provide: LOGGER_TOKEN, useClass: PinoLoggerService }],
  exports: [LOGGER_TOKEN],
})
export class LoggerModule {}
