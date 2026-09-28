import { Injectable } from "@nestjs/common";
import { InjectPinoLogger, PinoLogger } from "nestjs-pino";
import type { ILogger } from "./logger.interface";

@Injectable()
export class PinoLoggerService implements ILogger {
  constructor(
    @InjectPinoLogger(PinoLoggerService.name)
    private readonly logger: PinoLogger,
  ) {}

  log(message: string, context?: Record<string, unknown>): void {
    this.logger.info(context ?? {}, message);
  }

  warn(message: string, context?: Record<string, unknown>): void {
    this.logger.warn(context ?? {}, message);
  }

  error(message: string, err: Error, context?: Record<string, unknown>): void {
    this.logger.error({ err, ...context }, message);
  }

  debug(message: string, context?: Record<string, unknown>): void {
    this.logger.debug(context ?? {}, message);
  }
}
