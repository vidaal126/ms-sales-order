export const LOGGER_TOKEN = Symbol("LOGGER");

export interface ILogger {
  log(message: string, context?: Record<string, unknown>): void;
  warn(message: string, context?: Record<string, unknown>): void;
  error(message: string, err: Error, context?: Record<string, unknown>): void;
  debug(message: string, context?: Record<string, unknown>): void;
}
