import {
  type ArgumentsHost,
  Catch,
  type ExceptionFilter,
  HttpException,
  HttpStatus,
  Inject,
} from "@nestjs/common";
import type { Response } from "express";
import { type ILogger, LOGGER_TOKEN } from "@common/logger/logger.interface";
import {
  DomainError,
  EntityConflictError,
  EntityNotFoundError,
  InvariantViolationError,
} from "@domain/errors/domain.error";

export interface ErrorResponseBody {
  readonly statusCode: number;
  readonly error: string;
  readonly message: string | string[];
}

// Ponto unico de traducao erro -> HTTP. Erros de dominio viram 404/409/422,
// HttpException (400 do ValidationPipe, 429 do throttler, 503 do health)
// passa como esta, e qualquer outra coisa vira 500 generico: o detalhe fica
// so no log.
@Catch()
export class GlobalExceptionFilter implements ExceptionFilter {
  constructor(@Inject(LOGGER_TOKEN) private readonly logger: ILogger) {}

  catch(exception: unknown, host: ArgumentsHost): void {
    const response = host.switchToHttp().getResponse<Response>();

    if (exception instanceof HttpException) {
      const { statusCode, body } = fromHttpException(exception);
      response.status(statusCode).json(body);
      return;
    }

    const body = this.toBody(exception);
    response.status(body.statusCode).json(body);
  }

  private toBody(exception: unknown): ErrorResponseBody {
    if (exception instanceof DomainError) {
      return {
        statusCode: statusForDomainError(exception),
        error: exception.name,
        message: exception.message,
      };
    }

    this.logger.error(
      "Erro nao tratado na requisicao HTTP",
      exception instanceof Error ? exception : new Error(String(exception)),
    );
    return {
      statusCode: HttpStatus.INTERNAL_SERVER_ERROR,
      error: "Internal Server Error",
      message: "Erro interno",
    };
  }
}

function statusForDomainError(error: DomainError): HttpStatus {
  if (error instanceof EntityNotFoundError) return HttpStatus.NOT_FOUND;
  if (error instanceof EntityConflictError) return HttpStatus.CONFLICT;
  if (error instanceof InvariantViolationError) {
    return HttpStatus.UNPROCESSABLE_ENTITY;
  }
  return HttpStatus.BAD_REQUEST;
}

// Resposta de HttpException sai como foi construida (inclusive corpos sem
// "message", como o 503 do terminus com status por dependencia).
function fromHttpException(
  exception: HttpException,
): { statusCode: number; body: object } {
  const statusCode = exception.getStatus();
  const raw = exception.getResponse();

  if (typeof raw === "object") {
    return { statusCode, body: raw };
  }

  return {
    statusCode,
    body: { statusCode, error: exception.name, message: raw },
  };
}

