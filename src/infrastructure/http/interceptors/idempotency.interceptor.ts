import { createHash } from "node:crypto";
import {
  BadRequestException,
  type CallHandler,
  ConflictException,
  type ExecutionContext,
  HttpStatus,
  Inject,
  Injectable,
  type NestInterceptor,
  UnprocessableEntityException,
} from "@nestjs/common";
import { HTTP_CODE_METADATA } from "@nestjs/common/constants";
import { Reflector } from "@nestjs/core";
import type { Request, Response } from "express";
import {
  concatMap,
  defer,
  type Observable,
  of,
  switchMap,
  throwError,
  catchError,
  map,
} from "rxjs";
import { canonicalJson, toJsonValue } from "@common/json";
import { type ILogger, LOGGER_TOKEN } from "@common/logger/logger.interface";
import {
  type ClaimResult,
  IDEMPOTENCY_STORE,
  type IdempotencyStore,
} from "@infrastructure/idempotency/idempotency.store";

export const IDEMPOTENCY_KEY_HEADER = "idempotency-key";
export const IDEMPOTENT_REPLAYED_HEADER = "idempotent-replayed";

// ASCII imprimivel sem espaco, ate 255 chars (mesmo limite do CHECK no banco).
const IDEMPOTENCY_KEY_PATTERN = /^[\x21-\x7E]{1,255}$/;

// Header opcional: sem ele a rota se comporta como antes. Com ele:
// mesma chave + mesmo corpo => resposta original; corpo diferente => 422;
// primeira ainda em andamento => 409. So respostas de sucesso sao guardadas:
// erro libera a chave para uma nova tentativa.
@Injectable()
export class IdempotencyInterceptor implements NestInterceptor {
  constructor(
    @Inject(IDEMPOTENCY_STORE) private readonly store: IdempotencyStore,
    @Inject(LOGGER_TOKEN) private readonly logger: ILogger,
    private readonly reflector: Reflector,
  ) {}

  intercept(context: ExecutionContext, next: CallHandler): Observable<unknown> {
    const http = context.switchToHttp();
    const request = http.getRequest<Request>();
    const response = http.getResponse<Response>();

    const key = request.header(IDEMPOTENCY_KEY_HEADER);
    if (key === undefined) return next.handle();

    if (!IDEMPOTENCY_KEY_PATTERN.test(key)) {
      throw new BadRequestException(
        "Idempotency-Key deve ter de 1 a 255 caracteres ASCII visiveis",
      );
    }

    const body: unknown = request.body;
    const requestHash = hashRequest(request.method, request.originalUrl, body);
    const successStatus = this.successStatusFor(context, request.method);

    return defer(() => this.store.claim(key, requestHash, new Date())).pipe(
      switchMap((claim) =>
        this.resolveClaim(claim, key, successStatus, response, next),
      ),
    );
  }

  private resolveClaim(
    claim: ClaimResult,
    key: string,
    successStatus: number,
    response: Response,
    next: CallHandler,
  ): Observable<unknown> {
    switch (claim.kind) {
      case "mismatch":
        return throwError(
          () =>
            new UnprocessableEntityException(
              "Idempotency-Key ja usada com um corpo de requisicao diferente",
            ),
        );
      case "in_progress":
        return throwError(
          () =>
            new ConflictException(
              "Requisicao com esta Idempotency-Key ainda em processamento",
            ),
        );
      case "completed":
        // Replay fiel: o status gravado vence o default da rota.
        response.status(claim.responseStatus);
        response.setHeader(IDEMPOTENT_REPLAYED_HEADER, "true");
        return of(claim.responseBody);
      case "claimed":
        // Ordem importa: catchError cobre so o erro do handler (nada foi
        // criado => libera a chave). Falha ao gravar a resposta depois do
        // sucesso nao pode virar erro para o cliente: o item ja existe.
        return next.handle().pipe(
          catchError((err: unknown) =>
            defer(() => this.releaseQuietly(key, claim.lockedAt)).pipe(
              concatMap(() => throwError(() => err)),
            ),
          ),
          concatMap((result: unknown) =>
            defer(() =>
              this.completeQuietly(key, claim.lockedAt, successStatus, result),
            ).pipe(map(() => result)),
          ),
        );
    }
  }

  private async completeQuietly(
    key: string,
    lockedAt: Date,
    status: number,
    result: unknown,
  ): Promise<void> {
    try {
      const wasStored = await this.store.complete(
        key,
        lockedAt,
        status,
        toJsonValue(result),
      );
      if (!wasStored) {
        this.logger.warn("Chave de idempotencia perdeu a posse antes de concluir", {
          idempotencyKey: key,
        });
      }
    } catch (err) {
      // A chave fica em andamento ate o lock vencer; retentativas recebem 409
      // ate la. Preferivel a responder erro para uma criacao que aconteceu.
      this.logger.error(
        "Falha ao gravar resposta da chave de idempotencia",
        err instanceof Error ? err : new Error(String(err)),
        { idempotencyKey: key },
      );
    }
  }

  private async releaseQuietly(key: string, lockedAt: Date): Promise<void> {
    try {
      await this.store.release(key, lockedAt);
    } catch (err) {
      // Nao mascara o erro original; a chave expira pelo lock timeout.
      this.logger.error(
        "Falha ao liberar chave de idempotencia",
        err instanceof Error ? err : new Error(String(err)),
        { idempotencyKey: key },
      );
    }
  }

  private successStatusFor(context: ExecutionContext, method: string): number {
    const explicit = this.reflector.get<number | undefined>(
      HTTP_CODE_METADATA,
      context.getHandler(),
    );
    if (explicit !== undefined) return explicit;
    return method === "POST" ? HttpStatus.CREATED : HttpStatus.OK;
  }
}

// Metodo e URL entram no hash: a mesma chave em outra rota e outro pedido.
export function hashRequest(method: string, url: string, body: unknown): string {
  return createHash("sha256")
    .update(`${method} ${url}\n${canonicalJson(body)}`)
    .digest("hex");
}
