import {
  BadRequestException,
  type CallHandler,
  ConflictException,
  type ExecutionContext,
  UnprocessableEntityException,
} from "@nestjs/common";
import { Reflector } from "@nestjs/core";
import { lastValueFrom, of, throwError } from "rxjs";
import type { ILogger } from "@common/logger/logger.interface";
import type { JsonValue } from "@common/json";
import type {
  ClaimResult,
  IdempotencyStore,
} from "@infrastructure/idempotency/idempotency.store";
import { hashRequest, IdempotencyInterceptor } from "./idempotency.interceptor";

class FakeStore implements IdempotencyStore {
  claimResult: ClaimResult = { kind: "claimed", lockedAt: new Date(0) };
  readonly claims: Array<{ key: string; hash: string }> = [];
  readonly completed: Array<{ key: string; status: number; body: JsonValue }> = [];
  readonly released: string[] = [];
  completeError: Error | undefined;

  async claim(key: string, requestHash: string): Promise<ClaimResult> {
    this.claims.push({ key, hash: requestHash });
    return this.claimResult;
  }

  async complete(
    key: string,
    _lockedAt: Date,
    status: number,
    body: JsonValue,
  ): Promise<boolean> {
    if (this.completeError) throw this.completeError;
    this.completed.push({ key, status, body });
    return true;
  }

  async release(key: string): Promise<void> {
    this.released.push(key);
  }

  async deleteExpired(): Promise<number> {
    return 0;
  }
}

interface FakeHttp {
  context: ExecutionContext;
  responseHeaders: Record<string, string>;
  responseStatus: { value: number | undefined };
}

function httpContext(headers: Record<string, string>, body: unknown): FakeHttp {
  const responseHeaders: Record<string, string> = {};
  const responseStatus: { value: number | undefined } = { value: undefined };
  const request = {
    method: "POST",
    originalUrl: "/items",
    body,
    header: (name: string): string | undefined => headers[name.toLowerCase()],
  };
  const response = {
    status: (code: number): void => {
      responseStatus.value = code;
    },
    setHeader: (name: string, value: string): void => {
      responseHeaders[name] = value;
    },
  };
  const context: Pick<ExecutionContext, "switchToHttp" | "getHandler"> = {
    switchToHttp: () => ({
      getRequest: <T>() => request as T,
      getResponse: <T>() => response as T,
      getNext: <T>() => (() => undefined) as T,
    }),
    getHandler: () => function handler(): void {},
  };
  return { context: context as ExecutionContext, responseHeaders, responseStatus };
}

const silentLogger: ILogger = {
  log: () => undefined,
  warn: () => undefined,
  error: () => undefined,
  debug: () => undefined,
};

describe("IdempotencyInterceptor", () => {
  let store: FakeStore;
  let interceptor: IdempotencyInterceptor;
  const handlerCalls = { count: 0 };
  const handler = (result: unknown): CallHandler => ({
    handle: () => {
      handlerCalls.count++;
      return of(result);
    },
  });
  const body = { sku: "BOX-001", name: "Caixa" };

  beforeEach(() => {
    store = new FakeStore();
    interceptor = new IdempotencyInterceptor(store, silentLogger, new Reflector());
    handlerCalls.count = 0;
  });

  it("sem header: executa normalmente e nao toca no store", async () => {
    const { context } = httpContext({}, body);

    await expect(lastValueFrom(interceptor.intercept(context, handler("ok")))).resolves.toBe("ok");
    expect(store.claims).toHaveLength(0);
  });

  it("chave invalida: 400", () => {
    const { context } = httpContext({ "idempotency-key": "com espaco" }, body);

    expect(() => interceptor.intercept(context, handler("ok"))).toThrow(BadRequestException);
  });

  it("chave nova: executa e guarda status 201 e corpo", async () => {
    const { context } = httpContext({ "idempotency-key": "k1" }, body);

    const result = await lastValueFrom(interceptor.intercept(context, handler({ id: "1" })));

    expect(result).toEqual({ id: "1" });
    expect(store.claims).toEqual([{ key: "k1", hash: hashRequest("POST", "/items", body) }]);
    expect(store.completed).toEqual([{ key: "k1", status: 201, body: { id: "1" } }]);
  });

  it("mesmo corpo com chaves em outra ordem gera o mesmo hash", () => {
    expect(hashRequest("POST", "/items", { a: 1, b: 2 })).toBe(
      hashRequest("POST", "/items", { b: 2, a: 1 }),
    );
  });

  it("concluida: devolve a resposta original sem executar o handler", async () => {
    store.claimResult = { kind: "completed", responseStatus: 201, responseBody: { id: "1" } };
    const { context, responseHeaders, responseStatus } = httpContext(
      { "idempotency-key": "k1" },
      body,
    );

    const result = await lastValueFrom(interceptor.intercept(context, handler({ id: "2" })));

    expect(result).toEqual({ id: "1" });
    expect(handlerCalls.count).toBe(0);
    expect(responseHeaders["idempotent-replayed"]).toBe("true");
    expect(responseStatus.value).toBe(201);
  });

  it("corpo diferente: 422", async () => {
    store.claimResult = { kind: "mismatch" };
    const { context } = httpContext({ "idempotency-key": "k1" }, body);

    await expect(lastValueFrom(interceptor.intercept(context, handler("ok")))).rejects.toBeInstanceOf(
      UnprocessableEntityException,
    );
    expect(handlerCalls.count).toBe(0);
  });

  it("em andamento: 409", async () => {
    store.claimResult = { kind: "in_progress" };
    const { context } = httpContext({ "idempotency-key": "k1" }, body);

    await expect(lastValueFrom(interceptor.intercept(context, handler("ok")))).rejects.toBeInstanceOf(
      ConflictException,
    );
  });

  it("erro no handler: libera a chave e repassa o erro original", async () => {
    const { context } = httpContext({ "idempotency-key": "k1" }, body);
    const failure = new Error("falhou");
    const failing: CallHandler = { handle: () => throwError(() => failure) };

    await expect(lastValueFrom(interceptor.intercept(context, failing))).rejects.toBe(failure);
    expect(store.released).toEqual(["k1"]);
    expect(store.completed).toHaveLength(0);
  });

  it("falha ao gravar a resposta nao vira erro nem libera a chave", async () => {
    store.completeError = new Error("banco indisponivel");
    const { context } = httpContext({ "idempotency-key": "k1" }, body);

    const result = await lastValueFrom(interceptor.intercept(context, handler({ id: "1" })));

    expect(result).toEqual({ id: "1" });
    expect(store.released).toHaveLength(0);
  });
});
