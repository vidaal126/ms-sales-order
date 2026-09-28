import { Prisma } from "@infrastructure/database/generated/client";
import { isIntegrityViolation } from "./integrity-violation";

const known = (code: string, meta?: Record<string, unknown>): Prisma.PrismaClientKnownRequestError =>
  new Prisma.PrismaClientKnownRequestError("erro", { code, clientVersion: "test", meta });

// Formato observado com @prisma/adapter-pg em $executeRaw.
const rawFailure = (sqlState: string): Prisma.PrismaClientKnownRequestError =>
  known("P2010", { driverAdapterError: { name: "DriverAdapterError", cause: { originalCode: sqlState } } });

describe("isIntegrityViolation", () => {
  it.each(["P2000", "P2004", "P2011", "P2020"])("codigo Prisma %s: dado rejeitado", (code) => {
    expect(isIntegrityViolation(known(code))).toBe(true);
  });

  it.each(["23514", "23502", "22003"])("SQLSTATE %s via driver adapter: dado rejeitado", (sqlState) => {
    expect(isIntegrityViolation(rawFailure(sqlState))).toBe(true);
  });

  it.each([
    ["conexao recusada (codigo observado com o banco fora)", known("ECONNREFUSED")],
    ["pool esgotado", known("P2024")],
    ["SQLSTATE de conexao", rawFailure("08006")],
    ["P2010 sem meta", known("P2010")],
    ["AggregateError do adapter em transacao", new AggregateError([], "")],
    ["erro qualquer", new Error("x")],
  ])("%s: transitorio", (_label, err) => {
    expect(isIntegrityViolation(err)).toBe(false);
  });
});
