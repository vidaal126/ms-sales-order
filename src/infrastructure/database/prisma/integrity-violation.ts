import { z } from "zod";
import { Prisma } from "@infrastructure/database/generated/client";

// Codigos do Prisma para dado rejeitado pelo schema do banco.
// P2000: valor maior que a coluna; P2004: constraint violada;
// P2011: null em coluna NOT NULL; P2020: valor fora do range do tipo.
const PRISMA_INTEGRITY_CODES = new Set(["P2000", "P2004", "P2011", "P2020"]);

// SQLSTATE do Postgres: classe 22 (data exception, ex. 22003 numeric out of
// range) e 23 (integrity constraint, ex. 23514 check violation).
const SQLSTATE_INTEGRITY_CLASSES = ["22", "23"];

// $executeRaw via driver adapter: P2010 com o SQLSTATE original no meta.
const driverAdapterMetaSchema = z.object({
  driverAdapterError: z.object({
    cause: z.object({ originalCode: z.string() }),
  }),
});

// true = o banco rejeitou o DADO: repetir nunca vai funcionar. Qualquer outro
// erro (conexao recusada, timeout, pool esgotado, AggregateError do adapter)
// e tratado como transitorio pelo chamador.
export function isIntegrityViolation(err: unknown): boolean {
  if (!(err instanceof Prisma.PrismaClientKnownRequestError)) return false;
  if (PRISMA_INTEGRITY_CODES.has(err.code)) return true;

  const meta = driverAdapterMetaSchema.safeParse(err.meta);
  if (!meta.success) return false;
  const sqlState = meta.data.driverAdapterError.cause.originalCode;
  return SQLSTATE_INTEGRITY_CLASSES.some((sqlClass) => sqlState.startsWith(sqlClass));
}
