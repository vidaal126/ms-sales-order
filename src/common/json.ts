import { z } from "zod";

export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  // Chaves opcionais: mesmo formato do JsonObject do Prisma, para que valores
  // lidos de colunas Json sejam atribuiveis sem conversao.
  | { [key in string]?: JsonValue };

const jsonSchema = z.json();

// Converte um valor arbitrario (ex.: corpo de resposta) no JSON que de fato
// seria serializado, validando o resultado em vez de confiar em cast.
export function toJsonValue(value: unknown): JsonValue {
  // JSON.stringify devolve undefined (apesar do tipo) para estes valores.
  if (isUnserializable(value)) return null;
  const parsed: unknown = JSON.parse(JSON.stringify(value));
  return jsonSchema.parse(parsed);
}

// Serializacao deterministica: mesmas chaves em ordem diferente geram a mesma
// string. Usada para comparar corpos de requisicao.
export function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map(canonicalJson).join(",")}]`;
  }
  if (value !== null && typeof value === "object") {
    const entries = Object.entries(value)
      .filter(([, v]) => v !== undefined)
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
    return `{${entries.map(([k, v]) => `${JSON.stringify(k)}:${canonicalJson(v)}`).join(",")}}`;
  }
  if (isUnserializable(value)) return "null";
  return JSON.stringify(value);
}

function isUnserializable(value: unknown): boolean {
  return (
    value === undefined ||
    typeof value === "function" ||
    typeof value === "symbol"
  );
}
