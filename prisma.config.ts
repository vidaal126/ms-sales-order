import "dotenv/config";
import { defineConfig } from "prisma/config";

export default defineConfig({
  schema: "src/infrastructure/database/prisma",
  migrations: {
    path: "src/infrastructure/database/prisma/migrations",
  },
  datasource: {
    url: process.env["DATABASE_URL"],
    // So para `prisma migrate dev` (autoria de migration): o usuario do servico
    // nao cria databases, entao a shadow database e provisionada a parte.
    shadowDatabaseUrl: process.env["SHADOW_DATABASE_URL"],
  },
});
