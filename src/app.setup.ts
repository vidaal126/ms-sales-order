import { ValidationPipe } from "@nestjs/common";
import type { NestExpressApplication } from "@nestjs/platform-express";
import helmet from "helmet";

// O ms-sales-order so e alcancado pelo ms-gateway, que anexa o IP do cliente
// ao X-Forwarded-For. Confiar em exatamente 1 salto faz o throttler contar por
// cliente (e nao pelo IP do gateway) sem aceitar entradas forjadas a esquerda.
const TRUSTED_PROXY_HOPS = 1;

// Configuracao HTTP compartilhada entre o bootstrap (main.ts) e os testes de
// integracao, para que os testes exercitem a mesma aplicacao.
export function configureApp(app: NestExpressApplication): void {
  app.use(helmet());
  app.set("trust proxy", TRUSTED_PROXY_HOPS);
  app.useGlobalPipes(
    new ValidationPipe({
      transform: true,
      whitelist: true,
      forbidNonWhitelisted: true,
      transformOptions: { enableImplicitConversion: false },
    }),
  );
}
