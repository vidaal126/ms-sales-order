import { type INestApplication, ValidationPipe } from "@nestjs/common";
import helmet from "helmet";

// Configuracao HTTP compartilhada entre o bootstrap (main.ts) e os testes de
// integracao, para que os testes exercitem a mesma aplicacao.
export function configureApp(app: INestApplication): void {
  app.use(helmet());
  app.useGlobalPipes(
    new ValidationPipe({
      transform: true,
      whitelist: true,
      forbidNonWhitelisted: true,
      transformOptions: { enableImplicitConversion: false },
    }),
  );
}
