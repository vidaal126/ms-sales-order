import { createParamDecorator, type ExecutionContext } from "@nestjs/common";
import type { IncomingMessage } from "node:http";
import { resolveCorrelationId } from "@common/correlation/correlation-id";

// req.id e definido pelo genReqId do pino-http (LoggerModule) a partir do
// header X-Correlation-Id; o fallback so atua se o middleware nao rodou.
export const CorrelationId = createParamDecorator(
  (_data: unknown, context: ExecutionContext): string =>
    resolveCorrelationId(context.switchToHttp().getRequest<IncomingMessage>().id),
);
