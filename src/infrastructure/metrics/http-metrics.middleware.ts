import { Injectable, type NestMiddleware } from "@nestjs/common";
import type { NextFunction, Request, Response } from "express";
import { MetricsService } from "./metrics.service";

// Mede cada requisicao pelo template da rota (req.route.path, ex.:
// /transport-types/:id), nunca pelo path concreto: cardinalidade limitada.
@Injectable()
export class HttpMetricsMiddleware implements NestMiddleware {
  constructor(private readonly metrics: MetricsService) {}

  use(req: Request, res: Response, next: NextFunction): void {
    const startedAt = process.hrtime.bigint();
    res.once("finish", () => {
      const seconds = Number(process.hrtime.bigint() - startedAt) / 1e9;
      this.metrics.observeHttp(req.method, routeTemplate(req), res.statusCode, seconds);
    });
    next();
  }
}

function routeTemplate(req: Request): string {
  const route: unknown = req.route;
  if (typeof route === "object" && route !== null && "path" in route && typeof route.path === "string") {
    return `${req.baseUrl}${route.path}`;
  }
  return "unmatched";
}
