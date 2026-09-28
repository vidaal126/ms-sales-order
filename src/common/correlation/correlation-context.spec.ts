import pino from "pino";
import { Store, storage } from "nestjs-pino/storage";
import { runWithCorrelationId } from "./correlation-context";

describe("runWithCorrelationId", () => {
  const base = pino({ level: "silent" });
  const inBaseContext = <T>(fn: () => Promise<T>): Promise<T> =>
    storage.run(new Store(base), fn);

  it("expoe um logger filho com correlationId durante a execucao", async () => {
    const bindings = await inBaseContext(() =>
      runWithCorrelationId("corr-1", async () => storage.getStore()?.logger.bindings()),
    );

    expect(bindings).toMatchObject({ correlationId: "corr-1" });
  });

  it("propaga o resultado e o erro de fn", async () => {
    await expect(inBaseContext(() => runWithCorrelationId("c", async () => 7))).resolves.toBe(7);
    await expect(
      inBaseContext(() =>
        runWithCorrelationId("c", async () => {
          throw new Error("x");
        }),
      ),
    ).rejects.toThrow("x");
  });

  it("sem logger base (antes do bootstrap) apenas executa fn", async () => {
    await expect(runWithCorrelationId("c", async () => "ok")).resolves.toBe("ok");
  });
});
