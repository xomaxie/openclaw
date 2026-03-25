import { describe, expect, it } from "vitest";
import plugin from "./index.js";

describe("opencode-go plugin", () => {
  it("wraps kimi-k2.5 with Moonshot thinking compat", () => {
    let registeredProvider: any;
    plugin.register({
      registerProvider(provider: any) {
        registeredProvider = provider;
      },
    } as any);

    expect(registeredProvider).toBeTruthy();
    expect(typeof registeredProvider.wrapStreamFn).toBe("function");

    const baseStreamFn = (() => undefined) as any;
    const wrapped = registeredProvider.wrapStreamFn({
      modelId: "kimi-k2.5",
      extraParams: { thinking: { type: "enabled" } },
      thinkingLevel: "low",
      streamFn: baseStreamFn,
    });

    expect(typeof wrapped).toBe("function");
    expect(wrapped).not.toBe(baseStreamFn);
  });

  it("does not wrap non-kimi models", () => {
    let registeredProvider: any;
    plugin.register({
      registerProvider(provider: any) {
        registeredProvider = provider;
      },
    } as any);

    const baseStreamFn = (() => undefined) as any;
    const wrapped = registeredProvider.wrapStreamFn({
      modelId: "glm-5",
      extraParams: { thinking: { type: "enabled" } },
      thinkingLevel: "low",
      streamFn: baseStreamFn,
    });

    expect(wrapped).toBe(baseStreamFn);
  });
});
