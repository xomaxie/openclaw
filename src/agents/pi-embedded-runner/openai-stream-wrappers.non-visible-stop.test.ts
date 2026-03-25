import { describe, expect, it } from "vitest";
import type { StreamFn } from "@mariozechner/pi-agent-core";
import { createOpenAINonVisibleStopGuardWrapper } from "./openai-stream-wrappers.js";

describe("createOpenAINonVisibleStopGuardWrapper", () => {
  it("turns thinking-only stop completions into error events", async () => {
    const base: StreamFn = (() => {
      async function* gen() {
        yield {
          type: "done",
          reason: "stop",
          message: {
            role: "assistant",
            content: [{ type: "thinking", thinking: "let me inspect more files" }],
            stopReason: "stop",
            api: "openai-completions",
            provider: "opencode-go",
            model: "kimi-k2.5",
            usage: {
              input: 0,
              output: 0,
              cacheRead: 0,
              cacheWrite: 0,
              totalTokens: 0,
              cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
            },
            timestamp: Date.now(),
          },
        } as any;
      }
      return gen() as any;
    }) as any;

    const wrapped = createOpenAINonVisibleStopGuardWrapper(base);
    const events: any[] = [];
    for await (const event of wrapped(
      { api: "openai-completions", provider: "opencode-go", id: "kimi-k2.5" } as any,
      { messages: [] } as any,
      undefined,
    )) {
      events.push(event);
    }

    expect(events).toHaveLength(1);
    expect(events[0].type).toBe("error");
    expect(events[0].error.errorMessage).toContain("non-visible assistant completion");
  });
});
