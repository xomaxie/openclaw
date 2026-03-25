import { describe, expect, it } from "vitest";
import { buildAssistantMessageFromResponse } from "./openai-ws-stream.js";

describe("buildAssistantMessageFromResponse", () => {
  it("can surface an empty assistant completion shape from the provider", () => {
    const msg = buildAssistantMessageFromResponse(
      {
        output: [],
        usage: { input_tokens: 1, output_tokens: 0, total_tokens: 1 },
      } as any,
      { api: "openai-completions", provider: "opencode-go", id: "kimi-k2.5" },
    );

    expect(msg.content).toEqual([]);
    expect(msg.stopReason).toBe("stop");
  });

  it("treats reasoning-only responses as having no visible assistant content", () => {
    const msg = buildAssistantMessageFromResponse(
      {
        output: [
          {
            type: "reasoning",
            id: "rs_123",
            summary: [],
          },
        ],
        usage: { input_tokens: 1, output_tokens: 0, total_tokens: 1 },
      } as any,
      { api: "openai-completions", provider: "opencode-go", id: "kimi-k2.5" },
    );

    expect(msg.content).toEqual([]);
    expect(msg.stopReason).toBe("stop");
  });
});
