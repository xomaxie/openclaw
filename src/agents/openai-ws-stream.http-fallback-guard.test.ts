import { describe, expect, it } from "vitest";

describe("openai ws stream fallback guard", () => {
  it("documents that HTTP fallback may emit done events with only thinking blocks", () => {
    const doneEvent = {
      type: "done",
      reason: "stop",
      message: {
        role: "assistant",
        content: [{ type: "thinking", thinking: "Let me inspect more files" }],
        stopReason: "stop",
      },
    } as const;

    expect(doneEvent.message.content.every((block) => block.type === "thinking")).toBe(true);
  });
});
