import { describe, expect, it } from "vitest";
import { extractAssistantText } from "./tools/sessions-helpers.js";

describe("subagent output selection prerequisites", () => {
  it("currently extracts visible text even from assistant messages that also contain tool calls", () => {
    const text = extractAssistantText({
      role: "assistant",
      stopReason: "toolUse",
      content: [
        { type: "text", text: "I'll inspect the codebase first." },
        { type: "toolCall", id: "t1", name: "exec", arguments: {} },
      ],
    });

    expect(text).toBe("I'll inspect the codebase first.");
  });
});
