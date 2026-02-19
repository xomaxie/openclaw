import { describe, expect, it } from "vitest";
import { normalizeToolParameters } from "./pi-tools.schema.js";
import type { AnyAgentTool } from "./pi-tools.types.js";

function makeTool(parameters: Record<string, unknown>): AnyAgentTool {
  return {
    name: "firecrawl.firecrawl_search",
    label: "firecrawl.firecrawl_search",
    description: "test tool",
    parameters,
    execute: async () => ({ content: [], details: {} }),
  } as AnyAgentTool;
}

describe("normalizeToolParameters", () => {
  it("removes top-level $schema so draft-specific refs do not break validation", () => {
    const tool = makeTool({
      $schema: "https://json-schema.org/draft/2020-12/schema",
      type: "object",
      properties: {
        query: { type: "string" },
      },
      required: ["query"],
      additionalProperties: false,
    });

    const normalized = normalizeToolParameters(tool, { modelProvider: "openai" });
    const schema = normalized.parameters as Record<string, unknown>;

    expect(schema.$schema).toBeUndefined();
    expect(schema.type).toBe("object");

    const properties = schema.properties as Record<string, unknown>;
    const query = properties.query as Record<string, unknown>;
    expect(query.type).toBe("string");
  });
});
