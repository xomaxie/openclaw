import { describe, expect, test } from "vitest";
import register from "./index.js";

type RegisteredToolEntry = {
  tool: unknown;
  opts?: { name?: string; names?: string[]; optional?: boolean };
};

type PluginApiMock = {
  pluginConfig?: Record<string, unknown>;
  registerTool: (
    tool: unknown,
    opts?: { name?: string; names?: string[]; optional?: boolean },
  ) => void;
};

function createPluginApi(pluginConfig?: Record<string, unknown>): {
  api: PluginApiMock;
  registered: RegisteredToolEntry[];
} {
  const registered: RegisteredToolEntry[] = [];
  const api: PluginApiMock = {
    pluginConfig,
    registerTool: (tool, opts) => {
      registered.push({ tool, opts });
    },
  };
  return { api, registered };
}

function withSnapshot(raw: string, fn: () => void) {
  const previous = process.env.SENTINEL_MCP_TOOLS_JSON;
  process.env.SENTINEL_MCP_TOOLS_JSON = raw;
  try {
    fn();
  } finally {
    if (previous === undefined) {
      delete process.env.SENTINEL_MCP_TOOLS_JSON;
    } else {
      process.env.SENTINEL_MCP_TOOLS_JSON = previous;
    }
  }
}

describe("sentinel-mcp plugin", () => {
  test("auto-exposes MCP tools from SENTINEL_MCP_TOOLS_JSON as first-class tool names", () => {
    withSnapshot(
      JSON.stringify([
        {
          name: "firecrawl.firecrawl_search",
          description: "Search via Firecrawl",
          inputSchema: {
            type: "object",
            properties: {
              query: { type: "string" },
            },
            required: ["query"],
          },
        },
        {
          name: "searxng.searxng_web_search",
          description: "Search via SearXNG",
          inputSchema: {
            type: "object",
            properties: {
              query: { type: "string" },
            },
            required: ["query"],
          },
        },
      ]),
      () => {
        const { api, registered } = createPluginApi({ autoExpose: true });
        register(api as unknown as Parameters<typeof register>[0]);

        const toolFactory = registered[0]?.tool as
          | ((ctx: unknown) => Array<{ name: string }>)
          | undefined;
        expect(toolFactory).toBeTypeOf("function");

        const tools = toolFactory?.({}) ?? [];
        const names = tools.map((tool) => tool.name);

        expect(names).toContain("sentinel_mcp_tools_list");
        expect(names).toContain("sentinel_mcp_tools_call");
        expect(names).toContain("firecrawl.firecrawl_search");
        expect(names).toContain("searxng.searxng_web_search");
      },
    );
  });

  test("does not expose dynamic tools when autoExpose is disabled", () => {
    withSnapshot(
      JSON.stringify([
        {
          name: "firecrawl.firecrawl_search",
          description: "Search via Firecrawl",
          inputSchema: {
            type: "object",
            properties: {
              query: { type: "string" },
            },
            required: ["query"],
          },
        },
      ]),
      () => {
        const { api, registered } = createPluginApi({ autoExpose: false });
        register(api as unknown as Parameters<typeof register>[0]);

        const toolFactory = registered[0]?.tool as
          | ((ctx: unknown) => Array<{ name: string }>)
          | undefined;
        expect(toolFactory).toBeTypeOf("function");

        const tools = toolFactory?.({}) ?? [];
        const names = tools.map((tool) => tool.name);

        expect(names).toContain("sentinel_mcp_tools_list");
        expect(names).toContain("sentinel_mcp_tools_call");
        expect(names).not.toContain("firecrawl.firecrawl_search");
      },
    );
  });
});
