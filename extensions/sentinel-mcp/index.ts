import { Type } from "@sinclair/typebox";
import type { AnyAgentTool, OpenClawPluginApi } from "../../src/plugins/types.js";
import { McpHttpClient } from "./src/mcp-http.js";

type PluginCfg = {
  url?: string;
  tokenEnv?: string;
  token?: string;
};

function readStr(v: unknown): string | undefined {
  return typeof v === "string" && v.trim() ? v.trim() : undefined;
}

export default function register(api: OpenClawPluginApi) {
  api.registerTool(
    ((ctx) => {
      const cfg = (api.pluginConfig ?? {}) as PluginCfg;
      const url = readStr(cfg.url) || readStr(process.env.SENTINEL_MCP_URL) || "http://mcp:18790/mcp";
      const tokenEnv = readStr(cfg.tokenEnv) || readStr(process.env.SENTINEL_MCP_TOKEN_ENV) || "OPENCLAW_GATEWAY_TOKEN";
      const token = readStr(cfg.token) || readStr(process.env[tokenEnv]);

      const client = new McpHttpClient({ url, token });

      const toolsList: AnyAgentTool = {
        name: "sentinel_mcp_tools_list",
        label: "Sentinel MCP Tools",
        description: "List tools available from the Sentinel MCP gateway.",
        parameters: Type.Object({}),
        execute: async () => {
          const tools = await client.listTools();
          return {
            content: [{ type: "text", text: JSON.stringify({ tools }, null, 2) }],
            details: { toolsCount: tools.length, url },
          };
        },
      };

      const toolsCall: AnyAgentTool = {
        name: "sentinel_mcp_tools_call",
        label: "Sentinel MCP Call",
        description: "Call a tool on the Sentinel MCP gateway (name must match tools/list output).",
        parameters: Type.Object({
          name: Type.String({ description: "Tool name, e.g. foundry.systemd_status" }),
          arguments: Type.Optional(
            Type.Object({}, { additionalProperties: true, description: "Tool arguments object" }),
          ),
        }),
        execute: async (_id, args) => {
          const params = args as Record<string, unknown>;
          const name = typeof params.name === "string" ? params.name.trim() : "";
          if (!name) {
            throw new Error("name required");
          }
          const a =
            params.arguments && typeof params.arguments === "object" && !Array.isArray(params.arguments)
              ? (params.arguments as Record<string, unknown>)
              : {};
          return await client.callTool(name, a);
        },
      };

      return [toolsList, toolsCall];
    }) as unknown as (ctx: unknown) => AnyAgentTool | AnyAgentTool[] | null,
    {
      names: ["sentinel_mcp_tools_list", "sentinel_mcp_tools_call"],
    },
  );
}