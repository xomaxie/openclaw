import { Type } from "@sinclair/typebox";
import type { AnyAgentTool, OpenClawPluginApi } from "../../src/plugins/types.js";
import { McpHttpClient, type McpToolDef } from "./src/mcp-http.js";

type PluginCfg = {
  url?: string;
  tokenEnv?: string;
  token?: string;
  autoExpose?: boolean;
  autoExposeRefreshMs?: number;
  toolsJsonEnv?: string;
};

type AutoExposeState = {
  tools: McpToolDef[];
  refreshing?: Promise<void>;
  lastRefreshMs: number;
};

const DEFAULT_TOOLS_JSON_ENV = "SENTINEL_MCP_TOOLS_JSON";
const DEFAULT_AUTO_EXPOSE_REFRESH_MS = 60_000;
const RESERVED_TOOL_NAMES = new Set(["sentinel_mcp_tools_list", "sentinel_mcp_tools_call"]);
const autoExposeState = new Map<string, AutoExposeState>();

function readStr(v: unknown): string | undefined {
  return typeof v === "string" && v.trim() ? v.trim() : undefined;
}

function readBool(v: unknown, fallback: boolean): boolean {
  if (typeof v === "boolean") return v;
  const s = readStr(v)?.toLowerCase();
  if (!s) return fallback;
  if (s === "1" || s === "true" || s === "yes" || s === "on") return true;
  if (s === "0" || s === "false" || s === "no" || s === "off") return false;
  return fallback;
}

function readPositiveInt(v: unknown, fallback: number): number {
  if (typeof v === "number" && Number.isFinite(v) && v > 0) return Math.floor(v);
  const s = readStr(v);
  if (!s) return fallback;
  const parsed = Number.parseInt(s, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return parsed;
}

function normalizeToolDefs(tools: unknown): McpToolDef[] {
  if (!Array.isArray(tools)) return [];

  const dedup = new Map<string, McpToolDef>();
  for (const entry of tools) {
    if (!entry || typeof entry !== "object") continue;
    const obj = entry as Record<string, unknown>;
    const name = readStr(obj.name);
    if (!name || RESERVED_TOOL_NAMES.has(name)) continue;
    dedup.set(name, {
      name,
      description: readStr(obj.description),
      inputSchema: obj.inputSchema,
    });
  }

  return Array.from(dedup.values());
}

function parseToolsSnapshot(raw: string | undefined): McpToolDef[] {
  const source = readStr(raw);
  if (!source) return [];

  try {
    const parsed = JSON.parse(source) as unknown;
    if (Array.isArray(parsed)) return normalizeToolDefs(parsed);
    if (
      parsed &&
      typeof parsed === "object" &&
      Array.isArray((parsed as { tools?: unknown[] }).tools)
    ) {
      return normalizeToolDefs((parsed as { tools: unknown[] }).tools);
    }
  } catch {
    // Ignore malformed snapshots and rely on live refresh.
  }

  return [];
}

function resolveCallArgs(input: unknown): Record<string, unknown> {
  if (input && typeof input === "object" && !Array.isArray(input)) {
    return input as Record<string, unknown>;
  }
  return {};
}

function resolveToolParams(def: McpToolDef): Record<string, unknown> {
  if (def.inputSchema && typeof def.inputSchema === "object" && !Array.isArray(def.inputSchema)) {
    return def.inputSchema as Record<string, unknown>;
  }
  return Type.Object({}, { additionalProperties: true });
}

function getState(cacheKey: string, seedTools: McpToolDef[]): AutoExposeState {
  const existing = autoExposeState.get(cacheKey);
  if (existing) {
    if (existing.tools.length === 0 && seedTools.length > 0) {
      existing.tools = seedTools;
    }
    return existing;
  }

  const next: AutoExposeState = {
    tools: seedTools,
    lastRefreshMs: 0,
  };
  autoExposeState.set(cacheKey, next);
  return next;
}

function scheduleRefresh(opts: {
  client: McpHttpClient;
  state: AutoExposeState;
  refreshMs: number;
  logger: OpenClawPluginApi["logger"];
}) {
  const now = Date.now();
  if (opts.state.refreshing) return;
  if (now - opts.state.lastRefreshMs < opts.refreshMs) return;

  opts.state.refreshing = (async () => {
    try {
      const listed = await opts.client.listTools();
      opts.state.tools = normalizeToolDefs(listed);
      opts.state.lastRefreshMs = Date.now();
    } catch (err) {
      opts.logger.warn("[sentinel-mcp] tools/list refresh failed: " + String(err));
      opts.state.lastRefreshMs = Date.now();
    } finally {
      opts.state.refreshing = undefined;
    }
  })();
}

function toAutoTool(def: McpToolDef, client: McpHttpClient): AnyAgentTool {
  return {
    name: def.name,
    label: def.name,
    description: def.description || "Sentinel MCP tool: " + def.name,
    parameters: resolveToolParams(def),
    execute: async (_id, args) => {
      return await client.callTool(def.name, resolveCallArgs(args));
    },
  };
}

export default function register(api: OpenClawPluginApi) {
  api.registerTool(
    ((ctx) => {
      const cfg = (api.pluginConfig ?? {}) as PluginCfg;
      const url =
        readStr(cfg.url) || readStr(process.env.SENTINEL_MCP_URL) || "http://mcp:18790/mcp";
      const tokenEnv =
        readStr(cfg.tokenEnv) ||
        readStr(process.env.SENTINEL_MCP_TOKEN_ENV) ||
        "OPENCLAW_GATEWAY_TOKEN";
      const token = readStr(cfg.token) || readStr(process.env[tokenEnv]);
      const autoExpose = readBool(cfg.autoExpose ?? process.env.SENTINEL_MCP_AUTO_EXPOSE, true);
      const refreshMs = readPositiveInt(
        cfg.autoExposeRefreshMs ?? process.env.SENTINEL_MCP_AUTO_EXPOSE_REFRESH_MS,
        DEFAULT_AUTO_EXPOSE_REFRESH_MS,
      );
      const toolsJsonEnv = readStr(cfg.toolsJsonEnv) || DEFAULT_TOOLS_JSON_ENV;

      const client = new McpHttpClient({ url, token });
      const seedTools = parseToolsSnapshot(process.env[toolsJsonEnv]);
      const state = getState(url + "::" + (token || ""), seedTools);

      const toolsList: AnyAgentTool = {
        name: "sentinel_mcp_tools_list",
        label: "Sentinel MCP Tools",
        description: "List tools available from the Sentinel MCP gateway.",
        parameters: Type.Object({}),
        execute: async () => {
          const tools = await client.listTools();
          const normalized = normalizeToolDefs(tools);
          state.tools = normalized;
          state.lastRefreshMs = Date.now();
          return {
            content: [{ type: "text", text: JSON.stringify({ tools: normalized }, null, 2) }],
            details: { toolsCount: normalized.length, url },
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
          const a = resolveCallArgs(params.arguments);
          return await client.callTool(name, a);
        },
      };

      const out: AnyAgentTool[] = [toolsList, toolsCall];
      if (!autoExpose) {
        return out;
      }

      scheduleRefresh({ client, state, refreshMs, logger: api.logger });
      for (const def of state.tools) {
        out.push(toAutoTool(def, client));
      }

      return out;
    }) as unknown as (ctx: unknown) => AnyAgentTool | AnyAgentTool[] | null,
    {
      names: ["sentinel_mcp_tools_list", "sentinel_mcp_tools_call"],
    },
  );
}
