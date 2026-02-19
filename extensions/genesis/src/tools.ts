import { Type } from "@sinclair/typebox";
import type { AnyAgentTool, OpenClawConfig, OpenClawPluginApi } from "openclaw/plugin-sdk";

function readStr(v: unknown): string | undefined {
  return typeof v === "string" && v.trim() ? v.trim() : undefined;
}

function resolveGenesisBaseUrl(cfg: OpenClawConfig): string {
  const cfgAny = cfg as unknown as { channels?: { genesis?: { baseUrl?: string } } };
  const fromCfg = readStr(cfgAny.channels?.genesis?.baseUrl);
  const fromEnv = readStr(process.env.GENESIS_BRIDGE_BASE_URL);
  return fromCfg || fromEnv || "http://genesis-core:18400";
}

async function readJson(res: Response): Promise<{ text: string; json: any | null }> {
  const text = await res.text();
  try {
    return { text, json: text ? JSON.parse(text) : null };
  } catch {
    return { text, json: null };
  }
}

async function postJson(url: string, body: unknown): Promise<any> {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body ?? {}),
  });
  const { text, json } = await readJson(res);
  if (!res.ok) {
    throw new Error(`GENESIS HTTP ${res.status} ${url}: ${text.slice(0, 200)}`);
  }
  return json ?? { text };
}

async function getJson(url: string): Promise<any> {
  const res = await fetch(url, { method: 'GET' });
  const { text, json } = await readJson(res);
  if (!res.ok) {
    throw new Error(`GENESIS HTTP ${res.status} ${url}: ${text.slice(0, 200)}`);
  }
  return json ?? { text };
}

function defaultAgentForType(type: string): string {
  switch (type) {
    case "ui":
      return "ui_designer";
    case "docs":
      return "doc_writer";
    case "research":
      return "doc_writer";
    case "ops":
      return "backend_dev";
    case "backend":
    default:
      return "backend_dev";
  }
}

const CreateTaskParams = Type.Object(
  {
    title: Type.String({ description: "Task title" }),
    type: Type.Union(
      [
        Type.Literal("backend"),
        Type.Literal("ui"),
        Type.Literal("docs"),
        Type.Literal("research"),
        Type.Literal("ops"),
      ],
      { description: "Task type" },
    ),
    priority: Type.Optional(Type.Union([Type.Literal("low"), Type.Literal("normal"), Type.Literal("high")])),
    agent_id: Type.Optional(Type.String({ description: "Agent id (defaults based on type)" })),
    inputs: Type.Optional(
      Type.Object(
        {
          brief: Type.Optional(Type.String()),
          acceptance_criteria: Type.Optional(Type.Array(Type.String())),
          constraints: Type.Optional(Type.Array(Type.String())),
        },
        { additionalProperties: false },
      ),
    ),
  },
  { additionalProperties: false },
);

const DispatchTaskParams = Type.Object(
  {
    task_id: Type.String(),
    gateway: Type.Optional(Type.Union([Type.Literal("codex"), Type.Literal("opencode"), Type.Literal("gemini")])),
    model: Type.Optional(Type.String()),
  },
  { additionalProperties: false },
);

const TaskIdParams = Type.Object({ task_id: Type.String() }, { additionalProperties: false });

export function genesisTools(api: OpenClawPluginApi): AnyAgentTool[] {
  const baseUrl = resolveGenesisBaseUrl(api.config).replace(/\/+$/, "");

  const createTask: AnyAgentTool = {
    name: "genesis.create_task",
    label: "GENESIS Create Task",
    description: "Create a task in GENESIS core (creates a job+task row).",
    parameters: CreateTaskParams,
    execute: async (_id, args) => {
      const a = (args ?? {}) as Record<string, unknown>;
      const title = String(a.title ?? "").trim();
      const type = String(a.type ?? "").trim();
      const priority = typeof a.priority === "string" ? a.priority : undefined;
      const agent_id =
        typeof a.agent_id === "string" && a.agent_id.trim() ? a.agent_id.trim() : defaultAgentForType(type);
      const inputsRaw = (a.inputs && typeof a.inputs === "object" && !Array.isArray(a.inputs) ? a.inputs : {}) as Record<
        string,
        unknown
      >;
      const inputs = {
        brief: typeof inputsRaw.brief === "string" && inputsRaw.brief.trim() ? inputsRaw.brief : title,
        acceptance_criteria: Array.isArray(inputsRaw.acceptance_criteria)
          ? inputsRaw.acceptance_criteria.map((v) => String(v))
          : [],
        constraints: Array.isArray(inputsRaw.constraints) ? inputsRaw.constraints.map((v) => String(v)) : [],
      };

      const url = `${baseUrl}/api/tasks`;
      const res = await postJson(url, { title, type, priority, agent_id, inputs });
      return { content: [{ type: "text", text: JSON.stringify(res, null, 2) }], details: { genesis: { url } } };
    },
  };

  const dispatchTask: AnyAgentTool = {
    name: "genesis.dispatch_task",
    label: "GENESIS Dispatch Task",
    description: "Dispatch a GENESIS task through core (respects DISPATCH_MODE, e.g. n8n).",
    parameters: DispatchTaskParams,
    execute: async (_id, args) => {
      const a = (args ?? {}) as Record<string, unknown>;
      const task_id = String(a.task_id ?? "").trim();
      if (!task_id) throw new Error("task_id required");
      const body: Record<string, unknown> = {};
      if (typeof a.gateway === "string" && a.gateway.trim()) body.gateway = a.gateway.trim();
      if (typeof a.model === "string" && a.model.trim()) body.model = a.model.trim();

      const url = `${baseUrl}/api/tasks/${encodeURIComponent(task_id)}/dispatch`;
      const res = await postJson(url, body);
      return { content: [{ type: "text", text: JSON.stringify(res, null, 2) }], details: { genesis: { url } } };
    },
  };

  const getTask: AnyAgentTool = {
    name: "genesis.task_get",
    label: "GENESIS Get Task",
    description: "Fetch task details (status, runs, artifacts) from GENESIS core.",
    parameters: TaskIdParams,
    execute: async (_id, args) => {
      const a = (args ?? {}) as Record<string, unknown>;
      const task_id = String(a.task_id ?? "").trim();
      if (!task_id) throw new Error("task_id required");
      const url = `${baseUrl}/api/tasks/${encodeURIComponent(task_id)}`;
      const res = await getJson(url);
      return { content: [{ type: "text", text: JSON.stringify(res, null, 2) }], details: { genesis: { url } } };
    },
  };

  return [createTask, dispatchTask, getTask];
}

