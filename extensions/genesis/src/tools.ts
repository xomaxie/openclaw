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

async function requestJson(method: "GET" | "POST" | "PATCH", url: string, body?: unknown): Promise<any> {
  const res = await fetch(url, {
    method,
    headers: body === undefined ? undefined : { "content-type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body ?? {}),
  });
  const { text, json } = await readJson(res);
  if (!res.ok) {
    throw new Error(`GENESIS HTTP ${res.status} ${url}: ${text.slice(0, 200)}`);
  }
  return json ?? { text };
}

async function postJson(url: string, body: unknown): Promise<any> {
  return requestJson("POST", url, body);
}

async function patchJson(url: string, body: unknown): Promise<any> {
  return requestJson("PATCH", url, body);
}

async function getJson(url: string): Promise<any> {
  return requestJson("GET", url);
}

function defaultAgentForType(type: string): string {
  switch (type) {
    case "ui":
      return "ui_designer";
    case "docs":
    case "research":
      return "doc_writer";
    case "ops":
      return "backend_dev";
    case "backend":
    default:
      return "backend_dev";
  }
}

const TaskTypeSchema = Type.Union(
  [Type.Literal("backend"), Type.Literal("ui"), Type.Literal("docs"), Type.Literal("research"), Type.Literal("ops")],
  { description: "Task type" },
);

const PrioritySchema = Type.Union([Type.Literal("low"), Type.Literal("normal"), Type.Literal("high")]);

const BoardColumnSchema = Type.Union([
  Type.Literal("Backlog"),
  Type.Literal("In Progress"),
  Type.Literal("Needs Input"),
  Type.Literal("Blocked"),
  Type.Literal("Done"),
  Type.Literal("Trash"),
]);

const CreateTaskParams = Type.Object(
  {
    title: Type.String({ description: "Task title" }),
    type: TaskTypeSchema,
    priority: Type.Optional(PrioritySchema),
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

const BoardListParams = Type.Object(
  {
    column: Type.Optional(BoardColumnSchema),
    status: Type.Optional(Type.String({ description: "Alias for column/status filter" })),
    agent_id: Type.Optional(Type.String()),
    type: Type.Optional(TaskTypeSchema),
    limit: Type.Optional(Type.Number()),
  },
  { additionalProperties: false },
);

const BoardCreateParams = Type.Object(
  {
    title: Type.String(),
    type: TaskTypeSchema,
    priority: Type.Optional(PrioritySchema),
    agent_id: Type.Optional(Type.String()),
    brief: Type.Optional(Type.String()),
    acceptance_criteria: Type.Optional(Type.Array(Type.String())),
    constraints: Type.Optional(Type.Array(Type.String())),
  },
  { additionalProperties: false },
);

const BoardUpdateParams = Type.Object(
  {
    task_id: Type.String(),
    markdown: Type.String(),
    expected_mtime_ms: Type.Optional(Type.Number()),
  },
  { additionalProperties: false },
);

const BoardMoveParams = Type.Object(
  {
    task_id: Type.String(),
    to_column: BoardColumnSchema,
  },
  { additionalProperties: false },
);

function normalizeStatus(raw: unknown): string | undefined {
  if (typeof raw !== "string") return undefined;
  const s = raw.trim().toLowerCase();
  if (!s) return undefined;
  if (s === "backlog") return "Backlog";
  if (s === "in progress" || s === "in-progress" || s === "in_progress" || s === "progress" || s === "doing") return "In Progress";
  if (s === "needs input" || s === "needs-input" || s === "needs_input" || s === "input") return "Needs Input";
  if (s === "blocked") return "Blocked";
  if (s === "done" || s === "completed" || s === "complete") return "Done";
  if (s === "trash" || s === "deleted") return "Trash";
  return undefined;
}

function flattenBoardTasks(snapshot: any): any[] {
  const cols = snapshot?.columns;
  if (!cols || typeof cols !== "object") return [];
  const out: any[] = [];
  for (const value of Object.values(cols as Record<string, unknown>)) {
    const tasks = (value as any)?.tasks;
    if (Array.isArray(tasks)) out.push(...tasks);
  }
  return out;
}

function toToolResult(res: any, url: string) {
  return { content: [{ type: "text", text: JSON.stringify(res, null, 2) }], details: { genesis: { url } } };
}

export function genesisTools(api: OpenClawPluginApi): AnyAgentTool[] {
  const baseUrl = resolveGenesisBaseUrl(api.config).replace(/\/+$/, "");

  const boardList: AnyAgentTool = {
    name: "genesis.board_list",
    label: "GENESIS Board List",
    description: "Read-only: list tasks from the workspace kanban board with optional filters.",
    parameters: BoardListParams,
    execute: async (_id, args) => {
      const a = (args ?? {}) as Record<string, unknown>;
      const url = `${baseUrl}/api/kanban/board`;
      const snap = await getJson(url);
      let tasks = flattenBoardTasks(snap);
      const column = normalizeStatus(a.column) || normalizeStatus(a.status);
      const agent = readStr(a.agent_id);
      const type = readStr(a.type);
      const limitRaw = typeof a.limit === "number" && Number.isFinite(a.limit) ? Math.trunc(a.limit) : undefined;
      if (column) tasks = tasks.filter((t) => String((t as any)?.status ?? "") === column);
      if (agent) tasks = tasks.filter((t) => String((t as any)?.agent_id ?? "") === agent);
      if (type) tasks = tasks.filter((t) => String((t as any)?.type ?? "") === type);
      if (limitRaw && limitRaw > 0) tasks = tasks.slice(0, limitRaw);
      return toToolResult({ ok: true, tasks }, url);
    },
  };

  const boardGet: AnyAgentTool = {
    name: "genesis.board_get",
    label: "GENESIS Board Get",
    description: "Read-only: fetch one task from the workspace kanban board.",
    parameters: TaskIdParams,
    execute: async (_id, args) => {
      const a = (args ?? {}) as Record<string, unknown>;
      const task_id = String(a.task_id ?? "").trim();
      if (!task_id) throw new Error("task_id required");
      const url = `${baseUrl}/api/kanban/tasks/${encodeURIComponent(task_id)}`;
      const res = await getJson(url);
      return toToolResult({ ok: true, ...(res ?? {}) }, url);
    },
  };

  const boardCreate: AnyAgentTool = {
    name: "genesis.board_create",
    label: "GENESIS Board Create",
    description: "Create a new kanban task in the shared workspace.",
    parameters: BoardCreateParams,
    execute: async (_id, args) => {
      const a = (args ?? {}) as Record<string, unknown>;
      const title = String(a.title ?? "").trim();
      const type = String(a.type ?? "").trim() || "backend";
      if (!title) throw new Error("title required");
      const priority = typeof a.priority === "string" ? a.priority : "normal";
      const agent_id = typeof a.agent_id === "string" && a.agent_id.trim() ? a.agent_id.trim() : defaultAgentForType(type);
      const body = {
        title,
        type,
        priority,
        agent_id,
        brief: typeof a.brief === "string" ? a.brief : "",
        acceptance_criteria: Array.isArray(a.acceptance_criteria) ? a.acceptance_criteria.map((v) => String(v)) : [],
        constraints: Array.isArray(a.constraints) ? a.constraints.map((v) => String(v)) : [],
      };
      const url = `${baseUrl}/api/kanban/tasks`;
      const res = await postJson(url, body);
      return toToolResult({ ok: true, ...(res ?? {}), agent_id }, url);
    },
  };

  const boardUpdate: AnyAgentTool = {
    name: "genesis.board_update",
    label: "GENESIS Board Update",
    description: "Update an existing kanban task markdown.",
    parameters: BoardUpdateParams,
    execute: async (_id, args) => {
      const a = (args ?? {}) as Record<string, unknown>;
      const task_id = String(a.task_id ?? "").trim();
      const markdown = String(a.markdown ?? "");
      if (!task_id) throw new Error("task_id required");
      if (!markdown) throw new Error("markdown required");
      const body: Record<string, unknown> = { markdown };
      if (typeof a.expected_mtime_ms === "number" && Number.isFinite(a.expected_mtime_ms)) {
        body.expected_mtime_ms = a.expected_mtime_ms;
      }
      const url = `${baseUrl}/api/kanban/tasks/${encodeURIComponent(task_id)}`;
      const res = await patchJson(url, body);
      return toToolResult(res, url);
    },
  };

  const boardMove: AnyAgentTool = {
    name: "genesis.board_move",
    label: "GENESIS Board Move",
    description: "Move a kanban task to another board column.",
    parameters: BoardMoveParams,
    execute: async (_id, args) => {
      const a = (args ?? {}) as Record<string, unknown>;
      const task_id = String(a.task_id ?? "").trim();
      const to_column = String(a.to_column ?? "").trim();
      if (!task_id) throw new Error("task_id required");
      if (!to_column) throw new Error("to_column required");
      const url = `${baseUrl}/api/kanban/tasks/${encodeURIComponent(task_id)}/move`;
      const res = await postJson(url, { to: to_column });
      return toToolResult(res, url);
    },
  };

  const boardTrash: AnyAgentTool = {
    name: "genesis.board_trash",
    label: "GENESIS Board Trash",
    description: "Move a kanban task to Trash.",
    parameters: TaskIdParams,
    execute: async (_id, args) => {
      const a = (args ?? {}) as Record<string, unknown>;
      const task_id = String(a.task_id ?? "").trim();
      if (!task_id) throw new Error("task_id required");
      const url = `${baseUrl}/api/kanban/tasks/${encodeURIComponent(task_id)}/trash`;
      const res = await postJson(url, {});
      return toToolResult(res, url);
    },
  };

  // Compatibility aliases for legacy prompts.
  const createTask: AnyAgentTool = {
    name: "genesis.create_task",
    label: "GENESIS Create Task (Legacy Alias)",
    description: "Legacy alias of genesis.board_create.",
    parameters: CreateTaskParams,
    execute: async (_id, args) => {
      const a = (args ?? {}) as Record<string, unknown>;
      const inputsRaw = (a.inputs && typeof a.inputs === "object" && !Array.isArray(a.inputs) ? a.inputs : {}) as Record<string, unknown>;
      const mapped = {
        title: String(a.title ?? "").trim(),
        type: String(a.type ?? "").trim() || "backend",
        priority: typeof a.priority === "string" ? a.priority : "normal",
        agent_id: typeof a.agent_id === "string" ? a.agent_id : undefined,
        brief: typeof inputsRaw.brief === "string" ? inputsRaw.brief : String(a.title ?? "").trim(),
        acceptance_criteria: Array.isArray(inputsRaw.acceptance_criteria) ? inputsRaw.acceptance_criteria.map((v) => String(v)) : [],
        constraints: Array.isArray(inputsRaw.constraints) ? inputsRaw.constraints.map((v) => String(v)) : [],
      };
      const url = `${baseUrl}/api/kanban/tasks`;
      const res = await postJson(url, mapped);
      return toToolResult({ ok: true, legacy_alias: true, ...(res ?? {}) }, url);
    },
  };

  const dispatchTask: AnyAgentTool = {
    name: "genesis.dispatch_task",
    label: "GENESIS Dispatch Task",
    description: "Dispatch a GENESIS task through core.",
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
      return toToolResult(res, url);
    },
  };

  const getTask: AnyAgentTool = {
    name: "genesis.task_get",
    label: "GENESIS Get Task (Legacy Alias)",
    description: "Legacy alias of genesis.board_get.",
    parameters: TaskIdParams,
    execute: async (_id, args) => {
      const a = (args ?? {}) as Record<string, unknown>;
      const task_id = String(a.task_id ?? "").trim();
      if (!task_id) throw new Error("task_id required");
      const url = `${baseUrl}/api/kanban/tasks/${encodeURIComponent(task_id)}`;
      const res = await getJson(url);
      return toToolResult({ ok: true, legacy_alias: true, ...(res ?? {}) }, url);
    },
  };

  return [boardList, boardGet, boardCreate, boardUpdate, boardMove, boardTrash, createTask, dispatchTask, getTask];
}
