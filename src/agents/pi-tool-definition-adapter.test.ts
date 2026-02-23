import type { AgentTool } from "@mariozechner/pi-agent-core";
import { Type } from "@sinclair/typebox";
import { describe, expect, it } from "vitest";
import { toClientToolDefinitions, toToolDefinitions } from "./pi-tool-definition-adapter.js";

type ToolExecute = ReturnType<typeof toToolDefinitions>[number]["execute"];
const extensionContext = {} as Parameters<ToolExecute>[4];

async function executeThrowingTool(name: string, callId: string) {
  const tool = {
    name,
    label: name === "bash" ? "Bash" : "Boom",
    description: "throws",
    parameters: Type.Object({}),
    execute: async () => {
      throw new Error("nope");
    },
  } satisfies AgentTool;

  const defs = toToolDefinitions([tool]);
  const def = defs[0];
  if (!def) {
    throw new Error("missing tool definition");
  }
  return await def.execute(callId, {}, undefined, undefined, extensionContext);
}

describe("pi tool definition adapter", () => {
  it("aliases provider-unsafe built-in tool names", async () => {
    const dottedTool = {
      name: "sentinel.docs_search",
      label: "Docs Search",
      description: "search",
      parameters: Type.Object({}),
      execute: async () => ({ content: [{ type: "text", text: "ok" }] }),
    } satisfies AgentTool;

    const defs = toToolDefinitions([dottedTool]);
    const def = defs[0];
    if (!def) {
      throw new Error("missing tool definition");
    }

    expect(def.name).toMatch(/^[A-Za-z][A-Za-z0-9_-]*$/);
    expect(def.name).not.toBe("sentinel.docs_search");
  });

  it("wraps tool errors into a tool result", async () => {
    const result = await executeThrowingTool("boom", "call1");

    expect(result.details).toMatchObject({
      status: "error",
      tool: "boom",
    });
    expect(result.details).toMatchObject({ error: "nope" });
    expect(JSON.stringify(result.details)).not.toContain("\n    at ");
  });

  it("normalizes exec tool aliases in error results", async () => {
    const result = await executeThrowingTool("bash", "call2");

    expect(result.details).toMatchObject({
      status: "error",
      tool: "exec",
      error: "nope",
    });
  });
});

it("aliases provider-unsafe client tool names and preserves callback name", async () => {
  let callbackToolName = "";
  const defs = toClientToolDefinitions(
    [
      {
        type: "function",
        function: {
          name: "genesis.board_list",
          description: "List board tasks",
          parameters: Type.Object({ limit: Type.Optional(Type.Number()) }),
        },
      },
    ],
    (toolName) => {
      callbackToolName = toolName;
    },
    { agentId: "main", sessionKey: "main" },
  );

  const def = defs[0];
  if (!def) {
    throw new Error("missing client tool definition");
  }

  expect(def.name).toMatch(/^[A-Za-z][A-Za-z0-9_-]*$/);
  expect(def.name).not.toBe("genesis.board_list");

  await def.execute("call3", { limit: 1 }, undefined, undefined, extensionContext);
  expect(callbackToolName).toBe("genesis.board_list");
});
