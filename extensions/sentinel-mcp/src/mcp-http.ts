import type { AgentToolResult } from "@mariozechner/pi-agent-core";

export type McpToolDef = {
  name: string;
  description?: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  inputSchema?: any;
};

type JsonRpcReq = {
  jsonrpc: "2.0";
  id?: string | number;
  method: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  params?: any;
};

type JsonRpcResp = {
  jsonrpc: "2.0";
  id?: string | number;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  result?: any;
  error?: { code: number; message: string; data?: unknown };
};

export class McpHttpClient {
  private url: string;
  private token?: string;
  private sessionId: string | null = null;
  private initialized = false;
  private reqId = 0;

  constructor(opts: { url: string; token?: string }) {
    this.url = opts.url;
    this.token = opts.token?.trim() ? opts.token.trim() : undefined;
  }

  private nextId(): number {
    this.reqId += 1;
    return this.reqId;
  }

  private headers(): Record<string, string> {
    const h: Record<string, string> = {
      "content-type": "application/json",
    };
    if (this.token) {
      h.authorization = `Bearer ${this.token}`;
    }
    if (this.sessionId) {
      h["mcp-session-id"] = this.sessionId;
    }
    return h;
  }

  private async post(req: JsonRpcReq): Promise<{ resp: JsonRpcResp; sessionId?: string | null }> {
    const res = await fetch(this.url, {
      method: "POST",
      headers: this.headers(),
      body: JSON.stringify(req),
    });

    if (res.status === 204) {
      return { resp: { jsonrpc: "2.0" }, sessionId: res.headers.get("mcp-session-id") };
    }

    const text = await res.text();
    let parsed: JsonRpcResp;
    try {
      parsed = text ? (JSON.parse(text) as JsonRpcResp) : ({ jsonrpc: "2.0" } as JsonRpcResp);
    } catch {
      throw new Error(`MCP HTTP returned non-JSON (status=${res.status}): ${text.slice(0, 200)}`);
    }

    return { resp: parsed, sessionId: res.headers.get("mcp-session-id") };
  }

  private async ensureSession(): Promise<void> {
    if (this.initialized && this.sessionId) {
      return;
    }

    const initReq: JsonRpcReq = {
      jsonrpc: "2.0",
      id: this.nextId(),
      method: "initialize",
      params: {
        protocolVersion: "2024-11-05",
        capabilities: {},
        clientInfo: { name: "openclaw-sentinel-mcp", version: "0.0.0" },
      },
    };

    const { resp, sessionId } = await this.post(initReq);
    if (resp.error) {
      throw new Error(`MCP initialize failed: ${resp.error.message}`);
    }
    if (!sessionId) {
      throw new Error("MCP initialize did not return mcp-session-id header");
    }

    this.sessionId = sessionId;

    // no response expected
    await this.post({ jsonrpc: "2.0", method: "initialized", params: {} });

    this.initialized = true;
  }

  private isSessionError(resp: JsonRpcResp): boolean {
    const msg = resp.error?.message?.toLowerCase() ?? "";
    return msg.includes("missing mcp-session-id") || msg.includes("unknown session");
  }

  async listTools(): Promise<McpToolDef[]> {
    await this.ensureSession();

    const req: JsonRpcReq = {
      jsonrpc: "2.0",
      id: this.nextId(),
      method: "tools/list",
      params: {},
    };

    let { resp } = await this.post(req);

    if (resp.error && this.isSessionError(resp)) {
      this.sessionId = null;
      this.initialized = false;
      await this.ensureSession();
      resp = (await this.post(req)).resp;
    }

    if (resp.error) {
      throw new Error(`MCP tools/list failed: ${resp.error.message}`);
    }

    const tools = resp.result?.tools;
    if (!Array.isArray(tools)) {
      return [];
    }

    return tools
      .filter((t) => t && typeof t === "object" && typeof (t as McpToolDef).name === "string")
      .map((t) => t as McpToolDef);
  }

  async callTool(name: string, args: Record<string, unknown>): Promise<AgentToolResult<unknown>> {
    await this.ensureSession();

    const req: JsonRpcReq = {
      jsonrpc: "2.0",
      id: this.nextId(),
      method: "tools/call",
      params: { name, arguments: args },
    };

    let { resp } = await this.post(req);

    if (resp.error && this.isSessionError(resp)) {
      this.sessionId = null;
      this.initialized = false;
      await this.ensureSession();
      resp = (await this.post(req)).resp;
    }

    if (resp.error) {
      throw new Error(`MCP tools/call failed (${name}): ${resp.error.message}`);
    }

    const result = resp.result ?? {};
    const content = Array.isArray(result.content) ? result.content : [];

    return {
      content,
      details: { mcp: { name } },
    };
  }
}