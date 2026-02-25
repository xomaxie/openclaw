/**
 * OpenResponses HTTP Handler
 *
 * Implements the OpenResponses `/v1/responses` endpoint for OpenClaw Gateway.
 *
 * @see https://www.open-responses.com/
 */

import { randomUUID } from "node:crypto";
import type { IncomingMessage, ServerResponse } from "node:http";
import type { ClientToolDefinition } from "../agents/pi-embedded-runner/run/params.js";
import { createDefaultDeps } from "../cli/deps.js";
import { agentCommand } from "../commands/agent.js";
import type { ImageContent } from "../commands/agent/types.js";
import { loadConfig } from "../config/config.js";
import {
  resolveAgentMainSessionKey,
  resolveStorePath,
  updateLastRoute,
} from "../config/sessions.js";
import type { GatewayHttpResponsesConfig } from "../config/types.gateway.js";
import { emitAgentEvent, onAgentEvent } from "../infra/agent-events.js";
import { logWarn } from "../logger.js";
import {
  DEFAULT_INPUT_FILE_MAX_BYTES,
  DEFAULT_INPUT_FILE_MAX_CHARS,
  DEFAULT_INPUT_FILE_MIMES,
  DEFAULT_INPUT_IMAGE_MAX_BYTES,
  DEFAULT_INPUT_IMAGE_MIMES,
  DEFAULT_INPUT_MAX_REDIRECTS,
  DEFAULT_INPUT_PDF_MAX_PAGES,
  DEFAULT_INPUT_PDF_MAX_PIXELS,
  DEFAULT_INPUT_PDF_MIN_TEXT_CHARS,
  DEFAULT_INPUT_TIMEOUT_MS,
  extractFileContentFromSource,
  extractImageContentFromSource,
  normalizeMimeList,
  type InputFileLimits,
  type InputImageLimits,
  type InputImageSource,
} from "../media/input-files.js";
import { defaultRuntime } from "../runtime.js";
import {
  buildAgentMessageFromConversationEntries,
  type ConversationEntry,
} from "./agent-prompt.js";
import type { AuthRateLimiter } from "./auth-rate-limit.js";
import { authorizeGatewayConnect, type ResolvedGatewayAuth } from "./auth.js";
import {
  readJsonBodyOrError,
  sendGatewayAuthFailure,
  sendJson,
  sendMethodNotAllowed,
  setSseHeaders,
  writeDone,
} from "./http-common.js";
import { getBearerToken, resolveAgentIdForRequest, resolveSessionKey } from "./http-utils.js";
import {
  CreateResponseBodySchema,
  type ContentPart,
  type CreateResponseBody,
  type ItemParam,
  type OutputItem,
  type ResponseReasoning,
  type ResponseResource,
  type StreamingEvent,
  type Usage,
} from "./open-responses.schema.js";
import {
  getOpenResponsesToolCallRegistry,
  type OpenResponsesToolCallConsumeFailureReason,
} from "./openresponses-tool-call-registry.js";

type OpenResponsesHttpOptions = {
  auth: ResolvedGatewayAuth;
  maxBodyBytes?: number;
  config?: GatewayHttpResponsesConfig;
  trustedProxies?: string[];
  rateLimiter?: AuthRateLimiter;
};

const DEFAULT_BODY_BYTES = 20 * 1024 * 1024;
const DEFAULT_MAX_URL_PARTS = 8;

function writeSseEvent(res: ServerResponse, event: StreamingEvent) {
  res.write(`event: ${event.type}\n`);
  res.write(`data: ${JSON.stringify(event)}\n\n`);
}

function extractTextContent(content: string | ContentPart[]): string {
  if (typeof content === "string") {
    return content;
  }
  return content
    .map((part) => {
      if (part.type === "input_text") {
        return part.text;
      }
      if (part.type === "output_text") {
        return part.text;
      }
      return "";
    })
    .filter(Boolean)
    .join("\n");
}

type ResolvedResponsesLimits = {
  maxBodyBytes: number;
  maxUrlParts: number;
  files: InputFileLimits;
  images: InputImageLimits;
};

function normalizeHostnameAllowlist(values: string[] | undefined): string[] | undefined {
  if (!values || values.length === 0) {
    return undefined;
  }
  const normalized = values.map((value) => value.trim()).filter((value) => value.length > 0);
  return normalized.length > 0 ? normalized : undefined;
}

function resolveResponsesLimits(
  config: GatewayHttpResponsesConfig | undefined,
): ResolvedResponsesLimits {
  const files = config?.files;
  const images = config?.images;
  return {
    maxBodyBytes: config?.maxBodyBytes ?? DEFAULT_BODY_BYTES,
    maxUrlParts:
      typeof config?.maxUrlParts === "number"
        ? Math.max(0, Math.floor(config.maxUrlParts))
        : DEFAULT_MAX_URL_PARTS,
    files: {
      allowUrl: files?.allowUrl ?? true,
      urlAllowlist: normalizeHostnameAllowlist(files?.urlAllowlist),
      allowedMimes: normalizeMimeList(files?.allowedMimes, DEFAULT_INPUT_FILE_MIMES),
      maxBytes: files?.maxBytes ?? DEFAULT_INPUT_FILE_MAX_BYTES,
      maxChars: files?.maxChars ?? DEFAULT_INPUT_FILE_MAX_CHARS,
      maxRedirects: files?.maxRedirects ?? DEFAULT_INPUT_MAX_REDIRECTS,
      timeoutMs: files?.timeoutMs ?? DEFAULT_INPUT_TIMEOUT_MS,
      pdf: {
        maxPages: files?.pdf?.maxPages ?? DEFAULT_INPUT_PDF_MAX_PAGES,
        maxPixels: files?.pdf?.maxPixels ?? DEFAULT_INPUT_PDF_MAX_PIXELS,
        minTextChars: files?.pdf?.minTextChars ?? DEFAULT_INPUT_PDF_MIN_TEXT_CHARS,
      },
    },
    images: {
      allowUrl: images?.allowUrl ?? true,
      urlAllowlist: normalizeHostnameAllowlist(images?.urlAllowlist),
      allowedMimes: normalizeMimeList(images?.allowedMimes, DEFAULT_INPUT_IMAGE_MIMES),
      maxBytes: images?.maxBytes ?? DEFAULT_INPUT_IMAGE_MAX_BYTES,
      maxRedirects: images?.maxRedirects ?? DEFAULT_INPUT_MAX_REDIRECTS,
      timeoutMs: images?.timeoutMs ?? DEFAULT_INPUT_TIMEOUT_MS,
    },
  };
}

function extractClientTools(body: CreateResponseBody): ClientToolDefinition[] {
  return (body.tools ?? []) as ClientToolDefinition[];
}

function applyToolChoice(params: {
  tools: ClientToolDefinition[];
  toolChoice: CreateResponseBody["tool_choice"];
}): { tools: ClientToolDefinition[]; extraSystemPrompt?: string } {
  const { tools, toolChoice } = params;
  if (!toolChoice) {
    return { tools };
  }

  if (toolChoice === "none") {
    return { tools: [] };
  }

  if (toolChoice === "required") {
    if (tools.length === 0) {
      throw new Error("tool_choice=required but no tools were provided");
    }
    return {
      tools,
      extraSystemPrompt: "You must call one of the available tools before responding.",
    };
  }

  if (typeof toolChoice === "object" && toolChoice.type === "function") {
    const targetName = toolChoice.function?.name?.trim();
    if (!targetName) {
      throw new Error("tool_choice.function.name is required");
    }
    const matched = tools.filter((tool) => tool.function?.name === targetName);
    if (matched.length === 0) {
      throw new Error(`tool_choice requested unknown tool: ${targetName}`);
    }
    return {
      tools: matched,
      extraSystemPrompt: `You must call the ${targetName} tool before responding.`,
    };
  }

  return { tools };
}

export function buildAgentPrompt(input: string | ItemParam[]): {
  message: string;
  extraSystemPrompt?: string;
} {
  if (typeof input === "string") {
    return { message: input };
  }

  const systemParts: string[] = [];
  const conversationEntries: ConversationEntry[] = [];

  for (const item of input) {
    if (item.type === "message") {
      const content = extractTextContent(item.content).trim();
      if (!content) {
        continue;
      }

      if (item.role === "system" || item.role === "developer") {
        systemParts.push(content);
        continue;
      }

      const normalizedRole = item.role === "assistant" ? "assistant" : "user";
      const sender = normalizedRole === "assistant" ? "Assistant" : "User";

      conversationEntries.push({
        role: normalizedRole,
        entry: { sender, body: content },
      });
    } else if (item.type === "function_call_output") {
      conversationEntries.push({
        role: "tool",
        entry: { sender: `Tool:${item.call_id}`, body: item.output },
      });
    }
    // Skip reasoning and item_reference for prompt building (Phase 1)
  }

  const message = buildAgentMessageFromConversationEntries(conversationEntries);

  return {
    message,
    extraSystemPrompt: systemParts.length > 0 ? systemParts.join("\n\n") : undefined,
  };
}

function resolveOpenResponsesSessionKey(params: {
  req: IncomingMessage;
  agentId: string;
  user?: string | undefined;
}): string {
  return resolveSessionKey({ ...params, prefix: "openresponses" });
}

function createEmptyUsage(): Usage {
  return { input_tokens: 0, output_tokens: 0, total_tokens: 0 };
}

function toUsage(
  value:
    | {
        input?: number;
        output?: number;
        cacheRead?: number;
        cacheWrite?: number;
        total?: number;
      }
    | undefined,
): Usage {
  if (!value) {
    return createEmptyUsage();
  }
  const input = value.input ?? 0;
  const output = value.output ?? 0;
  const cacheRead = value.cacheRead ?? 0;
  const cacheWrite = value.cacheWrite ?? 0;
  const total = value.total ?? input + output + cacheRead + cacheWrite;
  return {
    input_tokens: Math.max(0, input),
    output_tokens: Math.max(0, output),
    total_tokens: Math.max(0, total),
  };
}

function extractUsageFromResult(result: unknown): Usage {
  const meta = (result as { meta?: { agentMeta?: { usage?: unknown } } } | null)?.meta;
  const usage = meta && typeof meta === "object" ? meta.agentMeta?.usage : undefined;
  return toUsage(
    usage as
      | { input?: number; output?: number; cacheRead?: number; cacheWrite?: number; total?: number }
      | undefined,
  );
}

function normalizeReasoningEffort(value: unknown): "low" | "medium" | "high" | "xhigh" | undefined {
  const normalized = String(value ?? "")
    .trim()
    .toLowerCase();
  if (
    normalized === "low" ||
    normalized === "medium" ||
    normalized === "high" ||
    normalized === "xhigh"
  ) {
    return normalized;
  }
  return undefined;
}

function buildResponseReasoning(params: {
  payload: CreateResponseBody;
  result?: unknown;
}): ResponseReasoning | undefined {
  const requested = normalizeReasoningEffort(params.payload.reasoning?.effort);
  const effectiveThinkingRaw =
    ((
      params.result as {
        meta?: {
          agentMeta?: {
            thinkingLevel?: unknown;
          };
        };
      } | null
    )?.meta?.agentMeta?.thinkingLevel ??
      undefined) ||
    undefined;
  const effectiveThinking =
    typeof effectiveThinkingRaw === "string" && effectiveThinkingRaw.trim().length > 0
      ? effectiveThinkingRaw.trim()
      : undefined;
  const effectiveEffort = normalizeReasoningEffort(effectiveThinkingRaw) ?? requested;
  if (!requested && !effectiveEffort && !effectiveThinking) {
    return undefined;
  }
  return {
    requested_effort: requested,
    effective_effort: effectiveEffort,
    effective_thinking: effectiveThinking,
  };
}

function createResponseResource(params: {
  id: string;
  model: string;
  status: ResponseResource["status"];
  output: OutputItem[];
  usage?: Usage;
  error?: { code: string; message: string };
  reasoning?: ResponseReasoning;
}): ResponseResource {
  return {
    id: params.id,
    object: "response",
    created_at: Math.floor(Date.now() / 1000),
    status: params.status,
    model: params.model,
    output: params.output,
    usage: params.usage ?? createEmptyUsage(),
    error: params.error,
    reasoning: params.reasoning,
  };
}

function createAssistantOutputItem(params: {
  id: string;
  text: string;
  status?: "in_progress" | "completed";
}): OutputItem {
  return {
    type: "message",
    id: params.id,
    role: "assistant",
    content: [{ type: "output_text", text: params.text }],
    status: params.status,
  };
}

type ObservedToolCall = {
  id?: string;
  name: string;
  status: "success" | "error";
  meta?: string;
  error?: string;
};

function extractExecutedToolCalls(meta: unknown): ObservedToolCall[] {
  if (!meta || typeof meta !== "object") {
    return [];
  }
  const candidate = (meta as { executedToolCalls?: unknown }).executedToolCalls;
  if (!Array.isArray(candidate)) {
    return [];
  }
  const calls: ObservedToolCall[] = [];
  for (const item of candidate) {
    if (!item || typeof item !== "object") {
      continue;
    }
    const id = (item as { id?: unknown }).id;
    const name = (item as { name?: unknown }).name;
    const status = (item as { status?: unknown }).status;
    const metaText = (item as { meta?: unknown }).meta;
    const errorText = (item as { error?: unknown }).error;
    if (typeof name !== "string" || name.trim().length === 0) {
      continue;
    }
    if (status !== "success" && status !== "error") {
      continue;
    }
    calls.push({
      id: typeof id === "string" && id.trim().length > 0 ? id : undefined,
      name,
      status,
      meta: typeof metaText === "string" ? metaText : undefined,
      error: typeof errorText === "string" ? errorText : undefined,
    });
  }
  return calls;
}

function createObservedToolOutputItems(calls: ObservedToolCall[]): OutputItem[] {
  return calls.map((call) => ({
    type: "mcp_call",
    id: call.id ?? `mcp_${randomUUID()}`,
    name: call.name,
    status: call.status,
    meta: call.meta,
    error: call.error,
  }));
}

function extractFunctionCallOutputItems(input: CreateResponseBody["input"]): string[] {
  if (!Array.isArray(input)) {
    return [];
  }
  const callIds: string[] = [];
  for (const item of input) {
    if (item.type !== "function_call_output") {
      continue;
    }
    callIds.push(item.call_id);
  }
  return callIds;
}

function sendToolCallSessionMismatch(
  res: ServerResponse,
  details: OpenResponsesToolCallConsumeFailureReason | "missing_previous_response_id",
) {
  sendJson(res, 409, {
    error: {
      type: "invalid_request_error",
      code: "tool_call_session_mismatch",
      message: `function_call_output rejected (${details})`,
    },
  });
}

export async function handleOpenResponsesHttpRequest(
  req: IncomingMessage,
  res: ServerResponse,
  opts: OpenResponsesHttpOptions,
): Promise<boolean> {
  const url = new URL(req.url ?? "/", `http://${req.headers.host || "localhost"}`);
  const isResponsesPath = url.pathname === "/v1/responses";
  const isSessionsResetPath = url.pathname === "/v1/responses/sessions/reset";
  if (!isResponsesPath && !isSessionsResetPath) {
    return false;
  }

  if (req.method !== "POST") {
    sendMethodNotAllowed(res);
    return true;
  }

  const token = getBearerToken(req);
  const authResult = await authorizeGatewayConnect({
    auth: opts.auth,
    connectAuth: { token, password: token },
    req,
    trustedProxies: opts.trustedProxies,
    rateLimiter: opts.rateLimiter,
  });
  if (!authResult.ok) {
    sendGatewayAuthFailure(res, authResult);
    return true;
  }

  const limits = resolveResponsesLimits(opts.config);
  const maxBodyBytes =
    opts.maxBodyBytes ??
    (opts.config?.maxBodyBytes
      ? limits.maxBodyBytes
      : Math.max(limits.maxBodyBytes, limits.files.maxBytes * 2, limits.images.maxBytes * 2));
  const body = await readJsonBodyOrError(req, res, maxBodyBytes);
  if (body === undefined) {
    return true;
  }

  const toolCallRegistry = getOpenResponsesToolCallRegistry();

  if (isSessionsResetPath) {
    const sessionKey =
      typeof (body as { session_key?: unknown } | null)?.session_key === "string"
        ? (body as { session_key: string }).session_key.trim()
        : "";
    if (!sessionKey) {
      sendJson(res, 400, {
        error: {
          type: "invalid_request_error",
          message: "session_key is required",
        },
      });
      return true;
    }
    const cleared = toolCallRegistry.resetSession(sessionKey);
    toolCallRegistry.prune();
    sendJson(res, 200, { ok: true, key: sessionKey, cleared });
    return true;
  }

  // Validate request body with Zod
  const parseResult = CreateResponseBodySchema.safeParse(body);
  if (!parseResult.success) {
    const issue = parseResult.error.issues[0];
    const message = issue ? `${issue.path.join(".")}: ${issue.message}` : "Invalid request body";
    sendJson(res, 400, {
      error: { message, type: "invalid_request_error" },
    });
    return true;
  }

  const payload: CreateResponseBody = parseResult.data;
  const stream = Boolean(payload.stream);
  const model = payload.model;
  const user = payload.user;

  // Extract images + files from input (Phase 2)
  let images: ImageContent[] = [];
  let fileContexts: string[] = [];
  let urlParts = 0;
  const markUrlPart = () => {
    urlParts += 1;
    if (urlParts > limits.maxUrlParts) {
      throw new Error(
        `Too many URL-based input sources: ${urlParts} (limit: ${limits.maxUrlParts})`,
      );
    }
  };
  try {
    if (Array.isArray(payload.input)) {
      for (const item of payload.input) {
        if (item.type === "message" && typeof item.content !== "string") {
          for (const part of item.content) {
            if (part.type === "input_image") {
              const source = part.source as {
                type?: string;
                url?: string;
                data?: string;
                media_type?: string;
              };
              const sourceType =
                source.type === "base64" || source.type === "url" ? source.type : undefined;
              if (!sourceType) {
                throw new Error("input_image must have 'source.url' or 'source.data'");
              }
              if (sourceType === "url") {
                markUrlPart();
              }
              const imageSource: InputImageSource = {
                type: sourceType,
                url: source.url,
                data: source.data,
                mediaType: source.media_type,
              };
              const image = await extractImageContentFromSource(imageSource, limits.images);
              images.push(image);
              continue;
            }

            if (part.type === "input_file") {
              const source = part.source as {
                type?: string;
                url?: string;
                data?: string;
                media_type?: string;
                filename?: string;
              };
              const sourceType =
                source.type === "base64" || source.type === "url" ? source.type : undefined;
              if (!sourceType) {
                throw new Error("input_file must have 'source.url' or 'source.data'");
              }
              if (sourceType === "url") {
                markUrlPart();
              }
              const file = await extractFileContentFromSource({
                source: {
                  type: sourceType,
                  url: source.url,
                  data: source.data,
                  mediaType: source.media_type,
                  filename: source.filename,
                },
                limits: limits.files,
              });
              if (file.text?.trim()) {
                fileContexts.push(`<file name="${file.filename}">\n${file.text}\n</file>`);
              } else if (file.images && file.images.length > 0) {
                fileContexts.push(
                  `<file name="${file.filename}">[PDF content rendered to images]</file>`,
                );
              }
              if (file.images && file.images.length > 0) {
                images = images.concat(file.images);
              }
            }
          }
        }
      }
    }
  } catch (err) {
    logWarn(`openresponses: request parsing failed: ${String(err)}`);
    sendJson(res, 400, {
      error: { message: "invalid request", type: "invalid_request_error" },
    });
    return true;
  }

  const clientTools = extractClientTools(payload);
  let toolChoicePrompt: string | undefined;
  let resolvedClientTools = clientTools;
  try {
    const toolChoiceResult = applyToolChoice({
      tools: clientTools,
      toolChoice: payload.tool_choice,
    });
    resolvedClientTools = toolChoiceResult.tools;
    toolChoicePrompt = toolChoiceResult.extraSystemPrompt;
  } catch (err) {
    logWarn(`openresponses: tool configuration failed: ${String(err)}`);
    sendJson(res, 400, {
      error: { message: "invalid tool configuration", type: "invalid_request_error" },
    });
    return true;
  }
  const agentId = resolveAgentIdForRequest({ req, model });
  const sessionKey = resolveOpenResponsesSessionKey({ req, agentId, user });
  const functionCallOutputs = extractFunctionCallOutputItems(payload.input);
  if (functionCallOutputs.length > 0) {
    const previousResponseId =
      typeof payload.previous_response_id === "string" ? payload.previous_response_id.trim() : "";
    if (!previousResponseId) {
      sendToolCallSessionMismatch(res, "missing_previous_response_id");
      return true;
    }

    for (const callId of functionCallOutputs) {
      const exists = toolCallRegistry.hasPendingCall({
        sessionKey,
        responseId: previousResponseId,
        callId,
      });
      if (!exists) {
        sendToolCallSessionMismatch(res, "call_not_found");
        return true;
      }
    }

    for (const callId of functionCallOutputs) {
      const consumed = toolCallRegistry.consumeToolOutput({
        sessionKey,
        previousResponseId,
        callId,
      });
      if (!consumed.ok) {
        sendToolCallSessionMismatch(res, consumed.reason);
        return true;
      }
    }
    toolCallRegistry.prune();
  }

  const explicitSessionKeyHeaderRaw = req.headers["x-openclaw-session-key"];
  const explicitSessionKeyHeader =
    typeof explicitSessionKeyHeaderRaw === "string"
      ? explicitSessionKeyHeaderRaw.trim()
      : Array.isArray(explicitSessionKeyHeaderRaw)
        ? (explicitSessionKeyHeaderRaw[0] ?? "").trim()
        : "";

  // GENESIS uses x-openclaw-session-key to pin a webchat session. Persist that
  // routing as the agent main session's last route so isolated cron jobs can
  // announce back without needing an explicit delivery.to.
  if (explicitSessionKeyHeader) {
    try {
      const cfg = loadConfig();
      const storePath = resolveStorePath(cfg.session?.store, { agentId });
      const mainSessionKey = resolveAgentMainSessionKey({ cfg, agentId });
      await updateLastRoute({
        storePath,
        sessionKey: mainSessionKey,
        channel: "genesis",
        to: explicitSessionKeyHeader,
      });
    } catch (err) {
      logWarn(`openresponses: failed to update last route: ${String(err)}`);
    }
  }

  // Build prompt from input
  const prompt = buildAgentPrompt(payload.input);

  const fileContext = fileContexts.length > 0 ? fileContexts.join("\n\n") : undefined;
  const toolChoiceContext = toolChoicePrompt?.trim();

  // Handle instructions + file context as extra system prompt
  const extraSystemPrompt = [
    payload.instructions,
    prompt.extraSystemPrompt,
    toolChoiceContext,
    fileContext,
  ]
    .filter(Boolean)
    .join("\n\n");

  if (!prompt.message) {
    sendJson(res, 400, {
      error: {
        message: "Missing user message in `input`.",
        type: "invalid_request_error",
      },
    });
    return true;
  }

  const responseId = `resp_${randomUUID()}`;
  const outputItemId = `msg_${randomUUID()}`;
  const deps = createDefaultDeps();
  const streamParams =
    typeof payload.max_output_tokens === "number"
      ? { maxTokens: payload.max_output_tokens }
      : undefined;
  const reasoningLevel = stream && payload.reasoning ? "stream" : "off";

  if (!stream) {
    try {
      const result = await agentCommand(
        {
          message: prompt.message,
          model,
          thinkingOnce: payload.reasoning?.effort,
          reasoningLevel,
          images: images.length > 0 ? images : undefined,
          clientTools: resolvedClientTools.length > 0 ? resolvedClientTools : undefined,
          extraSystemPrompt: extraSystemPrompt || undefined,
          streamParams: streamParams ?? undefined,
          sessionKey,
          runId: responseId,
          deliver: false,
          messageChannel: "webchat",
          bestEffortDeliver: false,
        },
        defaultRuntime,
        deps,
      );
      const responseReasoning = buildResponseReasoning({ payload, result });

      const payloads = (result as { payloads?: Array<{ text?: string }> } | null)?.payloads;
      const usage = extractUsageFromResult(result);
      const meta = (result as { meta?: unknown } | null)?.meta;
      const stopReason =
        meta && typeof meta === "object" ? (meta as { stopReason?: string }).stopReason : undefined;
      const pendingToolCalls =
        meta && typeof meta === "object"
          ? (meta as { pendingToolCalls?: Array<{ id: string; name: string; arguments: string }> })
              .pendingToolCalls
          : undefined;
      const observedToolItems = createObservedToolOutputItems(extractExecutedToolCalls(meta));

      // If agent called a client tool, return function_call instead of text
      if (stopReason === "tool_calls" && pendingToolCalls && pendingToolCalls.length > 0) {
        const functionCall = pendingToolCalls[0];
        for (const pendingToolCall of pendingToolCalls) {
          toolCallRegistry.registerPendingCall({
            sessionKey,
            responseId,
            callId: pendingToolCall.id,
            toolName: pendingToolCall.name,
            arguments: pendingToolCall.arguments,
          });
        }
        toolCallRegistry.prune();
        const functionCallItemId = `call_${randomUUID()}`;
        const response = createResponseResource({
          id: responseId,
          model,
          status: "incomplete",
          output: [
            {
              type: "function_call",
              id: functionCallItemId,
              call_id: functionCall.id,
              name: functionCall.name,
              arguments: functionCall.arguments,
            },
            ...observedToolItems,
          ],
          usage,
          reasoning: responseReasoning,
        });
        sendJson(res, 200, response);
        return true;
      }

      const content =
        Array.isArray(payloads) && payloads.length > 0
          ? payloads
              .map((p) => (typeof p.text === "string" ? p.text : ""))
              .filter(Boolean)
              .join("\n\n")
          : "No response from OpenClaw.";

      const response = createResponseResource({
        id: responseId,
        model,
        status: "completed",
        output: [
          createAssistantOutputItem({ id: outputItemId, text: content, status: "completed" }),
          ...observedToolItems,
        ],
        usage,
          reasoning: responseReasoning,
        });
        toolCallRegistry.completeTurn({ sessionKey, responseId });
        toolCallRegistry.prune();

        sendJson(res, 200, response);
      } catch (err) {
        logWarn(`openresponses: non-stream response failed: ${String(err)}`);
        toolCallRegistry.completeTurn({ sessionKey, responseId });
        toolCallRegistry.prune();
        const response = createResponseResource({
        id: responseId,
        model,
        status: "failed",
        output: [],
        error: { code: "api_error", message: "internal error" },
        reasoning: buildResponseReasoning({ payload }),
      });
      sendJson(res, 500, response);
    }
    return true;
  }

  // ─────────────────────────────────────────────────────────────────────────
  // Streaming mode
  // ─────────────────────────────────────────────────────────────────────────

  setSseHeaders(res);

  let accumulatedText = "";
  let sawAssistantDelta = false;
  let closed = false;
  let unsubscribe = () => {};
  let finalUsage: Usage | undefined;
  const requestedReasoning = buildResponseReasoning({ payload });
  let finalReasoning: ResponseReasoning | undefined = requestedReasoning;
  let finalObservedToolItems: OutputItem[] = [];
  let streamedReasoning = "";
  const reasoningItemId = `rsn_${randomUUID()}`;
  let finalizeRequested: { status: ResponseResource["status"]; text: string } | null = null;

  const maybeFinalize = () => {
    if (closed) {
      return;
    }
    if (!finalizeRequested) {
      return;
    }
    if (!finalUsage) {
      return;
    }
    const usage = finalUsage;
    toolCallRegistry.completeTurn({ sessionKey, responseId });
    toolCallRegistry.prune();

    closed = true;
    unsubscribe();

    writeSseEvent(res, {
      type: "response.output_text.done",
      item_id: outputItemId,
      output_index: 0,
      content_index: 0,
      text: finalizeRequested.text,
    });

    writeSseEvent(res, {
      type: "response.content_part.done",
      item_id: outputItemId,
      output_index: 0,
      content_index: 0,
      part: { type: "output_text", text: finalizeRequested.text },
    });

    const completedItem = createAssistantOutputItem({
      id: outputItemId,
      text: finalizeRequested.text,
      status: "completed",
    });

    writeSseEvent(res, {
      type: "response.output_item.done",
      output_index: 0,
      item: completedItem,
    });

    const finalOutputItems: OutputItem[] = [completedItem];
    let outputIndex = 1;

    const reasoningText = streamedReasoning.trim();
    if (reasoningText) {
      const reasoningItem: OutputItem = {
        type: "reasoning",
        id: reasoningItemId,
        content: reasoningText,
      };
      writeSseEvent(res, {
        type: "response.output_item.added",
        output_index: outputIndex,
        item: reasoningItem,
      });
      writeSseEvent(res, {
        type: "response.output_item.done",
        output_index: outputIndex,
        item: reasoningItem,
      });
      finalOutputItems.push(reasoningItem);
      outputIndex += 1;
    }

    for (const observedItem of finalObservedToolItems) {
      writeSseEvent(res, {
        type: "response.output_item.added",
        output_index: outputIndex,
        item: observedItem,
      });
      writeSseEvent(res, {
        type: "response.output_item.done",
        output_index: outputIndex,
        item: observedItem,
      });
      finalOutputItems.push(observedItem);
      outputIndex += 1;
    }

    const finalResponse = createResponseResource({
      id: responseId,
      model,
      status: finalizeRequested.status,
      output: finalOutputItems,
      usage,
      reasoning: finalReasoning,
    });

    writeSseEvent(res, { type: "response.completed", response: finalResponse });
    writeDone(res);
    res.end();
  };

  const requestFinalize = (status: ResponseResource["status"], text: string) => {
    if (finalizeRequested) {
      return;
    }
    finalizeRequested = { status, text };
    maybeFinalize();
  };

  // Send initial events
  const initialResponse = createResponseResource({
    id: responseId,
    model,
    status: "in_progress",
    output: [],
    reasoning: requestedReasoning,
  });

  writeSseEvent(res, { type: "response.created", response: initialResponse });
  writeSseEvent(res, { type: "response.in_progress", response: initialResponse });

  // Add output item
  const outputItem = createAssistantOutputItem({
    id: outputItemId,
    text: "",
    status: "in_progress",
  });

  writeSseEvent(res, {
    type: "response.output_item.added",
    output_index: 0,
    item: outputItem,
  });

  // Add content part
  writeSseEvent(res, {
    type: "response.content_part.added",
    item_id: outputItemId,
    output_index: 0,
    content_index: 0,
    part: { type: "output_text", text: "" },
  });

  unsubscribe = onAgentEvent((evt) => {
    if (evt.runId !== responseId) {
      return;
    }
    if (closed) {
      return;
    }

    if (evt.stream === "assistant") {
      const delta = evt.data?.delta;
      const text = evt.data?.text;
      const content = typeof delta === "string" ? delta : typeof text === "string" ? text : "";
      if (!content) {
        return;
      }

      sawAssistantDelta = true;
      accumulatedText += content;

      writeSseEvent(res, {
        type: "response.output_text.delta",
        item_id: outputItemId,
        output_index: 0,
        content_index: 0,
        delta: content,
      });
      return;
    }

    if (evt.stream === "thinking") {
      const delta = evt.data?.delta;
      const text = evt.data?.text;
      const content = typeof delta === "string" ? delta : typeof text === "string" ? text : "";
      if (!content) {
        return;
      }
      if (
        typeof delta !== "string" &&
        typeof text === "string" &&
        streamedReasoning &&
        text.startsWith(streamedReasoning)
      ) {
        streamedReasoning += text.slice(streamedReasoning.length);
      } else {
        streamedReasoning += content;
      }
      return;
    }

    if (evt.stream === "lifecycle") {
      const phase = evt.data?.phase;
      if (phase === "end" || phase === "error") {
        const finalText = accumulatedText || "No response from OpenClaw.";
        const finalStatus = phase === "error" ? "failed" : "completed";
        requestFinalize(finalStatus, finalText);
      }
    }
  });

  req.on("close", () => {
    closed = true;
    unsubscribe();
  });

  void (async () => {
    try {
      const result = await agentCommand(
        {
          message: prompt.message,
          model,
          thinkingOnce: payload.reasoning?.effort,
          reasoningLevel,
          images: images.length > 0 ? images : undefined,
          clientTools: resolvedClientTools.length > 0 ? resolvedClientTools : undefined,
          extraSystemPrompt: extraSystemPrompt || undefined,
          streamParams: streamParams ?? undefined,
          sessionKey,
          runId: responseId,
          deliver: false,
          messageChannel: "webchat",
          bestEffortDeliver: false,
        },
        defaultRuntime,
        deps,
      );

      finalUsage = extractUsageFromResult(result);
      finalReasoning = buildResponseReasoning({ payload, result });
      finalObservedToolItems = createObservedToolOutputItems(
        extractExecutedToolCalls((result as { meta?: unknown } | null)?.meta),
      );
      maybeFinalize();

      if (closed) {
        return;
      }

      // Fallback: if no streaming deltas were received, send the full response
      if (!sawAssistantDelta) {
        const resultAny = result as { payloads?: Array<{ text?: string }>; meta?: unknown };
        const payloads = resultAny.payloads;
        const meta = resultAny.meta;
        const observedToolItems = createObservedToolOutputItems(extractExecutedToolCalls(meta));
        const stopReason =
          meta && typeof meta === "object"
            ? (meta as { stopReason?: string }).stopReason
            : undefined;
        const pendingToolCalls =
          meta && typeof meta === "object"
            ? (
                meta as {
                  pendingToolCalls?: Array<{ id: string; name: string; arguments: string }>;
                }
              ).pendingToolCalls
            : undefined;

        // If agent called a client tool, emit function_call instead of text
        if (stopReason === "tool_calls" && pendingToolCalls && pendingToolCalls.length > 0) {
          const functionCall = pendingToolCalls[0];
          const usage = finalUsage ?? createEmptyUsage();
          for (const pendingToolCall of pendingToolCalls) {
            toolCallRegistry.registerPendingCall({
              sessionKey,
              responseId,
              callId: pendingToolCall.id,
              toolName: pendingToolCall.name,
              arguments: pendingToolCall.arguments,
            });
          }
          toolCallRegistry.prune();

          writeSseEvent(res, {
            type: "response.output_text.done",
            item_id: outputItemId,
            output_index: 0,
            content_index: 0,
            text: "",
          });
          writeSseEvent(res, {
            type: "response.content_part.done",
            item_id: outputItemId,
            output_index: 0,
            content_index: 0,
            part: { type: "output_text", text: "" },
          });

          const completedItem = createAssistantOutputItem({
            id: outputItemId,
            text: "",
            status: "completed",
          });
          writeSseEvent(res, {
            type: "response.output_item.done",
            output_index: 0,
            item: completedItem,
          });

          const functionCallItemId = `call_${randomUUID()}`;
          const functionCallItem = {
            type: "function_call" as const,
            id: functionCallItemId,
            call_id: functionCall.id,
            name: functionCall.name,
            arguments: functionCall.arguments,
          };
          writeSseEvent(res, {
            type: "response.output_item.added",
            output_index: 1,
            item: functionCallItem,
          });
          writeSseEvent(res, {
            type: "response.output_item.done",
            output_index: 1,
            item: { ...functionCallItem, status: "completed" as const },
          });

          let toolOutputIndex = 2;
          for (const observedItem of observedToolItems) {
            writeSseEvent(res, {
              type: "response.output_item.added",
              output_index: toolOutputIndex,
              item: observedItem,
            });
            writeSseEvent(res, {
              type: "response.output_item.done",
              output_index: toolOutputIndex,
              item: observedItem,
            });
            toolOutputIndex += 1;
          }

          const incompleteResponse = createResponseResource({
            id: responseId,
            model,
            status: "incomplete",
            output: [completedItem, functionCallItem, ...observedToolItems],
            usage,
            reasoning: finalReasoning,
          });
          closed = true;
          unsubscribe();
          writeSseEvent(res, { type: "response.completed", response: incompleteResponse });
          writeDone(res);
          res.end();
          return;
        }

        const content =
          Array.isArray(payloads) && payloads.length > 0
            ? payloads
                .map((p) => (typeof p.text === "string" ? p.text : ""))
                .filter(Boolean)
                .join("\n\n")
            : "No response from OpenClaw.";

        accumulatedText = content;
        sawAssistantDelta = true;

        writeSseEvent(res, {
          type: "response.output_text.delta",
          item_id: outputItemId,
          output_index: 0,
          content_index: 0,
          delta: content,
        });
      }
    } catch (err) {
      logWarn(`openresponses: streaming response failed: ${String(err)}`);
      if (closed) {
        return;
      }
      toolCallRegistry.completeTurn({ sessionKey, responseId });
      toolCallRegistry.prune();

      finalUsage = finalUsage ?? createEmptyUsage();
      const errorResponse = createResponseResource({
        id: responseId,
        model,
        status: "failed",
        output: [],
        error: { code: "api_error", message: "internal error" },
        usage: finalUsage,
        reasoning: requestedReasoning,
      });

      writeSseEvent(res, { type: "response.failed", response: errorResponse });
      emitAgentEvent({
        runId: responseId,
        stream: "lifecycle",
        data: { phase: "error" },
      });
    } finally {
      if (!closed) {
        // Emit lifecycle end to trigger completion
        emitAgentEvent({
          runId: responseId,
          stream: "lifecycle",
          data: { phase: "end" },
        });
      }
    }
  })();

  return true;
}
