import { Type } from "@sinclair/typebox";
import type { OpenClawConfig } from "../../config/config.js";
import type { AnyAgentTool } from "./common.js";
import { fetchWithSsrFGuard } from "../../infra/net/fetch-guard.js";
import { SsrFBlockedError } from "../../infra/net/ssrf.js";
import { logDebug } from "../../logger.js";
import { wrapExternalContent, wrapWebContent } from "../../security/external-content.js";
import { normalizeSecretInput } from "../../utils/normalize-secret-input.js";
import { stringEnum } from "../schema/typebox.js";
import { jsonResult, readNumberParam, readStringParam } from "./common.js";
import {
  extractReadableContent,
  htmlToMarkdown,
  markdownToText,
  truncateText,
  type ExtractMode,
} from "./web-fetch-utils.js";
import {
  CacheEntry,
  DEFAULT_CACHE_TTL_MINUTES,
  DEFAULT_TIMEOUT_SECONDS,
  normalizeCacheKey,
  readCache,
  readResponseText,
  resolveCacheTtlMs,
  resolveTimeoutSeconds,
  withTimeout,
  writeCache,
} from "./web-shared.js";

export { extractReadableContent } from "./web-fetch-utils.js";

const EXTRACT_MODES = ["markdown", "text"] as const;

const DEFAULT_FETCH_MAX_CHARS = 50_000;
const DEFAULT_FETCH_MAX_REDIRECTS = 3;
const DEFAULT_ERROR_MAX_CHARS = 4_000;
const DEFAULT_FIRECRAWL_BASE_URL = "https://api.firecrawl.dev";
const DEFAULT_FIRECRAWL_MAX_AGE_MS = 172_800_000;
const DEFAULT_SENTINEL_MCP_URL = "http://mcp:18790/mcp";
const DEFAULT_SENTINEL_MCP_TOKEN_ENV = "OPENCLAW_GATEWAY_TOKEN";
const DEFAULT_FETCH_USER_AGENT =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36";

const FETCH_CACHE = new Map<string, CacheEntry<Record<string, unknown>>>();

const WebFetchSchema = Type.Object({
  url: Type.String({ description: "HTTP or HTTPS URL to fetch." }),
  extractMode: Type.Optional(
    stringEnum(EXTRACT_MODES, {
      description: 'Extraction mode ("markdown" or "text").',
      default: "markdown",
    }),
  ),
  maxChars: Type.Optional(
    Type.Number({
      description: "Maximum characters to return (truncates when exceeded).",
      minimum: 100,
    }),
  ),
});

type WebFetchConfig = NonNullable<OpenClawConfig["tools"]>["web"] extends infer Web
  ? Web extends { fetch?: infer Fetch }
    ? Fetch
    : undefined
  : undefined;

type FirecrawlFetchConfig =
  | {
      enabled?: boolean;
      apiKey?: string;
      baseUrl?: string;
      onlyMainContent?: boolean;
      maxAgeMs?: number;
      timeoutSeconds?: number;
      aliasWebFetch?: boolean;
    }
  | undefined;

type SentinelMcpAliasConfig = {
  enabled: boolean;
  url: string;
  token?: string;
};

type JsonRpcResponse = {
  error?: { message?: string };
  result?: unknown;
};

function resolveFetchConfig(cfg?: OpenClawConfig): WebFetchConfig {
  const fetch = cfg?.tools?.web?.fetch;
  if (!fetch || typeof fetch !== "object") {
    return undefined;
  }
  return fetch as WebFetchConfig;
}

function resolveFetchEnabled(params: { fetch?: WebFetchConfig; sandboxed?: boolean }): boolean {
  if (typeof params.fetch?.enabled === "boolean") {
    return params.fetch.enabled;
  }
  return true;
}

function resolveFetchReadabilityEnabled(fetch?: WebFetchConfig): boolean {
  if (typeof fetch?.readability === "boolean") {
    return fetch.readability;
  }
  return true;
}

function resolveFetchMaxCharsCap(fetch?: WebFetchConfig): number {
  const raw =
    fetch && "maxCharsCap" in fetch && typeof fetch.maxCharsCap === "number"
      ? fetch.maxCharsCap
      : undefined;
  if (typeof raw !== "number" || !Number.isFinite(raw)) {
    return DEFAULT_FETCH_MAX_CHARS;
  }
  return Math.max(100, Math.floor(raw));
}

function resolveFirecrawlConfig(fetch?: WebFetchConfig): FirecrawlFetchConfig {
  if (!fetch || typeof fetch !== "object") {
    return undefined;
  }
  const firecrawl = "firecrawl" in fetch ? fetch.firecrawl : undefined;
  if (!firecrawl || typeof firecrawl !== "object") {
    return undefined;
  }
  return firecrawl as FirecrawlFetchConfig;
}

function resolveFirecrawlApiKey(firecrawl?: FirecrawlFetchConfig): string | undefined {
  const fromConfig =
    firecrawl && "apiKey" in firecrawl && typeof firecrawl.apiKey === "string"
      ? normalizeSecretInput(firecrawl.apiKey)
      : "";
  const fromEnv = normalizeSecretInput(process.env.FIRECRAWL_API_KEY);
  return fromConfig || fromEnv || undefined;
}

function resolveFirecrawlEnabled(params: {
  firecrawl?: FirecrawlFetchConfig;
  apiKey?: string;
}): boolean {
  if (typeof params.firecrawl?.enabled === "boolean") {
    return params.firecrawl.enabled;
  }
  return Boolean(params.apiKey);
}

function resolveFirecrawlBaseUrl(firecrawl?: FirecrawlFetchConfig): string {
  const raw =
    firecrawl && "baseUrl" in firecrawl && typeof firecrawl.baseUrl === "string"
      ? firecrawl.baseUrl.trim()
      : "";
  return raw || DEFAULT_FIRECRAWL_BASE_URL;
}

function resolveFirecrawlOnlyMainContent(firecrawl?: FirecrawlFetchConfig): boolean {
  if (typeof firecrawl?.onlyMainContent === "boolean") {
    return firecrawl.onlyMainContent;
  }
  return true;
}

function resolveFirecrawlMaxAgeMs(firecrawl?: FirecrawlFetchConfig): number | undefined {
  const raw =
    firecrawl && "maxAgeMs" in firecrawl && typeof firecrawl.maxAgeMs === "number"
      ? firecrawl.maxAgeMs
      : undefined;
  if (typeof raw !== "number" || !Number.isFinite(raw)) {
    return undefined;
  }
  const parsed = Math.max(0, Math.floor(raw));
  return parsed > 0 ? parsed : undefined;
}

function resolveFirecrawlMaxAgeMsOrDefault(firecrawl?: FirecrawlFetchConfig): number {
  const resolved = resolveFirecrawlMaxAgeMs(firecrawl);
  if (typeof resolved === "number") {
    return resolved;
  }
  return DEFAULT_FIRECRAWL_MAX_AGE_MS;
}

function resolveFirecrawlAliasWebFetch(firecrawl?: FirecrawlFetchConfig): boolean {
  if (typeof firecrawl?.aliasWebFetch === "boolean") {
    return firecrawl.aliasWebFetch;
  }
  return false;
}

function readOptionalRecordString(input: unknown, key: string): string | undefined {
  if (!input || typeof input !== "object") {
    return undefined;
  }
  const raw = (input as Record<string, unknown>)[key];
  if (typeof raw !== "string") {
    return undefined;
  }
  const trimmed = raw.trim();
  return trimmed || undefined;
}

function resolveSentinelMcpAliasConfig(cfg?: OpenClawConfig): SentinelMcpAliasConfig {
  const pluginEntry = cfg?.plugins?.entries?.["sentinel-mcp"];
  const pluginConfig =
    pluginEntry && typeof pluginEntry.config === "object" ? pluginEntry.config : undefined;
  const url =
    readOptionalRecordString(pluginConfig, "url") ||
    normalizeSecretInput(process.env.SENTINEL_MCP_URL) ||
    DEFAULT_SENTINEL_MCP_URL;
  const tokenEnv =
    readOptionalRecordString(pluginConfig, "tokenEnv") ||
    normalizeSecretInput(process.env.SENTINEL_MCP_TOKEN_ENV) ||
    DEFAULT_SENTINEL_MCP_TOKEN_ENV;
  const token =
    normalizeSecretInput(readOptionalRecordString(pluginConfig, "token")) ||
    normalizeSecretInput(process.env[tokenEnv]) ||
    undefined;
  return {
    enabled: pluginEntry?.enabled !== false,
    url,
    token,
  };
}

function resolveMaxChars(value: unknown, fallback: number, cap: number): number {
  const parsed = typeof value === "number" && Number.isFinite(value) ? value : fallback;
  const clamped = Math.max(100, Math.floor(parsed));
  return Math.min(clamped, cap);
}

function resolveMaxRedirects(value: unknown, fallback: number): number {
  const parsed = typeof value === "number" && Number.isFinite(value) ? value : fallback;
  return Math.max(0, Math.floor(parsed));
}

function looksLikeHtml(value: string): boolean {
  const trimmed = value.trimStart();
  if (!trimmed) {
    return false;
  }
  const head = trimmed.slice(0, 256).toLowerCase();
  return head.startsWith("<!doctype html") || head.startsWith("<html");
}

function formatWebFetchErrorDetail(params: {
  detail: string;
  contentType?: string | null;
  maxChars: number;
}): string {
  const { detail, contentType, maxChars } = params;
  if (!detail) {
    return "";
  }
  let text = detail;
  const contentTypeLower = contentType?.toLowerCase();
  if (contentTypeLower?.includes("text/html") || looksLikeHtml(detail)) {
    const rendered = htmlToMarkdown(detail);
    const withTitle = rendered.title ? `${rendered.title}\n${rendered.text}` : rendered.text;
    text = markdownToText(withTitle);
  }
  const truncated = truncateText(text.trim(), maxChars);
  return truncated.text;
}

function redactUrlForDebugLog(rawUrl: string): string {
  try {
    const parsed = new URL(rawUrl);
    return parsed.pathname && parsed.pathname !== "/" ? `${parsed.origin}/...` : parsed.origin;
  } catch {
    return "[invalid-url]";
  }
}

const WEB_FETCH_WRAPPER_WITH_WARNING_OVERHEAD = wrapWebContent("", "web_fetch").length;
const WEB_FETCH_WRAPPER_NO_WARNING_OVERHEAD = wrapExternalContent("", {
  source: "web_fetch",
  includeWarning: false,
}).length;

function wrapWebFetchContent(
  value: string,
  maxChars: number,
): {
  text: string;
  truncated: boolean;
  rawLength: number;
  wrappedLength: number;
} {
  if (maxChars <= 0) {
    return { text: "", truncated: true, rawLength: 0, wrappedLength: 0 };
  }
  const includeWarning = maxChars >= WEB_FETCH_WRAPPER_WITH_WARNING_OVERHEAD;
  const wrapperOverhead = includeWarning
    ? WEB_FETCH_WRAPPER_WITH_WARNING_OVERHEAD
    : WEB_FETCH_WRAPPER_NO_WARNING_OVERHEAD;
  if (wrapperOverhead > maxChars) {
    const minimal = includeWarning
      ? wrapWebContent("", "web_fetch")
      : wrapExternalContent("", { source: "web_fetch", includeWarning: false });
    const truncatedWrapper = truncateText(minimal, maxChars);
    return {
      text: truncatedWrapper.text,
      truncated: true,
      rawLength: 0,
      wrappedLength: truncatedWrapper.text.length,
    };
  }
  const maxInner = Math.max(0, maxChars - wrapperOverhead);
  let truncated = truncateText(value, maxInner);
  let wrappedText = includeWarning
    ? wrapWebContent(truncated.text, "web_fetch")
    : wrapExternalContent(truncated.text, { source: "web_fetch", includeWarning: false });

  if (wrappedText.length > maxChars) {
    const excess = wrappedText.length - maxChars;
    const adjustedMaxInner = Math.max(0, maxInner - excess);
    truncated = truncateText(value, adjustedMaxInner);
    wrappedText = includeWarning
      ? wrapWebContent(truncated.text, "web_fetch")
      : wrapExternalContent(truncated.text, { source: "web_fetch", includeWarning: false });
  }

  return {
    text: wrappedText,
    truncated: truncated.truncated,
    rawLength: truncated.text.length,
    wrappedLength: wrappedText.length,
  };
}

function wrapWebFetchField(value: string | undefined): string | undefined {
  if (!value) {
    return value;
  }
  return wrapExternalContent(value, { source: "web_fetch", includeWarning: false });
}

function normalizeContentType(value: string | null | undefined): string | undefined {
  if (!value) {
    return undefined;
  }
  const [raw] = value.split(";");
  const trimmed = raw?.trim();
  return trimmed || undefined;
}

export async function fetchFirecrawlContent(params: {
  url: string;
  extractMode: ExtractMode;
  apiKey: string;
  baseUrl: string;
  onlyMainContent: boolean;
  maxAgeMs: number;
  proxy: "auto" | "basic" | "stealth";
  storeInCache: boolean;
  timeoutSeconds: number;
}): Promise<{
  text: string;
  title?: string;
  finalUrl?: string;
  status?: number;
  warning?: string;
}> {
  const endpoint = resolveFirecrawlEndpoint(params.baseUrl);
  const body: Record<string, unknown> = {
    url: params.url,
    formats: ["markdown"],
    onlyMainContent: params.onlyMainContent,
    timeout: params.timeoutSeconds * 1000,
    maxAge: params.maxAgeMs,
    proxy: params.proxy,
    storeInCache: params.storeInCache,
  };

  const res = await fetch(endpoint, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${params.apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
    signal: withTimeout(undefined, params.timeoutSeconds * 1000),
  });

  const payload = (await res.json()) as {
    success?: boolean;
    data?: {
      markdown?: string;
      content?: string;
      metadata?: {
        title?: string;
        sourceURL?: string;
        statusCode?: number;
      };
    };
    warning?: string;
    error?: string;
  };

  if (!res.ok || payload?.success === false) {
    const detail = payload?.error ?? "";
    throw new Error(
      `Firecrawl fetch failed (${res.status}): ${wrapWebContent(detail || res.statusText, "web_fetch")}`.trim(),
    );
  }

  const data = payload?.data ?? {};
  const rawText =
    typeof data.markdown === "string"
      ? data.markdown
      : typeof data.content === "string"
        ? data.content
        : "";
  const text = params.extractMode === "text" ? markdownToText(rawText) : rawText;
  return {
    text,
    title: data.metadata?.title,
    finalUrl: data.metadata?.sourceURL,
    status: data.metadata?.statusCode,
    warning: payload?.warning,
  };
}

function parseJsonText(value: string): unknown {
  const trimmed = value.trim();
  if (!trimmed || (!trimmed.startsWith("{") && !trimmed.startsWith("["))) {
    return null;
  }
  try {
    return JSON.parse(trimmed);
  } catch {
    return null;
  }
}

function readUnknownString(value: unknown): string | undefined {
  return typeof value === "string" && value ? value : undefined;
}

function readUnknownNumber(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined;
}

function parseSentinelMcpFirecrawlResult(params: {
  payload: JsonRpcResponse;
  extractMode: ExtractMode;
}): {
  text: string;
  title?: string;
  finalUrl?: string;
  status?: number;
  warning?: string;
} {
  const result =
    params.payload.result && typeof params.payload.result === "object"
      ? (params.payload.result as Record<string, unknown>)
      : {};
  const content = Array.isArray(result.content) ? result.content : [];
  let fallbackText = "";
  const candidates: Array<Record<string, unknown>> = [];

  const structured = result.structuredContent;
  if (structured && typeof structured === "object") {
    candidates.push(structured as Record<string, unknown>);
  }

  for (const item of content) {
    if (!item || typeof item !== "object") {
      continue;
    }
    const typed = item as Record<string, unknown>;
    if (typed.type === "text") {
      const text = readUnknownString(typed.text) ?? "";
      if (!fallbackText && text) {
        fallbackText = text;
      }
      const parsed = parseJsonText(text);
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
        candidates.push(parsed as Record<string, unknown>);
      }
      continue;
    }
    if (typed.type === "json" && typed.json && typeof typed.json === "object") {
      candidates.push(typed.json as Record<string, unknown>);
    }
  }

  for (const candidate of candidates) {
    const data =
      candidate.data && typeof candidate.data === "object"
        ? (candidate.data as Record<string, unknown>)
        : candidate;
    const markdown = readUnknownString(data.markdown);
    const plain = readUnknownString(data.content);
    const rawText = markdown || plain;
    if (!rawText) {
      continue;
    }
    const metadata =
      data.metadata && typeof data.metadata === "object"
        ? (data.metadata as Record<string, unknown>)
        : {};
    const warning = readUnknownString(candidate.warning) ?? readUnknownString(data.warning);
    return {
      text: params.extractMode === "text" ? markdownToText(rawText) : rawText,
      title: readUnknownString(metadata.title) ?? readUnknownString(data.title),
      finalUrl: readUnknownString(metadata.sourceURL) ?? readUnknownString(data.sourceURL),
      status: readUnknownNumber(metadata.statusCode) ?? readUnknownNumber(data.statusCode),
      warning,
    };
  }

  if (fallbackText) {
    return {
      text: params.extractMode === "text" ? markdownToText(fallbackText) : fallbackText,
    };
  }

  throw new Error("Sentinel MCP Firecrawl returned no content.");
}

async function fetchSentinelMcpJsonRpc(params: {
  url: string;
  token?: string;
  sessionId?: string;
  body: Record<string, unknown>;
  timeoutSeconds: number;
}): Promise<{
  status: number;
  sessionId?: string;
  payload: JsonRpcResponse;
}> {
  const headers: Record<string, string> = {
    "content-type": "application/json",
  };
  if (params.token) {
    headers.authorization = `Bearer ${params.token}`;
  }
  if (params.sessionId) {
    headers["mcp-session-id"] = params.sessionId;
  }
  const res = await fetch(params.url, {
    method: "POST",
    headers,
    body: JSON.stringify(params.body),
    signal: withTimeout(undefined, params.timeoutSeconds * 1000),
  });
  const text = await res.text();
  const sessionId = res.headers.get("mcp-session-id") ?? undefined;
  if (!text) {
    return { status: res.status, sessionId, payload: {} };
  }
  let payload: JsonRpcResponse;
  try {
    payload = JSON.parse(text) as JsonRpcResponse;
  } catch {
    throw new Error(
      `Sentinel MCP returned non-JSON (status=${res.status}): ${wrapWebContent(text.slice(0, 200), "web_fetch")}`,
    );
  }
  return { status: res.status, sessionId, payload };
}

async function fetchFirecrawlViaSentinelMcp(params: {
  url: string;
  extractMode: ExtractMode;
  sentinelUrl: string;
  sentinelToken?: string;
  onlyMainContent: boolean;
  maxAgeMs: number;
  timeoutSeconds: number;
}): Promise<{
  text: string;
  title?: string;
  finalUrl?: string;
  status?: number;
  warning?: string;
}> {
  const init = await fetchSentinelMcpJsonRpc({
    url: params.sentinelUrl,
    token: params.sentinelToken,
    body: {
      jsonrpc: "2.0",
      id: 1,
      method: "initialize",
      params: {
        protocolVersion: "2024-11-05",
        capabilities: {},
        clientInfo: { name: "openclaw-web-fetch", version: "0.0.0" },
      },
    },
    timeoutSeconds: params.timeoutSeconds,
  });
  if (init.payload.error?.message) {
    throw new Error(
      `Sentinel MCP initialize failed: ${wrapWebContent(init.payload.error.message, "web_fetch")}`,
    );
  }
  const sessionId = init.sessionId;
  if (!sessionId) {
    throw new Error("Sentinel MCP initialize did not return mcp-session-id.");
  }

  await fetchSentinelMcpJsonRpc({
    url: params.sentinelUrl,
    token: params.sentinelToken,
    sessionId,
    body: {
      jsonrpc: "2.0",
      method: "initialized",
      params: {},
    },
    timeoutSeconds: params.timeoutSeconds,
  });

  const scrape = await fetchSentinelMcpJsonRpc({
    url: params.sentinelUrl,
    token: params.sentinelToken,
    sessionId,
    body: {
      jsonrpc: "2.0",
      id: 2,
      method: "tools/call",
      params: {
        name: "firecrawl.firecrawl_scrape",
        arguments: {
          url: params.url,
          formats: ["markdown"],
          onlyMainContent: params.onlyMainContent,
          maxAge: params.maxAgeMs,
        },
      },
    },
    timeoutSeconds: params.timeoutSeconds,
  });
  if (scrape.payload.error?.message) {
    throw new Error(
      `Sentinel MCP tools/call failed (firecrawl.firecrawl_scrape): ${wrapWebContent(scrape.payload.error.message, "web_fetch")}`,
    );
  }

  return parseSentinelMcpFirecrawlResult({
    payload: scrape.payload,
    extractMode: params.extractMode,
  });
}

async function runWebFetch(params: {
  url: string;
  extractMode: ExtractMode;
  maxChars: number;
  maxRedirects: number;
  timeoutSeconds: number;
  cacheTtlMs: number;
  userAgent: string;
  readabilityEnabled: boolean;
  firecrawlEnabled: boolean;
  firecrawlApiKey?: string;
  firecrawlBaseUrl: string;
  firecrawlOnlyMainContent: boolean;
  firecrawlMaxAgeMs: number;
  firecrawlAliasWebFetch: boolean;
  sentinelMcpEnabled: boolean;
  sentinelMcpUrl: string;
  sentinelMcpToken?: string;
  firecrawlProxy: "auto" | "basic" | "stealth";
  firecrawlStoreInCache: boolean;
  firecrawlTimeoutSeconds: number;
}): Promise<Record<string, unknown>> {
  const cacheKey = normalizeCacheKey(
    `fetch:${params.url}:${params.extractMode}:${params.maxChars}`,
  );
  const cached = readCache(FETCH_CACHE, cacheKey);
  if (cached) {
    return { ...cached.value, cached: true };
  }

  let parsedUrl: URL;
  try {
    parsedUrl = new URL(params.url);
  } catch {
    throw new Error("Invalid URL: must be http or https");
  }
  if (!["http:", "https:"].includes(parsedUrl.protocol)) {
    throw new Error("Invalid URL: must be http or https");
  }

  const start = Date.now();
  if (params.firecrawlAliasWebFetch) {
    let firecrawl:
      | {
          text: string;
          title?: string;
          finalUrl?: string;
          status?: number;
          warning?: string;
        }
      | undefined;
    let aliasError: unknown;

    if (params.sentinelMcpEnabled) {
      try {
        firecrawl = await fetchFirecrawlViaSentinelMcp({
          url: params.url,
          extractMode: params.extractMode,
          sentinelUrl: params.sentinelMcpUrl,
          sentinelToken: params.sentinelMcpToken,
          onlyMainContent: params.firecrawlOnlyMainContent,
          maxAgeMs: params.firecrawlMaxAgeMs,
          timeoutSeconds: params.firecrawlTimeoutSeconds,
        });
      } catch (error) {
        aliasError = error;
      }
    }

    if (!firecrawl && params.firecrawlEnabled && params.firecrawlApiKey) {
      firecrawl = await fetchFirecrawlContent({
        url: params.url,
        extractMode: params.extractMode,
        apiKey: params.firecrawlApiKey,
        baseUrl: params.firecrawlBaseUrl,
        onlyMainContent: params.firecrawlOnlyMainContent,
        maxAgeMs: params.firecrawlMaxAgeMs,
        proxy: params.firecrawlProxy,
        storeInCache: params.firecrawlStoreInCache,
        timeoutSeconds: params.firecrawlTimeoutSeconds,
      });
    }

    if (!firecrawl) {
      if (aliasError instanceof Error) {
        throw aliasError;
      }
      throw new Error(
        "Web fetch alias mode requires Sentinel MCP (sentinel-mcp) or Firecrawl API credentials.",
      );
    }

    const wrapped = wrapWebFetchContent(firecrawl.text, params.maxChars);
    const wrappedTitle = firecrawl.title ? wrapWebFetchField(firecrawl.title) : undefined;
    const payload = {
      url: params.url, // Keep raw for tool chaining
      finalUrl: firecrawl.finalUrl || params.url, // Keep raw
      status: firecrawl.status ?? 200,
      contentType: "text/markdown", // Protocol metadata, don't wrap
      title: wrappedTitle,
      extractMode: params.extractMode,
      extractor: "firecrawl",
      externalContent: {
        untrusted: true,
        source: "web_fetch",
        wrapped: true,
      },
      truncated: wrapped.truncated,
      length: wrapped.wrappedLength,
      rawLength: wrapped.rawLength, // Actual content length, not wrapped
      wrappedLength: wrapped.wrappedLength,
      fetchedAt: new Date().toISOString(),
      tookMs: Date.now() - start,
      text: wrapped.text,
      warning: wrapWebFetchField(firecrawl.warning),
    };
    writeCache(FETCH_CACHE, cacheKey, payload, params.cacheTtlMs);
    return payload;
  }

  let res: Response;
  let release: (() => Promise<void>) | null = null;
  let finalUrl = params.url;
  try {
    const result = await fetchWithSsrFGuard({
      url: params.url,
      maxRedirects: params.maxRedirects,
      timeoutMs: params.timeoutSeconds * 1000,
      init: {
        headers: {
          Accept: "text/markdown, text/html;q=0.9, */*;q=0.1",
          "User-Agent": params.userAgent,
          "Accept-Language": "en-US,en;q=0.9",
        },
      },
    });
    res = result.response;
    finalUrl = result.finalUrl;
    release = result.release;

    // Cloudflare Markdown for Agents — log token budget hint when present
    const markdownTokens = res.headers.get("x-markdown-tokens");
    if (markdownTokens) {
      logDebug(
        `[web-fetch] x-markdown-tokens: ${markdownTokens} (${redactUrlForDebugLog(finalUrl)})`,
      );
    }
  } catch (error) {
    if (error instanceof SsrFBlockedError) {
      throw error;
    }
    if (params.firecrawlEnabled && params.firecrawlApiKey) {
      const firecrawl = await fetchFirecrawlContent({
        url: finalUrl,
        extractMode: params.extractMode,
        apiKey: params.firecrawlApiKey,
        baseUrl: params.firecrawlBaseUrl,
        onlyMainContent: params.firecrawlOnlyMainContent,
        maxAgeMs: params.firecrawlMaxAgeMs,
        proxy: params.firecrawlProxy,
        storeInCache: params.firecrawlStoreInCache,
        timeoutSeconds: params.firecrawlTimeoutSeconds,
      });
      const wrapped = wrapWebFetchContent(firecrawl.text, params.maxChars);
      const wrappedTitle = firecrawl.title ? wrapWebFetchField(firecrawl.title) : undefined;
      const payload = {
        url: params.url, // Keep raw for tool chaining
        finalUrl: firecrawl.finalUrl || finalUrl, // Keep raw
        status: firecrawl.status ?? 200,
        contentType: "text/markdown", // Protocol metadata, don't wrap
        title: wrappedTitle,
        extractMode: params.extractMode,
        extractor: "firecrawl",
        externalContent: {
          untrusted: true,
          source: "web_fetch",
          wrapped: true,
        },
        truncated: wrapped.truncated,
        length: wrapped.wrappedLength,
        rawLength: wrapped.rawLength, // Actual content length, not wrapped
        wrappedLength: wrapped.wrappedLength,
        fetchedAt: new Date().toISOString(),
        tookMs: Date.now() - start,
        text: wrapped.text,
        warning: wrapWebFetchField(firecrawl.warning),
      };
      writeCache(FETCH_CACHE, cacheKey, payload, params.cacheTtlMs);
      return payload;
    }
    throw error;
  }

  try {
    if (!res.ok) {
      if (params.firecrawlEnabled && params.firecrawlApiKey) {
        const firecrawl = await fetchFirecrawlContent({
          url: params.url,
          extractMode: params.extractMode,
          apiKey: params.firecrawlApiKey,
          baseUrl: params.firecrawlBaseUrl,
          onlyMainContent: params.firecrawlOnlyMainContent,
          maxAgeMs: params.firecrawlMaxAgeMs,
          proxy: params.firecrawlProxy,
          storeInCache: params.firecrawlStoreInCache,
          timeoutSeconds: params.firecrawlTimeoutSeconds,
        });
        const wrapped = wrapWebFetchContent(firecrawl.text, params.maxChars);
        const wrappedTitle = firecrawl.title ? wrapWebFetchField(firecrawl.title) : undefined;
        const payload = {
          url: params.url, // Keep raw for tool chaining
          finalUrl: firecrawl.finalUrl || finalUrl, // Keep raw
          status: firecrawl.status ?? res.status,
          contentType: "text/markdown", // Protocol metadata, don't wrap
          title: wrappedTitle,
          extractMode: params.extractMode,
          extractor: "firecrawl",
          externalContent: {
            untrusted: true,
            source: "web_fetch",
            wrapped: true,
          },
          truncated: wrapped.truncated,
          length: wrapped.wrappedLength,
          rawLength: wrapped.rawLength, // Actual content length, not wrapped
          wrappedLength: wrapped.wrappedLength,
          fetchedAt: new Date().toISOString(),
          tookMs: Date.now() - start,
          text: wrapped.text,
          warning: wrapWebFetchField(firecrawl.warning),
        };
        writeCache(FETCH_CACHE, cacheKey, payload, params.cacheTtlMs);
        return payload;
      }
      const rawDetail = await readResponseText(res);
      const detail = formatWebFetchErrorDetail({
        detail: rawDetail,
        contentType: res.headers.get("content-type"),
        maxChars: DEFAULT_ERROR_MAX_CHARS,
      });
      const wrappedDetail = wrapWebFetchContent(detail || res.statusText, DEFAULT_ERROR_MAX_CHARS);
      throw new Error(`Web fetch failed (${res.status}): ${wrappedDetail.text}`);
    }

    const contentType = res.headers.get("content-type") ?? "application/octet-stream";
    const normalizedContentType = normalizeContentType(contentType) ?? "application/octet-stream";
    const body = await readResponseText(res);

    let title: string | undefined;
    let extractor = "raw";
    let text = body;
    if (contentType.includes("text/markdown")) {
      // Cloudflare Markdown for Agents: server returned pre-rendered markdown
      extractor = "cf-markdown";
      if (params.extractMode === "text") {
        text = markdownToText(body);
      }
    } else if (contentType.includes("text/html")) {
      if (params.readabilityEnabled) {
        const readable = await extractReadableContent({
          html: body,
          url: finalUrl,
          extractMode: params.extractMode,
        });
        if (readable?.text) {
          text = readable.text;
          title = readable.title;
          extractor = "readability";
        } else {
          const firecrawl = await tryFirecrawlFallback({ ...params, url: finalUrl });
          if (firecrawl) {
            text = firecrawl.text;
            title = firecrawl.title;
            extractor = "firecrawl";
          } else {
            throw new Error(
              "Web fetch extraction failed: Readability and Firecrawl returned no content.",
            );
          }
        }
      } else {
        throw new Error(
          "Web fetch extraction failed: Readability disabled and Firecrawl unavailable.",
        );
      }
    } else if (contentType.includes("application/json")) {
      try {
        text = JSON.stringify(JSON.parse(body), null, 2);
        extractor = "json";
      } catch {
        text = body;
        extractor = "raw";
      }
    }

    const wrapped = wrapWebFetchContent(text, params.maxChars);
    const wrappedTitle = title ? wrapWebFetchField(title) : undefined;
    const payload = {
      url: params.url, // Keep raw for tool chaining
      finalUrl, // Keep raw
      status: res.status,
      contentType: normalizedContentType, // Protocol metadata, don't wrap
      title: wrappedTitle,
      extractMode: params.extractMode,
      extractor,
      externalContent: {
        untrusted: true,
        source: "web_fetch",
        wrapped: true,
      },
      truncated: wrapped.truncated,
      length: wrapped.wrappedLength,
      rawLength: wrapped.rawLength, // Actual content length, not wrapped
      wrappedLength: wrapped.wrappedLength,
      fetchedAt: new Date().toISOString(),
      tookMs: Date.now() - start,
      text: wrapped.text,
    };
    writeCache(FETCH_CACHE, cacheKey, payload, params.cacheTtlMs);
    return payload;
  } finally {
    if (release) {
      await release();
    }
  }
}

async function tryFirecrawlFallback(params: {
  url: string;
  extractMode: ExtractMode;
  firecrawlEnabled: boolean;
  firecrawlApiKey?: string;
  firecrawlBaseUrl: string;
  firecrawlOnlyMainContent: boolean;
  firecrawlMaxAgeMs: number;
  firecrawlProxy: "auto" | "basic" | "stealth";
  firecrawlStoreInCache: boolean;
  firecrawlTimeoutSeconds: number;
}): Promise<{ text: string; title?: string } | null> {
  if (!params.firecrawlEnabled || !params.firecrawlApiKey) {
    return null;
  }
  try {
    const firecrawl = await fetchFirecrawlContent({
      url: params.url,
      extractMode: params.extractMode,
      apiKey: params.firecrawlApiKey,
      baseUrl: params.firecrawlBaseUrl,
      onlyMainContent: params.firecrawlOnlyMainContent,
      maxAgeMs: params.firecrawlMaxAgeMs,
      proxy: params.firecrawlProxy,
      storeInCache: params.firecrawlStoreInCache,
      timeoutSeconds: params.firecrawlTimeoutSeconds,
    });
    return { text: firecrawl.text, title: firecrawl.title };
  } catch {
    return null;
  }
}

function resolveFirecrawlEndpoint(baseUrl: string): string {
  const trimmed = baseUrl.trim();
  if (!trimmed) {
    return `${DEFAULT_FIRECRAWL_BASE_URL}/v2/scrape`;
  }
  try {
    const url = new URL(trimmed);
    if (url.pathname && url.pathname !== "/") {
      return url.toString();
    }
    url.pathname = "/v2/scrape";
    return url.toString();
  } catch {
    return `${DEFAULT_FIRECRAWL_BASE_URL}/v2/scrape`;
  }
}

export function createWebFetchTool(options?: {
  config?: OpenClawConfig;
  sandboxed?: boolean;
}): AnyAgentTool | null {
  const fetch = resolveFetchConfig(options?.config);
  if (!resolveFetchEnabled({ fetch, sandboxed: options?.sandboxed })) {
    return null;
  }
  const readabilityEnabled = resolveFetchReadabilityEnabled(fetch);
  const firecrawl = resolveFirecrawlConfig(fetch);
  const firecrawlApiKey = resolveFirecrawlApiKey(firecrawl);
  const firecrawlEnabled = resolveFirecrawlEnabled({ firecrawl, apiKey: firecrawlApiKey });
  const firecrawlBaseUrl = resolveFirecrawlBaseUrl(firecrawl);
  const firecrawlOnlyMainContent = resolveFirecrawlOnlyMainContent(firecrawl);
  const firecrawlMaxAgeMs = resolveFirecrawlMaxAgeMsOrDefault(firecrawl);
  const firecrawlAliasWebFetch = resolveFirecrawlAliasWebFetch(firecrawl);
  const sentinelMcp = resolveSentinelMcpAliasConfig(options?.config);
  const firecrawlTimeoutSeconds = resolveTimeoutSeconds(
    firecrawl?.timeoutSeconds ?? fetch?.timeoutSeconds,
    DEFAULT_TIMEOUT_SECONDS,
  );
  const userAgent =
    (fetch && "userAgent" in fetch && typeof fetch.userAgent === "string" && fetch.userAgent) ||
    DEFAULT_FETCH_USER_AGENT;
  return {
    label: "Web Fetch",
    name: "web_fetch",
    description:
      "Fetch and extract readable content from a URL (HTML → markdown/text). Use for lightweight page access without browser automation.",
    parameters: WebFetchSchema,
    execute: async (_toolCallId, args) => {
      const params = args as Record<string, unknown>;
      const url = readStringParam(params, "url", { required: true });
      const extractMode = readStringParam(params, "extractMode") === "text" ? "text" : "markdown";
      const maxChars = readNumberParam(params, "maxChars", { integer: true });
      const maxCharsCap = resolveFetchMaxCharsCap(fetch);
      const result = await runWebFetch({
        url,
        extractMode,
        maxChars: resolveMaxChars(
          maxChars ?? fetch?.maxChars,
          DEFAULT_FETCH_MAX_CHARS,
          maxCharsCap,
        ),
        maxRedirects: resolveMaxRedirects(fetch?.maxRedirects, DEFAULT_FETCH_MAX_REDIRECTS),
        timeoutSeconds: resolveTimeoutSeconds(fetch?.timeoutSeconds, DEFAULT_TIMEOUT_SECONDS),
        cacheTtlMs: resolveCacheTtlMs(fetch?.cacheTtlMinutes, DEFAULT_CACHE_TTL_MINUTES),
        userAgent,
        readabilityEnabled,
        firecrawlEnabled,
        firecrawlApiKey,
        firecrawlBaseUrl,
        firecrawlOnlyMainContent,
        firecrawlMaxAgeMs,
        firecrawlAliasWebFetch,
        sentinelMcpEnabled: sentinelMcp.enabled,
        sentinelMcpUrl: sentinelMcp.url,
        sentinelMcpToken: sentinelMcp.token,
        firecrawlProxy: "auto",
        firecrawlStoreInCache: true,
        firecrawlTimeoutSeconds,
      });
      return jsonResult(result);
    },
  };
}
