import fs from "node:fs";
import path from "node:path";
import { resolveStateDir } from "../config/paths.js";
import type { OpenResponsesPendingToolCallRecord } from "./openresponses-tool-call-registry.js";

type PersistedOpenResponsesToolCallRegistry = {
  version: 1;
  updatedAt: number;
  pending: OpenResponsesPendingToolCallRecord[];
};

const REGISTRY_VERSION = 1 as const;
const REGISTRY_FILENAME = "openresponses-tool-call-registry.json";

function isPendingToolCallRecord(value: unknown): value is OpenResponsesPendingToolCallRecord {
  if (!value || typeof value !== "object") {
    return false;
  }
  const record = value as Record<string, unknown>;
  return (
    typeof record.sessionKey === "string" &&
    record.sessionKey.trim().length > 0 &&
    typeof record.responseId === "string" &&
    record.responseId.trim().length > 0 &&
    typeof record.callId === "string" &&
    record.callId.trim().length > 0 &&
    typeof record.toolName === "string" &&
    typeof record.arguments === "string" &&
    typeof record.createdAt === "number" &&
    Number.isFinite(record.createdAt)
  );
}

function readJsonFile(pathname: string): unknown {
  try {
    if (!fs.existsSync(pathname)) {
      return undefined;
    }
    return JSON.parse(fs.readFileSync(pathname, "utf8")) as unknown;
  } catch {
    return undefined;
  }
}

function writeJsonFileAtomic(pathname: string, payload: unknown): void {
  const directory = path.dirname(pathname);
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const tmpPath = `${pathname}.tmp-${process.pid}-${Date.now().toString(36)}`;
  fs.writeFileSync(tmpPath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
  fs.renameSync(tmpPath, pathname);
  fs.chmodSync(pathname, 0o600);
}

export function resolveOpenResponsesToolCallRegistryStorePath(
  env: NodeJS.ProcessEnv = process.env,
): string {
  return path.join(resolveStateDir(env), "gateway", REGISTRY_FILENAME);
}

export function loadOpenResponsesToolCallRegistrySnapshot(params?: {
  pathname?: string;
  ttlMs?: number;
  now?: number;
}): OpenResponsesPendingToolCallRecord[] {
  const pathname = params?.pathname ?? resolveOpenResponsesToolCallRegistryStorePath(process.env);
  const ttlMs = Math.max(60_000, Math.floor(params?.ttlMs ?? 30 * 60 * 1000));
  const now = params?.now ?? Date.now();
  const raw = readJsonFile(pathname);
  if (!raw || typeof raw !== "object") {
    return [];
  }
  const parsed = raw as Partial<PersistedOpenResponsesToolCallRegistry>;
  if (parsed.version !== REGISTRY_VERSION || !Array.isArray(parsed.pending)) {
    return [];
  }
  const pending: OpenResponsesPendingToolCallRecord[] = [];
  for (const item of parsed.pending) {
    if (!isPendingToolCallRecord(item)) {
      continue;
    }
    if (now - item.createdAt > ttlMs) {
      continue;
    }
    pending.push({
      sessionKey: item.sessionKey,
      responseId: item.responseId,
      callId: item.callId,
      toolName: item.toolName,
      arguments: item.arguments,
      createdAt: item.createdAt,
    });
  }
  return pending;
}

export function saveOpenResponsesToolCallRegistrySnapshot(params: {
  pendingCalls: OpenResponsesPendingToolCallRecord[];
  pathname?: string;
  now?: number;
}): void {
  const pathname = params.pathname ?? resolveOpenResponsesToolCallRegistryStorePath(process.env);
  const payload: PersistedOpenResponsesToolCallRegistry = {
    version: REGISTRY_VERSION,
    updatedAt: params.now ?? Date.now(),
    pending: params.pendingCalls.filter(isPendingToolCallRecord),
  };
  writeJsonFileAtomic(pathname, payload);
}
