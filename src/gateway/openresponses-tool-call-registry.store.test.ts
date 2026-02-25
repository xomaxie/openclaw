import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import type { OpenResponsesPendingToolCallRecord } from "./openresponses-tool-call-registry.js";
import {
  loadOpenResponsesToolCallRegistrySnapshot,
  resolveOpenResponsesToolCallRegistryStorePath,
  saveOpenResponsesToolCallRegistrySnapshot,
} from "./openresponses-tool-call-registry.store.js";

const tempDirs: string[] = [];

afterEach(() => {
  for (const dir of tempDirs.splice(0)) {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

function createTempStorePath(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openresponses-tool-store-"));
  tempDirs.push(dir);
  return path.join(dir, "gateway", "openresponses-tool-call-registry.json");
}

function buildPendingCall(overrides?: Partial<OpenResponsesPendingToolCallRecord>) {
  const base: OpenResponsesPendingToolCallRecord = {
    sessionKey: "agent:main:openresponses-user:alice",
    responseId: "resp_1",
    callId: "call_1",
    toolName: "web_search",
    arguments: "{}",
    createdAt: 10_000,
  };
  return { ...base, ...overrides };
}

describe("openresponses tool-call registry store", () => {
  it("saves and restores pending calls while pruning stale entries", () => {
    const pathname = createTempStorePath();
    const fresh = buildPendingCall({ callId: "call_fresh", createdAt: 98_000 });
    const stale = buildPendingCall({ callId: "call_stale", createdAt: 1_000 });
    saveOpenResponsesToolCallRegistrySnapshot({ pathname, pendingCalls: [fresh, stale], now: 100_000 });

    const restored = loadOpenResponsesToolCallRegistrySnapshot({
      pathname,
      ttlMs: 5_000,
      now: 100_000,
    });
    expect(restored).toEqual([fresh]);
  });

  it("returns empty snapshot for missing or malformed files", () => {
    const missing = createTempStorePath();
    expect(loadOpenResponsesToolCallRegistrySnapshot({ pathname: missing })).toEqual([]);

    fs.mkdirSync(path.dirname(missing), { recursive: true });
    fs.writeFileSync(missing, JSON.stringify({ version: 99, pending: [{ bad: true }] }), "utf8");
    expect(loadOpenResponsesToolCallRegistrySnapshot({ pathname: missing })).toEqual([]);
  });

  it("resolves store path from OPENCLAW_STATE_DIR", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "openresponses-tool-store-state-"));
    tempDirs.push(dir);
    const resolved = resolveOpenResponsesToolCallRegistryStorePath({
      OPENCLAW_STATE_DIR: dir,
    } as NodeJS.ProcessEnv);
    expect(resolved).toBe(path.join(dir, "gateway", "openresponses-tool-call-registry.json"));
  });
});
