import { describe, expect, it } from "vitest";
import { createOpenResponsesToolCallRegistry } from "./openresponses-tool-call-registry.js";

describe("openresponses tool-call registry", () => {
  it("consumes only calls registered for same session and parent response", () => {
    const registry = createOpenResponsesToolCallRegistry();
    registry.registerPendingCall({
      sessionKey: "agent:main:openresponses-user:alice",
      responseId: "resp_1",
      callId: "call_1",
      toolName: "web_search",
      arguments: '{"query":"weather"}',
    });

    const ok = registry.consumeToolOutput({
      sessionKey: "agent:main:openresponses-user:alice",
      previousResponseId: "resp_1",
      callId: "call_1",
    });
    expect(ok.ok).toBe(true);

    registry.registerPendingCall({
      sessionKey: "agent:main:openresponses-user:alice",
      responseId: "resp_1",
      callId: "call_2",
      toolName: "web_search",
      arguments: '{"query":"news"}',
    });
    const mismatch = registry.consumeToolOutput({
      sessionKey: "agent:main:openresponses-user:alice",
      previousResponseId: "resp_2",
      callId: "call_2",
    });
    expect(mismatch).toEqual({ ok: false, reason: "response_mismatch" });
  });

  it("prevents replay after call output is consumed", () => {
    const registry = createOpenResponsesToolCallRegistry();
    registry.registerPendingCall({
      sessionKey: "agent:main:openresponses-user:bob",
      responseId: "resp_a",
      callId: "call_replay",
      toolName: "get_weather",
      arguments: '{"location":"SF"}',
    });

    const first = registry.consumeToolOutput({
      sessionKey: "agent:main:openresponses-user:bob",
      previousResponseId: "resp_a",
      callId: "call_replay",
    });
    expect(first.ok).toBe(true);

    const replay = registry.consumeToolOutput({
      sessionKey: "agent:main:openresponses-user:bob",
      previousResponseId: "resp_a",
      callId: "call_replay",
    });
    expect(replay).toEqual({ ok: false, reason: "call_already_consumed" });
  });

  it("drops pending calls for a response on completeTurn", () => {
    const registry = createOpenResponsesToolCallRegistry();
    registry.registerPendingCall({
      sessionKey: "agent:main:openresponses-user:carol",
      responseId: "resp_turn",
      callId: "call_1",
      toolName: "tool_a",
      arguments: "{}",
    });
    registry.registerPendingCall({
      sessionKey: "agent:main:openresponses-user:carol",
      responseId: "resp_turn",
      callId: "call_2",
      toolName: "tool_b",
      arguments: "{}",
    });

    expect(registry.pendingCountForSession("agent:main:openresponses-user:carol")).toBe(2);
    const removed = registry.completeTurn({
      sessionKey: "agent:main:openresponses-user:carol",
      responseId: "resp_turn",
    });
    expect(removed).toBe(2);
    expect(registry.pendingCountForSession("agent:main:openresponses-user:carol")).toBe(0);
  });

  it("resets all pending and consumed state for a session", () => {
    const registry = createOpenResponsesToolCallRegistry();
    registry.registerPendingCall({
      sessionKey: "agent:main:openresponses-user:dana",
      responseId: "resp_1",
      callId: "call_1",
      toolName: "tool_a",
      arguments: "{}",
    });
    registry.registerPendingCall({
      sessionKey: "agent:main:openresponses-user:dana",
      responseId: "resp_1",
      callId: "call_2",
      toolName: "tool_b",
      arguments: "{}",
    });
    registry.consumeToolOutput({
      sessionKey: "agent:main:openresponses-user:dana",
      previousResponseId: "resp_1",
      callId: "call_1",
    });

    const removed = registry.resetSession("agent:main:openresponses-user:dana");
    expect(removed).toBe(2);
    expect(registry.pendingCountForSession("agent:main:openresponses-user:dana")).toBe(0);
  });

  it("prunes stale pending and consumed entries", () => {
    const registry = createOpenResponsesToolCallRegistry({ ttlMs: 60_000 });
    const now = Date.now();

    registry.registerPendingCall({
      sessionKey: "agent:main:openresponses-user:erin",
      responseId: "resp_old",
      callId: "call_old",
      toolName: "tool_old",
      arguments: "{}",
      createdAt: now - 120_000,
    });
    registry.registerPendingCall({
      sessionKey: "agent:main:openresponses-user:erin",
      responseId: "resp_new",
      callId: "call_new",
      toolName: "tool_new",
      arguments: "{}",
      createdAt: now,
    });
    registry.consumeToolOutput({
      sessionKey: "agent:main:openresponses-user:erin",
      previousResponseId: "resp_new",
      callId: "call_new",
    });

    const removed = registry.prune(now + 120_000);
    expect(removed).toBeGreaterThanOrEqual(2);
    expect(registry.pendingCountForSession("agent:main:openresponses-user:erin")).toBe(0);
  });
});
