type PendingToolCallRecord = {
  sessionKey: string;
  responseId: string;
  callId: string;
  toolName: string;
  arguments: string;
  createdAt: number;
};

type SessionToolCallState = {
  responses: Map<string, Map<string, PendingToolCallRecord>>;
  callToResponse: Map<string, string>;
  consumedCalls: Map<string, number>;
  lastActivity: number;
};

export type OpenResponsesToolCallConsumeFailureReason =
  | "session_not_found"
  | "response_mismatch"
  | "call_not_found"
  | "call_already_consumed";

export type OpenResponsesToolCallConsumeResult =
  | { ok: true; call: PendingToolCallRecord }
  | { ok: false; reason: OpenResponsesToolCallConsumeFailureReason };

export type OpenResponsesToolCallRegistry = {
  registerPendingCall: (params: {
    sessionKey: string;
    responseId: string;
    callId: string;
    toolName: string;
    arguments: string;
    createdAt?: number;
  }) => void;
  consumeToolOutput: (params: {
    sessionKey: string;
    previousResponseId: string;
    callId: string;
  }) => OpenResponsesToolCallConsumeResult;
  completeTurn: (params: { sessionKey: string; responseId: string }) => number;
  resetSession: (sessionKey: string) => number;
  prune: (now?: number) => number;
  pendingCountForSession: (sessionKey: string) => number;
  hasPendingCall: (params: { sessionKey: string; responseId: string; callId: string }) => boolean;
};

const DEFAULT_TTL_MS = 30 * 60 * 1000;
const DEFAULT_MAX_SESSIONS = 2_000;

function createSessionState(now: number): SessionToolCallState {
  return {
    responses: new Map(),
    callToResponse: new Map(),
    consumedCalls: new Map(),
    lastActivity: now,
  };
}

export function createOpenResponsesToolCallRegistry(params?: {
  ttlMs?: number;
  maxSessions?: number;
}): OpenResponsesToolCallRegistry {
  const ttlMs = Math.max(60_000, Math.floor(params?.ttlMs ?? DEFAULT_TTL_MS));
  const maxSessions = Math.max(1, Math.floor(params?.maxSessions ?? DEFAULT_MAX_SESSIONS));
  const sessions = new Map<string, SessionToolCallState>();

  const touchSession = (sessionKey: string, now: number): SessionToolCallState => {
    const existing = sessions.get(sessionKey);
    if (existing) {
      existing.lastActivity = now;
      return existing;
    }
    const created = createSessionState(now);
    sessions.set(sessionKey, created);
    return created;
  };

  const cleanupEmptySession = (sessionKey: string, state: SessionToolCallState) => {
    if (state.responses.size > 0 || state.consumedCalls.size > 0) {
      return;
    }
    sessions.delete(sessionKey);
  };

  const dropResponse = (state: SessionToolCallState, responseId: string): number => {
    const pendingForResponse = state.responses.get(responseId);
    if (!pendingForResponse) {
      return 0;
    }
    for (const callId of pendingForResponse.keys()) {
      state.callToResponse.delete(callId);
    }
    state.responses.delete(responseId);
    return pendingForResponse.size;
  };

  const pruneState = (state: SessionToolCallState, now: number): number => {
    let removed = 0;
    for (const [responseId, calls] of state.responses.entries()) {
      for (const [callId, call] of calls.entries()) {
        if (now - call.createdAt > ttlMs) {
          calls.delete(callId);
          state.callToResponse.delete(callId);
          removed += 1;
        }
      }
      if (calls.size === 0) {
        state.responses.delete(responseId);
      }
    }
    for (const [callId, consumedAt] of state.consumedCalls.entries()) {
      if (now - consumedAt > ttlMs) {
        state.consumedCalls.delete(callId);
        removed += 1;
      }
    }
    return removed;
  };

  return {
    registerPendingCall: (params) => {
      const now = params.createdAt ?? Date.now();
      const sessionKey = params.sessionKey.trim();
      const responseId = params.responseId.trim();
      const callId = params.callId.trim();
      if (!sessionKey || !responseId || !callId) {
        return;
      }

      const state = touchSession(sessionKey, now);

      // Ensure call IDs map to only one response within a session.
      const previousResponseId = state.callToResponse.get(callId);
      if (previousResponseId && previousResponseId !== responseId) {
        dropResponse(state, previousResponseId);
      }

      let pendingForResponse = state.responses.get(responseId);
      if (!pendingForResponse) {
        pendingForResponse = new Map();
        state.responses.set(responseId, pendingForResponse);
      }

      const record: PendingToolCallRecord = {
        sessionKey,
        responseId,
        callId,
        toolName: params.toolName,
        arguments: params.arguments,
        createdAt: now,
      };
      pendingForResponse.set(callId, record);
      state.callToResponse.set(callId, responseId);
      state.consumedCalls.delete(callId);
    },

    consumeToolOutput: (params) => {
      const now = Date.now();
      const sessionKey = params.sessionKey.trim();
      const previousResponseId = params.previousResponseId.trim();
      const callId = params.callId.trim();
      if (!sessionKey || !previousResponseId || !callId) {
        return { ok: false, reason: "call_not_found" };
      }

      const state = sessions.get(sessionKey);
      if (!state) {
        return { ok: false, reason: "session_not_found" };
      }
      state.lastActivity = now;

      const pendingForResponse = state.responses.get(previousResponseId);
      if (!pendingForResponse) {
        if (state.callToResponse.has(callId)) {
          return { ok: false, reason: "response_mismatch" };
        }
        if (state.consumedCalls.has(callId)) {
          return { ok: false, reason: "call_already_consumed" };
        }
        return { ok: false, reason: "call_not_found" };
      }

      const call = pendingForResponse.get(callId);
      if (!call) {
        if (state.callToResponse.has(callId)) {
          return { ok: false, reason: "response_mismatch" };
        }
        if (state.consumedCalls.has(callId)) {
          return { ok: false, reason: "call_already_consumed" };
        }
        return { ok: false, reason: "call_not_found" };
      }

      pendingForResponse.delete(callId);
      state.callToResponse.delete(callId);
      state.consumedCalls.set(callId, now);
      if (pendingForResponse.size === 0) {
        state.responses.delete(previousResponseId);
      }
      cleanupEmptySession(sessionKey, state);
      return { ok: true, call };
    },

    completeTurn: (params) => {
      const sessionKey = params.sessionKey.trim();
      const responseId = params.responseId.trim();
      if (!sessionKey || !responseId) {
        return 0;
      }
      const state = sessions.get(sessionKey);
      if (!state) {
        return 0;
      }
      const removed = dropResponse(state, responseId);
      cleanupEmptySession(sessionKey, state);
      return removed;
    },

    resetSession: (sessionKey) => {
      const key = sessionKey.trim();
      if (!key) {
        return 0;
      }
      const state = sessions.get(key);
      if (!state) {
        return 0;
      }
      let removed = state.consumedCalls.size;
      for (const calls of state.responses.values()) {
        removed += calls.size;
      }
      sessions.delete(key);
      return removed;
    },

    prune: (now = Date.now()) => {
      let removed = 0;
      for (const [sessionKey, state] of sessions.entries()) {
        removed += pruneState(state, now);
        if (state.responses.size === 0 && state.consumedCalls.size === 0) {
          sessions.delete(sessionKey);
        }
      }

      if (sessions.size > maxSessions) {
        const ordered = Array.from(sessions.entries()).toSorted(
          (a, b) => a[1].lastActivity - b[1].lastActivity,
        );
        const excess = sessions.size - maxSessions;
        for (let i = 0; i < excess; i += 1) {
          const key = ordered[i]?.[0];
          if (!key) {
            continue;
          }
          const state = sessions.get(key);
          if (!state) {
            continue;
          }
          for (const calls of state.responses.values()) {
            removed += calls.size;
          }
          removed += state.consumedCalls.size;
          sessions.delete(key);
        }
      }
      return removed;
    },

    pendingCountForSession: (sessionKey) => {
      const key = sessionKey.trim();
      if (!key) {
        return 0;
      }
      const state = sessions.get(key);
      if (!state) {
        return 0;
      }
      let count = 0;
      for (const calls of state.responses.values()) {
        count += calls.size;
      }
      return count;
    },

    hasPendingCall: (params) => {
      const sessionKey = params.sessionKey.trim();
      const responseId = params.responseId.trim();
      const callId = params.callId.trim();
      if (!sessionKey || !responseId || !callId) {
        return false;
      }
      const state = sessions.get(sessionKey);
      if (!state) {
        return false;
      }
      return Boolean(state.responses.get(responseId)?.has(callId));
    },
  };
}

let sharedOpenResponsesToolCallRegistry: OpenResponsesToolCallRegistry | undefined;

export function getOpenResponsesToolCallRegistry(): OpenResponsesToolCallRegistry {
  sharedOpenResponsesToolCallRegistry ??= createOpenResponsesToolCallRegistry();
  return sharedOpenResponsesToolCallRegistry;
}

export function resetOpenResponsesToolCallRegistryBySessionKey(sessionKey: string): number {
  return getOpenResponsesToolCallRegistry().resetSession(sessionKey);
}

export function resetOpenResponsesToolCallRegistryForTest(): void {
  sharedOpenResponsesToolCallRegistry = undefined;
}
