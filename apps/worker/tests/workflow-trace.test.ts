import { beforeEach, describe, expect, it, vi } from "vitest";
import type { WorkflowSpanExport } from "../src/workflow-trace-protocol.js";
import { allowlistSpanAttributes, OTLP_SPAN_ATTRIBUTE_KEYS } from "../src/otlp-attributes.js";
import {
  argumentTrace,
  correlationFromArgs,
  decodeHeader,
  deterministicSpanId,
  encodeHeader,
  hasBackgroundContinuation,
  parseTraceContext
} from "../src/workflow-trace-protocol.js";
import { createWorkflowTraceSinks, exportWorkflowSpan } from "../src/workflow-trace-sink.js";
import { createWorkerOtlp } from "../src/worker-otlp.js";

const state = vi.hoisted(() => ({
  info: {
    workflowId: "workflow:1",
    runId: "run-1",
    workflowType: "codexAppServerTurnWorkflow",
    memo: {} as Record<string, unknown>
  },
  patched: true,
  records: [] as WorkflowSpanExport[]
}));

vi.mock("@temporalio/workflow", () => ({
  ContinueAsNew: class ContinueAsNew extends Error {},
  isCancellation: (error: unknown) => error instanceof Error && error.name === "CancelledFailure",
  patched: () => state.patched,
  proxySinks: () => ({ spaceOtlp: { record: (span: WorkflowSpanExport) => state.records.push(span) } }),
  workflowInfo: () => state.info
}));

import { ContinueAsNew } from "@temporalio/workflow";
import { interceptors } from "../src/workflow-trace.js";

const traceId = "4bf92f3577b34da6a3ce929d0e0e4736";
const parentId = "00f067aa0ba902b7";
const traceparent = `00-${traceId}-${parentId}-01`;
const otherTraceparent = "00-11111111111111111111111111111111-2222222222222222-01";

beforeEach(() => {
  state.records = [];
  state.patched = true;
  state.info.memo = {};
  state.info.workflowType = "codexAppServerTurnWorkflow";
  state.info.runId = "run-1";
});

function activity(args: unknown[] = []) {
  return { activityType: "runTurn", args, headers: {}, options: { startToCloseTimeout: "1 minute" } };
}

describe("workflow trace protocol", () => {
  it("parses W3C traceparents and derives a stable span id without Math.random", () => {
    const random = vi.spyOn(Math, "random");
    expect(parseTraceContext(traceparent)).toEqual({ traceId, spanId: parentId, flags: "01" });
    expect(parseTraceContext("00-00000000000000000000000000000000-00f067aa0ba902b7-01")).toBeNull();
    expect(parseTraceContext("summarize my private prompt")).toBeNull();
    expect(decodeHeader(encodeHeader(traceparent))).toBe(traceparent);
    expect(deterministicSpanId("run-1")).toMatch(/^[0-9a-f]{16}$/);
    expect(deterministicSpanId("run-1")).toBe(deterministicSpanId("run-1"));
    expect(deterministicSpanId("run-1")).not.toBe(deterministicSpanId("run-2"));
    expect(random).not.toHaveBeenCalled();
    random.mockRestore();
  });

  it("reads nested identifiers and continuation markers, and ignores free text", () => {
    const args = [{ item: { missionId: "mission:1", turn: { traceparent, roomId: "room:1", prompt: "private text" } } }];
    expect(argumentTrace(args)?.traceId).toBe(traceId);
    expect(correlationFromArgs(args)).toEqual({ missionId: "mission:1", roomId: "room:1" });
    expect(argumentTrace([{ arbitrary: { traceparent } }])).toBeNull();
    expect(correlationFromArgs([{ roomId: "https://private.example/secret", prompt: "private" }])).toEqual({});
    expect(hasBackgroundContinuation([{ turn: { roomAgentContinuation: { sourceRunId: "agent_run:1", generation: 1 } } }])).toBe(true);
    expect(hasBackgroundContinuation([{ roomAgentContinuation: { sourceRunId: "not an id" } }])).toBe(false);
  });
});

describe("span attribute allowlist", () => {
  it("keeps identifiers, outcomes and durations and drops prompts, paths and urls", () => {
    const attributes = allowlistSpanAttributes({
      "space.workflow_type": "codexAppServerTurnWorkflow",
      "space.activity_type": "runTurn",
      "space.outcome": "FAILED",
      "space.attempt": 2,
      "space.duration_ms": 40,
      "space.reason_code": "TOOL_UNAVAILABLE",
      "space.category": "TOOL_UNAVAILABLE",
      "space.room_id": "room:1",
      "space.link_type": "continue_as_new",
      "space.prompt": "PRIVATE PROMPT",
      "space.path": "/home/operator/secret.txt",
      "http.url": "https://user:secret@example/private",
      "space.outcome_note": "the model said hello",
      "space.attempt_label": "second try with a secret",
      "space.workflow_id": "has spaces",
      "space.category_text": "NOT_A_CATEGORY"
    });
    expect(attributes).toEqual({
      "space.workflow_type": "codexAppServerTurnWorkflow",
      "space.activity_type": "runTurn",
      "space.outcome": "FAILED",
      "space.attempt": 2,
      "space.duration_ms": 40,
      "space.reason_code": "TOOL_UNAVAILABLE",
      "space.category": "TOOL_UNAVAILABLE",
      "space.room_id": "room:1",
      "space.link_type": "continue_as_new"
    });
    expect(Object.keys(attributes).every((key) => (OTLP_SPAN_ATTRIBUTE_KEYS as readonly string[]).includes(key))).toBe(true);
    expect(JSON.stringify(attributes)).not.toMatch(/PRIVATE|secret|https:|spaces/);
  });
});

describe("workflow interceptor", () => {
  it("parents same-trace activities and children under the workflow span", async () => {
    const hooks = interceptors();
    const outbound = hooks.outbound![0]!;
    const activityNext = vi.fn(async () => "ok");
    const childNext = vi.fn(async () => [Promise.resolve("child-run"), Promise.resolve("child")] as [Promise<string>, Promise<unknown>]);
    await hooks.inbound![0]!.execute!({ args: [{ traceparent, roomId: "room:1" }], headers: {} }, async () => {
      await outbound.scheduleActivity!(activity([{ traceparent }]) as never, activityNext);
      await outbound.startChildWorkflowExecution!({
        workflowType: "memoryConsolidationWorkflow",
        options: { args: [{ traceparent }] },
        headers: {}
      } as never, childNext);
      return "done";
    });
    const root = state.records.at(-1)!;
    expect(root).toMatchObject({ traceId, parentSpanId: parentId, outcome: "COMPLETED" });
    expect(decodeHeader(activityNext.mock.calls[0]![0].headers["space-otlp-parent"])).toBe(`00-${traceId}-${root.spanId}-01`);
    expect(decodeHeader(childNext.mock.calls[0]![0].headers["space-otlp-parent"])).toBe(`00-${traceId}-${root.spanId}-01`);
    expect(JSON.stringify(state.records)).not.toMatch(/prompt|private/);
  });

  it("does not change commands or export spans for a history recorded before the patch", async () => {
    state.patched = false;
    const hooks = interceptors();
    const next = vi.fn(async (input: { headers: Record<string, unknown> }) => input.headers);
    const headers = { existing: "kept" };
    await hooks.inbound![0]!.execute!({ args: [{ traceparent }], headers }, async () => {
      return hooks.outbound![0]!.scheduleActivity!(activity([{ traceparent }]) as never, next);
    });
    expect(state.records).toEqual([]);
    expect(next.mock.calls[0]![0].headers).toEqual({});
  });

  it("links a background continuation to the trace it continues and nests it under the parent workflow", async () => {
    const hooks = interceptors();
    const childNext = vi.fn(async () => [Promise.resolve("run"), Promise.resolve("ok")] as [Promise<string>, Promise<unknown>]);
    await hooks.inbound![0]!.execute!({ args: [{ traceparent }], headers: {} }, async () => {
      await hooks.outbound![0]!.startChildWorkflowExecution!({
        workflowType: "codexAppServerTurnWorkflow",
        options: { args: [{ traceparent, traceId: "trace:bg-1", roomAgentContinuation: { sourceRunId: "agent_run:1", generation: 1 } }] },
        headers: {}
      } as never, childNext);
      return "done";
    });
    const parentHeader = decodeHeader(childNext.mock.calls[0]![0].headers["space-otlp-parent"])!;
    const child = interceptors();
    await child.inbound![0]!.execute!({
      args: [{ traceparent, traceId: "trace:bg-1", roomAgentContinuation: { sourceRunId: "agent_run:1", generation: 1 } }],
      headers: childNext.mock.calls[0]![0].headers
    }, async () => "ok");
    const linked = state.records.at(-1)!;
    expect(linked.parentSpanId).toBe(parseTraceContext(parentHeader)?.spanId);
    expect(linked.links).toEqual([{ traceId, spanId: parentId, type: "background_continuation" }]);
  });

  it("parents a later request's child to that request and links the supervisor span", async () => {
    const hooks = interceptors();
    const childNext = vi.fn(async () => [Promise.resolve("run"), Promise.resolve("ok")] as [Promise<string>, Promise<unknown>]);
    await hooks.inbound![0]!.execute!({ args: [{ traceparent }], headers: {} }, async () => {
      await hooks.outbound![0]!.startChildWorkflowExecution!({
        workflowType: "codexAppServerTurnWorkflow",
        options: { args: [{ traceparent: otherTraceparent }] },
        headers: {}
      } as never, childNext);
      return "done";
    });
    const root = state.records.at(-1)!;
    expect(childNext.mock.calls[0]![0].headers["space-otlp-parent"]).toBeUndefined();
    expect(decodeHeader(childNext.mock.calls[0]![0].headers["space-otlp-link"])).toBe(`00-${traceId}-${root.spanId}-01`);
    expect(decodeHeader(childNext.mock.calls[0]![0].headers["space-otlp-link-type"])).toBe("related");
  });

  it("carries a continue-as-new link to the previous run and keeps the API span as parent", async () => {
    const hooks = interceptors();
    const continued = new ContinueAsNew("continue");
    const next = vi.fn(async () => { throw continued; });
    await expect(hooks.inbound![0]!.execute!({ args: [{ traceparent }], headers: {} }, async () => {
      return hooks.outbound![0]!.continueAsNew!({ args: [{ traceparent }], headers: {}, options: {} } as never, next);
    })).rejects.toBe(continued);
    const root = state.records.at(-1)!;
    expect(root.outcome).toBe("CONTINUED");
    const headers = next.mock.calls[0]![0].headers;
    expect(decodeHeader(headers["space-otlp-parent"])).toBe(traceparent);
    expect(decodeHeader(headers["space-otlp-link"])).toBe(`00-${traceId}-${root.spanId}-01`);
    const resumed = interceptors();
    state.info.runId = "run-2";
    await resumed.inbound![0]!.execute!({ args: [{ traceparent }], headers }, async () => "ok");
    expect(state.records.at(-1)).toMatchObject({
      parentSpanId: parentId,
      links: [{ traceId, spanId: root.spanId, type: "continue_as_new" }]
    });
    expect(state.records.at(-1)!.spanId).not.toBe(root.spanId);
  });

  it("uses the memo when arguments and headers have no context, and records failure without the message", async () => {
    state.info.memo = { traceparent };
    const hooks = interceptors();
    await expect(hooks.inbound![0]!.execute!({ args: [{ prompt: "private prompt" }], headers: {} }, async () => {
      throw new Error("private exception /tmp/secret");
    })).rejects.toThrow(/private exception/);
    expect(state.records[0]).toMatchObject({ traceId, parentSpanId: parentId, outcome: "FAILED" });
    expect(JSON.stringify(state.records)).not.toMatch(/private|secret/);
  });
});

describe("workflow sink and worker wiring", () => {
  it("suppresses replay and exports only allowlisted attributes and links", () => {
    const record = vi.fn();
    const sinks = createWorkflowTraceSinks({ record, flush: async () => undefined, close: async () => undefined, stats: () => ({ exported: 0, dropped: 0, failed: 0 }) });
    expect(sinks.spaceOtlp.record.callDuringReplay).toBe(false);
    const sample: WorkflowSpanExport = {
      traceId, spanId: "aaaaaaaaaaaaaaaa", parentSpanId: parentId,
      startedAtMs: 1_000, endedAtMs: 1_250, outcome: "COMPLETED",
      links: [{ traceId, spanId: parentId, type: "continue_as_new" }, { traceId: "nope", spanId: parentId, type: "continue_as_new" }],
      correlation: { roomId: "room:1", prompt: "private", path: "/secret" }
    };
    sinks.spaceOtlp.record.fn({ workflowType: "codexAppServerTurnWorkflow", workflowId: "workflow:1", runId: "run-1" } as never, sample);
    const exported = record.mock.calls[0]![0];
    expect(exported.attributes).toEqual({
      "space.workflow_type": "codexAppServerTurnWorkflow",
      "space.outcome": "COMPLETED",
      "space.workflow_id": "workflow:1",
      "space.run_id": "run-1",
      "space.duration_ms": 250,
      "space.room_id": "room:1"
    });
    expect(exported.links).toEqual([{
      traceId, spanId: parentId, attributes: { "space.link_type": "continue_as_new" }
    }]);
    expect(JSON.stringify(exported, (_key, value) => typeof value === "bigint" ? String(value) : value)).not.toMatch(/private|secret/);
    record.mockImplementation(() => { throw new Error("collector unavailable"); });
    expect(() => sinks.spaceOtlp.record.fn({ workflowType: "x", workflowId: "workflow:1", runId: "run-1" } as never, sample)).not.toThrow();
  });

  it("rejects a workflow span whose ids are not W3C ids", () => {
    const record = vi.fn();
    exportWorkflowSpan({ record } as never, "workflow", "workflow:1", "run-1", {
      traceId: "not-a-trace", spanId: "aaaaaaaaaaaaaaaa", parentSpanId: parentId,
      startedAtMs: 1, endedAtMs: 2, outcome: "COMPLETED", links: [], correlation: {}
    });
    expect(record).not.toHaveBeenCalled();
  });

  it("omits the workflow module and sink unless the endpoint is set", async () => {
    const disabled = createWorkerOtlp({}, "space-worker");
    expect(disabled.sinks).toBeUndefined();
    expect(disabled.interceptors.workflowModules).toBeUndefined();
    expect(disabled.interceptors.activity).toHaveLength(1);
    await disabled.close();

    const enabled = createWorkerOtlp({ SPACE_OTEL_EXPORTER_OTLP_ENDPOINT: "http://127.0.0.1:4318" }, "space-worker");
    expect(enabled.sinks?.spaceOtlp.record.callDuringReplay).toBe(false);
    expect(enabled.interceptors.workflowModules?.[0]).toMatch(/workflow-trace\.ts$/);
    await enabled.close();
  });
});
