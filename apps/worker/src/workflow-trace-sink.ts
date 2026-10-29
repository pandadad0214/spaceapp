import type { InjectedSinks } from "@temporalio/worker";
import type { Sinks } from "@temporalio/workflow";
import type { SpanExporter } from "@space/runtime";
import { allowlistSpanAttributes } from "./otlp-attributes.js";
import { parseTraceContext, type WorkflowSpanExport } from "./workflow-trace-protocol.js";

export interface WorkflowTraceSinks extends Sinks {
  spaceOtlp: { record(span: WorkflowSpanExport): void };
}

const identifierPattern = /^[A-Za-z0-9:._-]{1,200}$/;

/**
 * Sink implementation. It only enqueues a span. HTTP export stays on the
 * worker side of the sandbox, and `callDuringReplay: false` drops the call
 * while a history is replayed.
 */
export function createWorkflowTraceSinks(spans: SpanExporter): InjectedSinks<WorkflowTraceSinks> {
  return {
    spaceOtlp: {
      record: {
        callDuringReplay: false,
        fn(info, span) {
          try {
            exportWorkflowSpan(spans, info.workflowType, info.workflowId, info.runId, span);
          } catch {
            // Tracing must not fail the workflow task.
          }
        }
      }
    }
  };
}

export function exportWorkflowSpan(
  spans: SpanExporter,
  workflowType: string,
  workflowId: string,
  runId: string,
  span: WorkflowSpanExport
): void {
  if (!["COMPLETED", "FAILED", "CANCELLED", "CONTINUED"].includes(span.outcome)) return;
  if (!Number.isFinite(span.startedAtMs) || !Number.isFinite(span.endedAtMs)) return;
  const context = parseTraceContext(`00-${span.traceId}-${span.spanId}-01`);
  if (!context || !span.parentSpanId || !parseTraceContext(`00-${span.traceId}-${span.parentSpanId}-01`)) return;
  const safeType = identifierPattern.test(workflowType) ? workflowType : "unknown";
  const missionId = span.correlation.missionId ?? span.correlation.roomAgentMissionId;
  const startedAtMs = Math.trunc(span.startedAtMs);
  const endedAtMs = Math.trunc(Math.max(span.startedAtMs, span.endedAtMs));
  spans.record({
    traceId: context.traceId,
    spanId: context.spanId,
    parentSpanId: span.parentSpanId,
    name: `workflow ${safeType}`,
    kind: 1,
    startTimeUnixNano: BigInt(startedAtMs) * 1_000_000n,
    endTimeUnixNano: BigInt(endedAtMs) * 1_000_000n,
    error: span.outcome === "FAILED",
    attributes: allowlistSpanAttributes({
      "space.workflow_type": safeType,
      "space.outcome": span.outcome,
      "space.workflow_id": workflowId,
      "space.run_id": runId,
      "space.duration_ms": Math.max(0, endedAtMs - startedAtMs),
      "space.room_id": span.correlation.roomId,
      "space.pane_id": span.correlation.paneId,
      "space.mission_id": missionId
    }),
    links: span.links.flatMap((link) => {
      if (!parseTraceContext(`00-${link.traceId}-${link.spanId}-01`)) return [];
      return [{
        traceId: link.traceId,
        spanId: link.spanId,
        attributes: allowlistSpanAttributes({ "space.link_type": link.type })
      }];
    })
  });
}
