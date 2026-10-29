import {
  ContinueAsNew,
  isCancellation,
  patched,
  proxySinks,
  workflowInfo,
  type Headers,
  type Sinks,
  type WorkflowInterceptors
} from "@temporalio/workflow";
import {
  TRACE_LINK_HEADER,
  TRACE_LINK_TYPE_HEADER,
  TRACE_PARENT_HEADER,
  WORKFLOW_TRACE_PATCH,
  argumentTrace,
  correlationFromArgs,
  decodeHeader,
  deterministicSpanId,
  encodeHeader,
  formatTraceContext,
  hasBackgroundContinuation,
  parseTraceContext,
  type SpanLink,
  type TraceContext,
  type WorkflowSpanExport
} from "./workflow-trace-protocol.js";

interface WorkflowTraceSinks extends Sinks {
  spaceOtlp: { record(span: WorkflowSpanExport): void };
}

interface OpenSpan {
  traceId: string;
  spanId: string;
  parentSpanId: string;
  origin: TraceContext;
  startedAtMs: number;
  endedAtMs: number;
  outcome: WorkflowSpanExport["outcome"];
  links: SpanLink[];
  correlation: Record<string, string>;
  inflight: number;
  emitted: boolean;
}

type HeaderCarrier = { headers: Headers };

/**
 * Workflow-sandbox interceptor. It only reads deterministic workflow state and
 * hands spans to the `spaceOtlp` sink. The sink is registered with
 * `callDuringReplay: false`, so replay does not export. Histories recorded
 * before this patch keep their original commands: `patched` is false and no
 * headers are added.
 */
export function interceptors(): WorkflowInterceptors {
  const { spaceOtlp } = proxySinks<WorkflowTraceSinks>();
  const info = workflowInfo();
  let enabled = false;
  let root: OpenSpan | null = null;
  const extras = new Map<string, OpenSpan>();

  const emit = (span: OpenSpan, outcome: WorkflowSpanExport["outcome"]) => {
    if (span.emitted) return;
    span.emitted = true;
    span.outcome = outcome;
    span.endedAtMs = Date.now();
    const record: WorkflowSpanExport = {
      traceId: span.traceId,
      spanId: span.spanId,
      parentSpanId: span.parentSpanId,
      startedAtMs: span.startedAtMs,
      endedAtMs: span.endedAtMs,
      outcome,
      links: span.links,
      correlation: span.correlation
    };
    spaceOtlp.record(record);
  };

  const open = (parent: TraceContext, args: readonly unknown[], links: SpanLink[], discriminator: string): OpenSpan => ({
    traceId: parent.traceId,
    spanId: deterministicSpanId(`${info.runId}:${parent.traceId}:${parent.spanId}:${discriminator}`),
    parentSpanId: parent.spanId,
    origin: parent,
    startedAtMs: Date.now(),
    endedAtMs: Date.now(),
    outcome: "COMPLETED",
    links,
    correlation: correlationFromArgs(args),
    inflight: 0,
    emitted: false
  });

  const parentOf = (headers: Headers, args: readonly unknown[]): TraceContext | null => {
    const fromHeader = parseTraceContext(decodeHeader(headers[TRACE_PARENT_HEADER]));
    if (fromHeader) return fromHeader;
    return argumentTrace(args) ?? parseTraceContext(info.memo?.traceparent);
  };

  const linksOf = (headers: Headers, args: readonly unknown[], parent: TraceContext): SpanLink[] => {
    const links: SpanLink[] = [];
    const headerLink = parseTraceContext(decodeHeader(headers[TRACE_LINK_HEADER]));
    const headerType = decodeHeader(headers[TRACE_LINK_TYPE_HEADER]) ?? "continue_as_new";
    if (headerLink && (headerLink.traceId !== parent.traceId || headerLink.spanId !== parent.spanId)) {
      links.push({ traceId: headerLink.traceId, spanId: headerLink.spanId, type: headerType });
    }
    const continued = argumentTrace(args);
    if (hasBackgroundContinuation(args) && continued
      && (continued.traceId !== parent.traceId || continued.spanId !== parent.spanId)
      && !links.some((link) => link.traceId === continued.traceId && link.spanId === continued.spanId)) {
      links.push({ traceId: continued.traceId, spanId: continued.spanId, type: "background_continuation" });
    }
    return links;
  };

  const withParent = <T extends HeaderCarrier>(input: T, span: OpenSpan): T => ({
    ...input,
    headers: {
      ...input.headers,
      [TRACE_PARENT_HEADER]: encodeHeader(formatTraceContext({ traceId: span.traceId, spanId: span.spanId, flags: "01" }))
    }
  });

  const spanForActivity = (args: readonly unknown[]): OpenSpan => {
    const traced = argumentTrace(args);
    if (!root || !traced || traced.traceId === root.traceId) return root!;
    const key = `${traced.traceId}:${traced.spanId}`;
    let span = extras.get(key);
    if (!span) {
      span = open(traced, args, [{ traceId: root.traceId, spanId: root.spanId, type: "related" }], key);
      extras.set(key, span);
    }
    return span;
  };

  const finishExtra = (span: OpenSpan) => {
    if (span === root) return;
    span.inflight -= 1;
    if (span.inflight <= 0) emit(span, span.outcome);
  };

  return {
    inbound: [{
      async execute(input, next) {
        enabled = patched(WORKFLOW_TRACE_PATCH);
        if (!enabled) return next(input);
        const parent = parentOf(input.headers, input.args);
        if (!parent) return next(input);
        root = open(parent, input.args, linksOf(input.headers, input.args, parent), "workflow");
        try {
          const result = await next(input);
          for (const span of extras.values()) emit(span, span.outcome);
          emit(root, "COMPLETED");
          return result;
        } catch (error) {
          const outcome: WorkflowSpanExport["outcome"] = error instanceof ContinueAsNew
            ? "CONTINUED"
            : isCancellation(error) ? "CANCELLED" : "FAILED";
          for (const span of extras.values()) emit(span, span.outcome === "FAILED" ? "FAILED" : outcome);
          emit(root, outcome);
          throw error;
        }
      }
    }],
    outbound: [{
      async scheduleActivity(input, next) {
        if (!enabled || !root) return next(input);
        const span = spanForActivity(input.args);
        if (span !== root) span.inflight += 1;
        try {
          return await next(withParent(input, span));
        } catch (error) {
          if (span !== root) span.outcome = "FAILED";
          throw error;
        } finally {
          finishExtra(span);
        }
      },
      async scheduleLocalActivity(input, next) {
        if (!enabled || !root) return next(input);
        const span = spanForActivity(input.args);
        if (span !== root) span.inflight += 1;
        try {
          return await next(withParent(input, span));
        } catch (error) {
          if (span !== root) span.outcome = "FAILED";
          throw error;
        } finally {
          finishExtra(span);
        }
      },
      async startChildWorkflowExecution(input, next) {
        if (!enabled || !root) return next(input);
        const args = input.options?.args ?? [];
        const childTrace = argumentTrace(args);
        if (!childTrace || childTrace.traceId === root.traceId) return next(withParent(input, root));
        return next({
          ...input,
          headers: {
            ...input.headers,
            [TRACE_LINK_HEADER]: encodeHeader(formatTraceContext({ traceId: root.traceId, spanId: root.spanId, flags: "01" })),
            [TRACE_LINK_TYPE_HEADER]: encodeHeader("related")
          }
        });
      },
      async continueAsNew(input, next) {
        if (!enabled || !root) return next(input);
        return next({
          ...input,
          headers: {
            ...input.headers,
            [TRACE_PARENT_HEADER]: encodeHeader(formatTraceContext(root.origin)),
            [TRACE_LINK_HEADER]: encodeHeader(formatTraceContext({ traceId: root.traceId, spanId: root.spanId, flags: "01" })),
            [TRACE_LINK_TYPE_HEADER]: encodeHeader("continue_as_new")
          }
        });
      }
    }]
  };
}
