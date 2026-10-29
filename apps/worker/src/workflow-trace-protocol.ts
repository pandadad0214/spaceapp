/**
 * Pure helpers shared by the workflow sandbox and the worker process.
 * No Node I/O and no Temporal imports: the sandbox can load this file.
 */

export const TRACE_PARENT_HEADER = "space-otlp-parent";
export const TRACE_LINK_HEADER = "space-otlp-link";
export const TRACE_LINK_TYPE_HEADER = "space-otlp-link-type";
export const WORKFLOW_TRACE_PATCH = "space-otlp-workflow-spans-v1";

export interface TraceContext {
  traceId: string;
  spanId: string;
  flags: string;
}

export interface SpanLink {
  traceId: string;
  spanId: string;
  type: string;
}

export interface WorkflowSpanExport {
  traceId: string;
  spanId: string;
  parentSpanId: string | null;
  startedAtMs: number;
  endedAtMs: number;
  outcome: "COMPLETED" | "FAILED" | "CANCELLED" | "CONTINUED";
  links: SpanLink[];
  correlation: Record<string, string>;
}

const traceparentPattern = /^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$/;
const identifierPattern = /^[A-Za-z0-9:._-]{1,200}$/;
const nestedKeys = ["turn", "item", "bridge", "input", "turnInput"] as const;
const correlationKeys = ["roomId", "paneId", "missionId", "roomAgentMissionId"] as const;

export function parseTraceContext(value: unknown): TraceContext | null {
  if (typeof value !== "string") return null;
  const match = traceparentPattern.exec(value.trim().toLowerCase());
  if (!match || /^0+$/.test(match[1]!) || /^0+$/.test(match[2]!)) return null;
  return { traceId: match[1]!, spanId: match[2]!, flags: match[3]! };
}

export function formatTraceContext(context: TraceContext): string {
  return `00-${context.traceId}-${context.spanId}-${context.flags}`;
}

/** 16 hex chars from the seed. Does not touch the workflow random stream. */
export function deterministicSpanId(seed: string): string {
  let high = 0x811c9dc5;
  let low = 0x01000193;
  for (let index = 0; index < seed.length; index += 1) {
    const code = seed.charCodeAt(index);
    high ^= code;
    high = Math.imul(high, 0x01000193);
    low ^= code + index;
    low = Math.imul(low, 0x85ebca6b);
  }
  const hex = `${(high >>> 0).toString(16).padStart(8, "0")}${(low >>> 0).toString(16).padStart(8, "0")}`;
  return /^0+$/.test(hex) ? "0000000000000001" : hex;
}

export function encodeHeader(value: string): { metadata: { encoding: Uint8Array }; data: Uint8Array } {
  return {
    metadata: { encoding: Uint8Array.from("json/plain", (char) => char.charCodeAt(0)) },
    data: Uint8Array.from(JSON.stringify(value), (char) => char.charCodeAt(0))
  };
}

export function decodeHeader(payload: { data?: Uint8Array | null } | null | undefined): string | null {
  const data = payload?.data;
  if (!data || data.length === 0 || data.length > 512) return null;
  try {
    const parsed: unknown = JSON.parse(String.fromCharCode(...data));
    return typeof parsed === "string" ? parsed : null;
  } catch {
    return null;
  }
}

function visitArgs(args: readonly unknown[], depth: number, visit: (record: Record<string, unknown>, depth: number) => boolean): void {
  const walk = (value: unknown, remaining: number): boolean => {
    if (!value || typeof value !== "object" || Array.isArray(value)) return false;
    const record = value as Record<string, unknown>;
    if (visit(record, remaining)) return true;
    if (remaining <= 0) return false;
    for (const key of nestedKeys) {
      if (walk(record[key], remaining - 1)) return true;
    }
    return false;
  };
  for (const arg of args) {
    if (walk(arg, depth)) return;
  }
}

export function argumentTrace(args: readonly unknown[]): TraceContext | null {
  let found: TraceContext | null = null;
  visitArgs(args, 2, (record) => {
    found = parseTraceContext(record.traceparent);
    return found !== null;
  });
  return found;
}

export function correlationFromArgs(args: readonly unknown[]): Record<string, string> {
  const found: Record<string, string> = {};
  const walk = (value: unknown, remaining: number): void => {
    if (!value || typeof value !== "object" || Array.isArray(value)) return;
    const record = value as Record<string, unknown>;
    for (const key of correlationKeys) {
      const candidate = record[key];
      if (found[key] === undefined && typeof candidate === "string" && identifierPattern.test(candidate)) found[key] = candidate;
    }
    if (remaining <= 0) return;
    for (const key of nestedKeys) walk(record[key], remaining - 1);
  };
  for (const arg of args) walk(arg, 2);
  return found;
}

/** True when the arguments carry a Room Agent background continuation. */
export function hasBackgroundContinuation(args: readonly unknown[]): boolean {
  let found = false;
  const walk = (value: unknown, remaining: number): void => {
    if (found || !value || typeof value !== "object" || Array.isArray(value)) return;
    const record = value as Record<string, unknown>;
    const continuation = record.roomAgentContinuation;
    if (continuation && typeof continuation === "object" && !Array.isArray(continuation)) {
      const sourceRunId = (continuation as { sourceRunId?: unknown }).sourceRunId;
      if (typeof sourceRunId === "string" && identifierPattern.test(sourceRunId)) found = true;
    }
    if (remaining <= 0) return;
    for (const key of nestedKeys) walk(record[key], remaining - 1);
  };
  for (const arg of args) walk(arg, 3);
  return found;
}
