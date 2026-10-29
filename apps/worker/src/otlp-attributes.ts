/**
 * Attribute keys a worker may put on an OTLP span. Anything else — prompts,
 * model output, file paths, URLs, error messages, user text — is dropped.
 * Names are snake_case, matching the activity attributes already exported.
 */
export const OTLP_SPAN_ATTRIBUTE_KEYS = [
  "space.workflow_type",
  "space.activity_type",
  "space.workflow_id",
  "space.run_id",
  "space.room_id",
  "space.pane_id",
  "space.mission_id",
  "space.attempt",
  "space.outcome",
  "space.reason_code",
  "space.category",
  "space.duration_ms",
  "space.link_type"
] as const;

const allowedKeys = new Set<string>(OTLP_SPAN_ATTRIBUTE_KEYS);
const identifierPattern = /^[A-Za-z0-9:._-]{1,200}$/;
const outcomes = new Set(["COMPLETED", "FAILED", "FAILED_RESULT", "CANCELLED", "CONTINUED"]);
const categories = new Set([
  "TIMEOUT", "TOOL_UNAVAILABLE", "PERMISSION_DENIED", "NOT_FOUND", "INVALID_INPUT",
  "CONFLICT", "RATE_LIMITED", "DISABLED", "CONFIGURATION", "STATE_UNAVAILABLE",
  "PROTOCOL_ERROR", "PROVIDER_ERROR", "UPSTREAM_UNAVAILABLE", "CANCELLED", "INTERNAL"
]);
const linkTypes = new Set(["continue_as_new", "background_continuation", "related"]);
const numericKeys = new Set(["space.attempt", "space.duration_ms"]);

/** Keep only allowlisted keys and identifier-or-bounded values. */
export function allowlistSpanAttributes(
  input: Readonly<Record<string, string | number | null | undefined>>
): Record<string, string | number> {
  const attributes: Record<string, string | number> = {};
  for (const [key, value] of Object.entries(input)) {
    if (!allowedKeys.has(key) || value === undefined || value === null) continue;
    if (typeof value === "number") {
      if (!numericKeys.has(key) || !Number.isSafeInteger(value) || value < 0) continue;
      attributes[key] = value;
      continue;
    }
    if (key === "space.outcome" && !outcomes.has(value)) continue;
    if (key === "space.category" && !categories.has(value)) continue;
    if (key === "space.link_type" && !linkTypes.has(value)) continue;
    if (!identifierPattern.test(value)) continue;
    attributes[key] = value;
  }
  return attributes;
}
