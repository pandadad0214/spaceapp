import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import type { InjectedSinks } from "@temporalio/worker";
import { otlpSpanExporterFromEnv } from "@space/runtime";
import { createActivityObservabilityInterceptors } from "./activity-observability.js";
import { createWorkflowTraceSinks, type WorkflowTraceSinks } from "./workflow-trace-sink.js";

export interface WorkerOtlp {
  sinks?: InjectedSinks<WorkflowTraceSinks>;
  interceptors: {
    workflowModules?: string[];
    activity: Array<ReturnType<typeof createActivityObservabilityInterceptors>>;
  };
  close(): Promise<void>;
}

function workflowTraceModule(): string {
  const compiled = fileURLToPath(new URL("./workflow-trace.js", import.meta.url));
  if (existsSync(compiled)) return compiled;
  const source = compiled.replace(/\.js$/, ".ts");
  return existsSync(source) ? source : compiled;
}

/**
 * OTLP wiring for one Temporal worker. With no endpoint the worker registers
 * the existing activity logs only: no workflow module and no sink, so workflow
 * histories stay as they are. With an endpoint, workflow spans leave the
 * sandbox through the sink and activity spans parent to them.
 */
export function createWorkerOtlp(env: NodeJS.ProcessEnv, serviceName: string): WorkerOtlp {
  const spans = otlpSpanExporterFromEnv(env, serviceName);
  if (!spans) {
    return {
      interceptors: { activity: [createActivityObservabilityInterceptors()] },
      async close() { /* export is off */ }
    };
  }
  return {
    sinks: createWorkflowTraceSinks(spans),
    interceptors: {
      workflowModules: [workflowTraceModule()],
      activity: [createActivityObservabilityInterceptors({ spans })]
    },
    close: () => spans.close()
  };
}
