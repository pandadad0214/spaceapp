import { createServer } from "node:http";
import { once } from "node:events";
import { fileURLToPath } from "node:url";
import { Context } from "@temporalio/activity";
import { Worker } from "@temporalio/worker";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import { describe, expect, it } from "vitest";
import { createOtlpSpanExporter, type ServerSpan } from "@space/runtime";
import { OTLP_SPAN_ATTRIBUTE_KEYS } from "../src/otlp-attributes.js";
import { createActivityObservabilityInterceptors } from "../src/activity-observability.js";
import { createWorkflowTraceSinks } from "../src/workflow-trace-sink.js";

const workflowsPath = fileURLToPath(new URL("./fixtures/otlp-trace-workflows.ts", import.meta.url));
const workflowModules = [fileURLToPath(new URL("../src/workflow-trace.ts", import.meta.url))];
const traceId = "4bf92f3577b34da6a3ce929d0e0e4736";
const apiSpanId = "00f067aa0ba902b7";
const foreignTraceId = "11111111111111111111111111111111";
const traceparent = `00-${traceId}-${apiSpanId}-01`;
const secret = "PRIVATE-CONTROL prompt /tmp/secret.txt https://collector.invalid/token";

interface CollectedSpan {
  traceId: string;
  spanId: string;
  parentSpanId?: string;
  name: string;
  attributes?: { key: string; value: { stringValue?: string; intValue?: string } }[];
  links?: { traceId: string; spanId: string; attributes?: { key: string; value: { stringValue?: string } }[] }[];
}

describe("OTLP workflow span integration", () => {
  it("connects an API span, workflow, retries, child and continuation, and replay emits nothing", async () => {
    const collected: CollectedSpan[] = [];
    const collector = createServer(async (request, response) => {
      const chunks: Buffer[] = [];
      for await (const chunk of request) chunks.push(Buffer.from(chunk));
      if (chunks.length) {
        const body = JSON.parse(Buffer.concat(chunks).toString()) as { resourceSpans?: Array<{ scopeSpans?: Array<{ spans?: CollectedSpan[] }> }> };
        for (const resource of body.resourceSpans ?? []) {
          for (const scope of resource.scopeSpans ?? []) collected.push(...(scope.spans ?? []));
        }
      }
      response.writeHead(200, { "content-type": "application/json" });
      response.end("{}");
    });
    collector.listen(0, "127.0.0.1");
    await once(collector, "listening");
    const port = (collector.address() as { port: number }).port;
    const exporter = createOtlpSpanExporter({ endpoint: `http://127.0.0.1:${port}`, serviceName: "space-test", batchSize: 1, flushIntervalMs: 60_000 });
    const environment = await TestWorkflowEnvironment.createTimeSkipping().catch(async (error: unknown) => {
      await exporter.close();
      await new Promise<void>((resolve) => collector.close(() => resolve()));
      throw error;
    });
    const workerOptions = {
      connection: environment.nativeConnection,
      namespace: "default",
      taskQueue: "otlp-spans",
      workflowsPath,
      activities: {
        flaky: async (input: { prompt?: string }) => {
          if (Context.current().info.attempt === 1) throw new Error(`attempt failed ${secret} ${input.prompt ?? ""}`);
          return "ok";
        },
        leaf: async () => "leaf"
      },
      sinks: createWorkflowTraceSinks(exporter),
      interceptors: {
        workflowModules,
        activity: [createActivityObservabilityInterceptors({ spans: exporter })]
      }
    };
    const serverSpan: ServerSpan = {
      traceId,
      spanId: apiSpanId,
      parentSpanId: null,
      name: "POST /api/turns",
      kind: 2,
      error: false,
      startTimeUnixNano: 1_700_000_000_000_000_000n,
      endTimeUnixNano: 1_700_000_000_010_000_000n,
      attributes: { "http.request.method": "POST", "http.route": "/api/turns", "space.request_id": "req:abcdefghijkl" }
    };
    try {
      exporter.record(serverSpan);
      const worker = await Worker.create(workerOptions);
      await worker.runUntil(async () => {
        const result = await environment.client.workflow.execute("tracedRoot", {
          workflowId: "otlp-root",
          taskQueue: "otlp-spans",
          args: [{ traceparent, prompt: secret }]
        });
        expect(result).toBe("complete");
      });
      const continuation = await Worker.create(workerOptions);
      await continuation.runUntil(() => environment.client.workflow.execute("tracedContinue", {
        workflowId: "otlp-continue",
        taskQueue: "otlp-spans",
        args: [{ traceparent, generation: 0 }]
      }));
      await exporter.flush();

      const root = collected.find((span) => span.name === "workflow tracedRoot");
      const child = collected.find((span) => span.name === "workflow tracedContinuation");
      const foreign = collected.find((span) => span.name === "workflow tracedForeign");
      const retries = collected.filter((span) => span.name === "activity flaky" && span.traceId === traceId && span.parentSpanId === root?.spanId);
      const childActivity = collected.find((span) => span.name === "activity leaf" && span.parentSpanId === child?.spanId);
      const foreignActivity = collected.find((span) => span.name === "activity leaf" && span.parentSpanId === foreign?.spanId);
      expect(root).toMatchObject({ traceId, parentSpanId: apiSpanId });
      expect(child).toMatchObject({ traceId, parentSpanId: root?.spanId });
      expect(child?.links).toEqual([expect.objectContaining({ traceId, spanId: apiSpanId })]);
      expect(foreign).toMatchObject({ traceId: foreignTraceId, parentSpanId: "2222222222222222" });
      expect(foreign?.links).toEqual([expect.objectContaining({ traceId, spanId: root?.spanId })]);
      expect(retries).toHaveLength(2);
      expect(retries.map((span) => span.attributes?.find((attribute) => attribute.key === "space.attempt")?.value.intValue).sort()).toEqual(["1", "2"]);
      expect(retries.some((span) => span.attributes?.some((attribute) => attribute.key === "space.reason_code" && attribute.value.stringValue === "ACTIVITY_ERROR"))).toBe(true);
      expect(childActivity?.traceId).toBe(traceId);
      expect(foreignActivity?.traceId).toBe(foreignTraceId);

      const continued = collected.filter((span) => span.name === "workflow tracedContinue");
      expect(continued).toHaveLength(2);
      expect(continued[1]?.parentSpanId).toBe(apiSpanId);
      expect(continued[1]?.links).toEqual([expect.objectContaining({ traceId, spanId: continued[0]?.spanId })]);
      expect(continued[0]?.spanId).not.toBe(continued[1]?.spanId);

      const workerSpans = collected.filter((span) => span.name.startsWith("workflow ") || span.name.startsWith("activity "));
      for (const span of workerSpans) {
        for (const attribute of span.attributes ?? []) {
          expect(OTLP_SPAN_ATTRIBUTE_KEYS as readonly string[], attribute.key).toContain(attribute.key);
        }
      }
      expect(JSON.stringify(collected)).not.toContain("PRIVATE-CONTROL");
      expect(JSON.stringify(collected)).not.toContain("/tmp/secret");
      expect(JSON.stringify(collected)).not.toContain("https://");

      const beforeReplay = collected.length;
      const history = await environment.client.workflow.getHandle("otlp-root").fetchHistory();
      await Worker.runReplayHistory({
        workflowsPath,
        interceptors: { workflowModules },
        sinks: createWorkflowTraceSinks(exporter)
      }, history);
      await exporter.flush();
      expect(collected).toHaveLength(beforeReplay);
    } finally {
      await exporter.close();
      await environment.teardown();
      await new Promise<void>((resolve) => collector.close(() => resolve()));
    }
  }, 180_000);
});
