import { continueAsNew, executeChild, proxyActivities } from "@temporalio/workflow";

const { flaky, leaf } = proxyActivities<{
  flaky(input: { prompt?: string }): Promise<string>;
  leaf(): Promise<string>;
}>({
  startToCloseTimeout: "10 seconds",
  retry: { maximumAttempts: 2, initialInterval: "1 second" }
});

export async function tracedRoot(input: { traceparent: string; prompt?: string }): Promise<string> {
  await flaky(input);
  await executeChild(tracedContinuation, {
    workflowId: "otlp-child",
    args: [{
      traceparent: input.traceparent,
      traceId: "trace:bg-otlp",
      prompt: input.prompt,
      roomAgentContinuation: { sourceRunId: "agent_run:source", generation: 1 }
    }]
  });
  await executeChild(tracedForeign, {
    workflowId: "otlp-foreign",
    args: [{ traceparent: "00-11111111111111111111111111111111-2222222222222222-01", prompt: input.prompt }]
  });
  return "complete";
}

export async function tracedContinuation(_input: { prompt?: string }): Promise<string> {
  await leaf();
  return "child";
}

export async function tracedForeign(): Promise<string> {
  await leaf();
  return "foreign";
}

export async function tracedContinue(input: { traceparent: string; generation: number }): Promise<string> {
  await flaky({ prompt: input.traceparent });
  if (input.generation === 0) await continueAsNew<typeof tracedContinue>({ ...input, generation: 1 });
  return "continued";
}
