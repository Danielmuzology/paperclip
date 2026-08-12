import { afterEach, describe, expect, it, vi } from "vitest";
import { execute } from "./execute.js";

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("http adapter execute", () => {
  const binding = {
    runId: "00000000-0000-4000-8000-000000000001",
    companyId: "00000000-0000-4000-8000-000000000002",
    agentId: "00000000-0000-4000-8000-000000000003",
    issueId: "00000000-0000-4000-8000-000000000004",
  };

  function context() {
    return {
      runId: binding.runId,
      agent: {
        id: binding.agentId,
        companyId: binding.companyId,
        name: "Agent",
        adapterType: "http",
        adapterConfig: {},
      },
      runtime: {
        sessionId: null,
        sessionParams: null,
        sessionDisplayId: null,
        taskKey: null,
      },
      config: { url: "https://example.test/webhook" },
      context: { issueId: binding.issueId },
      onLog: async () => {},
    };
  }

  function dispositionResponse(disposition: Record<string, unknown>) {
    return new Response(
      JSON.stringify({
        schemaVersion: "paperclip.adapter-disposition.v1",
        status: "completed",
        binding,
        disposition,
      }),
      {
        status: 200,
        headers: {
          "content-type": "application/json",
          "x-paperclip-adapter-contract": "paperclip.adapter-disposition.v1",
        },
      },
    );
  }

  it.each([
    { kind: "done", summary: "Work is complete." },
    {
      kind: "blocked",
      summary: "Waiting for the governed blocker.",
      blockerIssueId: "00000000-0000-4000-8000-000000000005",
    },
    { kind: "continue", summary: "Run the bounded follow-up." },
    {
      kind: "in_review",
      summary: "Ready for governed review.",
      reviewerAgentId: "00000000-0000-4000-8000-000000000006",
    },
  ])("accepts strict bound $kind disposition responses", async (candidate) => {
    const disposition = {
      ...candidate,
      evidenceSha256: "a".repeat(64),
    };
    vi.stubGlobal("fetch", vi.fn(async () => dispositionResponse(disposition)));

    const result = await execute(context());

    expect(result).toMatchObject({
      exitCode: 0,
      timedOut: false,
      summary: candidate.summary,
      resultJson: {
        paperclipAdapterDisposition: {
          binding,
          disposition,
        },
      },
    });
    if (candidate.kind === "continue") {
      expect(result.resultJson).toMatchObject({ nextAction: candidate.summary });
    } else {
      expect(result.resultJson).not.toHaveProperty("nextAction");
    }
  });

  it.each([
    {
      name: "unsupported contract version",
      response: () =>
        new Response("{}", {
          status: 200,
          headers: {
            "content-type": "application/json",
            "x-paperclip-adapter-contract": "paperclip.adapter-disposition.v2",
          },
        }),
    },
    {
      name: "wrong content type",
      response: () =>
        new Response("<html>not a contract</html>", {
          status: 200,
          headers: {
            "content-type": "text/html",
            "x-paperclip-adapter-contract": "paperclip.adapter-disposition.v1",
          },
        }),
    },
    {
      name: "wrong binding",
      response: () =>
        new Response(
          JSON.stringify({
            schemaVersion: "paperclip.adapter-disposition.v1",
            status: "completed",
            binding: { ...binding, issueId: "00000000-0000-4000-8000-000000000099" },
            disposition: {
              kind: "done",
              summary: "Complete.",
              evidenceSha256: "b".repeat(64),
            },
          }),
          {
            status: 200,
            headers: {
              "content-type": "application/json",
              "x-paperclip-adapter-contract": "paperclip.adapter-disposition.v1",
            },
          },
        ),
    },
    {
      name: "extra field",
      response: () =>
        new Response(
          JSON.stringify({
            schemaVersion: "paperclip.adapter-disposition.v1",
            status: "completed",
            binding,
            disposition: {
              kind: "done",
              summary: "Complete.",
              evidenceSha256: "c".repeat(64),
              rawEvidence: "not allowed",
            },
          }),
          {
            status: 200,
            headers: {
              "content-type": "application/json",
              "x-paperclip-adapter-contract": "paperclip.adapter-disposition.v1",
            },
          },
        ),
    },
    {
      name: "secret-bearing summary",
      response: () =>
        dispositionResponse({
          kind: "done",
          summary: '{"DO_SPACES_SECRET":"TOPSECRET"}',
          evidenceSha256: "d".repeat(64),
        }),
    },
    {
      name: "oversized body",
      response: () =>
        new Response("x".repeat(16 * 1024 + 1), {
          status: 200,
          headers: {
            "content-type": "application/json",
            "x-paperclip-adapter-contract": "paperclip.adapter-disposition.v1",
          },
        }),
    },
  ])("rejects $name without returning an issue mutation contract", async ({ response }) => {
    vi.stubGlobal("fetch", vi.fn(async () => response()));

    await expect(execute(context())).rejects.toThrow();
  });

  it("reports configured request timeout as timed_out", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn((_url: string, init?: RequestInit) => new Promise((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () => {
          reject(new DOMException("Aborted", "AbortError"));
        });
      })),
    );

    const result = await execute({
      runId: "run-1",
      agent: {
        id: "agent-1",
        companyId: "company-1",
        name: "Agent",
        adapterType: "http",
        adapterConfig: {},
      },
      runtime: {
        sessionId: null,
        sessionParams: null,
        sessionDisplayId: null,
        taskKey: null,
      },
      config: {
        url: "https://example.test/webhook",
        timeoutMs: 1,
      },
      context: {},
      onLog: async () => {},
    });

    expect(result.timedOut).toBe(true);
    expect(result.errorCode).toBe("timeout");
    expect(result.errorMessage).toContain("timed out after 1ms");
  });
});
