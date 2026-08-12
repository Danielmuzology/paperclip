import { createHash, randomUUID } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { eq } from "drizzle-orm";
import {
  agents,
  companies,
  createDb,
  heartbeatRuns,
  issues,
  linkedWorkCompletionOutbox,
} from "@paperclipai/db";
import {
  getEmbeddedPostgresTestSupport,
  startEmbeddedPostgresTestDatabase,
} from "./helpers/embedded-postgres.js";
import { createLinkedWorkCompletionWorker } from "../services/linked-work-completion.js";

const support = await getEmbeddedPostgresTestSupport();
const describePg = support.supported ? describe : describe.skip;

interface Fixture {
  providerEventId: string;
  companyId: string;
  issueId: string;
  agentId: string;
  runId: string;
  linkedWorkId: string;
  correlationId: string;
  originCompanyId: string;
  originIssueId: string;
  evidenceSha256: string;
}

describePg("linked-work completion worker", () => {
  let db!: ReturnType<typeof createDb>;
  let temporary: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>> | null = null;

  beforeAll(async () => {
    temporary = await startEmbeddedPostgresTestDatabase("paperclip-linked-work-completion-");
    db = createDb(temporary.connectionString);
  }, 25_000);

  afterAll(async () => {
    await temporary?.cleanup();
  });

  async function seed(overrides: Partial<typeof linkedWorkCompletionOutbox.$inferInsert> = {}): Promise<Fixture> {
    const companyId = randomUUID();
    const agentId = randomUUID();
    const issueId = randomUUID();
    const runId = randomUUID();
    const linkedWorkId = `linked_work_${randomUUID()}`;
    const correlationId = `cross_org_${randomUUID()}`;
    const originCompanyId = randomUUID();
    const originIssueId = randomUUID();
    const evidenceSha256 = "a".repeat(64);
    const providerEventId = `linked-work-completion-v1:${createHash("sha256").update(`${companyId}\n${issueId}\n${agentId}\n${runId}`).digest("hex")}`;
    await db.insert(companies).values({ id: companyId, name: `C-${companyId}`, issuePrefix: `C${companyId.slice(0, 5)}`, requireBoardApprovalForNewAgents: false });
    await db.insert(agents).values({ id: agentId, companyId, name: "Worker", role: "worker", status: "idle", adapterType: "http", adapterConfig: {}, runtimeConfig: {}, permissions: {} });
    await db.insert(heartbeatRuns).values({
      id: runId,
      companyId,
      agentId,
      status: "succeeded",
      invocationSource: "assignment",
      resultJson: {
        paperclipAdapterDisposition: {
          schemaVersion: "paperclip.adapter-disposition.v1",
          status: "completed",
          binding: { runId, companyId, agentId, issueId },
          disposition: { kind: "done", summary: "Completed.", evidenceSha256 },
        },
      },
    });
    await db.insert(issues).values({ id: issueId, companyId, title: "Linked child", status: "done", priority: "medium", assigneeAgentId: agentId, issueNumber: 1, identifier: `C-${issueId.slice(0, 8)}` });
    const identity = { providerEventId, linkedWorkId, correlationId, originCompanyId, originIssueId, companyId, issueId, agentId, runId, evidenceSha256 };
    await db.insert(linkedWorkCompletionOutbox).values({
      ...identity,
      identityHash: stableHash(identity),
      ...overrides,
    });
    return identity;
  }

  function ack(row: Fixture): Response {
    return Response.json({
      schemaVersion: "cross-org-linked-work.callback-ack.v1",
      status: "completed",
      providerEventIdSha256: createHash("sha256").update(row.providerEventId).digest("hex"),
      linkedWorkId: row.linkedWorkId,
      correlationId: row.correlationId,
      sourceCompanyId: row.companyId,
      issueId: row.issueId,
      sourceRunId: row.runId,
    });
  }

  it("delivers an exact acknowledgement once and preserves immutable evidence", async () => {
    const row = await seed();
    const fetchFn = vi.fn<typeof fetch>().mockResolvedValue(ack(row));
    const worker = createLinkedWorkCompletionWorker(db, { callbackUrl: "https://origin.example.test/integrations/paperclip/linked-work/completion", callbackSecret: "x".repeat(32) }, { fetchFn });
    expect(await worker.processNext()).toBe(true);
    expect((await db.select().from(linkedWorkCompletionOutbox).where(eq(linkedWorkCompletionOutbox.providerEventId, row.providerEventId)))[0]).toMatchObject({ status: "delivered", attemptCount: 1 });
    expect(await worker.processNext()).toBe(false);
    expect(fetchFn).toHaveBeenCalledOnce();
    await expect(db.delete(linkedWorkCompletionOutbox).where(eq(linkedWorkCompletionOutbox.providerEventId, row.providerEventId))).rejects.toThrow();
  });

  it("lets two worker instances contend while only one fenced POST is emitted", async () => {
    const row = await seed();
    let release!: () => void;
    const blocked = new Promise<void>((resolve) => { release = resolve; });
    const fetchFn = vi.fn<typeof fetch>().mockImplementation(async () => {
      await blocked;
      return ack(row);
    });
    const config = { callbackUrl: "https://origin.example.test/integrations/paperclip/linked-work/completion", callbackSecret: "x".repeat(32) };
    const first = createLinkedWorkCompletionWorker(db, config, { fetchFn });
    const second = createLinkedWorkCompletionWorker(db, config, { fetchFn });
    const firstRun = first.processNext();
    await vi.waitFor(() => expect(fetchFn).toHaveBeenCalledOnce());
    expect(await second.processNext()).toBe(false);
    release();
    expect(await firstRun).toBe(true);
    expect(fetchFn).toHaveBeenCalledOnce();
    expect((await db.select().from(linkedWorkCompletionOutbox).where(eq(linkedWorkCompletionOutbox.providerEventId, row.providerEventId)))[0]).toMatchObject({ status: "delivered", attemptCount: 1, leaseFence: 1 });
  });

  it("does not POST after authority expires during preflight and lets recovery send once", async () => {
    const claimedAt = new Date("2026-08-12T12:00:00.000Z");
    const expiredAt = new Date(claimedAt.getTime() + 60_001);
    const row = await seed({ nextAttemptAt: claimedAt });
    const fetchFn = vi.fn<typeof fetch>().mockResolvedValue(ack(row));
    let clockReads = 0;
    const stale = createLinkedWorkCompletionWorker(
      db,
      {
        callbackUrl:
          "https://origin.example.test/integrations/paperclip/linked-work/completion",
        callbackSecret: "x".repeat(32),
      },
      {
        fetchFn,
        now: () => (clockReads++ === 0 ? claimedAt : expiredAt),
      },
    );

    expect(await stale.processNext()).toBe(true);
    expect(fetchFn).not.toHaveBeenCalled();
    expect(
      (
        await db
          .select()
          .from(linkedWorkCompletionOutbox)
          .where(eq(linkedWorkCompletionOutbox.providerEventId, row.providerEventId))
      )[0],
    ).toMatchObject({
      status: "processing",
      sendStarted: false,
      attemptCount: 1,
      leaseFence: 1,
    });

    const recovered = createLinkedWorkCompletionWorker(
      db,
      {
        callbackUrl:
          "https://origin.example.test/integrations/paperclip/linked-work/completion",
        callbackSecret: "x".repeat(32),
      },
      { fetchFn, now: () => expiredAt },
    );
    expect(await recovered.recoverExpired()).toEqual({ released: 1, quarantined: 0 });
    expect(await recovered.processNext()).toBe(true);
    expect(fetchFn).toHaveBeenCalledOnce();
    expect(
      (
        await db
          .select()
          .from(linkedWorkCompletionOutbox)
          .where(eq(linkedWorkCompletionOutbox.providerEventId, row.providerEventId))
      )[0],
    ).toMatchObject({ status: "delivered", attemptCount: 2, leaseFence: 2 });
  });

  it("retries the same deterministic event after an ambiguous accept and after 429 backoff", async () => {
    let clock = new Date("2026-08-12T12:00:00.000Z");
    const row = await seed({ nextAttemptAt: clock });
    const fetchFn = vi.fn<typeof fetch>()
      .mockRejectedValueOnce(new Error("socket closed after accept"))
      .mockResolvedValueOnce(new Response("rate", { status: 429 }))
      .mockResolvedValueOnce(ack(row));
    const worker = createLinkedWorkCompletionWorker(db, { callbackUrl: "https://origin.example.test/integrations/paperclip/linked-work/completion", callbackSecret: "x".repeat(32) }, { fetchFn, now: () => clock });
    expect(await worker.processNext()).toBe(true);
    let durable = (await db.select().from(linkedWorkCompletionOutbox).where(eq(linkedWorkCompletionOutbox.providerEventId, row.providerEventId)))[0]!;
    expect(durable).toMatchObject({ status: "pending", attemptCount: 1, lastErrorCode: "callback_transport_ambiguous" });
    expect(await worker.processNext()).toBe(false);
    clock = new Date(durable.nextAttemptAt.getTime() + 1);
    expect(await worker.processNext()).toBe(true);
    durable = (await db.select().from(linkedWorkCompletionOutbox).where(eq(linkedWorkCompletionOutbox.providerEventId, row.providerEventId)))[0]!;
    expect(durable).toMatchObject({ status: "pending", attemptCount: 2, lastErrorCode: "callback_http_429" });
    clock = new Date(durable.nextAttemptAt.getTime() + 1);
    expect(await worker.processNext()).toBe(true);
    expect((await db.select().from(linkedWorkCompletionOutbox).where(eq(linkedWorkCompletionOutbox.providerEventId, row.providerEventId)))[0]).toMatchObject({ status: "delivered", attemptCount: 3 });
    expect(fetchFn.mock.calls.map((call) => JSON.parse(String(call[1]?.body)).providerEventId)).toEqual([row.providerEventId, row.providerEventId, row.providerEventId]);
  });

  it("manual-reconciles definite rejection, mismatched ack, and chunked oversized ack", async () => {
    const cases: Array<(row: Fixture) => Response> = [
      () => new Response("denied", { status: 403 }),
      (row) => Response.json({ ...JSON.parse(awaitableAck(row)), sourceRunId: randomUUID() }),
      () => new Response(new ReadableStream({ start(controller) { controller.enqueue(new Uint8Array(40_000)); controller.enqueue(new Uint8Array(40_000)); controller.close(); } }), { status: 200, headers: { "content-type": "application/json" } }),
    ];
    for (const responseFor of cases) {
      const row = await seed();
      const worker = createLinkedWorkCompletionWorker(db, { callbackUrl: "https://origin.example.test/integrations/paperclip/linked-work/completion", callbackSecret: "x".repeat(32) }, { fetchFn: vi.fn<typeof fetch>().mockResolvedValue(responseFor(row)) });
      expect(await worker.processNext()).toBe(true);
      expect((await db.select().from(linkedWorkCompletionOutbox).where(eq(linkedWorkCompletionOutbox.providerEventId, row.providerEventId)))[0]?.status).toBe("manual_reconcile");
    }
  });

  it("does not follow redirects or forward the callback credential to another host", async () => {
    const row = await seed();
    const calls: string[] = [];
    const fetchFn = vi.fn<typeof fetch>().mockImplementation((input, init) => {
      calls.push(`${String(input)}:${String(init?.redirect)}`);
      return Promise.resolve(new Response(null, { status: 307, headers: { location: "https://attacker.example.test/collect" } }));
    });
    const worker = createLinkedWorkCompletionWorker(db, { callbackUrl: "https://origin.example.test/integrations/paperclip/linked-work/completion", callbackSecret: "x".repeat(32) }, { fetchFn });
    expect(await worker.processNext()).toBe(true);
    expect(calls).toEqual(["https://origin.example.test/integrations/paperclip/linked-work/completion:manual"]);
    expect((await db.select().from(linkedWorkCompletionOutbox).where(eq(linkedWorkCompletionOutbox.providerEventId, row.providerEventId)))[0]).toMatchObject({ status: "manual_reconcile", lastErrorCode: "callback_http_307" });
  });

  it("cancels unread retryable error bodies before releasing the durable row", async () => {
    const row = await seed();
    let cancelled = false;
    const body = new ReadableStream({
      pull(controller) { controller.enqueue(new Uint8Array(1024)); },
      cancel() { cancelled = true; },
    });
    const worker = createLinkedWorkCompletionWorker(db, { callbackUrl: "https://origin.example.test/integrations/paperclip/linked-work/completion", callbackSecret: "x".repeat(32) }, {
      fetchFn: vi.fn<typeof fetch>().mockResolvedValue(new Response(body, { status: 503 })),
    });
    expect(await worker.processNext()).toBe(true);
    expect(cancelled).toBe(true);
    expect((await db.select().from(linkedWorkCompletionOutbox).where(eq(linkedWorkCompletionOutbox.providerEventId, row.providerEventId)))[0]).toMatchObject({ status: "pending", lastErrorCode: "callback_http_503" });
  });

  it("recovers an expired send lease for idempotent replay and exhausts bounded attempts", async () => {
    const now = new Date("2026-08-12T12:00:00.000Z");
    const expired = await seed({ status: "processing", attemptCount: 1, leaseFence: 1, leaseOwner: "dead", leaseTokenHash: "b".repeat(64), leaseExpiresAt: new Date(now.getTime() - 1), sendStarted: true, nextAttemptAt: now });
    const exhausted = await seed({ attemptCount: 8, maxAttempts: 8, nextAttemptAt: now });
    const fetchFn = vi.fn<typeof fetch>().mockResolvedValue(ack(expired));
    const worker = createLinkedWorkCompletionWorker(db, { callbackUrl: "https://origin.example.test/integrations/paperclip/linked-work/completion", callbackSecret: "x".repeat(32) }, { fetchFn, now: () => now });
    expect(await worker.recoverExpired()).toEqual({ released: 1, quarantined: 0 });
    expect(await worker.processNext()).toBe(true);
    expect((await db.select().from(linkedWorkCompletionOutbox).where(eq(linkedWorkCompletionOutbox.providerEventId, expired.providerEventId)))[0]).toMatchObject({ status: "delivered", attemptCount: 2 });
    expect(await worker.processNext()).toBe(false);
    expect((await db.select().from(linkedWorkCompletionOutbox).where(eq(linkedWorkCompletionOutbox.providerEventId, exhausted.providerEventId)))[0]).toMatchObject({ status: "manual_reconcile", lastErrorCode: "attempts_exhausted" });
  });

  it("does not send when the authoritative strict disposition envelope was mutated", async () => {
    const row = await seed();
    await db.update(heartbeatRuns).set({ resultJson: { paperclipAdapterDisposition: { schemaVersion: "paperclip.adapter-disposition.v1", status: "completed", extra: true } } }).where(eq(heartbeatRuns.id, row.runId));
    const fetchFn = vi.fn<typeof fetch>();
    const worker = createLinkedWorkCompletionWorker(db, { callbackUrl: "https://origin.example.test/integrations/paperclip/linked-work/completion", callbackSecret: "x".repeat(32) }, { fetchFn });
    expect(await worker.processNext()).toBe(true);
    expect(fetchFn).not.toHaveBeenCalled();
    expect((await db.select().from(linkedWorkCompletionOutbox).where(eq(linkedWorkCompletionOutbox.providerEventId, row.providerEventId)))[0]).toMatchObject({ status: "manual_reconcile", lastErrorCode: "completion_binding_mismatch" });
  });

  it("rejects a second successful run for the same linked target issue", async () => {
    const first = await seed();
    const secondRunId = randomUUID();
    await db.insert(heartbeatRuns).values({ id: secondRunId, companyId: first.companyId, agentId: first.agentId, status: "succeeded", invocationSource: "assignment" });
    await expect(db.insert(linkedWorkCompletionOutbox).values({
      providerEventId: `linked-work-completion-v1:${"f".repeat(64)}`,
      identityHash: "e".repeat(64),
      companyId: first.companyId,
      issueId: first.issueId,
      agentId: first.agentId,
      runId: secondRunId,
      linkedWorkId: first.linkedWorkId,
      correlationId: first.correlationId,
      originCompanyId: first.originCompanyId,
      originIssueId: first.originIssueId,
      evidenceSha256: first.evidenceSha256,
    })).rejects.toThrow();
  });
});

function stableHash(value: Record<string, string>): string {
  return createHash("sha256").update(JSON.stringify(Object.fromEntries(Object.entries(value).sort(([a], [b]) => a.localeCompare(b))))).digest("hex");
}

function awaitableAck(row: Fixture): string {
  return JSON.stringify({
    schemaVersion: "cross-org-linked-work.callback-ack.v1",
    status: "completed",
    providerEventIdSha256: createHash("sha256").update(row.providerEventId).digest("hex"),
    linkedWorkId: row.linkedWorkId,
    correlationId: row.correlationId,
    sourceCompanyId: row.companyId,
    issueId: row.issueId,
    sourceRunId: row.runId,
  });
}
