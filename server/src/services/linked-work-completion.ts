import { createHash, randomBytes } from "node:crypto";
import { and, eq, lte, sql } from "drizzle-orm";
import type { Db } from "@paperclipai/db";
import {
  heartbeatRuns,
  issues,
  linkedWorkCompletionOutbox,
} from "@paperclipai/db";
import { logger } from "../middleware/logger.js";
import { paperclipAdapterDispositionEnvelopeSchema } from "../adapters/http/disposition.js";

const leaseMs = 60_000;
const responseLimitBytes = 64 * 1024;
const requestTimeoutMs = 20_000;

export interface LinkedWorkCompletionWorkerConfig {
  callbackUrl: string;
  callbackSecret: string;
}

export function createLinkedWorkCompletionWorker(
  db: Db,
  config: LinkedWorkCompletionWorkerConfig,
  options: { fetchFn?: typeof fetch; now?: () => Date } = {},
) {
  const fetchFn = options.fetchFn ?? fetch;
  const now = options.now ?? (() => new Date());
  const owner = `linked-work-completion-${process.pid}-${randomBytes(8).toString("hex")}`;
  let stopped = false;
  let running: Promise<boolean> | undefined;

  async function recoverExpired(): Promise<{ released: number; quarantined: number }> {
    const instant = now();
    const expired = await db
      .select({
        providerEventId: linkedWorkCompletionOutbox.providerEventId,
        sendStarted: linkedWorkCompletionOutbox.sendStarted,
      })
      .from(linkedWorkCompletionOutbox)
      .where(
        and(
          eq(linkedWorkCompletionOutbox.status, "processing"),
          lte(linkedWorkCompletionOutbox.leaseExpiresAt, instant),
        ),
      );
    let released = 0;
    let quarantined = 0;
    for (const row of expired) {
      const updated = await db
        .update(linkedWorkCompletionOutbox)
        .set({
          status: "pending",
          leaseOwner: null,
          leaseTokenHash: null,
          leaseExpiresAt: null,
          sendStarted: false,
          nextAttemptAt: instant,
          updatedAt: instant,
          ...(row.sendStarted ? {
            lastErrorCode: "expired_after_send_started_retrying_idempotent_event",
            lastErrorSummary: "The deterministic callback event will be retried after an expired send lease.",
          } : {}),
        })
        .where(
          and(
            eq(linkedWorkCompletionOutbox.providerEventId, row.providerEventId),
            eq(linkedWorkCompletionOutbox.status, "processing"),
            lte(linkedWorkCompletionOutbox.leaseExpiresAt, instant),
          ),
        )
        .returning({ id: linkedWorkCompletionOutbox.providerEventId });
      if (updated.length === 1) {
        released += 1;
      }
    }
    return { released, quarantined };
  }

  async function processNext(): Promise<boolean> {
    if (stopped) return false;
    const current = running ?? processOne();
    running = current;
    try {
      return await current;
    } finally {
      if (running === current) running = undefined;
    }
  }

  async function processOne(): Promise<boolean> {
    const instant = now();
    await db
      .update(linkedWorkCompletionOutbox)
      .set({
        status: "manual_reconcile",
        lastErrorCode: "attempts_exhausted",
        lastErrorSummary: "Completion delivery attempts were exhausted.",
        updatedAt: instant,
      })
      .where(
        and(
          eq(linkedWorkCompletionOutbox.status, "pending"),
          sql`${linkedWorkCompletionOutbox.attemptCount} >= ${linkedWorkCompletionOutbox.maxAttempts}`,
        ),
      );

    const tokenHash = createHash("sha256")
      .update(randomBytes(32))
      .digest("hex");
    const leaseExpiresAt = new Date(instant.getTime() + leaseMs);
    const claimed = await db.transaction(async (tx) => {
      const row = await tx
        .select()
        .from(linkedWorkCompletionOutbox)
        .where(and(
          eq(linkedWorkCompletionOutbox.status, "pending"),
          lte(linkedWorkCompletionOutbox.nextAttemptAt, instant),
          sql`${linkedWorkCompletionOutbox.attemptCount} < ${linkedWorkCompletionOutbox.maxAttempts}`,
        ))
        .orderBy(
          linkedWorkCompletionOutbox.createdAt,
          linkedWorkCompletionOutbox.providerEventId,
        )
        .limit(1)
        .for("update", { skipLocked: true })
        .then((rows) => rows[0] ?? null);
      if (!row) return null;
      return tx
        .update(linkedWorkCompletionOutbox)
        .set({
          status: "processing",
          attemptCount: row.attemptCount + 1,
          leaseFence: row.leaseFence + 1,
          leaseOwner: owner,
          leaseTokenHash: tokenHash,
          leaseExpiresAt,
          sendStarted: false,
          updatedAt: instant,
        })
        .where(
          and(
            eq(linkedWorkCompletionOutbox.providerEventId, row.providerEventId),
            eq(linkedWorkCompletionOutbox.status, "pending"),
          ),
        )
        .returning()
        .then((rows) => rows[0] ?? null);
    });
    if (!claimed) return false;

    const fence = and(
      eq(linkedWorkCompletionOutbox.providerEventId, claimed.providerEventId),
      eq(linkedWorkCompletionOutbox.status, "processing"),
      eq(linkedWorkCompletionOutbox.leaseOwner, owner),
      eq(linkedWorkCompletionOutbox.leaseFence, claimed.leaseFence),
      eq(linkedWorkCompletionOutbox.leaseTokenHash, tokenHash),
    );
    try {
      const authoritative = await loadAuthoritativeCompletion(db, claimed);
      if (!authoritative) {
        await markManual(
          db,
          fence,
          instant,
          "completion_binding_mismatch",
          "Durable completion no longer matches the authoritative issue and run.",
        );
        return true;
      }
      const marked = await db
        .update(linkedWorkCompletionOutbox)
        .set({ sendStarted: true, updatedAt: now() })
        .where(fence)
        .returning({ id: linkedWorkCompletionOutbox.providerEventId });
      if (marked.length !== 1) throw new Error("Completion delivery lease was lost.");

      const response = await postCallback(fetchFn, config, claimed);
      if (response.kind === "manual") {
        await markManual(db, fence, now(), response.code, response.summary);
        return true;
      }
      if (response.kind === "retry") {
        await markRetry(db, fence, claimed, now(), response.code, response.summary);
        return true;
      }
      const delivered = await db
        .update(linkedWorkCompletionOutbox)
        .set({
          status: "delivered",
          leaseOwner: null,
          leaseTokenHash: null,
          leaseExpiresAt: null,
          deliveredAt: now(),
          updatedAt: now(),
          lastErrorCode: null,
          lastErrorSummary: null,
        })
        .where(fence)
        .returning({ id: linkedWorkCompletionOutbox.providerEventId });
      if (delivered.length !== 1) {
        throw new Error("Completion delivery acknowledgement lost its durable fence.");
      }
      return true;
    } catch (error) {
      try {
        await markRetry(db, fence, claimed, now(), "callback_outcome_ambiguous_retry", "The deterministic callback event will be retried after an ambiguous outcome.");
      } catch {
        // The durable lease/state fence is authoritative if another recovery won.
      }
      logger.error(
        { providerEventIdSha256: sha256(claimed.providerEventId) },
        "Linked-work completion callback will retry under its durable fence.",
      );
      return true;
    }
  }

  return {
    recoverExpired,
    processNext,
    async stop(): Promise<void> {
      stopped = true;
      await running;
    },
  };
}

async function loadAuthoritativeCompletion(
  db: Db,
  row: typeof linkedWorkCompletionOutbox.$inferSelect,
): Promise<boolean> {
  const [issue, run] = await Promise.all([
    db
      .select({
        id: issues.id,
        companyId: issues.companyId,
        status: issues.status,
        assigneeAgentId: issues.assigneeAgentId,
      })
      .from(issues)
      .where(and(eq(issues.id, row.issueId), eq(issues.companyId, row.companyId)))
      .then((rows) => rows[0] ?? null),
    db
      .select({
        id: heartbeatRuns.id,
        companyId: heartbeatRuns.companyId,
        agentId: heartbeatRuns.agentId,
        status: heartbeatRuns.status,
        resultJson: heartbeatRuns.resultJson,
      })
      .from(heartbeatRuns)
      .where(eq(heartbeatRuns.id, row.runId))
      .then((rows) => rows[0] ?? null),
  ]);
  if (
    !issue ||
    issue.status !== "done" ||
    issue.assigneeAgentId !== row.agentId ||
    !run ||
    run.status !== "succeeded" ||
    run.companyId !== row.companyId ||
    run.agentId !== row.agentId
  ) {
    return false;
  }
  const envelope = readDispositionEnvelope(run.resultJson);
  return Boolean(
    envelope &&
      envelope.binding.runId === row.runId &&
      envelope.binding.companyId === row.companyId &&
      envelope.binding.agentId === row.agentId &&
      envelope.binding.issueId === row.issueId &&
      envelope.disposition.kind === "done" &&
      envelope.disposition.evidenceSha256 === row.evidenceSha256 &&
      row.identityHash === identityHash(row),
  );
}

async function postCallback(
  fetchFn: typeof fetch,
  config: LinkedWorkCompletionWorkerConfig,
  row: typeof linkedWorkCompletionOutbox.$inferSelect,
): Promise<
  | { kind: "accepted" }
  | { kind: "retry"; code: string; summary: string }
  | { kind: "manual"; code: string; summary: string }
> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), requestTimeoutMs);
  try {
    let response: Response;
    try {
      response = await fetchFn(config.callbackUrl, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-paperclip-webhook-secret": config.callbackSecret,
      },
      body: JSON.stringify({
        schemaVersion: "cross-org-linked-work.callback.v2",
        sourceCompanyId: row.companyId,
        issueId: row.issueId,
        sourceAgentId: row.agentId,
        sourceRunId: row.runId,
        providerEventId: row.providerEventId,
        issueStatus: "done",
        linkedWorkId: row.linkedWorkId,
        correlationId: row.correlationId,
        originCompanyId: row.originCompanyId,
        originIssueId: row.originIssueId,
        evidenceSha256: row.evidenceSha256,
      }),
      redirect: "manual",
      signal: controller.signal,
      });
    } catch {
      return { kind: "retry", code: "callback_transport_ambiguous", summary: "The deterministic callback transport outcome was ambiguous." };
    }
    if (!response.ok) {
      await response.body?.cancel().catch(() => undefined);
      return {
        kind: response.status === 429 || response.status >= 500 ? "retry" : "manual",
        code: `callback_http_${response.status}`,
        summary: "Origin callback returned a non-success status.",
      };
    }
    const contentType = response.headers.get("content-type")?.split(";", 1)[0]?.trim();
    const length = Number(response.headers.get("content-length") ?? "0");
    if (contentType !== "application/json" || length > responseLimitBytes) {
      await response.body?.cancel().catch(() => undefined);
      return { kind: "manual", code: "callback_ack_invalid", summary: "Origin callback acknowledgement was invalid." };
    }
    const bytes = await readBoundedBody(response, responseLimitBytes);
    if (!bytes) return { kind: "manual", code: "callback_ack_oversized", summary: "Origin callback acknowledgement exceeded its bound." };
    const ack = parseAck(new TextDecoder().decode(bytes));
    if (
      !ack ||
      ack.providerEventIdSha256 !== sha256(row.providerEventId) ||
      ack.linkedWorkId !== row.linkedWorkId ||
      ack.correlationId !== row.correlationId ||
      ack.sourceCompanyId !== row.companyId ||
      ack.issueId !== row.issueId ||
      ack.sourceRunId !== row.runId
    ) {
      return { kind: "manual", code: "callback_ack_mismatch", summary: "Origin callback acknowledgement did not match the exact event." };
    }
    return { kind: "accepted" };
  } finally {
    clearTimeout(timeout);
  }
}

async function readBoundedBody(response: Response, limit: number): Promise<Uint8Array | null> {
  if (!response.body) return new Uint8Array();
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    while (true) {
      const part = await reader.read();
      if (part.done) break;
      size += part.value.byteLength;
      if (size > limit) {
        await reader.cancel();
        return null;
      }
      chunks.push(part.value);
    }
  } finally {
    reader.releaseLock();
  }
  const result = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    result.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return result;
}

function readDispositionEnvelope(value: unknown): {
  binding: { runId: string; companyId: string; agentId: string; issueId: string };
  disposition: { kind: string; evidenceSha256: string };
} | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const envelope = (value as Record<string, unknown>).paperclipAdapterDisposition;
  if (!envelope || typeof envelope !== "object" || Array.isArray(envelope)) return null;
  const parsed = paperclipAdapterDispositionEnvelopeSchema.safeParse(envelope);
  return parsed.success ? parsed.data : null;
}

function parseAck(value: string): {
  providerEventIdSha256: string;
  linkedWorkId: string;
  correlationId: string;
  sourceCompanyId: string;
  issueId: string;
  sourceRunId: string;
} | null {
  try {
    const decoded = JSON.parse(value) as Record<string, unknown>;
    const keys = Object.keys(decoded).sort();
    const expected = ["correlationId", "issueId", "linkedWorkId", "providerEventIdSha256", "schemaVersion", "sourceCompanyId", "sourceRunId", "status"];
    if (keys.join("\n") !== expected.join("\n") || decoded.schemaVersion !== "cross-org-linked-work.callback-ack.v1" || decoded.status !== "completed") return null;
    if ([decoded.providerEventIdSha256, decoded.linkedWorkId, decoded.correlationId, decoded.sourceCompanyId, decoded.issueId, decoded.sourceRunId].some((entry) => typeof entry !== "string")) return null;
    return decoded as never;
  } catch {
    return null;
  }
}

async function markManual(
  db: Db,
  fence: ReturnType<typeof and>,
  at: Date,
  code: string,
  summary: string,
): Promise<void> {
  await db
    .update(linkedWorkCompletionOutbox)
    .set({
      status: "manual_reconcile",
      leaseOwner: null,
      leaseTokenHash: null,
      leaseExpiresAt: null,
      lastErrorCode: code,
      lastErrorSummary: summary,
      updatedAt: at,
    })
    .where(fence);
}

async function markRetry(
  db: Db,
  fence: ReturnType<typeof and>,
  row: typeof linkedWorkCompletionOutbox.$inferSelect,
  at: Date,
  code: string,
  summary: string,
): Promise<void> {
  if (row.attemptCount >= row.maxAttempts) {
    await markManual(db, fence, at, "attempts_exhausted", "Completion delivery attempts were exhausted.");
    return;
  }
  const delayMs = Math.min(300_000, 15_000 * 2 ** Math.max(0, row.attemptCount - 1));
  await db.update(linkedWorkCompletionOutbox).set({
    status: "pending",
    leaseOwner: null,
    leaseTokenHash: null,
    leaseExpiresAt: null,
    sendStarted: false,
    nextAttemptAt: new Date(at.getTime() + delayMs),
    lastErrorCode: code,
    lastErrorSummary: summary,
    updatedAt: at,
  }).where(fence);
}

function identityHash(row: typeof linkedWorkCompletionOutbox.$inferSelect): string {
  return createHash("sha256")
    .update(JSON.stringify(Object.fromEntries(Object.entries({
      providerEventId: row.providerEventId,
      linkedWorkId: row.linkedWorkId,
      correlationId: row.correlationId,
      originCompanyId: row.originCompanyId,
      originIssueId: row.originIssueId,
      companyId: row.companyId,
      issueId: row.issueId,
      agentId: row.agentId,
      runId: row.runId,
      evidenceSha256: row.evidenceSha256,
    }).sort(([left], [right]) => left.localeCompare(right)))))
    .digest("hex");
}

function sha256(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}
