import {
  boolean,
  index,
  integer,
  pgTable,
  text,
  timestamp,
  uniqueIndex,
  uuid,
} from "drizzle-orm/pg-core";
import { companies } from "./companies.js";
import { agents } from "./agents.js";
import { issues } from "./issues.js";
import { heartbeatRuns } from "./heartbeat_runs.js";

export const linkedWorkCompletionOutbox = pgTable(
  "linked_work_completion_outbox",
  {
    providerEventId: text("provider_event_id").primaryKey(),
    identityHash: text("identity_hash").notNull(),
    companyId: uuid("company_id").notNull().references(() => companies.id),
    issueId: uuid("issue_id").notNull().references(() => issues.id),
    agentId: uuid("agent_id").notNull().references(() => agents.id),
    runId: uuid("run_id").notNull().references(() => heartbeatRuns.id),
    linkedWorkId: text("linked_work_id").notNull(),
    correlationId: text("correlation_id").notNull(),
    originCompanyId: uuid("origin_company_id").notNull(),
    originIssueId: uuid("origin_issue_id").notNull(),
    evidenceSha256: text("evidence_sha256").notNull(),
    status: text("status").notNull().default("pending"),
    attemptCount: integer("attempt_count").notNull().default(0),
    maxAttempts: integer("max_attempts").notNull().default(8),
    nextAttemptAt: timestamp("next_attempt_at", { withTimezone: true }).notNull().defaultNow(),
    leaseFence: integer("lease_fence").notNull().default(0),
    leaseOwner: text("lease_owner"),
    leaseTokenHash: text("lease_token_hash"),
    leaseExpiresAt: timestamp("lease_expires_at", { withTimezone: true }),
    sendStarted: boolean("send_started").notNull().default(false),
    lastErrorCode: text("last_error_code"),
    lastErrorSummary: text("last_error_summary"),
    createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
    deliveredAt: timestamp("delivered_at", { withTimezone: true }),
  },
  (table) => ({
    runIdentity: uniqueIndex("linked_work_completion_outbox_run_uq").on(
      table.companyId,
      table.runId,
    ),
    issueIdentity: uniqueIndex("linked_work_completion_outbox_issue_uq").on(
      table.companyId,
      table.issueId,
    ),
    due: index("linked_work_completion_outbox_due_idx").on(
      table.status,
      table.nextAttemptAt,
      table.providerEventId,
    ),
  }),
);
