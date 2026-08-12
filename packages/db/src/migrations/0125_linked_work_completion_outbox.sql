CREATE TABLE "linked_work_completion_outbox" (
  "provider_event_id" text PRIMARY KEY NOT NULL,
  "identity_hash" text NOT NULL,
  "company_id" uuid NOT NULL REFERENCES "companies"("id") ON DELETE restrict,
  "issue_id" uuid NOT NULL REFERENCES "issues"("id") ON DELETE restrict,
  "agent_id" uuid NOT NULL REFERENCES "agents"("id") ON DELETE restrict,
  "run_id" uuid NOT NULL REFERENCES "heartbeat_runs"("id") ON DELETE restrict,
  "linked_work_id" text NOT NULL,
  "correlation_id" text NOT NULL,
  "origin_company_id" uuid NOT NULL,
  "origin_issue_id" uuid NOT NULL,
  "evidence_sha256" text NOT NULL CHECK (length("evidence_sha256") = 64),
  "status" text NOT NULL DEFAULT 'pending' CHECK ("status" IN ('pending','processing','delivered','manual_reconcile')),
  "attempt_count" integer NOT NULL DEFAULT 0 CHECK ("attempt_count" >= 0),
  "max_attempts" integer NOT NULL DEFAULT 8 CHECK ("max_attempts" BETWEEN 1 AND 20),
  "next_attempt_at" timestamp with time zone NOT NULL DEFAULT now(),
  "lease_fence" integer NOT NULL DEFAULT 0 CHECK ("lease_fence" >= 0),
  "lease_owner" text,
  "lease_token_hash" text,
  "lease_expires_at" timestamp with time zone,
  "send_started" boolean NOT NULL DEFAULT false,
  "last_error_code" text,
  "last_error_summary" text,
  "created_at" timestamp with time zone NOT NULL DEFAULT now(),
  "updated_at" timestamp with time zone NOT NULL DEFAULT now(),
  "delivered_at" timestamp with time zone,
  CHECK (("lease_owner" IS NULL AND "lease_token_hash" IS NULL AND "lease_expires_at" IS NULL)
    OR ("lease_owner" IS NOT NULL AND "lease_token_hash" IS NOT NULL AND "lease_expires_at" IS NOT NULL))
);
CREATE UNIQUE INDEX "linked_work_completion_outbox_run_uq"
  ON "linked_work_completion_outbox" ("company_id", "run_id");
CREATE UNIQUE INDEX "linked_work_completion_outbox_issue_uq"
  ON "linked_work_completion_outbox" ("company_id", "issue_id");
CREATE INDEX "linked_work_completion_outbox_due_idx"
  ON "linked_work_completion_outbox" ("status", "next_attempt_at", "provider_event_id");
CREATE OR REPLACE FUNCTION reject_linked_work_completion_identity_change()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF ROW(NEW.provider_event_id, NEW.identity_hash, NEW.company_id, NEW.issue_id,
         NEW.agent_id, NEW.run_id, NEW.linked_work_id, NEW.correlation_id,
         NEW.origin_company_id, NEW.origin_issue_id, NEW.evidence_sha256)
     IS DISTINCT FROM
     ROW(OLD.provider_event_id, OLD.identity_hash, OLD.company_id, OLD.issue_id,
         OLD.agent_id, OLD.run_id, OLD.linked_work_id, OLD.correlation_id,
         OLD.origin_company_id, OLD.origin_issue_id, OLD.evidence_sha256) THEN
    RAISE EXCEPTION 'Linked-work completion identity is immutable';
  END IF;
  RETURN NEW;
END;
$$;
CREATE TRIGGER linked_work_completion_identity_immutable
  BEFORE UPDATE ON "linked_work_completion_outbox"
  FOR EACH ROW EXECUTE FUNCTION reject_linked_work_completion_identity_change();
CREATE OR REPLACE FUNCTION reject_linked_work_completion_delete()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'Linked-work completion evidence cannot be deleted';
END;
$$;
CREATE TRIGGER linked_work_completion_no_delete
  BEFORE DELETE ON "linked_work_completion_outbox"
  FOR EACH ROW EXECUTE FUNCTION reject_linked_work_completion_delete();
