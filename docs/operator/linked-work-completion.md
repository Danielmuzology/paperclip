# Governed linked-work completion

Paperclip can propagate a strictly bound terminal MuzProductions child result to
the Muzology employee runtime through a durable outbox. This path is disabled
unless all three protected values are configured together:

- `PAPERCLIP_LINKED_WORK_CONTROL_SECRET`
- `PAPERCLIP_LINKED_WORK_CALLBACK_URL` (exact
  `/integrations/paperclip/linked-work/completion` route)
- `PAPERCLIP_LINKED_WORK_CALLBACK_SECRET`

The control secret is accepted only on issue creation and only for an exact
`executionPolicy.linkedWorkCompletion` whose target company and assignee match
the new issue. The field is immutable after creation. Never put the control
secret in a general Paperclip client header or issue text.

The `done` disposition, successful heartbeat run, and outbox row commit in one
Postgres transaction. The outbox is append-only and permits only one completion
per company/issue. Its worker sends the deterministic provider event. Timeouts,
expired send leases, 429, and 5xx responses retry the same event with bounded
backoff. Definite 4xx responses, malformed or mismatched acknowledgements,
strict binding drift, oversized bodies, and exhausted attempts enter
`manual_reconcile` without mutating the evidence identity.

Before enabling, require an exact database backup, migration verification,
runtime callback authentication, and a scrubbed alternate-port canary. Stop if
the outbox has `manual_reconcile`, the worker is not configured, the issue/run
binding differs, or logs contain callback secret material. Roll back only before
accepted traffic; after an ambiguous accepted callback, preserve the outbox and
replay its deterministic event rather than deleting or synthesizing evidence.
