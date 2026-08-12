# Governed HTTP adapter dispositions

Paperclip accepts issue dispositions from an HTTP adapter only when the
response advertises `paperclip.adapter-disposition.v1`. The body is streamed
under a 16 KiB ceiling, must be UTF-8 `application/json`, and must match the
strict schema and the active run/company/agent/issue binding.

Application is atomic with successful run terminalization under locked run and
issue rows. A crash cannot commit the issue state without committing the run
state. Replay sees the terminal run and does not reapply the disposition or
activity record.

Governed state prerequisites:

- `done`: the bound assignee/run still owns active work;
- `continue`: the issue remains in progress and the bounded summary becomes
  the existing next-action signal;
- `blocked`: the named blocker is an existing, visible, unresolved same-company
  issue connected by an existing `blocks` relation;
- `in_review`: the named reviewer is active, same-company, and already the
  current participant in the issue's governed execution state.

Malformed, oversized, wrong-content-type, secret-bearing, stale, or mismatched
responses fail without issue mutation. Legacy HTTP adapters without a contract
header keep their prior transport-only behavior. An unknown contract version
fails closed.

## Release and rollback

Run the HTTP adapter unit tests, heartbeat disposition tests against Postgres,
server typecheck/lint, and build. Deploy employee runtime and Paperclip from
reviewed immutable commits as one compatibility window. If either side fails,
restore both prior releases; do not manually edit the issue to imitate a
contract result. Before retrying a canary, prove the previous run is terminal
and the issue has no ambiguous partial transition.
