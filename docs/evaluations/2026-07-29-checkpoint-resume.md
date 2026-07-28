# Checkpoint resume evaluation — 2026-07-29

## Scope

Issue #3's checkpoint contract was evaluated through an implementation-neutral
HTTP client over real TCP listeners and an isolated PostgreSQL 18.4 database
with pgvector 0.8.5. The public seam is one tenant-, subject-, agent-, and
thread-scoped checkpoint head; every accepted save creates an immutable full
snapshot revision.

## Scenarios

| Scenario | Evidence | Result |
| --- | --- | --- |
| Create and read | `If-None-Match: *` creates revision 1; `GET` returns identical JSON and ETag | Pass |
| Exact replay | Identical create and completion retries return the original status, body, ETag, and `Idempotency-Replayed: true` | Pass |
| Linear advance | Strong `If-Match` plus the named parent creates one successor and preserves the logical checkpoint ID | Pass |
| Stale writer | An old ETag returns stable `412 stale-checkpoint` without advancing the head | Pass |
| Effect preparation | A server-created UUIDv7 effect ID is durably returned before provider execution | Pass |
| Effect completion | A later revision records the bounded receipt and cumulative completed effect | Pass |
| Invalid effects | Duplicate effect keys and duplicate completions return stable redacted `409` problems | Pass |
| Provider-window termination | The server process is killed after provider success but before completion is recorded; restart exposes the same prepared effect ID | Pass |
| Provider retry safety | The caller retries the same effect ID; the mock provider records two attempts but one application | Pass |
| Response-window termination | A child server exits with code 86 after the completion transaction returns but before its response is delivered | Pass |
| Process restart | The production server binary restarts with a fresh pool against the same database after each interruption | Pass |
| Completion replay | The lost completion response replays exactly after restart without a third provider attempt | Pass |
| Retention | A 30-day root advances to a one-second head; after expiry `GET` and replay of the older root both return `404`, while a sibling remains readable | Pass |
| Scope isolation | Sibling-subject and unauthorized-subject reads return indistinguishable RFC 9457 `404` responses | Pass |
| Governance | Each committed revision has one audit/outbox pair; failures and replays add none | Pass |
| Redaction | Private state and external-reference markers do not appear in audit authorization context or outbox payloads | Pass |
| Bounds | More than 100 effect transitions returns stable `413 checkpoint-too-large` before persistence | Pass |

The two-window hard-crash scenario also verifies exactly three revisions, one
prepared effect, one completion receipt, three completed idempotency records,
and three audit/outbox pairs after restart.

## Commands and gates

```text
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace --all-targets
npm exec --yes @redocly/cli@2.18.1 lint api/openapi.yaml
bash scripts/check-repo.sh
git diff --check
```

All gates passed locally. The PostgreSQL conformance test creates and removes a
fresh database per run. Its crash child is an ignored test fixture that runs
only when explicitly spawned by the parent scenario.

## Boundaries

- Palimpsest records effect recovery state but does not execute provider calls.
  Exactly-once external behavior still requires provider idempotency or reliable
  reconciliation; the API and ADR make that limitation explicit.
- The mock provider proves caller behavior at the crash seam, not compatibility
  with any named third-party provider.
- The local default database role is a superuser. Existing-record collision
  fixtures prove application-level tenant and subject cloaking locally; GitHub
  CI runs the same suite as a `NOSUPERUSER NOBYPASSRLS` database owner so forced
  RLS is exercised at the PostgreSQL boundary.
- Expired heads are hidden immediately. Physical retention cleanup and recreation
  of an expired logical scope are intentionally outside issue #3.
- This is local development and CI conformance evidence, not a production
  deployment, performance benchmark, or production-readiness claim.
