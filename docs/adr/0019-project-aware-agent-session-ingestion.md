# ADR-0019: Project-aware agent-session ingestion

Status: accepted

Date: 2026-08-03

## Context

Coding agents already persist useful conversation history, but each agent uses
a different local format. Codex and Claude Code append JSONL transcripts while
Hermes persists its canonical messages in `state.db`. Palimpsest needs a
governed ingestion method that can follow new events without treating every
agent's private files as an implicit data source. It also needs to distinguish
one repository from another when the same subject uses several projects.

## Decision

Ship a dependency-free Python ingestion library and the
`scripts/palimpsest_ingest.py` `once`/`watch` bridge. Source paths are required
explicitly; there are no default Codex, Claude, or Hermes paths. A source owner
must authorize access to a path, and the bridge never widens the configured
tenant, subject, case, or bearer-token scope.

The first adapters read these source seams:

- Codex `event_msg` records with `user_message` or `agent_message` payloads;
- Claude Code `user` and `assistant` records containing text blocks;
- Hermes `state.db` `messages` joined to `sessions`, limited to `user` and
  `assistant` rows and opened read-only.

Tool calls, tool results, system prompts, and private thinking blocks are not
ingested. Common credential-shaped values are redacted before a write. This
redaction is a safety floor, not a guarantee of secret detection; callers
remain responsible for selecting appropriate sensitivity and retention
policies.

Each event carries a stable project identity derived from the Git repository
root when available, otherwise from its recorded working directory. The
identity is a non-reversible short SHA-256 label such as
`project-0123456789abcdef`; the human-readable root remains metadata for the
authorized subject. Facts use the exact namespace
`agent_session:<project-id>`, making the existing retrieval `namespaces` filter
the project boundary. A `--project-root` filter can restrict a poller to one
repository.

The default first poll baselines already-present files and Hermes message IDs.
`--backfill` is an explicit opt-in for historical import. JSONL cursors track
file identity, byte offset, and line number. Hermes cursors track message ID.
State is written atomically with owner-only permissions. A cursor advances only
after the governed `remember` operation succeeds, and event-derived
idempotency keys make a crash between the remote write and cursor save safe to
retry.

## Consequences

New Codex, Claude Code, and Hermes text can flow into Palimpsest while the
canonical HTTP service continues to own authorization, temporal semantics,
write policy, retention, and deletion. Retrieval can keep projects separate or
deliberately search several project namespaces together.

The Python and TypeScript clients provide per-project recall helpers that issue
one exact-namespace retrieval per requested project and return the responses
separately. They are evidence-bundling conveniences, not model-based project
comparison engines.

`watch` is an explicit local process, not an unconfigured background daemon.
The companion `--discover` mode is covered by ADR-0021. Native hooks, provider
APIs, richer secret classification, consolidation of raw episodes into
higher-level facts, and a server-managed ingestion worker remain future
slices. Source formats are treated as replaceable adapters and their tests
must fail closed when the expected structure is absent.
