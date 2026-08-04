# 006 — Agent session ingestion

Status: active
Owner: AI CEO

## Purpose

Resumable, idempotent ingestion of observed local coding-agent sessions
(Codex, Claude Code, Hermes) into stable per-project namespaces through the
authorized HTTP API, with common credential redaction.

## Requirements

- R1. Ingestion MUST target the observed local seams (Codex, Claude Code,
  Hermes session stores), not provider APIs, native hooks, or a universal
  transcript parser.
- R2. Ingestion MUST use resumable cursors and idempotent writes so retries
  do not duplicate records.
- R3. Ingestion MUST redact common credential-shaped values and MUST exclude
  tool rows, private thinking, system prompts, and tool results.
- R4. Each repository MUST receive a stable project identity and an exact
  project namespace so memories from multiple projects do not share one
  undifferentiated search pool.
- R5. `watch --discover` MUST check the conventional current-user stores, and
  the optional Linux systemd user service MUST be able to supervise the
  watcher continuously with owner-only permissions.

## Acceptance criteria

- [ ] A1. The ingest and MCP test suites pass (`scripts/test_palimpsest_mcp.py`
      and client ingest tests).
- [ ] A2. `scripts/install-palimpsest-ingest-service.sh` installs an owner-only
      systemd user service on Linux.
- [ ] A3. Redaction and exclusion rules are covered by tests: credential-shaped
      values never reach the API; private thinking and tool rows never do.

## Out of scope

- Provider APIs, native hooks, universal parsers; automatic consolidation of
  raw session messages into higher-level facts (see spec 007 and backlog).

## Open questions

- None beyond the backlog entries for provider-native adapters.

## Links

Code: `scripts/palimpsest_ingest.py` · `scripts/palimpsest-ingest.service`
Tests: `scripts/test_palimpsest_mcp.py`
Decisions: 0019, 0021, 0022
