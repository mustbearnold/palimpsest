# ADR-0021: Narrow local agent-source discovery

Status: accepted

Date: 2026-08-03

## Context

The project-aware ingestion bridge can follow Codex, Claude Code, and Hermes,
but requiring three path arguments makes the common single-user setup awkward.
Removing the explicit opt-in would make a memory service silently inspect local
agent history, including data that may belong to another account.

## Decision

Add an explicit `--discover` mode to `scripts/palimpsest_ingest.py` and the
dependency-free Python helper `discover_local_sources`. Discovery checks only
these exact locations for the current user:

- `~/.codex/sessions`;
- `~/.claude/projects`;
- `~/.hermes/state.db`.

Missing locations are omitted. A watcher recalculates discovery on every poll,
so a provider started after the watcher can be picked up. Non-standard paths
can be supplied through the three `PALIMPSEST_INGEST_*` path variables or the
existing explicit `--source KIND=PATH` option. Explicit sources and discovered
sources may be combined, with duplicate kind/path pairs removed.

Discovery does not recurse through the home directory, search for files by
content, or cross a symlinked conventional root. It does not grant access to
the Palimpsest API: the configured bearer token, tenant, subject, case,
sensitivity, retention, and server write policy still govern every write.

## Consequences

The normal local setup can use one supervised command:

```bash
python3 scripts/palimpsest_ingest.py watch --discover
```

The bridge can discover newly created conventional stores while running, while
the narrow path set keeps the privacy boundary legible. A separate account,
provider API, native hook, or server-managed ingestion worker still requires
explicit configuration and remains outside this adapter.
