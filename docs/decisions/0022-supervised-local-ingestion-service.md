# ADR-0022: Supervised local ingestion service

Status: accepted

Date: 2026-08-03

## Context

The ingestion bridge can now discover the conventional current-user stores,
but a foreground `watch` process is easy to forget. The server itself must not
silently gain access to an agent's home directory, so continuous ingestion
needs a separately supervised local process with an explicit lifecycle.

## Decision

Provide `scripts/palimpsest-ingest.service` and the opt-in
`scripts/install-palimpsest-ingest-service.sh` for a systemd user manager on
the canonical Linux checkout path. The service runs
`palimpsest_ingest.py watch --discover`, uses the local development HTTP
defaults unless an owner-only environment file overrides them, and stores its
cursor at `~/.local/state/palimpsest/ingest-state.json`.

The unit is constrained with an owner-only umask, `NoNewPrivileges`, a private
temporary directory, a read-only system, read-only home protection, and one
write exception for the cursor directory. The ingestion process still reads
only the exact conventional Codex, Claude Code, and Hermes locations and still
redacts/excludes the same content as the foreground bridge.

The installer enables and starts the service only when the user invokes it.
Uninstallation is the explicit `systemctl --user disable --now` operation.
This is a local convenience, not a server-managed daemon, provider hook, or
production deployment mechanism.

## Consequences

New agent text can be ingested without a terminal remaining open, while the
source access, API authorization, project namespace, cursor, and shutdown
boundaries remain visible and reversible. Linux systemd is an optional local
operator integration; other platforms continue to use the foreground watcher
or their own supervisor.
