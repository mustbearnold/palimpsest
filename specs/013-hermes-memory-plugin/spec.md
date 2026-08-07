# 013 — Hermes Agent memory plugin

Status: active

Owner: AI CEO

## Purpose

Make Palimpsest usable as a first-class memory provider for Hermes Agent (CLI, gateway, and desktop app) through Hermes' official memory-provider plugin interface, without making Palimpsest Hermes-exclusive: the plugin is a thin consumer of the same versioned HTTP API that serves the Codex MCP adapter and the thin clients, and the service remains the single authority for authorization, temporal semantics, write policies, and deletion state.

## Requirements

- R1. The plugin MUST implement the official Hermes `MemoryProvider` ABC (`agent/memory_provider.py`), register under the provider name `palimpsest`, and be discoverable as a user-installed plugin in `$HERMES_HOME/plugins/` without any change to the Hermes core.
- R2. The plugin MUST talk only to the Palimpsest HTTP API over the configured bearer token, tenant, subject, and case scope; it MUST NEVER connect to PostgreSQL directly and MUST NOT widen the server's configured scope.
- R3. The plugin MUST expose `palimpsest_recall`, `palimpsest_remember`, and `palimpsest_status` tools and MUST NOT expose delete or export operations. The `palimpsest_remember` tool description MUST require explicit user approval before a write. All input-facing entry points (tools and CLI) MUST validate input client-side — non-empty, UTF-8 at most 4096 bytes for queries — before any network call. `palimpsest_recall` MUST target the configured namespace by default and MAY accept an explicit namespace override; subject-wide recall is intentionally unavailable (the server rejects empty filter arrays, and any non-empty array narrows the search).
- R4. Turn persistence MUST be non-blocking and crash-safe: `sync_turn` MUST enqueue one immutable episode per turn (user and assistant text only, not full tool-call transcripts) into a durable local write-behind queue flushed by a background thread with idempotency keys, so a crash never duplicates or loses a committed turn. Each episode MUST carry the turn's observed time (not the flush time), and the plugin MUST declare the journal via the ABC's `backup_paths()` so `hermes backup` captures it. Startup replay and the idle flush loop MUST both drain backlog rows beyond a single bounded batch, so a backlog larger than one batch never stalls until restart.
- R5. Fact promotion MUST be limited to attributable writes: episodes only for `sync_turn`; an episode plus a `direct-evidence` governed fact only for the `palimpsest_remember` tool and for `on_memory_write` mirrors of the built-in memory tool (`add` action). `replace` and `remove` mirrors MUST be skipped, not emulated, because the plugin has no delete authority. Mirrored writes MUST be retried with bounded backoff before being dropped, and a partial episode/fact failure MUST be logged rather than silently hidden. Retries MUST resend a byte-identical body (observed time fixed per logical write) so a retry after a lost response replays against the server's idempotency store instead of 409ing; a duplicate logical write (same session and content) MUST surface an idempotent outcome — the original receipt replayed with its identifiers, or `already_saved` where the server 409s — rather than a raw error. The observed time is frozen per logical write on every path: the mirror freezes it in the queue row, and the tool path freezes it in-process keyed by the deterministic base key (session + content hash), so a same-process duplicate resends a byte-identical body and replays the original `episode_id`/`fact_id`. Known corner (documented, not fixed by design): after a process restart the tool-path freeze is cold, so a first write that commits the episode but loses the fact response cannot be completed by a same-content retry — the deterministic episode key is consumed, the retry 409s and reports `already_saved`, and the governed fact stays absent; the mirror path (the only one with retries) is replay-safe via the frozen body.
- R6. Recall MUST be pre-warmed per turn: `queue_prefetch` runs a bounded background retrieval after each turn and `prefetch` returns the cached result before the next API call; both MUST skip trivial prompts (`is_trivial_prompt`, with a standalone fallback that replicates the core's trivial-prompt grammar so behavior matches with or without the Hermes core). `queue_prefetch` MUST never block the agent loop: when a previous background recall is still running, the new recall is skipped rather than joined. A static `system_prompt_block()` SHOULD announce provider presence.
- R7. Configuration MUST work through `hermes memory setup`: the config schema MUST contain `base_url` (default `http://127.0.0.1:8080`), `bearer_token` (secret, env var `PALIMPSEST_BEARER_TOKEN`), `tenant_id`, `subject_id`, `case_id` (optional), and `namespace` (default `hermes`), plus an optional `timeout_seconds`; non-secret values MUST be saved to `$HERMES_HOME/palimpsest.json` and environment variables MUST take precedence over the file. Configuration changes MUST take effect on subsequent tool calls without a provider restart. `PALIMPSEST_BASE_URL` MAY fall back to `PALIMPSEST_MCP_BASE_URL` so the Codex MCP adapter's environment configures this plugin too, and a loopback endpoint MAY assume the local development token when `bearer_token` is unset (mirroring the MCP adapter). `is_available` MUST make no network calls.
- R8. The plugin MUST ship a `cli.py` registering `hermes palimpsest` subcommands (`status`, `config`, `recall`, `remember`) through the official convention-based discovery.
- R9. The plugin MUST ship a Hermes Desktop surface: a `dashboard/plugin_api.py` backend router (mounted at `/api/plugins/palimpsest`) exposing status, recall, and remember, and a desktop plugin pane (`desktop-plugins/palimpsest`) with status, recall, and remember UI. The desktop app MUST work with the provider without any extra step beyond CLI configuration.
- R10. The plugin MUST be self-contained and dependency-free beyond the Python standard library (stdlib `urllib` HTTP client, SQLite write-behind queue), so `hermes plugins install` needs no pip installs, and MUST NOT import the Hermes core outside the guarded `MemoryProvider` base-class import (module stays importable standalone for tests and the backend router). The dashboard backend router is exempt from the dependency-free rule: it runs inside the Hermes gateway process and MAY import the host-provided `fastapi`/`pydantic` (never declared as plugin pip dependencies, because Hermes ships them).
- R11. All plugin code, tests, and plugin documentation MUST live under `integrations/hermes/` in this repository so the plugin installs as a unit (GitHub-subdirectory install or symlink) and remains versioned with the HTTP contract it consumes. Repo-level documentation (quickstart, architecture) MAY reference the integration from outside the subtree.

## Acceptance criteria

- [ ] A1. `integrations/hermes/tests/test_hermes_plugin.py` passes: provider identity and availability, config precedence (env over file over defaults), recall/remember/status tool routing against a fake HTTP server, write-behind queue flush and retry with idempotency keys, prefetch caching and trivial-prompt skip, session switch, and the no-database / no-delete-tool invariants.
- [ ] A2. The plugin installs into a fresh `$HERMES_HOME/plugins/palimpsest/` and `plugins.memory.discover_memory_providers()` lists it as available when the Palimpsest HTTP service is reachable, with no change to Hermes core files.
- [ ] A3. `hermes palimpsest status` prints scope, endpoint, and reachability without exposing the bearer token or memory content.
- [ ] A4. The desktop pane renders status, recall results, and remember confirmation when the Palimpsest service is running; the backend router answers on `/api/plugins/palimpsest/status` and `/recall`.
- [ ] A5. The plugin README instructions work end to end for the CLI and the desktop app and point to the same `PALIMPSEST_*` environment variables used by the Codex MCP adapter.

## Out of scope

- Changes to the Hermes core repository; bundled shipping inside `plugins/memory/` of the Hermes repo.
- Delete/export/restore operations from Hermes tools; automatic fact extraction from conversation turns without an attributable write policy.
- Remote or hosted Hermes surfaces; multi-tenant mapping beyond the configured single scope.

## Open questions

- None.

## Links

Code: `integrations/hermes/`

Tests: `integrations/hermes/tests/test_hermes_plugin.py`

Decisions: 0030, 0012, 0020

Issue: #42
