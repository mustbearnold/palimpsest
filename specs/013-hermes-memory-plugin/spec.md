# 013 — Hermes Agent memory plugin

Status: active
Owner: AI CEO

## Purpose

Make Palimpsest usable as a first-class memory provider for Hermes Agent
(CLI, gateway, and desktop app) through Hermes' official memory-provider
plugin interface, without making Palimpsest Hermes-exclusive: the plugin is a
thin consumer of the same versioned HTTP API that serves the Codex MCP adapter
and the thin clients, and the service remains the single authority for
authorization, temporal semantics, write policies, and deletion state.

## Requirements

- R1. The plugin MUST implement the official Hermes `MemoryProvider` ABC
  (`agent/memory_provider.py`), register under the provider name `palimpsest`,
  and be discoverable as a user-installed plugin in `$HERMES_HOME/plugins/`
  without any change to the Hermes core.
- R2. The plugin MUST talk only to the Palimpsest HTTP API over the configured
  bearer token, tenant, subject, and case scope; it MUST NEVER connect to
  PostgreSQL directly and MUST NOT widen the server's configured scope.
- R3. The plugin MUST expose `palimpsest_recall`, `palimpsest_remember`, and
  `palimpsest_status` tools and MUST NOT expose delete or export operations.
  The `palimpsest_remember` tool description MUST require explicit user
  approval before a write.
- R4. Turn persistence MUST be non-blocking and crash-safe: `sync_turn` MUST
  enqueue one immutable episode per turn (user and assistant text only, not
  full tool-call transcripts) into a durable local write-behind queue flushed
  by a background thread with idempotency keys, so a crash never duplicates or
  loses a committed turn.
- R5. Fact promotion MUST be limited to attributable writes: episodes only for
  `sync_turn`; an episode plus a `direct-evidence` governed fact only for the
  `palimpsest_remember` tool and for `on_memory_write` mirrors of the built-in
  memory tool (`add` action). `replace` and `remove` mirrors MUST be skipped,
  not emulated, because the plugin has no delete authority.
- R6. Recall MUST be pre-warmed per turn: `queue_prefetch` runs a bounded
  background retrieval after each turn and `prefetch` returns the cached
  result before the next API call; both MUST skip trivial prompts
  (`is_trivial_prompt`).
- R7. Configuration MUST work through `hermes memory setup`: the config schema
  MUST contain `base_url` (default `http://127.0.0.1:8080`), `bearer_token`
  (secret, env var `PALIMPSEST_BEARER_TOKEN`), `tenant_id`, `subject_id`,
  `case_id` (optional), and `namespace` (default `hermes`); non-secret values
  MUST be saved to `$HERMES_HOME/palimpsest.json` and environment variables
  MUST take precedence over the file. `is_available` MUST make no network
  calls.
- R8. The plugin MUST ship a `cli.py` registering `hermes palimpsest`
  subcommands (`status`, `config`, `recall`, `remember`) through the official
  convention-based discovery.
- R9. The plugin MUST ship a Hermes Desktop surface: a `dashboard/plugin_api.py`
  backend router (mounted at `/api/plugins/palimpsest`) exposing status,
  recall, and remember, and a desktop plugin pane (`desktop-plugins/palimpsest`)
  with status, recall, and remember UI. The desktop app MUST work with the
  provider without any extra step beyond CLI configuration.
- R10. The plugin MUST be self-contained and dependency-free beyond the Python
  standard library (stdlib `urllib` HTTP client, SQLite write-behind queue), so
  `hermes plugins install` needs no pip installs, and MUST NOT import the
  Hermes core outside the guarded `MemoryProvider` base-class import (module
  stays importable standalone for tests and the backend router).
- R11. All plugin code, tests, and docs MUST live under
  `integrations/hermes/` in this repository so the plugin installs as a unit
  (GitHub-subdirectory install or symlink) and remains versioned with the
  HTTP contract it consumes.

## Acceptance criteria

- [ ] A1. `integrations/hermes/tests/test_hermes_plugin.py` passes: provider
      identity and availability, config precedence (env over file over
      defaults), recall/remember/status tool routing against a fake HTTP
      server, write-behind queue flush and retry with idempotency keys,
      prefetch caching and trivial-prompt skip, session switch, and the
      no-database / no-delete-tool invariants.
- [ ] A2. The plugin installs into a fresh `$HERMES_HOME/plugins/palimpsest/`
      and `plugins.memory.discover_memory_providers()` lists it as available
      when the Palimpsest HTTP service is reachable, with no change to Hermes
      core files.
- [ ] A3. `hermes palimpsest status` prints scope, endpoint, and reachability
      without exposing the bearer token or memory content.
- [ ] A4. The desktop pane renders status, recall results, and remember
      confirmation when the Palimpsest service is running; the backend router
      answers on `/api/plugins/palimpsest/status` and `/recall`.
- [ ] A5. The plugin README instructions work end to end for the CLI and the
      desktop app and point to the same `PALIMPSEST_*` environment variables
      used by the Codex MCP adapter.

## Out of scope

- Changes to the Hermes core repository; bundled shipping inside
  `plugins/memory/` of the Hermes repo.
- Delete/export/restore operations from Hermes tools; automatic fact
  extraction from conversation turns without an attributable write policy.
- Remote or hosted Hermes surfaces; multi-tenant mapping beyond the configured
  single scope.

## Open questions

- None.

## Links

Code: `integrations/hermes/`
Tests: `integrations/hermes/tests/test_hermes_plugin.py`
Decisions: 0030, 0012, 0020
Issue: #42
