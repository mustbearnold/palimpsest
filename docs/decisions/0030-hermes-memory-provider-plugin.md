# ADR-0030: Hermes Agent memory provider plugin

## Status

Accepted for the Hermes Agent integration.

## Context

Palimpsest's canonical public seam is the versioned HTTP/OpenAPI service, and
its existing agent integrations are thin consumers of that seam: a local
Codex MCP adapter (ADR-0012) and thin Python/TypeScript clients (spec 009).
Hermes Agent has an official memory-provider plugin interface: plugins
implement the `MemoryProvider` ABC, ship `plugin.yaml` plus an optional
`cli.py`, and are discovered from `plugins/memory/<name>/` (bundled) or
`$HERMES_HOME/plugins/<name>/` (user-installed), then activated with
`memory.provider` in config.yaml through `hermes memory setup`. The Hermes
desktop app runs the same agent core, so a configured provider works there
automatically; the desktop additionally supports UI plugins
(`$HERMES_HOME/desktop-plugins/`) with a shared Python backend namespace at
`/api/plugins/<name>`.

The alternative of contributing `plugins/memory/palimpsest/` into the Hermes
core tree was rejected: the Hermes repo's own policy keeps third-party
products in standalone plugin repositories, and Palimpsest must remain usable
by Codex, Claude Code, and any future agent through the same HTTP contract —
never Hermes-exclusive.

## Decision

Add `integrations/hermes/` to this repository as a standalone, user-installable
Hermes memory provider plugin named `palimpsest`.

- It implements the official `MemoryProvider` ABC (name `palimpsest`), with a
  guarded base-class import so the module stays importable standalone; it is
  installed by copying or symlinking the directory into
  `$HERMES_HOME/plugins/palimpsest/` (or `hermes plugins install <repo URL
  subtree>`), with no changes to Hermes core files.
- It talks only to the running Palimpsest HTTP service with the configured
  bearer token, tenant, subject, and optional case, reusing the same
  `PALIMPSEST_BASE_URL` / `PALIMPSEST_BEARER_TOKEN` /
  `PALIMPSEST_TENANT_ID` / `PALIMPSEST_SUBJECT_ID` / `PALIMPSEST_CASE_ID`
  environment variables as the Codex MCP adapter so one local configuration
  serves every agent.
- Tools: `palimpsest_recall` (authorized current retrieval), `palimpsest_remember`
  (explicitly user-approved episode plus `direct-evidence` governed fact), and
  `palimpsest_status` (content-free reachability and scope). No delete or
  export tool exists in the plugin, mirroring the MCP adapter's boundary.
- Lifecycle: `sync_turn` enqueues one immutable episode per turn (user and
  assistant text only) into a durable SQLite write-behind queue with
  idempotency keys; `queue_prefetch`/`prefetch` pre-warm recall per turn and
  skip trivial prompts; `on_memory_write` mirrors only `add` actions as
  governed facts; `on_session_switch`, `on_session_end`, and `shutdown`
  maintain session state and flush the queue. Fact promotion never happens
  without an attributable write policy.
- Configuration is collected by `hermes memory setup` (schema: `base_url`,
  secret `bearer_token` in `.env`, `tenant_id`, `subject_id`, optional
  `case_id`, `namespace`), with non-secrets saved to
  `$HERMES_HOME/palimpsest.json` and environment variables taking precedence.
- It ships `cli.py` (`hermes palimpsest status|config|recall|remember`), a
  `dashboard/plugin_api.py` FastAPI router mounted at
  `/api/plugins/palimpsest`, and a desktop pane
  (`desktop-plugins/palimpsest/plugin.js`) for status, recall, and remember.
- The plugin is dependency-free beyond the Python standard library (stdlib
  `urllib` HTTP client, SQLite), so installation needs no pip steps.

## Consequences

Hermes Agent (CLI, gateway, and desktop app) can use Palimpsest as its memory
provider after `hermes memory setup` selects `palimpsest` and the local
service is running; the desktop app picks the provider up without extra
steps, and a desktop pane exposes status, recall, and remember. Writes are
attributable by design: turns become episodes only, explicit remembers and
built-in memory mirrors promote governed facts, and the plugin never deletes
or exports. Idempotency keys and the durable queue make turn persistence
crash-safe. The plugin stays a thin consumer: the HTTP API remains the
contract, Hermes core is untouched, and Palimpsest remains general-purpose
infrastructure for any agent.
