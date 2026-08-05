# Palimpsest memory provider for Hermes Agent

Make Palimpsest the long-term memory backend for Hermes Agent (CLI, gateway,
and desktop app) through Hermes' official memory-provider plugin interface.
Palimpsest itself stays general-purpose infrastructure: this plugin is a thin
consumer of the same versioned HTTP API that serves the Codex MCP adapter
(`tools/palimpsest_mcp.py`) and the Python/TypeScript clients — one local
Palimpsest service can feed every agent, and no agent is privileged.

What the plugin gives Hermes:

- `palimpsest_recall` — authorized current retrieval of saved facts.
- `palimpsest_remember` — explicitly user-approved save (episode + governed
  `direct-evidence` fact). The tool description requires explicit user
  approval before a write.
- `palimpsest_status` — content-free endpoint/scope/reachability.
- Automatic turn persistence: every completed turn is written as one
  immutable episode through a durable SQLite write-behind queue (crash-safe,
  idempotent, never blocks the agent loop). Turns become evidence only —
  facts are never auto-extracted without an attributable write policy.
- Built-in memory mirroring: `add` writes to Hermes' own memory tool are
  mirrored as governed facts; `replace`/`remove` are intentionally skipped
  (the plugin has no delete authority).
- Per-turn recall pre-warming (`queue_prefetch`/`prefetch`) that skips
  trivial prompts.
- A desktop pane for Hermes Desktop (status, recall, remember) backed by the
  same service.

No delete or export tools exist in this plugin. The provider never connects
to PostgreSQL — only to the Palimpsest HTTP service, within the configured
tenant/subject/case scope.

## Requirements

- A running Palimpsest HTTP service (see the repository quickstart runbook:
  `docs/runbooks/quickstart.md`). Local development default:
  `http://127.0.0.1:8080`.
- Hermes Agent (CLI, gateway, or desktop app) with Python 3.10+.

## Install

The plugin directory is dependency-free (Python standard library only — no
pip installs).

```bash
# Option A — install from this repository (GitHub subdirectory):
hermes plugins install https://github.com/<owner>/Palimpsest/tree/main/integrations/hermes

# Option B — symlink for development (this repo checked out locally):
ln -s "$HOME/Projects/Palimpsest/integrations/hermes" "$HERMES_HOME/plugins/palimpsest"
```

`$HERMES_HOME` is `~/.hermes` by default, or `~/.hermes/profiles/<name>` under
a named profile (run `hermes doctor` if unsure).

## Configure

```bash
# Interactive: pick "palimpsest" and fill in endpoint/scope (bearer token is
# a secret and lands in .env):
hermes memory setup

# Non-interactive: set the provider directly and use environment variables
# (shared with the Codex MCP adapter — one config serves every agent):
hermes config set memory.provider palimpsest
export PALIMPSEST_BASE_URL='http://127.0.0.1:8080'
export PALIMPSEST_BEARER_TOKEN='palimpsest-local-development-token'   # only needed off-localhost
export PALIMPSEST_TENANT_ID='019be000-0000-7000-8000-000000000010'
export PALIMPSEST_SUBJECT_ID='019be000-0000-7000-8000-000000000020'
export PALIMPSEST_CASE_ID='019be000-0000-7000-8000-000000000030'
```

Configuration precedence: environment variables → `$HERMES_HOME/palimpsest.json`
(written by `hermes memory setup`) → local-development defaults. For a
non-local endpoint, `PALIMPSEST_BEARER_TOKEN` is required.

Only one external memory provider can be active at a time (Hermes rule).

## Verify

```bash
hermes memory status          # Provider: palimpsest · Plugin: installed ✓ · Status: available ✓
hermes palimpsest status      # endpoint, scope, reachability (never the token)
hermes palimpsest recall "what did we decide about X"
hermes palimpsest remember "an explicit memory" --key my-key
```

## Hermes Desktop

The provider works in the desktop app with no extra steps (the desktop runs
the same agent core). The desktop pane adds a visual surface:

```bash
mkdir -p "$HERMES_HOME/desktop-plugins/palimpsest"
ln -s "$HOME/Projects/Palimpsest/integrations/hermes/desktop/plugin.js" "$HERMES_HOME/desktop-plugins/palimpsest/plugin.js"
hermes plugins enable palimpsest     # enables the /api/plugins/palimpsest backend
```

Then run **Reload desktop plugins** from ⌘K in the desktop app. A
**Palimpsest** pane (right side) shows connection status, lets you recall
saved memory, and save explicit memories. The pane talks to the plugin
backend at `/api/plugins/palimpsest` (status / recall / remember) — the
bearer token never leaves the gateway process.

## Privacy and write policy

- `sync_turn` sends user and assistant text only — never raw tool-call
  transcripts or tool results.
- Facts are promoted only for attributable writes: `palimpsest_remember`
  (explicit user approval) and mirrored built-in memory `add` writes.
- The provider never deletes or exports; `replace`/`remove` mirrors are
  skipped by design.

## Development

```bash
python3 -m unittest discover -s integrations/hermes/tests -p 'test_*.py'
```

Tests run standalone against a fake HTTP server (no Hermes core, no live
Palimpsest needed); one integration test additionally proves discovery by
the real Hermes plugin loader when the Hermes source is importable.

See `specs/013-hermes-memory-plugin/spec.md` and
`docs/decisions/0030-hermes-memory-provider-plugin.md`.
