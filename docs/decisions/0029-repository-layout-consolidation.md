# ADR-0029: Repository layout consolidation

Date: 2026-08-05 · Status: accepted

## Context

After the SDD migration the repository tree is contract-clean but has three
structural weaknesses. `scripts/` is a grab-bag mixing agent-facing product
tools (the MCP adapter, the ingestion bridge, corpus generators), operator
tooling (dev launcher, repo gate, scale probe, backup rehearsal, service
installer), and test scripts. `docker/` is a single-file tree
(`docker/postgres/init/001-local-runtime.sql`). Stray empty tool directories
(`.codex/`, `.codex-tmp/`) and untracked caches (`scripts/__pycache__/`,
`.ruff_cache/`) accumulate on disk.

## Decision

- Split `scripts/` by purpose:
  - `tools/` holds agent-facing and data-generation Python tooling:
    `palimpsest_mcp.py`, `palimpsest_ingest.py`,
    `generate-retrieval-corpus.py`, `generate-q63-exp2.py`, and their test
    `test_palimpsest_mcp.py`.
  - `scripts/` keeps operator and gate tooling: `dev-up.sh`, `check-repo.sh`,
    the scale probe, the logical-backup rehearsal, the ingest service
    installer and unit, `test-fixtures/`, and `test_palimpsest_scale_probe.sh`.
- Flatten `docker/` to `docker/init/001-local-runtime.sql`; the Compose mount
  becomes `./docker/init:/docker-entrypoint-initdb.d:ro`.
- Delete empty tool directories and ignore `ruff`'s cache (`.ruff_cache/`).
- Code layout that already follows ecosystem standards is deliberately
  untouched: the Rust workspace (`crates/`), `migrations/`, `clients/`
  package layouts, `api/`, `specs/`, `docs/` (per invariant 9 of the SDD
  migration: no aesthetic code relocation). `evaluations/retrieval-corpus-v1/`
  stays at its code-pinned path (`CARGO_MANIFEST_DIR`-relative in
  `palimpsest-conformance`); its digests are pinned and it is excluded from
  formatters.

## Consequences

- Easier: "which tool runs as an agent surface vs. which is operator
  plumbing" is visible in the directory name; CI, quickstart, decisions,
  specs, the systemd unit, and the client README are updated to the new
  paths.
- Harder: any external script that referenced the old paths must be
  re-pointed (the systemd unit installed on a live host is only fixed by
  re-running the installer).
- Locked in: `tools/` and `scripts/` as the two tool layers; the docker init
  mount shape.
