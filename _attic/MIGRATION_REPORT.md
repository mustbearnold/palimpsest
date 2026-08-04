# SDD Migration Report

Migrating Palimpsest to strict Spec-Driven Development per `SDD_MIGRATION.md`.

## Stage assessment

**LATE** — mature, contract-gated codebase (Rust workspace + PostgreSQL 18 +
pgvector, Python and TypeScript clients, MCP/ingest tooling, 20 schema
migrations, 28 ADRs) with accumulated, partially contradictory documentation
(51 files under `docs/`, status and research documents, product spec).

## Toolchain findings

| Concern | Finding |
| --- | --- |
| Language | Rust (workspace: application, conformance, domain, http, postgres, server crates), Python 3 (clients + scripts), TypeScript (client), Bash (scripts) |
| Toolchain pin | `rust-toolchain.toml` (1.97.1); `Cargo.lock` |
| Build | `cargo build --locked --workspace` |
| Tests | `cargo test --locked --workspace`; `python3 -m unittest discover -s clients/python/tests`; `node --test clients/typescript/test/*.test.mjs`; `python3 scripts/test_palimpsest_mcp.py`; `bash scripts/test_palimpsest_scale_probe.sh` |
| Gate | `bash scripts/check-repo.sh` (repo contract + 41 pinned skills) |
| Formatters | `cargo fmt` (Rust), `.editorconfig` present; no ruff/prettier/shfmt configs found |
| Database | PostgreSQL 18, pgvector 0.8.5, migrations 1–20, dev profile on port 55432 |

## Inventory (path → class)

Legend: CODE · TEST · CONFIG · ASSET · DOC · GENERATED · JUNK · UNKNOWN

### CODE
- `crates/palimpsest-application/src/…` · `crates/palimpsest-domain/src/…` ·
  `crates/palimpsest-http/src/…` — domain rules, HTTP layer, application wiring
- `crates/palimpsest-postgres/src/lib.rs` — persistence + migrations + retrieval
- `crates/palimpsest-server/src/…` — service binary (serve/migrate/doctor/restore-verify)
- `clients/python/src/palimpsest/…` · `clients/typescript/src/…` — thin clients
- `scripts/palimpsest_ingest.py` · `scripts/palimpsest_mcp.py` ·
  `scripts/generate-retrieval-corpus.py` · `scripts/generate-q63-exp2.py` —
  operational Python tooling

### TEST
- `crates/palimpsest-*/tests/…` — Postgres 18 conformance, doctor, migrate,
  metrics, lexical upgrade, subject lifecycle, restore verify
- `clients/python/tests/test_*.py` (35) · `clients/typescript/test/*.test.mjs`
- `scripts/test_palimpsest_mcp.py` · `scripts/test_palimpsest_scale_probe.sh`

### CONFIG
- `Cargo.toml` · `Cargo.lock` · `rust-toolchain.toml` · `.editorconfig` ·
  `.gitignore` · `compose.yaml` · `docker/postgres/init/001-local-runtime.sql`
- `api/openapi.yaml` — versioned HTTP contract (the public API spec, code-adjacent)
- `.github/` — workflows (ci.yml, repository-quality.yml), CODEOWNERS,
  dependabot.yml, issue templates, PR template
- `skills-lock.json` · `skills-tree.sha256` · `.agents/skills/` (41 pinned
  Matt Pocock skills) — pinned procedure layer
- `clients/python/pyproject.toml` · `clients/typescript/package.json`

### ASSET
- `evaluations/retrieval-corpus-v1/corpus.json` · `manifest.json` ·
  `predictions.json` — versioned retrieval evaluation fixture (used by
  `generate-retrieval-corpus.py` and the scale evaluation)

### DOC
- Root: `README.md` · `AGENTS.md` · `CONTEXT.md` (domain glossary) ·
  `CONTRIBUTING.md` · `SECURITY.md` · `SDD_MIGRATION.md` (protocol)
- `docs/PRODUCT_SPEC.md` — product specification (merge source)
- `docs/adr/0001…0028-*.md` — 28 architecture decision records
- `docs/agents/domain.md` · `issue-tracker.md` · `triage-labels.md`
- `docs/evaluations/2026-07-29…2026-08-03-*.md` — 11 evidence evaluations
- `docs/research/2026-07-29-*.md` — 5 research notes
- `docs/SKILLS_PROVENANCE.md` · `docs/V2_STATUS.md` · `docs/V3_STATUS.md`

### GENERATED
- None tracked (`target/`, `node_modules/`, `__pycache__/` are ignored)

### JUNK
- None tracked

### UNKNOWN
- None

## Phase checklist

- [ ] Phase 1 — snapshot and inventory
- [ ] Phase 2 — purge junk, quarantine ambiguity
- [ ] Phase 3 — extract specs, consolidate docs
- [ ] Phase 4 — install constitution and conventions
- [ ] Phase 5 — mechanical formatting, no logic changes
- [ ] Phase 6 — verify and seal

## Judgments (documented deviations)

- `LICENSE`, `SECURITY.md`, `CONTRIBUTING.md` stay at root: platform-standard
  repository files (GitHub community standards), analogous to the protected
  legal set; `check-repo.sh` requires `SECURITY.md`.
- `api/openapi.yaml`, `evaluations/retrieval-corpus-v1/`, `docker/`,
  `.github/`, `.agents/` are code/contract/config/fixture, not documentation;
  they are not relocated by this protocol.
- `check-repo.sh` pins pre-migration doc paths; it is re-pointed at the
  post-migration tree in Phase 3 (its own phase) so the repo contract gate
  stays green.
