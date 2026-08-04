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
| Formatters | `cargo fmt` (Rust), `ruff` (Python, config in `clients/python/pyproject.toml`), `prettier` (TS/JSON/YAML), `shfmt` (Shell, `[*.sh]` indent 4 in `.editorconfig`), markdown per `specs/conventions.md` |
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

### DOC (sentenced in Phase 3)
- Root: `README.md` (rewritten thin) · `AGENTS.md` (rewritten thin) ·
  `CONTEXT.md` (merged into constitution, atticked) · `CONTRIBUTING.md` ·
  `SECURITY.md` (kept: platform-standard files) · `SDD_MIGRATION.md` (deleted
  in Phase 6)
- `docs/PRODUCT_SPEC.md` — merged into `specs/001`–`specs/010`, atticked
- `docs/adr/0001…0028-*.md` — moved to `docs/decisions/`
- `docs/agents/domain.md` — merged into constitution, atticked
- `docs/agents/issue-tracker.md` · `triage-labels.md` — moved to
  `docs/runbooks/`
- `docs/SKILLS_PROVENANCE.md` — moved to `docs/runbooks/skills-provenance.md`
- `docs/evaluations/*.md` (11) — atticked (evidence archive)
- `docs/research/*.md` (5) — atticked (research archive)
- `docs/V2_STATUS.md` · `docs/V3_STATUS.md` — atticked (status snapshots)

### GENERATED
- None tracked (`target/`, `node_modules/`, `__pycache__/` are ignored)

### JUNK
- None tracked

### UNKNOWN
- None

## Phase checklist

- [x] Phase 1 — snapshot and inventory
- [x] Phase 2 — purge junk, quarantine ambiguity
- [x] Phase 3 — extract specs, consolidate docs
- [x] Phase 4 — install constitution and conventions
- [x] Phase 5 — mechanical formatting, no logic changes
- [x] Phase 6 — verify and seal

## Phase 5 report

- Formatters adopted: `cargo fmt` (already enforced), `ruff` (12 Python files),
  `prettier` (TypeScript client + `api/openapi.yaml`), `shfmt` (all shell
  scripts), markdown normalization (35 files: paragraphs and list continuations
  unwrapped per conventions; fences, tables, and blocks preserved).
- Zero logic changes; all suites green after formatting (repo contract, MCP,
  Python 35, TypeScript 18, scale-probe script, `cargo fmt --check`, clippy
  `-D warnings`, full `cargo test --locked --workspace` including the
  PostgreSQL conformance suite).
- Links fixed: ADR-0003 research pointer and ADR-0008 research pointer
  re-pointed to `_attic/research/`; `CONTRIBUTING.md` vocabulary pointer
  re-pointed to the constitution.

## Judgment calls (documented deviations)

- `LICENSE`, `SECURITY.md`, `CONTRIBUTING.md` stay at root: platform-standard
  repository files (GitHub community standards), analogous to the protected
  legal set; `check-repo.sh` requires `SECURITY.md`.
- `clients/*/README.md`, `.github/pull_request_template.md`, and
  `.agents/skills/**/SKILL.md` remain outside `specs/`/`docs/`/`_attic/`: they
  are package-local and platform/pinned-upstream artifacts, not project
  documentation. The pinned skills still reference the pre-migration layout
  (`CONTEXT.md`, `docs/adr/`, `docs/agents/`) upstream; they were not edited
  because `skills-lock.json`/`skills-tree.sha256` pin them byte-for-byte.
- `api/openapi.yaml`, `evaluations/retrieval-corpus-v1/`, `docker/`,
  `.github/`, `.agents/` are code/contract/config/fixture, not documentation;
  they are not relocated by this protocol.
- `check-repo.sh` re-pointed at the post-migration tree in Phase 3, extended
  with the constitution and conventions in Phase 4.
- The conformance/doctor local test environment requires a non-superuser
  `CREATEDB` runtime role (`palimpsest_local_runtime` locally) plus a separate
  superuser migration URL; see the local test recipe in the handoff note.

## Deleted / atticked counts

- Deleted: `SDD_MIGRATION.md` (protocol self-delete, Phase 6).
- Atticked: 26 files + 2 directories worth of entries (11 evaluations, 5
  research, PRODUCT_SPEC, domain.md, CONTEXT.md, V2_STATUS, V3_STATUS,
  SKILLS_PROVENANCE history) — see `_attic/ATTIC.md` for fates.
- Moved: 28 ADRs → `docs/decisions/`; 3 agent docs → `docs/runbooks/`.
- Created: 10 capability specs, `specs/constitution.md`, `specs/conventions.md`,
  `specs/BACKLOG.md`, `docs/architecture.md`, `docs/runbooks/quickstart.md`.

## Open questions

- Attic fates (human): keep or delete each atticked item; empty `_attic/`
  when ready.
- The pinned upstream skills still expect the old doc layout; they will
  follow once the upstream templates are updated or the pin is rotated.
