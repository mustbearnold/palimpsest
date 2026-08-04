# SDD Migration Report

Migrating Palimpsest to strict Spec-Driven Development per `SDD_MIGRATION.md`.

## Stage assessment

**LATE** — mature, contract-gated codebase (Rust workspace + PostgreSQL 18 + pgvector, Python and TypeScript clients, MCP/ingest tooling, 20 schema migrations, 28 ADRs) with accumulated, partially contradictory documentation (51 files under `docs/`, status and research documents, product spec).

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
- `crates/palimpsest-application/src/…` · `crates/palimpsest-domain/src/…` · `crates/palimpsest-http/src/…` — domain rules, HTTP layer, application wiring
- `crates/palimpsest-postgres/src/lib.rs` — persistence + migrations + retrieval
- `crates/palimpsest-server/src/…` — service binary (serve/migrate/doctor/restore-verify)
- `clients/python/src/palimpsest/…` · `clients/typescript/src/…` — thin clients
- `scripts/palimpsest_ingest.py` · `scripts/palimpsest_mcp.py` · `scripts/generate-retrieval-corpus.py` · `scripts/generate-q63-exp2.py` — operational Python tooling

### TEST
- `crates/palimpsest-*/tests/…` — Postgres 18 conformance, doctor, migrate, metrics, lexical upgrade, subject lifecycle, restore verify
- `clients/python/tests/test_*.py` (35) · `clients/typescript/test/*.test.mjs`
- `scripts/test_palimpsest_mcp.py` · `scripts/test_palimpsest_scale_probe.sh`

### CONFIG
- `Cargo.toml` · `Cargo.lock` · `rust-toolchain.toml` · `.editorconfig` · `.gitignore` · `compose.yaml` · `docker/postgres/init/001-local-runtime.sql`
- `api/openapi.yaml` — versioned HTTP contract (the public API spec, code-adjacent)
- `.github/` — workflows (ci.yml, repository-quality.yml), CODEOWNERS, dependabot.yml, issue templates, PR template
- `skills-lock.json` · `skills-tree.sha256` · `.agents/skills/` (41 pinned Matt Pocock skills) — pinned procedure layer
- `clients/python/pyproject.toml` · `clients/typescript/package.json`

### ASSET
- `evaluations/retrieval-corpus-v1/corpus.json` · `manifest.json` · `predictions.json` — versioned retrieval evaluation fixture (used by `generate-retrieval-corpus.py` and the scale evaluation)

### DOC (sentenced in Phase 3)
- Root: `README.md` (rewritten thin) · `AGENTS.md` (rewritten thin) · `CONTEXT.md` (merged into constitution, atticked) · `CONTRIBUTING.md` · `SECURITY.md` (kept: platform-standard files) · `SDD_MIGRATION.md` (deleted in Phase 6)
- `docs/PRODUCT_SPEC.md` — merged into `specs/001`–`specs/010`, atticked
- `docs/adr/0001…0028-*.md` — moved to `docs/decisions/`
- `docs/agents/domain.md` — merged into constitution, atticked
- `docs/agents/issue-tracker.md` · `triage-labels.md` — moved to `docs/runbooks/`
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

- Formatters adopted: `cargo fmt` (already enforced), `ruff` (12 Python files), `prettier` (TypeScript client + `api/openapi.yaml`), `shfmt` (all shell scripts), markdown normalization (35 files: paragraphs and list continuations unwrapped per conventions; fences, tables, and blocks preserved).
- Zero logic changes; all suites green after formatting (repo contract, MCP, Python 35, TypeScript 18, scale-probe script, `cargo fmt --check`, clippy `-D warnings`, full `cargo test --locked --workspace` including the PostgreSQL conformance suite).
- Links fixed: ADR-0003 research pointer and ADR-0008 research pointer re-pointed to `_attic/research/`; `CONTRIBUTING.md` vocabulary pointer re-pointed to the constitution.

## Judgment calls (documented deviations)

- `LICENSE` stays at root: protected by the protocol's legal-file invariant.
- `clients/python/README.md` and `clients/typescript/README.md` remain package-local: they are package metadata for PyPI/npm publication, part of the client packages rather than project documentation.
- `.github/pull_request_template.md` and the issue templates remain in `.github/`: platform configuration for the GitHub contribution surface, alongside workflows and CODEOWNERS.
- `.agents/skills/**/SKILL.md` (41 files) remain untouched: pinned byte-for-byte by `skills-lock.json`/`skills-tree.sha256`. The pinned upstream skills still reference the pre-migration layout (`CONTEXT.md`, `docs/adr/`, `docs/agents/`) and will follow when the pin is rotated.
- `SECURITY.md` and `CONTRIBUTING.md` were sentenced in a follow-up pass: moved to `docs/runbooks/security.md` and `docs/runbooks/contributing.md` per the placement law, and `check-repo.sh` re-pointed. Tradeoff recorded here: GitHub's security/community tabs only recognize `SECURITY.md` at the root, `docs/`, or `.github/` — the runbook path is not surfaced by the platform. If the human values the platform tab over placement law, restore the file at root and note the deviation here.
- `api/openapi.yaml`, `evaluations/retrieval-corpus-v1/`, `docker/`, `.github/`, `.agents/` are code/contract/config/fixture, not documentation; they are not relocated by this protocol.

## Deleted / atticked counts

- Deleted: `SDD_MIGRATION.md` (protocol self-delete, Phase 6).
- Atticked: 21 files (11 evaluations, 5 research, PRODUCT_SPEC, domain.md, CONTEXT.md, V2_STATUS, V3_STATUS) — see `_attic/ATTIC.md` for fates.
- Moved: 28 ADRs → `docs/decisions/`; 3 agent docs → `docs/runbooks/` (issue-tracker, triage-labels, skills-provenance).
- Created: 10 capability specs, `specs/constitution.md`, `specs/conventions.md`, `specs/BACKLOG.md`, `docs/architecture.md`, `docs/runbooks/quickstart.md`.

## Open questions

- Attic fates (human): keep or delete each atticked item; empty `_attic/` when ready.
- The pinned upstream skills still expect the old doc layout; they will follow once the upstream templates are updated or the pin is rotated.

## Appendix — Complete per-file inventory (274 files)

Generated from `git ls-files` after Phase 6. Path → class; the pinned
`.agents/skills/` tree is CONFIG (41 skills, byte-pinned).

| `.agents/skills/ask-matt/SKILL.md` | CONFIG |
| `.agents/skills/ask-matt/agents/openai.yaml` | CONFIG |
| `.agents/skills/batch-grill-me/SKILL.md` | CONFIG |
| `.agents/skills/batch-grill-me/agents/openai.yaml` | CONFIG |
| `.agents/skills/claude-handoff/SKILL.md` | CONFIG |
| `.agents/skills/claude-handoff/agents/openai.yaml` | CONFIG |
| `.agents/skills/code-review/SKILL.md` | CONFIG |
| `.agents/skills/code-review/agents/openai.yaml` | CONFIG |
| `.agents/skills/codebase-design/DEEPENING.md` | CONFIG |
| `.agents/skills/codebase-design/DESIGN-IT-TWICE.md` | CONFIG |
| `.agents/skills/codebase-design/SKILL.md` | CONFIG |
| `.agents/skills/codebase-design/agents/openai.yaml` | CONFIG |
| `.agents/skills/design-an-interface/SKILL.md` | CONFIG |
| `.agents/skills/design-an-interface/agents/openai.yaml` | CONFIG |
| `.agents/skills/diagnosing-bugs/SKILL.md` | CONFIG |
| `.agents/skills/diagnosing-bugs/agents/openai.yaml` | CONFIG |
| `.agents/skills/diagnosing-bugs/scripts/hitl-loop.template.sh` | CONFIG |
| `.agents/skills/domain-modeling/ADR-FORMAT.md` | CONFIG |
| `.agents/skills/domain-modeling/CONTEXT-FORMAT.md` | CONFIG |
| `.agents/skills/domain-modeling/SKILL.md` | CONFIG |
| `.agents/skills/domain-modeling/agents/openai.yaml` | CONFIG |
| `.agents/skills/edit-article/SKILL.md` | CONFIG |
| `.agents/skills/edit-article/agents/openai.yaml` | CONFIG |
| `.agents/skills/git-guardrails-claude-code/SKILL.md` | CONFIG |
| `.agents/skills/git-guardrails-claude-code/agents/openai.yaml` | CONFIG |
| `.agents/skills/git-guardrails-claude-code/scripts/block-dangerous-git.sh` | CONFIG |
| `.agents/skills/grill-me/SKILL.md` | CONFIG |
| `.agents/skills/grill-me/agents/openai.yaml` | CONFIG |
| `.agents/skills/grill-with-docs/SKILL.md` | CONFIG |
| `.agents/skills/grill-with-docs/agents/openai.yaml` | CONFIG |
| `.agents/skills/grilling/SKILL.md` | CONFIG |
| `.agents/skills/grilling/agents/openai.yaml` | CONFIG |
| `.agents/skills/handoff/SKILL.md` | CONFIG |
| `.agents/skills/handoff/agents/openai.yaml` | CONFIG |
| `.agents/skills/implement/SKILL.md` | CONFIG |
| `.agents/skills/implement/agents/openai.yaml` | CONFIG |
| `.agents/skills/improve-codebase-architecture/HTML-REPORT.md` | CONFIG |
| `.agents/skills/improve-codebase-architecture/SKILL.md` | CONFIG |
| `.agents/skills/improve-codebase-architecture/agents/openai.yaml` | CONFIG |
| `.agents/skills/loop-me/SKILL.md` | CONFIG |
| `.agents/skills/loop-me/agents/openai.yaml` | CONFIG |
| `.agents/skills/migrate-to-shoehorn/SKILL.md` | CONFIG |
| `.agents/skills/migrate-to-shoehorn/agents/openai.yaml` | CONFIG |
| `.agents/skills/obsidian-vault/SKILL.md` | CONFIG |
| `.agents/skills/obsidian-vault/agents/openai.yaml` | CONFIG |
| `.agents/skills/prototype/LOGIC.md` | CONFIG |
| `.agents/skills/prototype/SKILL.md` | CONFIG |
| `.agents/skills/prototype/UI.md` | CONFIG |
| `.agents/skills/prototype/agents/openai.yaml` | CONFIG |
| `.agents/skills/qa/SKILL.md` | CONFIG |
| `.agents/skills/qa/agents/openai.yaml` | CONFIG |
| `.agents/skills/request-refactor-plan/SKILL.md` | CONFIG |
| `.agents/skills/request-refactor-plan/agents/openai.yaml` | CONFIG |
| `.agents/skills/research/SKILL.md` | CONFIG |
| `.agents/skills/research/agents/openai.yaml` | CONFIG |
| `.agents/skills/resolving-merge-conflicts/SKILL.md` | CONFIG |
| `.agents/skills/resolving-merge-conflicts/agents/openai.yaml` | CONFIG |
| `.agents/skills/scaffold-exercises/SKILL.md` | CONFIG |
| `.agents/skills/scaffold-exercises/agents/openai.yaml` | CONFIG |
| `.agents/skills/setup-matt-pocock-skills/SKILL.md` | CONFIG |
| `.agents/skills/setup-matt-pocock-skills/agents/openai.yaml` | CONFIG |
| `.agents/skills/setup-matt-pocock-skills/domain.md` | CONFIG |
| `.agents/skills/setup-matt-pocock-skills/issue-tracker-github.md` | CONFIG |
| `.agents/skills/setup-matt-pocock-skills/issue-tracker-gitlab.md` | CONFIG |
| `.agents/skills/setup-matt-pocock-skills/issue-tracker-local.md` | CONFIG |
| `.agents/skills/setup-matt-pocock-skills/triage-labels.md` | CONFIG |
| `.agents/skills/setup-pre-commit/SKILL.md` | CONFIG |
| `.agents/skills/setup-pre-commit/agents/openai.yaml` | CONFIG |
| `.agents/skills/setup-ts-deep-modules/SKILL.md` | CONFIG |
| `.agents/skills/setup-ts-deep-modules/agents/openai.yaml` | CONFIG |
| `.agents/skills/setup-ts-deep-modules/dependency-cruiser.config.cjs` | CONFIG |
| `.agents/skills/tdd/SKILL.md` | CONFIG |
| `.agents/skills/tdd/agents/openai.yaml` | CONFIG |
| `.agents/skills/tdd/mocking.md` | CONFIG |
| `.agents/skills/tdd/tests.md` | CONFIG |
| `.agents/skills/teach/GLOSSARY-FORMAT.md` | CONFIG |
| `.agents/skills/teach/LEARNING-RECORD-FORMAT.md` | CONFIG |
| `.agents/skills/teach/MISSION-FORMAT.md` | CONFIG |
| `.agents/skills/teach/RESOURCES-FORMAT.md` | CONFIG |
| `.agents/skills/teach/SKILL.md` | CONFIG |
| `.agents/skills/teach/agents/openai.yaml` | CONFIG |
| `.agents/skills/to-questionnaire/SKILL.md` | CONFIG |
| `.agents/skills/to-questionnaire/agents/openai.yaml` | CONFIG |
| `.agents/skills/to-spec/SKILL.md` | CONFIG |
| `.agents/skills/to-spec/agents/openai.yaml` | CONFIG |
| `.agents/skills/to-tickets/SKILL.md` | CONFIG |
| `.agents/skills/to-tickets/agents/openai.yaml` | CONFIG |
| `.agents/skills/triage/AGENT-BRIEF.md` | CONFIG |
| `.agents/skills/triage/OUT-OF-SCOPE.md` | CONFIG |
| `.agents/skills/triage/SKILL.md` | CONFIG |
| `.agents/skills/triage/agents/openai.yaml` | CONFIG |
| `.agents/skills/ubiquitous-language/SKILL.md` | CONFIG |
| `.agents/skills/ubiquitous-language/agents/openai.yaml` | CONFIG |
| `.agents/skills/wayfinder/SKILL.md` | CONFIG |
| `.agents/skills/wayfinder/agents/openai.yaml` | CONFIG |
| `.agents/skills/wizard/SKILL.md` | CONFIG |
| `.agents/skills/wizard/agents/openai.yaml` | CONFIG |
| `.agents/skills/wizard/template.sh` | CONFIG |
| `.agents/skills/writing-beats/SKILL.md` | CONFIG |
| `.agents/skills/writing-beats/agents/openai.yaml` | CONFIG |
| `.agents/skills/writing-fragments/SKILL.md` | CONFIG |
| `.agents/skills/writing-fragments/agents/openai.yaml` | CONFIG |
| `.agents/skills/writing-great-skills/GLOSSARY.md` | CONFIG |
| `.agents/skills/writing-great-skills/SKILL.md` | CONFIG |
| `.agents/skills/writing-great-skills/agents/openai.yaml` | CONFIG |
| `.agents/skills/writing-shape/SKILL.md` | CONFIG |
| `.agents/skills/writing-shape/agents/openai.yaml` | CONFIG |
| `.editorconfig` | CONFIG |
| `.github/CODEOWNERS` | CONFIG |
| `.github/ISSUE_TEMPLATE/bug.yml` | CONFIG |
| `.github/ISSUE_TEMPLATE/config.yml` | CONFIG |
| `.github/ISSUE_TEMPLATE/proposal.yml` | CONFIG |
| `.github/dependabot.yml` | CONFIG |
| `.github/pull_request_template.md` | CONFIG |
| `.github/workflows/ci.yml` | CONFIG |
| `.github/workflows/repository-quality.yml` | CONFIG |
| `.gitignore` | CONFIG |
| `AGENTS.md` | DOC |
| `Cargo.lock` | CONFIG |
| `Cargo.toml` | CONFIG |
| `LICENSE` | DOC |
| `README.md` | DOC |
| `_attic/ATTIC.md` | DOC |
| `_attic/CONTEXT.md` | DOC |
| `_attic/MIGRATION_REPORT.md` | DOC |
| `_attic/PRODUCT_SPEC.md` | DOC |
| `_attic/V2_STATUS.md` | DOC |
| `_attic/V3_STATUS.md` | DOC |
| `_attic/domain.md` | DOC |
| `_attic/evaluations/2026-07-29-authorized-lexical-retrieval.md` | DOC |
| `_attic/evaluations/2026-07-29-bitemporal-lifecycle.md` | DOC |
| `_attic/evaluations/2026-07-29-checkpoint-resume.md` | DOC |
| `_attic/evaluations/2026-07-29-deterministic-temporal-retrieval.md` | DOC |
| `_attic/evaluations/2026-07-29-exact-vector-fusion.md` | DOC |
| `_attic/evaluations/2026-07-29-retrieval-conformance-corpus.md` | DOC |
| `_attic/evaluations/2026-07-29-subject-lifecycle-fence.md` | DOC |
| `_attic/evaluations/2026-08-02-export-deletion-recovery.md` | DOC |
| `_attic/evaluations/2026-08-02-restore-fence-replay.md` | DOC |
| `_attic/evaluations/2026-08-03-authorized-lexical-scale-probe.md` | DOC |
| `_attic/evaluations/2026-08-03-logical-backup-rehearsal.md` | DOC |
| `_attic/research/2026-07-29-agent-memory-ecosystem.md` | DOC |
| `_attic/research/2026-07-29-architecture-operations-baseline.md` | DOC |
| `_attic/research/2026-07-29-deterministic-temporal-scoring.md` | DOC |
| `_attic/research/2026-07-29-export-deletion-lifecycle.md` | DOC |
| `_attic/research/2026-07-29-first-release-wedge.md` | DOC |
| `api/openapi.yaml` | CONFIG |
| `clients/python/README.md` | DOC |
| `clients/python/pyproject.toml` | CONFIG |
| `clients/python/src/palimpsest/__init__.py` | CODE |
| `clients/python/src/palimpsest/client.py` | CODE |
| `clients/python/src/palimpsest/comparison.py` | CODE |
| `clients/python/src/palimpsest/ingest.py` | CODE |
| `clients/python/src/palimpsest/review.py` | CODE |
| `clients/python/tests/test_client.py` | TEST |
| `clients/python/tests/test_ingest.py` | TEST |
| `clients/python/tests/test_review.py` | TEST |
| `clients/typescript/README.md` | DOC |
| `clients/typescript/package.json` | CONFIG |
| `clients/typescript/src/index.d.ts` | CODE |
| `clients/typescript/src/index.js` | CODE |
| `clients/typescript/test/client.test.mjs` | TEST |
| `clients/typescript/test/review.test.mjs` | TEST |
| `compose.yaml` | CONFIG |
| `crates/palimpsest-application/Cargo.toml` | CONFIG |
| `crates/palimpsest-application/src/export.rs` | CODE |
| `crates/palimpsest-application/src/lib.rs` | CODE |
| `crates/palimpsest-application/src/recovery.rs` | CODE |
| `crates/palimpsest-conformance/Cargo.toml` | CONFIG |
| `crates/palimpsest-conformance/src/lib.rs` | CODE |
| `crates/palimpsest-conformance/src/retrieval_evaluation.rs` | CODE |
| `crates/palimpsest-domain/Cargo.toml` | CONFIG |
| `crates/palimpsest-domain/src/lib.rs` | CODE |
| `crates/palimpsest-http/Cargo.toml` | CONFIG |
| `crates/palimpsest-http/src/lib.rs` | CODE |
| `crates/palimpsest-postgres/Cargo.toml` | CONFIG |
| `crates/palimpsest-postgres/src/lib.rs` | CODE |
| `crates/palimpsest-server/Cargo.toml` | CONFIG |
| `crates/palimpsest-server/src/lib.rs` | CODE |
| `crates/palimpsest-server/src/main.rs` | CODE |
| `crates/palimpsest-server/tests/conformance_postgres18.rs` | TEST |
| `crates/palimpsest-server/tests/doctor_postgres18.rs` | TEST |
| `crates/palimpsest-server/tests/lexical_receipt_upgrade_postgres18.rs` | TEST |
| `crates/palimpsest-server/tests/metrics.rs` | TEST |
| `crates/palimpsest-server/tests/migrate_postgres18.rs` | TEST |
| `crates/palimpsest-server/tests/restore_verify.rs` | TEST |
| `crates/palimpsest-server/tests/subject_lifecycle_postgres18.rs` | TEST |
| `docker/postgres/init/001-local-runtime.sql` | CONFIG |
| `docs/architecture.md` | DOC |
| `docs/decisions/0001-postgres-temporal-source-of-truth.md` | DOC |
| `docs/decisions/0002-ai-ceo-and-github-governance.md` | DOC |
| `docs/decisions/0003-first-release-architecture-and-contract-baseline.md` | DOC |
| `docs/decisions/0004-checkpoint-resume-and-effect-recovery.md` | DOC |
| `docs/decisions/0005-authorization-first-retrieval-receipts.md` | DOC |
| `docs/decisions/0006-exact-vector-rrf.md` | DOC |
| `docs/decisions/0007-deterministic-temporal-retrieval.md` | DOC |
| `docs/decisions/0008-durable-export-and-scoped-deletion.md` | DOC |
| `docs/decisions/0009-main-only-direct-commit-delivery.md` | DOC |
| `docs/decisions/0010-bounded-projection-leases.md` | DOC |
| `docs/decisions/0011-restore-fence-ledger-verification.md` | DOC |
| `docs/decisions/0012-local-codex-mcp-adapter.md` | DOC |
| `docs/decisions/0013-python-client-boundary.md` | DOC |
| `docs/decisions/0014-operator-doctor-boundary.md` | DOC |
| `docs/decisions/0015-explicit-migration-lifecycle.md` | DOC |
| `docs/decisions/0016-content-free-operational-metrics.md` | DOC |
| `docs/decisions/0017-explicit-restore-verification.md` | DOC |
| `docs/decisions/0018-typescript-client-boundary.md` | DOC |
| `docs/decisions/0019-project-aware-agent-session-ingestion.md` | DOC |
| `docs/decisions/0020-project-separated-recall.md` | DOC |
| `docs/decisions/0021-local-agent-source-discovery.md` | DOC |
| `docs/decisions/0022-supervised-local-ingestion-service.md` | DOC |
| `docs/decisions/0023-logical-backup-rehearsal.md` | DOC |
| `docs/decisions/0024-s3-compatible-export-package-store.md` | DOC |
| `docs/decisions/0025-structural-project-comparison.md` | DOC |
| `docs/decisions/0026-attributable-semantic-project-review.md` | DOC |
| `docs/decisions/0027-current-fact-revision-projection.md` | DOC |
| `docs/decisions/0028-governed-project-review-consolidation.md` | DOC |
| `docs/runbooks/contributing.md` | DOC |
| `docs/runbooks/issue-tracker.md` | DOC |
| `docs/runbooks/quickstart.md` | DOC |
| `docs/runbooks/security.md` | DOC |
| `docs/runbooks/skills-provenance.md` | DOC |
| `docs/runbooks/triage-labels.md` | DOC |
| `evaluations/retrieval-corpus-v1/corpus.json` | ASSET |
| `evaluations/retrieval-corpus-v1/manifest.json` | ASSET |
| `evaluations/retrieval-corpus-v1/predictions.json` | ASSET |
| `migrations/0001_episodes.sql` | CODE |
| `migrations/0002_facts.sql` | CODE |
| `migrations/0003_idempotency.sql` | CODE |
| `migrations/0004_governed_writes.sql` | CODE |
| `migrations/0005_checkpoints.sql` | CODE |
| `migrations/0006_authorized_lexical_retrieval.sql` | CODE |
| `migrations/0007_exact_vector_retrieval.sql` | CODE |
| `migrations/0008_deterministic_temporal_retrieval.sql` | CODE |
| `migrations/0009_subject_lifecycle_fence.sql` | CODE |
| `migrations/0010_deletion_operations.sql` | CODE |
| `migrations/0011_canonical_history_exports.sql` | CODE |
| `migrations/0012_deletion_rls_worker_paths.sql` | CODE |
| `migrations/0013_deletion_terminal_outcomes.sql` | CODE |
| `migrations/0014_bounded_projection_leases.sql` | CODE |
| `migrations/0015_restore_fence_replay.sql` | CODE |
| `migrations/0016_release_deletion_operation_lease.sql` | CODE |
| `migrations/0017_current_fact_revision_projection.sql` | CODE |
| `migrations/0018_current_fact_revision_lifecycle_policies.sql` | CODE |
| `migrations/0019_current_fact_revision_repair_and_restore.sql` | CODE |
| `migrations/0020_current_fact_revision_coverage.sql` | CODE |
| `rust-toolchain.toml` | CONFIG |
| `scripts/check-repo.sh` | CODE |
| `scripts/dev-up.sh` | CODE |
| `scripts/generate-q63-exp2.py` | CODE |
| `scripts/generate-retrieval-corpus.py` | CODE |
| `scripts/install-palimpsest-ingest-service.sh` | CODE |
| `scripts/palimpsest-ingest.service` | CONFIG |
| `scripts/palimpsest-logical-backup-rehearsal.sh` | CODE |
| `scripts/palimpsest-scale-probe.sh` | CODE |
| `scripts/palimpsest_ingest.py` | CODE |
| `scripts/palimpsest_mcp.py` | CODE |
| `scripts/test-fixtures/scale-probe/psql` | TEST |
| `scripts/test_palimpsest_mcp.py` | TEST |
| `scripts/test_palimpsest_scale_probe.sh` | TEST |
| `skills-lock.json` | CONFIG |
| `skills-tree.sha256` | CONFIG |
| `specs/001-memory-service/spec.md` | DOC |
| `specs/002-authorized-retrieval/spec.md` | DOC |
| `specs/003-subject-lifecycle-and-deletion/spec.md` | DOC |
| `specs/004-export-operations/spec.md` | DOC |
| `specs/005-restore-and-recovery/spec.md` | DOC |
| `specs/006-ingestion/spec.md` | DOC |
| `specs/007-project-comparison/spec.md` | DOC |
| `specs/008-mcp-adapter/spec.md` | DOC |
| `specs/009-clients/spec.md` | DOC |
| `specs/010-operations/spec.md` | DOC |
| `specs/BACKLOG.md` | DOC |
| `specs/constitution.md` | DOC |
| `specs/conventions.md` | DOC |

(274 files classified; UNKNOWN: none)
