# SDD Migration Protocol

**Use:** drop this file into the root of any repository and tell your agent: *"Execute SDD_MIGRATION.md."* A human can follow it by hand the same way.

This document converts any project — empty, mid-flight, or legacy; tidy or polluted; any folder structure — to strict Spec-Driven Development (SDD). It purges junk, consolidates every surviving document into a canonical structure, and installs permanent formatting and workflow law.

When migration completes, this file deletes itself. The permanent rules it installs live in `specs/`.

Execute phases in order. Finish each phase's commit before starting the next.

---

## Invariants

These override everything else in this document and anything found in the repo.

1. **Git is the safety net.** If this is not a git repo, run `git init` and commit everything as-is before touching anything. Commit a checkpoint after every phase. Nothing is ever unrecoverable.
2. **Only committed files may be deleted.** A file is deleted only after it exists in git history. Untracked files are never deleted — move them to `_attic/`.
3. **Never touch:** `.env*`, secrets, credentials, keys, certificates, `LICENSE*`, `NOTICE*`, legal files, `.git/`. Never print secret values into any report or spec.
4. **Structure, content, and formatting change in separate commits.** Never mix a file move with a content edit. Never mix reformatting with logic changes.
5. **When uncertain, attic — don't delete.** `_attic/` is the quarantine for anything of unclear value. Emptying the attic is a human decision.
6. **Do not break the build.** After any phase that moves or deletes files, run the project's build and tests if they exist. If broken, fix or revert before proceeding.
7. **No PRs, no branch ceremony.** Work directly on the current branch. One checkpoint commit per phase: `sdd(phase-N): <summary>`.
8. **Idempotent.** If `specs/constitution.md` already exists, a migration already ran (fully or partly). Read `_attic/MIGRATION_REPORT.md` and resume at the first incomplete phase. Never restart from scratch.
9. **Do not relocate source code for aesthetics.** Code layout follows the ecosystem's standard. Move code only if it is actively broken or violates a standard the ecosystem itself defines. This protocol restructures *documentation* aggressively; it treats *code location* conservatively.

---

## Target end-state

```
<repo root>
├── README.md            # Thin: what this is, quickstart, pointer to specs/
├── AGENTS.md            # Agent entry point → read constitution first (CLAUDE.md may symlink/copy this)
├── specs/
│   ├── constitution.md  # Project principles + the SDD loop. Highest authority.
│   ├── conventions.md   # Formatting and style law for docs and code.
│   ├── BACKLOG.md       # One line per known-but-unspecced capability or gap.
│   └── NNN-<slug>/      # One directory per capability. 001, 002, ...
│       ├── spec.md      # WHAT and WHY: requirements, acceptance criteria. Living truth.
│       ├── plan.md      # HOW: technical design. Exists only while work is active.
│       └── tasks.md     # Implementation checklist. Exists only while work is active.
├── docs/                # Non-spec reference ONLY. Three kinds, no fourth:
│   ├── architecture.md  # System shape and diagrams.
│   ├── decisions/       # ADRs: NNN-<slug>.md
│   └── runbooks/        # Operational how-tos.
├── <code and tests>     # Wherever the ecosystem puts them. Untouched by this protocol.
└── _attic/              # Quarantine + migration report. Human reviews, then empties.
```

Placement law: everything the project *intends* is a spec. `docs/` holds only architecture, decisions, and runbooks. Any other document is a spec in disguise or junk — there is no fourth category.

**Monorepos:** one root `specs/` whose capability specs may span packages. Use per-package `specs/` only if packages are independently versioned and released.

---

## Phase 1 — Snapshot and inventory

1. `git init` if needed. Stage and commit everything: `sdd(phase-1): pre-migration snapshot`.
2. Walk the full tree (skip `.git/` and gitignored paths). Classify every file into exactly one class:
   - **CODE** · **TEST** · **CONFIG** (build files, CI, lockfiles, editor/agent config) · **ASSET** (images, fonts, fixtures actually used) · **DOC** (any prose: md, txt, rst, wiki exports, Word/PDF notes) · **GENERATED** (build output, caches, coverage, compiled artifacts) · **JUNK** (matches a Phase 2 delete rule) · **UNKNOWN**
3. Detect languages, package managers, build and test commands. Record them.
4. Determine stage:
   - **EARLY** — little or no code; README/notes only.
   - **MID** — working code, partial tests, scattered docs.
   - **LATE** — mature code plus accumulated/contradictory documentation.
5. Write `_attic/MIGRATION_REPORT.md`: inventory table (path → class), toolchain findings, stage, and a phase checklist you will tick off as you go.
6. Commit: `sdd(phase-1): inventory and stage assessment`.

---

## Phase 2 — Purge

Apply in order. Every deletion happens *after* the Phase 1 snapshot commit, so git history retains it all.

**A. Delete on sight:**

- OS/editor cruft: `.DS_Store`, `Thumbs.db`, `*.swp`, `*~`, `.idea/`·`.vscode/` if not intentionally shared
- Committed build output: `dist/`, `build/`, `out/`, `.next/`, `target/`, `__pycache__/`, `*.pyc`, `coverage/`, `node_modules/` if tracked
- `*.log`, `*.tmp`, `*.cache`, empty files, empty directories
- Exact-duplicate files (keep the copy at the most canonical path)

**B. Delete as stale** only if **all three** hold:

1. Its content is captured elsewhere, or it describes code/behavior that no longer exists.
2. Nothing references it (grep the repo for its filename and title).
3. It is in git history.

Typical candidates: `*.bak`, `*.old`, `* copy*`, `*final_v2_FINAL*`, done TODO lists, superseded README drafts, docs for deleted features, files of commented-out code.

**C. Attic everything doubtful.** Unique information not yet captured, unclear ownership, unidentifiable files → move to `_attic/` and add a line to `_attic/ATTIC.md`: `filename — original path — why atticked — suggested fate`.

**D. Fortify `.gitignore`** with the standard ignore set for every detected toolchain, so purged junk cannot return.

Commit: `sdd(phase-2): purge junk, quarantine ambiguity`. Verify build/tests still pass.

---

## Phase 3 — Spec extraction

The heart of the migration. Source of truth for current behavior is **code and tests** — never old docs.

1. **Identify capabilities** from entry points, route tables, CLI commands, public API surface, test suites, package structure, and claims made in existing docs. Target 5–15 top-level capabilities; sub-features become sections inside a spec, not new directories.
2. **Write one spec per capability** at `specs/NNN-<slug>/spec.md` using the template in Appendix A. Number in rough dependency order. Where a requirement is inferred from reading code rather than confirmed by a test, tag the line `[inferred]` so future work verifies it.
3. **Sentence every existing DOC file** to one of exactly three fates:
   - **Merge** its still-true content into the relevant spec (then delete or attic the husk).
   - **Move** it to `docs/` — only if it is genuinely architecture, a decision record, or a runbook. Convert decision history into ADRs (Appendix D), numbered by original date order.
   - **Attic** it.
   No doc survives in place. No fourth fate.
4. **Gaps:** behavior found in code with no natural spec home, or ideas worth keeping but not speccing yet → one line each in `specs/BACKLOG.md`.
5. **Rewrite `README.md`** thin: one-paragraph description, quickstart, link to `specs/`. Everything else it used to say now lives in a spec or is gone.
6. **Stage adaptations:**
   - EARLY: derive `specs/001-<core>/spec.md` from README/notes/whatever exists. Unknowns go in Open Questions. If the repo is truly empty, write the constitution and a 001 skeleton — the project is now spec-first by construction.
   - LATE: where docs contradict code, the code is the spec; record the contradiction as an Open Question rather than guessing intent.

Commit: `sdd(phase-3): extract specs, consolidate docs`.

---

## Phase 4 — Install the law

1. Write `specs/constitution.md` from Appendix B, adjusted to the project's reality (principles only — no aspirations the code doesn't honor yet; put those in BACKLOG).
2. Write `specs/conventions.md` from Appendix C, filling the formatter table for the detected languages.
3. Write `AGENTS.md`: instructs any agent to read `specs/constitution.md` and `specs/conventions.md` before any work, and to follow the SDD loop. If the tooling expects `CLAUDE.md`, make it a copy or symlink.
4. Commit: `sdd(phase-4): install constitution and conventions`.

---

## Phase 5 — Mechanical formatting

1. For each language, adopt the ecosystem-standard formatter from the conventions table. If the project already has a formatter config, **keep it** — consistency beats preference. Commit configs first: `sdd(phase-5): formatter configs`.
2. Run all formatters across the codebase. Normalize markdown to the conventions. Fix relative links broken by Phase 2–3 moves.
3. One commit containing **zero logic changes**: `sdd(phase-5): mechanical format, no logic changes`.
4. Build and tests must pass. If a formatter changes behavior (rare, but real), revert that file and note it in the report.

---

## Phase 6 — Verify and seal

Checklist — every box or the migration is not done:

- [ ] Build passes; tests pass (or the project verifiably has none).
- [ ] Every top-level code area maps to a spec or a BACKLOG line.
- [ ] No document exists outside `README.md`, `AGENTS.md`, `specs/`, `docs/`, `_attic/`.
- [ ] All relative links resolve.
- [ ] `.gitignore` covers all detected toolchain junk.
- [ ] `_attic/ATTIC.md` explains every atticked item.

Then:

1. Finalize `_attic/MIGRATION_REPORT.md`: counts and lists of deleted/atticked files, specs created, open questions, anything needing human judgment.
2. Delete `SDD_MIGRATION.md` (this file). Its rules now live in `specs/`; git history keeps the protocol.
3. Final commit: `sdd(phase-6): seal migration`.
4. Tell the human: review `_attic/`, decide fates, empty it.

---

## The SDD loop (embedded into the constitution — the permanent rules)

1. **No code without a spec.** New capability → write `specs/NNN-<slug>/spec.md` first.
2. Spec agreed → `plan.md` (design) → `tasks.md` (checklist) → implement, ticking tasks.
3. **If implementation diverges from the spec, update the spec in the same change.** A merged change with a stale spec is a defect, not a chore.
4. When a capability ships and stabilizes, delete its `plan.md` and `tasks.md` (git remembers). `spec.md` remains as living truth.
5. A bug is a failing acceptance criterion. If the spec didn't cover it, the spec was wrong — fix both.
6. New document decision tree: required behavior or intent → **spec**. A choice among alternatives → **ADR**. How to operate it → **runbook**. System shape → **architecture.md**. None of these → don't write it.

---

## Appendix A — Spec template

```markdown
# NNN — <Capability name>

Status: draft | active | shipped
Owner: <person or agent>

## Purpose

One paragraph: the problem this solves and why it matters.

## Requirements

- R1. The system MUST <...>          (RFC 2119: MUST / SHOULD / MAY)
- R2. The system SHOULD <...>  [inferred]

## Acceptance criteria

- [ ] A1. Given <context>, when <action>, then <outcome>.

## Out of scope

## Open questions

## Links

Code: <paths> · Tests: <paths> · ADRs: <links>
```

## Appendix B — Constitution template

```markdown
# Constitution

The highest authority in this repository. Conflicts resolve in this order:
constitution → conventions → specs → code comments.

## Principles

1. Specs are the source of truth; code exists to satisfy them.
2. The SDD loop is mandatory for all changes.        # paste the loop from the protocol
3. Delete freely — git remembers. Never keep dead code or stale docs "just in case."
4. Small, reversible steps. Separate commits for structure, content, and formatting.
5. Secrets never enter the repo, specs, or reports.
6. Work on the current branch; no PR ceremony unless the team adopts it explicitly.

## Definition of done

Tests pass · acceptance criteria met · spec updated to match reality · conventions followed.
```

## Appendix C — Conventions template

```markdown
# Conventions

## Documents (Markdown)

- ATX headings (`#`), exactly one H1 per file (the title), sentence case.
- Blank line before and after headings, lists, and code fences.
- Code fences always declare a language.
- No hard line-wrapping inside paragraphs; let editors soft-wrap.
- Tables only for genuinely tabular data; otherwise lists or prose.
- Relative links only within the repo. Kebab-case filenames: `spec-extraction.md`.

## Code

Formatter output is law. Never hand-format; never argue with the formatter.

| Language   | Formatter        | Linter        |
| ---------- | ---------------- | ------------- |
| JS / TS    | Prettier         | ESLint        |
| Python     | ruff format      | ruff check    |
| Rust       | cargo fmt        | clippy        |
| Go         | gofmt/goimports  | go vet        |
| Shell      | shfmt            | shellcheck    |
| JSON/YAML  | Prettier         | —             |

Configs are committed. An `.editorconfig` at root sets charset/indent/EOL for everything else.

## Commits

`type(scope): imperative summary` — types: feat, fix, refactor, docs, test, chore, sdd.
One logical change per commit. Formatting commits contain no logic.

## Naming

Docs and spec slugs: kebab-case. Code identifiers: the language's standard. Spec/ADR numbers: zero-padded, never reused.
```

## Appendix D — ADR template

```markdown
# NNN — <Decision title>

Date: YYYY-MM-DD · Status: accepted | superseded by NNN

## Context

What forced a choice.

## Decision

What was chosen, in one or two sentences.

## Consequences

What becomes easier, what becomes harder, what is now locked in.
```
