# Review: wiki workspace recommendation (R2)

Reviewer: independent — Round 2.
Date: 2026-08-08.

## Scope

I read these files in full:

- `.steploop/wiki-workspace-recommendation.md` (v2.0).
- `.hermes/desktop-attachments/llm-wiki-revision-2026-08-08.md`.
- `.steploop/REVIEW-wiki-workspace-R1.md`.

I verified these sources:

- `specs/001-memory-service/spec.md` (R1, R2, R3, R5, R8, R9, R10).
- `specs/002-authorized-retrieval/spec.md` (R2).
- `specs/004-export-operations/spec.md` (R2).
- `specs/011-governed-consolidation/spec.md` (R1-R5).
- `specs/012-proactive-surfacing/spec.md` (R4, R6).
- `specs/constitution.md` (principles 13, 15).
- `specs/016-backup-and-pitr/spec.md` (status).
- `specs/BACKLOG.md`.
- `crates/palimpsest-application/src/export.rs` (export format).
- GitHub issues: `gh issue list --state all`.

Open issues: #1 (stale umbrella, ready-for-agent) and #38 (ready-for-agent).

## Blocker 1: export claim. FIXED.

The cell now reads "NEW markdown projection. NOT spec 004." The projection
section names a new markdown renderer with git sync and a new export kind.

I verified the export code. The package contains `records/*.ndjson` files.
The NDJSON claim is correct. Spec 004 R2 forbids derived summaries. The
vault renders derived pages. The fix is accurate. PASS.

## Blocker 2: write-back. FIXED.

The projection section states the vault is read-only by default. Human
edits do not touch the files. The third voice survives through governed
write-back. Annotations become attributable writes with a registered
policy (001 R9, principle 13). Filed answers become agent writes with
attribution. Schema co-evolution is a governed amendment.

The alternative (direct file edits with sync-back) is rejected with a
reason. It bypasses the attributable write policy. The design satisfies
the blocker. PASS.

## Blocker 3: priority. PARTIALLY FIXED.

The priority call is now evidence-based. #38 proceeds as the frontier.
Spec 016 is finalized with no open questions (verified). #38 carries
ready-for-agent (verified). The risk side is addressed: backup is risk
insurance; delay adds deployment risk. The decision rule is explicit.

Clause 3 is defective. The fix must name the issue for 017 and add a
BACKLOG marker. The document says "017 enters BACKLOG as scheduled with
a new issue." It does not name an issue. The alternatives section admits
"No issue exists for 017." BACKLOG.md has no entry for 017 or the wiki
(verified at HEAD c4c1ba8). The claim "The BACKLOG entry and issue land
now" is false against the live repo. The header claim "All 5 blockers
fixed" is therefore false. FAIL.

## Blocker 4: gap analysis. FIXED.

Items 1-5 now carry resolutions:

1. Open questions map to facts in a registered namespace (001 R9,
   011 R2).
2. Review queue is a worker job (011 R1). The job reads last-touched
   dates from page frontmatter. It flags due pages. A 012 surface
   informs the agent. It does not process the queue. Surfaces are
   advisory and bounded (012 R4, R6).
3. Lint is an operation. A periodic worker job checks contradictions,
   orphans, stale claims, and provenance gaps. It writes lint state to
   a governed fact namespace. It generates new open questions.
4. The hierarchical index is a generated read-only surface (012 R4,
   R6). The server renders the catalog from the semantic layer.
5. The schema file is tenant-owned configuration. Version changes need
   a governed amendment. No framework bias (principle 15).

log.md maps to episodes (001 R2). The working set maps to the MCP
adapter (008), out of scope. PASS.

## Blocker 5: scope and neutrality. FIXED.

The wiki workspace is opt-in. A tenant may enable it. No tenant is
forced (principle 15, verified). It is not a workflow engine and not a
prompt-management UI (001 out of scope, verified). It is a projection
and a write API. PASS.

## Non-blocking folds

All six folds are present and correct:

1. Confidence cites 001 R3 (verified).
2. Raw sources map to artifact references (001 R10).
3. log.md maps to episodes.
4. Lint and review queue reuse the 011 worker.
5. The MCP adapter (008) is the client seam.
6. The effort estimate has four phases with conformance scenarios.

## New-defect hunt

I found no new defects inside the fixes. One accuracy claim needs a
note. The priority section calls #38 "the only actionable frontier
item." Issue #1 also carries ready-for-agent. The repo treats #1 as a
stale umbrella. The document should cite that convention or name #1.

## Verdict

One blocker clause remains defective (blocker 3, clause 3). The fix is
small: create the issue for 017, add the `[scheduled → #N]` marker to
BACKLOG.md, and replace the "land now" claim with the named issue. The
rest of the document verifies cleanly.

VERDICT: FAIL
SCORE: 77/100 (PASS requires 80 or more)
BLOCKING ISSUES: 1. Blocker 3 clause 3 is defective. The document does not name an issue for 017. BACKLOG.md has no 017 marker. The claim "The BACKLOG entry and issue land now" is false against the live repo.
NON-BLOCKING SUGGESTIONS: 1. State the stale status of issue #1 when you call #38 the only actionable frontier item. 2. Enumerate page edits as an attributable-write kind in the write-back list. 3. State whether the new export kind extends spec 004 machinery or is a new surface. 4. Name the review-queue schedule rule (the pattern uses about 30 days).
REVIEWER: independent — Round 2
