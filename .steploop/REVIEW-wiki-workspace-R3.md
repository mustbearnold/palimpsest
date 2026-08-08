# Review: wiki workspace recommendation (R3)

Reviewer: independent — Round 3.
Date: 2026-08-08.
Revision under review: v2.1.

## Scope

I read these files in full:

- `.steploop/wiki-workspace-recommendation.md` (v2.1).
- `.hermes/desktop-attachments/llm-wiki-revision-2026-08-08.md`.
- `.steploop/REVIEW-wiki-workspace-R2.md` (the prior verdict).

I verified these live sources:

- `gh issue view 46 --json number,state,title,labels`.
- `gh issue view 38 --json number,state,title,labels`.
- `gh issue view 1 --json number,state,title,labels,createdAt`.
- `gh issue list --state open`.
- `specs/BACKLOG.md`.
- `specs/001-memory-service/spec.md` (R1-R11, out of scope).
- `specs/002-authorized-retrieval/spec.md` (R2).
- `specs/004-export-operations/spec.md` (R1, R2).
- `docs/decisions/0008-durable-export-and-scoped-deletion.md` (export format).
- `specs/011-governed-consolidation/spec.md` (R1-R5, jobs and claims).
- `specs/012-proactive-surfacing/spec.md` (R4, R6).
- `specs/constitution.md` (principles 13, 15).
- `specs/016-backup-and-pitr/spec.md` (status).
- `git log` (fdecb2b, first commit dates).

## Round 2 blocker: FIXED

The blocker required a named issue for 017 and a BACKLOG marker. I verified
each clause:

1. Issue #46 exists. State OPEN. Title: "Wiki workspace: markdown vault
   projection and governed write-back (spec 017)". Label: 🧠 memory-service.
   Verified with `gh issue view 46`.
2. `specs/BACKLOG.md` lines 11-12 carry the marker: `[scheduled → #46] Wiki
   workspace: markdown vault projection and governed write-back (spec 017,
   llm-wiki pattern)`.
3. Commit fdecb2b (2026-08-08) added the marker and the review gate record.
   Verified with `git show fdecb2b`.
4. The recommendation names issue #46 in the priority and tracker section
   (line 96) and in the alternatives section (lines 122-125).
5. The claim "The BACKLOG entry and issue #46 landed on 2026-08-08" is true
   against the live repo.

Result: the blocker is fixed. PASS.

## Folded notes: verified

1. Stale umbrella stated. The priority section names issue #1. The year is
   wrong. See the new-defect section.
2. Page edits as a write-back kind. Present in the write-back list (line 77).
   The pattern names "your annotations, your edits" as the third voice.
   Match.
3. Export kind reuses 004 machinery. Present (line 70). ADR-0008 confirms
   the package holds `records/*.ndjson` files. The NDJSON claim is correct.
4. Review queue due rule. The job flags pages not touched in 30 days. The
   pattern says "~30 days" (line 109 of the pattern). Match.

## Claim verification

I checked every changed claim against the specs:

- 001 R2: episodes are immutable timestamped observations. Match.
- 001 R3: facts are revision chains with confidence and superseded link.
  Match.
- 001 R5: supersession links revisions without deleting history. Match.
- 001 R1: provenance on every durable memory. Match.
- 001 R8: raw episodes stay distinguishable from derived facts. Match.
- 001 R9: no model output becomes durable memory without an attributable
  write policy. Match.
- 001 R10: artifact references with integrity checks. Match.
- 001 out of scope: workflow engines and prompt-management UIs. Match.
- 002 R2: durable retrieval receipts. Match. The "result-level explanation,
  not claim-level citations" caveat is honest.
- 004 R2: export packages hold canonical history, not derived summaries.
  Match. The vault renders derived pages. The boundary is accurate.
- 011 R1: durable server-side work items with claim-level retry. Match.
- 011 R2: attributable model boundary and policy gate. Match.
- 011 R5: derived facts stay distinguishable. Match.
- 011 "Jobs and claims" design section exists. Match.
- 012 R4: surfaces are advisory. Match.
- 012 R6: bundles are bounded. Match.
- Constitution principle 13: attributable write policy. Match.
- Constitution principle 15: no framework bias. Match.
- Spec 016: "Active. Finalized 2026-08-08. No open questions." Match.
- Issue #38: OPEN, ready-for-agent, memory-service. Title matches spec 016.
  Match.
- Open issues: #47 (bug), #46 (scheduled), #38 (ready-for-agent), #1
  (ready-for-agent, stale). The claim that #38 is the only actionable
  frontier item holds with the #1 caveat.
- Pattern elements in the mapping table: episodic and semantic layers,
  provenance tags, status tags, consolidation pass, query with citations,
  Obsidian, contradiction flags, raw sources, log.md, open questions, review
  queue, lint pass, hierarchical index, schema file, working set. All match
  the pattern text.

The changelog claims match the R2 verdict: FAIL 77/100, one blocker, four
notes folded.

## New-defect hunt

1. BLOCKING. The priority and tracker section says: "Issue #1 is a stale
   umbrella from 2025." This claim is false against the live tracker. Issue
   #1 was created 2026-07-28T11:54:37Z. The first repo commit is 2026-07-28.
   No 2025 date exists in the tracker or the repository. The round-2 fold
   introduced the year. The fix is one word: "from 2026", or no year.
   Round 2 failed this section for a claim false against the live repo. The
   same standard applies here. The clause is defective.

2. NON-BLOCKING. The review-queue job reads last-touched dates from page
   frontmatter. The vault is a projection. The renderer produces the
   frontmatter. The spec draft should source last-touched dates from
   canonical temporal metadata. A projection round-trip must not become the
   authority.

3. NON-BLOCKING. The introduction says "017 enters the backlog as a new
   issue" (present tense, line 13). The tracker section states the entry
   landed. Align the tenses.

4. NON-BLOCKING. The spec draft must define the interaction between git
   sync and the write-back API. Direct sync-back is rejected. The draft must
   state how a synced vault reconciles with governed writes.

## Verdict

The round-2 blocker is fixed and verified. The recommendation substance is
sound. All spec citations verify. One new defect remains: a false year for
issue #1 in the evidence section. Round 2 established the standard: a claim
false against the live tracker in this section is blocking. I apply the
same standard.

VERDICT: FAIL
SCORE: 78/100 (PASS requires 80 or more)
BLOCKING ISSUES: 1. The priority and tracker section claims issue #1 is "a stale umbrella from 2025". The live tracker shows issue #1 was created 2026-07-28T11:54:37Z. The repo's first commit is 2026-07-28. No 2025 date exists. The v2.1 fold introduced this false year. The clause is defective against the live repo.
NON-BLOCKING SUGGESTIONS: 1. Source review-queue last-touched dates from canonical temporal metadata in the spec draft; the renderer produces the frontmatter, so a projection round-trip must not become the authority. 2. Align the tense of the introduction ("017 enters the backlog") with the tracker section ("the BACKLOG entry and issue #46 landed"). 3. The spec draft must define how the git-synced vault reconciles with the governed write-back API, since direct sync-back is rejected.
REVIEWER: independent — Round 3
