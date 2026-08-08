# Review — wiki workspace recommendation (spec 017), Round 4

Document: `.steploop/wiki-workspace-recommendation.md`, revision v2.2.
Pattern: `.hermes/desktop-attachments/llm-wiki-revision-2026-08-08.md`.
Reviewer: independent. Round 4 (retry 3 after R3 FAIL 78/100).

## Verdict

PASS. The round-3 blocker is fixed and verified against the live tracker.
The full re-review found no new defects. Every spec citation checks out.

## Round-3 blocker verification

Blocker: the priority and tracker section claimed issue #1 was "a stale
umbrella from 2025". No 2025 date exists in the repo.

Fix: the phrase "from 2025" is deleted. The section now reads "Issue #1 is
a stale umbrella." (v2.2, lines 96-98).

Evidence:
- `gh issue view 1 --json createdAt` returns 2026-07-28T11:54:37Z. The
  repo's first commit is 2026-07-28. No 2025 date exists.
- A content search for "2025" across the recommendation returns zero
  matches. No false year remains anywhere in the document.

The fix is complete and correct.

## Round-3 non-blocking folds verification

1. Intro tense. The intro now reads "017 entered the backlog as issue #46."
   (line 12). The tracker section reads "017 entered BACKLOG as scheduled
   with issue #46 on 2026-08-08." (line 102). The tenses agree.
2. Last-touched dates. "The job reads last-touched dates from canonical
   fact metadata. The renderer writes these dates into frontmatter. The
   projection is never the authority." (lines 52-54). This matches 001 R3
   (facts are revision chains with recorded time). The frontmatter is a
   projection of canonical state. No write-back hole remains.
3. One-way vault. "The vault is one-way. Git sync pushes canonical state to
   the vault. The write-back API is the only write path. Direct sync-back
   is rejected." (lines 75-77). The write-back section (lines 81-85) keeps
   every human edit on the attributable-write path. No contradiction.

## Full re-review: mapping table vs. live sources

Each claim verified against the cited source.

| Claim | Source | Result |
| --- | --- | --- |
| Episodes (001 R2) | R2: episodes are immutable, timestamped observations | Match |
| Facts, revisions, supersede (001 R3, R5) | R3: revision chains with superseded revision; R5: supersession links | Match |
| Provenance (001 R1); confidence (001 R3); derived vs raw (001 R8, 011 R5) | R1: provenance on every memory; R3: confidence; R8: raw vs derived; 011 R5: provenance kind `derived` | Match |
| Supersede (001 R5); derived (011 R5) | Verified above | Match |
| Spec 011 worker derivation (011 R1-R3) | R1: durable work items with claim-level retry; R2: attributable model boundary; R3: replay | Match |
| Retrieval receipts (002 R2) | R2: durable receipt records policy version, temporal perspective, provenance to explain a result | Match |
| Spec 004 exports NDJSON canonical history | Spec 004 R2: canonical history, not derived summaries. ADR 0008: package is a ZIP with `records/*.ndjson` files | Match |
| Spec 004 R2 forbids derived summaries | R2 verbatim: "not derived summaries" | Match |
| Artifact references (001 R10) | R10: integrity-checked artifact references | Match |
| Registered write policy (001 R9) | R9: no model output without attributable write policy | Match |
| Advisory, bounded surfaces (012 R4, R6) | R4: advisory, never override agent; R6: bounded bundles | Match |
| Principle 13 | Constitution: no model output becomes durable memory without an attributable write policy | Match |
| Principle 15 | Constitution: no framework bias; integrations opt-in | Match |
| Working set is a client concern (008) | 008 R1-R2: MCP adapter over HTTP API; working set is agent context | Match |

## Live tracker verification

- Issue #1: created 2026-07-28T11:54:37Z, OPEN, ready-for-agent, title
  "Build Palimpsest: temporal MemoryService". The umbrella product spec.
- Issue #38: created 2026-08-04, OPEN, ready-for-agent. The doc's claim
  that #38 carries ready-for-agent is true.
- Issue #46: created 2026-08-08T05:07:03Z, OPEN, title "Wiki workspace:
  markdown vault projection and governed write-back (spec 017)". The doc's
  claim that 017 entered BACKLOG with issue #46 on 2026-08-08 is true.
- BACKLOG.md line 11: `[scheduled → #46] Wiki workspace: markdown vault
  projection and governed write-back (spec 017, llm-wiki pattern)`. Commit
  fdecb2b dated 2026-08-08T05:07:34Z.
- Spec 016 status: "Finalized 2026-08-08. ... No open questions." The
  doc's claim is true. Commit c4c1ba8.
- Spec 017 has no directory in `specs/`. The doc's claim that the draft
  waits is true.
- 001 out of scope names workflow engines and prompt-management UIs. The
  doc's claim is true.

## New-defect hunt

- No "2025" string remains. The false year cannot recur in this file.
- The changelog (v2.2) lists the blocker and the three folds. It matches
  the document body.
- The status line (line 5) states R3 FAIL 78/100 with one blocker and
  three folds. It matches this brief.
- The 30-day rule matches the pattern's "~30 days" (line 109 of the
  pattern file).
- The recommendation states the vault is not spec 004 and reuses the 004
  package machinery. The 004 package is a ZIP with NDJSON records. The new
  export kind adds a projection surface. Consistent.
- Every requirement number cited in the document resolves to the correct
  clause. I found no stale or invented citations.

## Residual observation (non-blocking)

The tracker section asserts "It is the only actionable frontier item" and
"Issue #1 is a stale umbrella. It is not a frontier item." The issue-tracker
runbook defines the frontier mechanically: an unassigned, unblocked
ready-for-agent issue is on the frontier. Issue #1 is open, unassigned, and
carries the label. The doc's counter is a staleness judgment, not a
runbook rule. The judgment is defensible: issue #1 is the original umbrella
product spec, decomposed into the per-spec issues that followed. The round-3
reviewer accepted this framing. I do not re-block it. Suggestion: the spec
017 draft should state the basis of the staleness call (umbrella
decomposed into issues 002-016) so the frontier claim is verifiable.

## Score

The blocker cost the document its R3 pass. The fix is verified. All other
claims pass. One verifiability nuance remains, priced small.

SCORE: 94/100.
