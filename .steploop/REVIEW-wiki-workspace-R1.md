# Review: wiki workspace recommendation (R1)

Reviewer: independent — Round 1.
Date: 2026-08-08.

## Scope

I read these files in full:

- `.steploop/wiki-workspace-recommendation.md`.
- `.hermes/desktop-attachments/llm-wiki-revision-2026-08-08.md`.

I verified these sources:

- `specs/001-memory-service/spec.md`.
- `specs/002-authorized-retrieval/spec.md`.
- `specs/004-export-operations/spec.md`.
- `specs/011-governed-consolidation/spec.md`.
- `specs/012-proactive-surfacing/spec.md`.
- `specs/015-hot-cache/spec.md`.
- `specs/016-backup-and-pitr/spec.md`.
- `specs/BACKLOG.md`.
- `specs/constitution.md`.
- Export code: `crates/palimpsest-application/src/export.rs`.
- GitHub issues: `gh issue list --state open`.

Open issues: #1 (stale umbrella) and #38 (ready-for-agent).

## Verdict summary

The direction is plausible. The pattern maps onto the architecture in part.
Two load-bearing claims are factually wrong. The priority case has no
evidence. The gap analysis misses real pattern elements.

VERDICT: FAIL. Score 55/100.

## Dimension 1: mapping table accuracy

I checked each cell against the spec text.

1. Episodic layer to Episodes. Accurate. Spec 001 R2 defines episodes as
   immutable, timestamped observations. PASS.

2. Semantic layer to facts with revisions and supersede. Accurate. Spec 001
   R3 and R5 define revision chains and supersession. PASS.

3. Provenance tags to provenance and confidence fields (spec 001 R1).
   Mis-cited. R1 lists provenance but not confidence. Confidence appears in
   R3. The pattern also defines status tags: confirmed, inference, disputed,
   superseded. No fact-status tag exists in spec 001. FAIL.

4. Consolidation pass to spec 011 worker. Partial. Spec 011 derives facts
   from episodes with attribution. This matches episodic-to-semantic
   promotion. The pattern pass also merges duplicate pages, updates the
   review queue, and writes a human digest. Spec 011 has none of these.
   PARTIAL.

5. Query with citations to retrieval receipts (spec 002). Partial. Receipts
   record the policy version, temporal perspective, and provenance that
   explain a result (002 R2). They explain result inclusion. They are not
   claim-level citations to sources. PARTIAL.

6. Obsidian as IDE to markdown export (spec 004). FACTUALLY WRONG. I
   verified the export code. The package contains NDJSON records:
   `records/episodes.ndjson`, `records/fact-revisions.ndjson`, and similar.
   No markdown renderer exists in the codebase. Spec 004 R2 forbids derived
   summaries in export packages. Wiki pages are derived summaries. Spec 004
   R1 freezes an immutable one-shot manifest. The vault needs continuous
   updates. FAIL.

7. Contradiction flags to supersede mechanism. Partial. Constitution
   principle 8 supports supersede-on-contradiction. The pattern's `disputed`
   tag has no counterpart. `disputed` marks conflicting sources with no
   replacement. FAIL.

8. Open questions page to gap. Correct. No such concept exists in the
   specs.

9. Review queue to gap. Correct.

10. Lint pass and meta.md to gap. Correct.

11. Hierarchical index to gap. Correct.

12. Schema file to gap. Correct.

The table omits pattern elements: raw sources layer, working set, log.md,
filing answers back, and human annotations. The table claims a complete
mapping. It is not complete.

## Dimension 2: gap analysis soundness

The listed gaps are real. The analysis is incomplete in four ways.

1. The gap list resolves four of five gaps. The hierarchical index has no
   resolution. The recommendation does not address it.

2. Lint is an operation. It is a periodic health check. It writes meta.md.
   It generates new open questions and new sources. The recommendation
   resolves "lint state" only. The operation itself has no home.

3. The review queue needs a write path. The pattern re-reads a due page,
   updates it, and marks it done. A spec 012 surface is advisory and
   read-only (012 R4 and R6). It is pull-with-context. It cannot process the
   queue. The queue also needs a schedule: last-touched dates and the
   30-day rule. The recommendation does not model this.

4. "The gaps are small" is an assertion. The schema-as-governed-configuration
   is a new governance concept. Who registers the schema? Who approves
   changes? How do schema versions map to write policies? The
   recommendation gives no design.

## Dimension 3: projection resolution soundness

The resolution fails on three points.

1. "Export (004) materializes it" is false. Export materializes frozen
   NDJSON canonical history. A wiki vault is live, interlinked markdown with
   frontmatter. The vault needs a new projection and a new export shape.
   The recommendation treats this as done.

2. The hot-cache analogy covers the read side only. Spec 015 D3 states the
   cache is rebuildable and canonical memory is the source of truth. This is
   directionally right for a read-only projection. The wiki is not read-only.
   The pattern defines three voices. The third voice is "what you believe":
   human annotations and edits. The schema co-evolves with the human. A
   one-way projection loses all of this. Write-back needs an attributable
   write policy (001 R9, constitution principle 13). The recommendation does
   not address write-back. This is the largest hole.

3. "Git versions it" needs a live git sync. Spec 004 has none. Frozen
   packages do not produce diffs as review artifacts.

## Dimension 4: priority argument

The argument fails on five points.

1. "017 is more product value than 016" is an assertion. No evidence
   supports it. No decision rule exists.

2. Spec 016 is finalized. It has no open questions. Issue #38 is
   ready-for-agent. It is the only actionable frontier item. Deferring it
   stalls the frontier.

3. Backup is risk insurance. The comparison must weigh downside risk.
   Every day without PITR evidence adds deployment risk. The
   recommendation ignores this side.

4. The recommendation understates scope. 017 adds a markdown projection,
   write-back, open-questions policy, review processing, lint operation,
   schema governance, and an index. That is several mechanisms, not one
   concept.

5. Tracker hygiene is unaddressed. GitHub Issues are the planning source of
   truth (constitution). Spec 017 has no issue and no BACKLOG entry. The
   recommendation proposes a queue jump with no issue.

## Dimension 5: completeness

The recommendation misses these items:

1. Raw sources layer. The pattern keeps immutable verbatim traces. Spec 001
   R10 artifact references could host them. Absent from the table.
2. Working set. The pattern keeps hot files in context. Attention
   allocation is a real constraint. Absent.
3. log.md. Episodes already form the chronological record. The
   recommendation does not say so. Absent.
4. Filing answers back. The pattern files query results into the wiki.
   This is a write path into durable memory. Absent.
5. Status tags: confirmed, inference, disputed. Only supersede is mapped.
6. Review scheduling. Last-touched dates and expanding intervals are
   temporal data. Absent.
7. Scope and neutrality. Principle 15 forbids bias toward any workflow. The
   schema file is agent-specific. Spec 001 out of scope lists workflow
   engines and prompt-management UIs. The recommendation needs an explicit
   scope decision. Absent.
8. Agent integration. Spec 008 defines the MCP adapter. It is the natural
   seam for the vault. Absent.

## BLOCKING ISSUES

1. The mapping cell "Obsidian as IDE to markdown export (spec 004)" is
   false. Export materializes NDJSON canonical records. Spec 004 R2 forbids
   derived summaries. Rewrite the cell and the projection section. Specify
   the vault as a new markdown projection with a live renderer and git sync.

2. The projection resolution omits write-back. Human annotations, page
   edits, filed answers, and schema co-evolution need a governed write
   path into canonical memory (001 R9, principle 13). Add the write-back
   design. Or state the vault as read-only and justify the loss of the
   third voice.

3. The priority claim is unsupported. Provide evidence or a decision rule
   for 017-before-016. Address the risk side of the trade. Spec 016 is
   finalized. Issue #38 is ready-for-agent. Name the issue for 017 and add
   a BACKLOG marker.

4. The gap analysis is incomplete. Resolve the hierarchical index. Resolve
   the lint pass as an operation. Resolve the review queue processing
   path. A spec 012 surface is advisory and read-only. Cover log.md and
   the working set. Map open questions to a registered write policy
   (001 R9, 011 R2).

5. Scope and neutrality are unaddressed. State whether the wiki workspace
   is core or an opt-in integration (principle 15). Resolve the tension
   with spec 001 out of scope: workflow engines and prompt-management UIs.

## NON-BLOCKING SUGGESTIONS

1. Correct the citation. Confidence is spec 001 R3, not R1.
2. Consider the raw-sources layer as artifact references (001 R10).
3. Consider log.md as an episode projection.
4. Reuse the spec 011 worker for lint and for consolidation extensions.
   Merge, prune, and digest writing are worker jobs.
5. Use the MCP adapter (spec 008) as the vault's agent-facing surface.
6. Estimate 017 effort before the next round: issues, migrations, and
   conformance scenarios.

VERDICT: FAIL
SCORE: 55/100 (PASS requires 80 or more)
BLOCKING ISSUES: 1. False export claim (004 exports NDJSON canonical history, not markdown; 004 R2 forbids derived summaries). 2. Write-back path omitted (annotations, edits, filed answers need governed writes, 001 R9). 3. Unsupported priority claim (016 finalized, #38 ready-for-agent; no issue or BACKLOG entry for 017). 4. Gap analysis incomplete (index unresolved, lint as operation unresolved, surface cannot process review queue). 5. Scope and neutrality unaddressed (principle 15, 001 out of scope).
NON-BLOCKING SUGGESTIONS: 1. Fix R1 citation (confidence is R3). 2. Map raw sources to 001 R10. 3. Treat log.md as an episode projection. 4. Reuse the 011 worker for lint and digests. 5. Use the 008 MCP adapter as the vault seam. 6. Estimate 017 effort before the next round.
REVIEWER: independent — Round 1
