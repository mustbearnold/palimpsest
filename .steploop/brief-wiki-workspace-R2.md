# Review brief: wiki workspace recommendation (R2)

You are the independent reviewer, Round 2 (retry). Round 1 verdict: FAIL,
55/100. The recommendation was revised to v2.0. Your task: verify the fixes,
then re-review the entire document for new defects.

## Read first

- `/home/mustbearn/Projects/Palimpsest/.steploop/wiki-workspace-recommendation.md` (v2.0)
- `/home/mustbearn/Projects/Palimpsest/.hermes/desktop-attachments/llm-wiki-revision-2026-08-08.md`

Read both files in full. Use read_file. Do not skip sections. The document
header carries the v2.0 revision mark. The changelog tail lists the fixes.

## Round 1 blockers, verbatim

1. The mapping cell "Obsidian as IDE to markdown export (spec 004)" is false.
   Export materializes NDJSON canonical records. Spec 004 R2 forbids derived
   summaries. Rewrite the cell and the projection section. Specify the vault
   as a new markdown projection with a live renderer and git sync.
2. The projection resolution omits write-back. Human annotations, page
   edits, filed answers, and schema co-evolution need a governed write path
   into canonical memory (001 R9, principle 13). Add the write-back design.
   Or state the vault as read-only and justify the loss of the third voice.
3. The priority claim is unsupported. Provide evidence or a decision rule
   for 017-before-016. Address the risk side of the trade. Spec 016 is
   finalized. Issue #38 is ready-for-agent. Name the issue for 017 and add
   a BACKLOG marker.
4. The gap analysis is incomplete. Resolve the hierarchical index. Resolve
   the lint pass as an operation. Resolve the review queue processing path.
   A spec 012 surface is advisory and read-only. Cover log.md and the
   working set. Map open questions to a registered write policy (001 R9,
   011 R2).
5. Scope and neutrality are unaddressed. State whether the wiki workspace is
   core or an opt-in integration (principle 15). Resolve the tension with
   spec 001 out of scope: workflow engines and prompt-management UIs.

## Resolution map (blocker to section)

1. Blocker 1 -> mapping table cell "Obsidian as IDE" and the projection
   resolution section (both v2.0).
2. Blocker 2 -> projection resolution, write-back list and alternative
   (v2.0).
3. Blocker 3 -> priority and tracker section (v2.0).
4. Blocker 4 -> gaps and resolutions section, items 1-5 (v2.0).
5. Blocker 5 -> scope and neutrality section (v2.0).

Non-blocking folds: confidence citation corrected to 001 R3; raw sources
mapped to 001 R10; log.md mapped to episodes; lint and review queue reuse
the 011 worker; MCP adapter (008) named as the client seam; effort estimate
added as four phases.

## Verify against the live source

The repo is `/home/mustbearn/Projects/Palimpsest`. Check the claims that
changed in v2.0:

- `specs/001-memory-service/spec.md` — R1, R2, R3, R5, R8, R9, R10.
- `specs/002-authorized-retrieval/spec.md` — R2 receipt scope.
- `specs/004-export-operations/spec.md` — R2 derived-summary ban.
- `specs/011-governed-consolidation/spec.md` — R1-R5.
- `specs/012-proactive-surfacing/spec.md` — R4, R6.
- `specs/constitution.md` — principles 13 and 15.
- GitHub issues: run `gh issue list --state open`. Confirm the #38 label
  state.

## Rules

- Verify the fixes against the actual text. Do NOT trust this map.
- Do NOT re-litigate fixed items unless the fix is defective. If defective,
  cite the failing clause.
- Hunt for NEW defects introduced by the fixes.
- You may not request clarification.
- Approving a weak recommendation is a failure, not a kindness.
- Recompute and verify. Do not trust the document numbers.

## Deliverable

Write the full review to
`/home/mustbearn/Projects/Palimpsest/.steploop/REVIEW-wiki-workspace-R2.md`.

End your chat reply with this block, verbatim:

```
VERDICT: PASS | FAIL
SCORE: N/100 (PASS requires 80 or more)
BLOCKING ISSUES: numbered list. Empty if PASS.
NON-BLOCKING SUGGESTIONS: numbered list.
REVIEWER: independent — Round 2
```

Chat response cap: 3 KB. The file may be longer.
