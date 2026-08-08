# Review brief: wiki workspace recommendation (R1)

You are the independent reviewer. Your task: evaluate the recommendation in
`/home/mustbearn/Projects/Palimpsest/.steploop/wiki-workspace-recommendation.md`.

The recommendation proposes a new Palimpsest capability (spec 017, wiki
workspace) based on the llm-wiki pattern.

## Read first

- `/home/mustbearn/Projects/Palimpsest/.steploop/wiki-workspace-recommendation.md`
- `/home/mustbearn/Projects/Palimpsest/.hermes/desktop-attachments/llm-wiki-revision-2026-08-08.md`

Read both files in full. Use read_file. Do not skip sections.

## Verify against the live source

The repo is `/home/mustbearn/Projects/Palimpsest`. Check each claim:

- `specs/001-memory-service/spec.md` — episodes, facts, provenance R1, supersede.
- `specs/002-authorized-retrieval/spec.md` — retrieval receipts.
- `specs/004-export-operations/spec.md` — export artifact shape.
- `specs/011-governed-consolidation/spec.md` — consolidation worker.
- `specs/012-proactive-surfacing/spec.md` — surface pattern.
- `specs/015-hot-cache/spec.md` — cache rule.
- `specs/016-backup-and-pitr/spec.md` — current state.
- `specs/BACKLOG.md` — scheduled items.
- GitHub issues: run `gh issue list --state open`.

## Evaluate five dimensions

1. Factual accuracy of the mapping table. Check each cell against the spec text.
2. Soundness of the gap analysis. Are the gaps real? Are any gaps missing?
3. Soundness of the projection resolution. Does the vault-as-projection lose
   anything? Is the hot-cache analogy valid?
4. Quality of the priority argument. Is "017 before 016" justified?
5. Completeness. What did the recommendation miss?

## Rules

- You may not request clarification.
- Approving a weak recommendation is a failure, not a kindness.
- A rejection with concrete fixes is the most valuable outcome.
- Recompute and verify. Do not trust the document numbers.

## Deliverable

Write the full review to
`/home/mustbearn/Projects/Palimpsest/.steploop/REVIEW-wiki-workspace-R1.md`.

End your chat reply with this block, verbatim:

```
VERDICT: PASS | FAIL
SCORE: N/100 (PASS requires 80 or more)
BLOCKING ISSUES: numbered list. Empty if PASS.
NON-BLOCKING SUGGESTIONS: numbered list.
REVIEWER: independent — Round 1
```

Chat response cap: 3 KB. The file may be longer.
