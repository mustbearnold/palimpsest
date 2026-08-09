# Review brief: spec 017 wiki workspace (R1)

You are the independent reviewer. Evaluate the spec draft at
`/home/mustbearn/Projects/Palimpsest/specs/017-wiki-workspace/spec.md`.

The spec implements the APPROVED recommendation at
`/home/mustbearn/Projects/Palimpsest/.steploop/wiki-workspace-recommendation.md`
(v2.3, R4 PASS 94/100). The capability source is the llm-wiki pattern at
`/home/mustbearn/Projects/Palimpsest/.hermes/desktop-attachments/llm-wiki-revision-2026-08-08.md`.

## Read first

Read all three files in full. Use read_file. Do not skip sections.

## Verify against the live source

The repo is `/home/mustbearn/Projects/Palimpsest`. Check each claim:

- `specs/001-memory-service/spec.md` — R9 (attributable writes) and R10.
- `specs/004-export-operations/spec.md` — R2 (canonical history, not
  derived summaries). Verify the export-boundary claim.
- `specs/011-governed-consolidation/spec.md` — worker pattern (jobs and
  claims, leases, crash-resume).
- `specs/012-proactive-surfacing/spec.md` — R4 (advisory) and R6
  (bounded, idempotent).
- `gh issue view 46` — the tracking issue. Every intended-scope bullet
  must appear in the spec.

## Evaluate five dimensions

1. Faithfulness. Every resolution in the recommendation must appear in
   the spec (5 gap resolutions + the projection resolution + write-back).
   Quote any drift.
2. Testability. Each AC (AC1-AC10) must be Given/when/then with an
   observable outcome. Flag vague ACs.
3. Citation accuracy. Quote the actual requirement text for every
   citation. A citation that does not mean what the spec says is a
   blocker.
4. Internal consistency. Check one-way sync vs write-back, advisory
   surfaces vs processing, opt-in vs mandatory, phase mapping vs the
   recommendation.
5. Completeness. What does the issue #46 scope demand that the spec
   misses? Is the [V-n] deferral honest?

## Rules

- READ-ONLY: write ONLY your verdict file. Edit nothing else.
- Adversarial check (mandatory): AC3 says a simulated direct sync-back
  "is rejected". What does "rejected" mean at the mechanism level?
  State whether the spec must name a mechanism now or whether [V-n]
  deferral is honest.
- STE100: sentences at most 20 words, no "etc.", no gerund forms.

## Verdict contract

Write `.steploop/REVIEW-spec017-R1.md` with:
- Verdict PASS or FAIL. A wrong citation, an internal contradiction, or
  a missing recommendation resolution is a FAIL.
- Score /100 (90-100 landable; 75-89 minor gaps; <75 material; <55
  fatal).
- Verified-against file list.
- Strengths (2-4 bullets), weaknesses (2-4 bullets), required changes
  (numbered, path:line where possible).

End your reply with exactly `VERDICT: PASS` or `VERDICT: FAIL`.

Reply cap: 3 KB. Verdict file cap: 4 KB.
