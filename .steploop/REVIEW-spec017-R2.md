# Review: spec 017 wiki workspace (R2)

Verdict: PASS.
Score: 93/100.

## Verified against

- specs/017-wiki-workspace/spec.md (current draft, live read)
- .steploop/wiki-workspace-recommendation.md (v2.3, R4 PASS 94/100)
- docs/decisions/0008-durable-export-and-scoped-deletion.md (line 77)
- specs/001-memory-service/spec.md (R9)
- specs/004-export-operations/spec.md (R2)
- specs/011-governed-consolidation/spec.md (R1, R2, R5)
- specs/012-proactive-surfacing/spec.md (R4, R6)
- GitHub issue #46

## Fix verification (R1 findings)

1. R5 (spec.md:45) says "the only inbound path for edits into
   canonical memory". The write-back API writes canonical facts, not
   vault files. R4 owns the outbound path (spec.md:38-43). R5 owns the
   inbound path. The contradiction is gone.
2. R4 names the mechanism (spec.md:39-41): push-only sync, no inbound
   merge path, rebuild discards non-renderer state. AC3 states the
   observable outcome (spec.md:92-96): "rejected (no inbound merge
   path)".
3. Resolved question 1 cites ADR-0008 (spec.md:162-163). ADR-0008
   line 77 confirms "UTF-8 JSON or NDJSON". The claim has a source.
4. R8 (spec.md:59-60) pairs 011 R2 with 001 R9. Issue #46 asks for
   the same pair.

## Strengths

- The R4/R5 ownership split is clean and testable. No new
  contradiction appears between R4, R5, AC3, and the out-of-scope
  bullet on direct file edits.
- AC3 is observable. No inbound merge path exists. A simulated
  sync-back fails. Canonical memory stays unchanged.
- All R1 citations still verify against the live specs (001 R9,
  004 R2, 011 R1-R5, 012 R4, R6).
- Every issue #46 scope bullet maps to a requirement and an
  acceptance criterion.

## Weaknesses

- Minor: resolved question 1 says "as NDJSON". ADR-0008 permits
  "UTF-8 JSON or NDJSON". The recommendation and issue #46 say NDJSON,
  so the narrower claim is safe.
- Minor: AC5 keeps provenance kind "derived" for filed answers.
  011 R5 names three kinds (raw episode, derived, externally
  reviewed). A filed answer is an agent write with attribution. One
  sentence would settle the kind.

## Required changes

None. The spec is landable. The two weaknesses are wording nits for
the author to fold at implementation time.

1. Optional: spec.md:162 — "as JSON or NDJSON (ADR-0008)".
2. Optional: spec.md:105 — settle the provenance kind for filed
   answers, or cite the 011 R5 kind list.
