# Review: spec 017 wiki workspace (R1)

Verdict: FAIL.
Score: 80/100.

## Verified against

- specs/017-wiki-workspace/spec.md (draft)
- .steploop/wiki-workspace-recommendation.md (v2.3, R4 PASS 94/100)
- .hermes/desktop-attachments/llm-wiki-revision-2026-08-08.md
- specs/001-memory-service/spec.md (R9, R10)
- specs/004-export-operations/spec.md (R2)
- specs/011-governed-consolidation/spec.md (R1-R6)
- specs/012-proactive-surfacing/spec.md (R4, R6)
- GitHub issue #46

## Strengths

- All five gap resolutions, the projection resolution, and write-back
  appear. Phase mapping matches the recommendation.
- Every citation means what the spec says. I verified 001 R9, 004 R2,
  011 R1-R5, 012 R4, and 012 R6 against the live specs.
- Issue #46 scope is complete. All seven bullets have a requirement and
  an acceptance criterion.
- AC1, AC2, and AC4-AC10 are given/when/then with observable outcomes.

## Weaknesses

- R5 contradicts R4. Git sync pushes into the vault, yet R5 names the
  write-back API as the only write path into the vault.
- AC3 names no mechanism for "rejected". The suite cannot simulate a
  rejection it cannot observe.
- The NDJSON claim traces to ADR-0008, not to 004 requirement text. The
  recommendation carries the same claim, so this is minor.
- R8 drops the 011 R2 citation that issue #46 and the recommendation
  pair with 001 R9.

## Adversarial check (AC3)

The [V-1] deferral is honest for the transport. It is not honest for the
direction. The spec must name the mechanism now. The sync path is
push-only, with no inbound merge. A rebuild discards any file state the
renderer did not produce. Then "rejected" is observable. The raw
material exists (R2 rebuildable, R4 one-way, out-of-scope bullet). Add
one sentence to R4 or [V-1].

## Required changes

1. spec.md:43 — Rewrite R5. The write-back API writes canonical facts.
   It does not write vault files. State: the write-back API is the only
   inbound path for external edits. Renderer output must not flow back
   into canonical memory except through attributable writes.
2. spec.md:38 or 177 — Name the rejection mechanism in R4 or [V-1].
   Push-only sync, no inbound merge path, rebuild discards foreign file
   state.
3. spec.md:159 — Cite ADR-0008 for the NDJSON format claim. 004 R2 does
   not name a format.
4. spec.md:57 — Pair 011 R2 with 001 R9 in R8, as issue #46 does.
