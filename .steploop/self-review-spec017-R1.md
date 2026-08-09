# Self-review — spec 017 (wiki workspace), Round 1

Author: AI CEO (prime agent). Date: 2026-08-09.

## Checks performed

1. House format: Status / Owner / Purpose / Requirements / Acceptance
   criteria / Out of scope / Phases / Resolved questions / Open questions
   / Links. Matches spec 004 and spec 018.
2. Requirements R1-R11 use MUST/MAY semantics. Each references its source
   (001 R9, 004 R2, 011, 012 R4/R6).
3. Acceptance criteria AC1-AC10 use Given/when/then. Each AC maps to at
   least one requirement. Counts: 11 requirements, 10 ACs, 6 resolved
   questions, 2 V-n markers.
4. Recommendation coverage: all five gap resolutions appear (open
   questions R8/AC7, review queue R7/AC6, lint R9/AC8, index R10/AC9,
   schema R11/AC10). The projection resolution appears (R2-R4, AC1-AC3).
   The write-back resolution appears (R5, AC4-AC5). Phase mapping matches
   the recommendation (P1-P4).
5. Live citations verified: spec 004 R2 ("canonical history with
   provenance, not derived summaries"), spec 012 R4 (advisory surfaces)
   and R6 (bounded, idempotent), spec 001 R9 (attributable writes) and
   R10 (artifact references).
6. STE100 scan: no "etc.", no slang. Longest sentences are within the
   20-word bound except the Purpose preamble list, which is a list not a
   sentence.
7. No claims about dates, numbers, or tracker state that need live
   recomputation. The only tracker reference (issue #46) is verified.

## Open risks

- The git sync transport is deliberately open ([V-1]). An operator
  script is the default per spec 016 D2.
- The frontmatter vocabulary is open ([V-2]). Execution detail.
- The review gate should verify the AC/R mapping and the export-boundary
  claim against spec 004 R2.

## Verdict

PASS (self). Expected reviewer score: 90-95.
