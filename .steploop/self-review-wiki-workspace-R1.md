# Self-review: wiki workspace recommendation (R1)

Reviewer: session agent.
Date: 2026-08-08.

## Verdict

The recommendation is directionally sound. The mapping is mostly accurate. The
gaps are real. Four weaknesses weaken the case. None is fatal.

Score estimate: 72/100.

## Findings

1. The priority claim is unquantified. "017 is more product value than 016" is
   an opinion, not a measure. 016 closes a real durability gap. Every day
   without PITR evidence adds deployment risk. The recommendation gives no
   decision rule for this trade-off.

2. The projection resolution has a hole: human edits. The pattern gives the
   human a role. The human adds annotations ("what you believe"). The human
   edits pages. A one-way projection loses these. The recommendation does not
   address write-back. This is the largest gap.

3. The export shape is an assumption. Spec 004 exports packages. A wiki vault
   needs per-page markdown files with frontmatter. This may require a new
   export mode. The recommendation treats this as done. It is not.

4. The size is unknown. 017 spans four gaps and a config schema. It may be
   several issues, not one. The recommendation presents it as one spec without
   bounds.

5. Tracker hygiene is unaddressed. A 017 draft first leaves #38 in draft
   state. The cost of 016 finalization is small. The recommendation does not
   weigh this cost.

## What holds

The mapping cells verify against the live spec text. Provenance and confidence
are first-class fields (001 R1). The consolidation worker exists (011). The
surface pattern exists (012). The supersede mechanism exists (001). The
pattern fits the architecture. The direction is correct.
