# ADR-0026: Attributable semantic project review

Status: accepted

Date: 2026-08-03

## Context

Structural project comparison can identify exact matches, project-specific facts, same-key value changes, and lexical review candidates, but it cannot decide whether two differently worded memories mean the same thing or conflict. That decision is useful for an agent, yet model output must not silently become canonical memory. A semantic reviewer also needs enough provenance to explain which authorized evidence supported its claim.

## Decision

Add `validate_project_review` to the dependency-free Python and TypeScript clients and to the local MCP adapter. It accepts a prior `compare_by_project` result and caller-supplied review claims. Every claim must:

- carry a unique bounded `claim_id`, a bounded human/model `summary`, and a numeric confidence from 0 through 1;
- name at least two projects from the comparison;
- use a closed classification set (`same_meaning`, `semantic_difference`, `semantic_conflict`, `rekeyed_equivalent`, or `insufficient_evidence`);
- cite returned fact and revision identifiers from each named project; and
- cite source episode identifiers that were present on those returned items.

The review is bounded to at most 100 claims and 20 evidence citations per claim. Claim IDs and project/evidence lists are unique. Canonical value digests come from the structural comparison result that the review cites; clients normalize integral and negative-zero JSON numbers before hashing newly produced structural comparisons. The validator recomputes that structural comparison from the supplied bundles and rejects a mismatched comparison. It does not independently authorize those bundles: callers must obtain them from the authorized `compare_by_project` operation.

The review also carries a reviewer principal, provider/model/revision metadata, a prompt digest, and a versioned review-policy digest. A `semantic_conflict` claim must cite at least two distinct canonical values. The validator returns a normalized, provenance-rich review, but performs no model call, does not decide semantic truth, and never writes memory. Explicit promotion of selected claims is defined separately by [ADR-0028](0028-governed-project-review-consolidation.md); it uses the cited source episodes and a durable write policy.

## Consequences

Agents can ask a model or human to interpret the isolated project evidence and then have Palimpsest reject ungrounded or unattributed claims before they are used by the explicit, per-claim consolidation workflow. The interface is provider-neutral and stores no prompt or private transcript, only its digest and bounded claim metadata. Palimpsest still does not provide automatic semantic understanding or raw-session consolidation; the external reviewer remains responsible for the interpretation, and the durable MemoryService remains responsible for authorization and write policy.
