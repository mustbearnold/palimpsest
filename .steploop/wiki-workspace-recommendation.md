# Recommendation: spec 017 — wiki workspace

Status: under review (R3).
Date: 2026-08-08.
Revision: v2.1 — R2 review FAIL 77/100. One blocker fixed. Four notes folded.
See changelog.

## The recommendation

Palimpsest shall add a wiki workspace capability (spec 017) as an opt-in,
tenant-level integration. The capability has two parts: a markdown vault
projection and a governed write-back path. 017 enters the backlog as a new
issue. It does not pre-empt issue #38. [v2.0]

## Context

The llm-wiki pattern (Karpathy, revised 2026-08-08) defines a persistent,
interlinked knowledge base. An LLM agent builds and maintains it over time.
The pattern file lives at `.hermes/desktop-attachments/llm-wiki-revision-2026-08-08.md`.

The pattern has two memory layers. The episodic layer records events with
dates. The semantic layer holds timeless concepts. A consolidation pass
promotes episodes into semantics. Provenance tags guard against hallucination.
Open questions drive future sourcing.

## The mapping [v2.0]

| Pattern element | Palimpsest element | Verdict |
| --- | --- | --- |
| Episodic layer | Episodes (001 R2) | Match |
| Semantic layer | Facts, revisions, supersede (001 R3, R5) | Match |
| Provenance tags | Provenance (001 R1); confidence (001 R3); derived vs raw (001 R8, 011 R5) | Partial |
| Status tags | Supersede (001 R5); derived (011 R5); disputed = open design item | Partial |
| Consolidation pass | Spec 011 worker derivation (011 R1-R3). Merge, prune, digest = new worker jobs | Partial |
| Query with citations | Retrieval receipts (002 R2). Result-level explanation, not claim-level citations | Partial |
| Obsidian as IDE | NEW markdown projection. NOT spec 004. Spec 004 exports NDJSON canonical history. Spec 004 R2 forbids derived summaries | Gap, new work |
| Contradiction flags | Supersede (001 R5) | Match |
| Raw sources | Artifact references (001 R10) | Match |
| log.md | Episodes (001 R2) | Match, projection |
| Open questions | Gap. Facts with a registered write policy (001 R9) | New |
| Review queue | Gap. Worker job plus last-touched temporal data | New |
| Lint pass | Gap. Worker job plus lint fact namespace | New |
| Hierarchical index | Gap. Generated read-only surface (012 R4, R6) | New |
| Schema file | Gap. Tenant-owned versioned configuration | New |
| Working set | Client concern via the MCP adapter (008). No server mechanism | Out of scope |

## The gaps and their resolutions [v2.0]

1. Open questions page. Resolution: facts in a registered namespace. Writes
   need attribution and policy (001 R9, 011 R2).
2. Review queue. Resolution: a worker job (011 R1 jobs and claims). The job
   reads last-touched dates from page frontmatter. It flags pages not touched
   in 30 days. A 012 surface informs the agent. It does not process the
   queue. Surfaces are advisory and bounded (012 R4, R6).
3. Lint pass. Resolution: an operation, not state. A periodic worker job
   checks contradictions, orphans, stale claims, and provenance gaps. It
   writes lint state to a governed fact namespace. It generates new open
   questions.
4. Hierarchical index. Resolution: a generated read-only surface (012 R4,
   R6). The server renders the catalog from the semantic layer.
5. Schema file. Resolution: tenant-owned configuration. The service stores
   it. Version changes need a governed amendment. No framework bias
   (principle 15).

## The projection resolution [v2.0]

The vault is a projection. It is a new markdown renderer with git sync. It is
not spec 004. Spec 004 exports frozen NDJSON canonical history. Spec 004 R2
forbids derived summaries. The vault renders derived pages. The new export
kind reuses the spec 004 package machinery. It adds a new projection surface.

The vault is read-only by default. Human edits do not touch the files. The
third voice survives through governed write-back:

- Annotations become attributable writes with a registered policy (001 R9,
  principle 13).
- Page edits become attributable writes with a registered policy (001 R9).
- Filed answers become agent writes with attribution.
- Schema co-evolution is a governed amendment.

Alternative considered: direct file edits with sync-back. Rejected. It
bypasses the attributable write policy.

The hot-cache analogy holds for the read side. The projection is rebuildable.
Canonical memory stays authoritative. The write-back path makes the human
voice canonical too.

## Priority and tracker [v2.0]

Evidence from the live tracker: issue #38 carries ready-for-agent. It is the
only actionable frontier item. Issue #1 is a stale umbrella from 2025. It is
not a frontier item. Spec 016 is finalized with no open questions. Backup is
risk insurance. Delay adds deployment risk.

Revised recommendation: #38 proceeds as the frontier. 017 entered BACKLOG as
scheduled with issue #46 on 2026-08-08. Draft 017 after the 016
implementation starts. No queue jump.

## Scope and neutrality [v2.0]

The wiki workspace is opt-in. A tenant may enable it. No tenant is forced
(principle 15). It is not a workflow engine and not a prompt-management UI
(001 out of scope). It is a projection and a write API. All clients may use
it or ignore it.

## Scope and effort estimate [v2.0]

Four phases:

- P1: markdown projection with renderer and git sync. New export kind.
  Conformance: vault pages rebuild from canonical records.
- P2: annotation write-back with a registered policy (001 R9).
- P3: open-questions and review-queue worker jobs.
- P4: lint job and index generation.

Each phase is a separate issue. Each phase carries named conformance
scenarios (A-* pattern).

## Alternatives

- Implement 016 first, then draft 017. CHOSEN.
- Draft 017 before 016. Rejected in v2.0. #38 is ready-for-agent. Issue #46
  exists for 017.
- Add 017 to BACKLOG only. Partial. The BACKLOG entry and issue #46 landed
  on 2026-08-08. The spec draft waits.

## Changelog

- v2.0: corrected the export claim (blocker 1). Added the write-back design
  (blocker 2). Reversed the priority call (blocker 3). Completed the gap
  resolutions (blocker 4). Added scope and neutrality (blocker 5). Folded
  non-blocking notes 1-6.
- v2.1: named issue #46 and the BACKLOG marker (blocker 1 of round 2).
  Folded notes: #1 stale umbrella stated; page edits as a write-back kind;
  export kind reuses 004 machinery; review queue due rule = 30 days.
