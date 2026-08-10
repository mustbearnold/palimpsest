# 017 — Wiki workspace

## Status

Active. Finalized 2026-08-09 (GitHub issue #46). Founder approved
2026-08-09. Review gate: R1 FAIL 80/100 (R4/R5 contradiction, AC3
mechanism); R2 PASS 93/100, no required changes. Recommendation:
APPROVED (`.steploop/wiki-workspace-recommendation.md`, v2.3, R4 PASS
94/100).

## Owner

Agent lane: palimpsest-server exports and surfaces; consolidation workers.

## Purpose

The llm-wiki pattern defines a persistent, interlinked knowledge base. An
LLM agent builds and maintains it over time. Palimpsest already hosts most
of the pattern: episodes, facts with supersede, provenance, consolidation
workers, and retrieval receipts. This spec adds the missing pieces. The
capability is an opt-in, tenant-level integration with two parts: a
markdown vault projection and a governed write-back path.

## Requirements

R1 [opt-in]. The wiki workspace MUST be opt-in per tenant. A tenant MAY
enable it. No tenant is forced. No framework-specific behavior may leak
into the core.

R2 [projection]. The markdown vault MUST be a projection of canonical
memory. The vault MUST be rebuildable. Canonical memory stays
authoritative. The projection MUST render derived pages (the semantic
layer), unlike spec 004 exports, which freeze canonical history only.

R3 [export boundary]. The vault MUST be a new export kind. The new kind
MUST reuse the spec 004 package machinery (immutable membership
manifest, materialization). The new kind MUST NOT change spec 004 exports
or the spec 004 R2 rule (canonical history, not derived summaries).

R4 [one-way]. The vault MUST be read-only by default. Git sync MUST push
canonical state into the vault. The sync MUST be push-only; it MUST NOT
provide an inbound merge path. A rebuild MUST discard any file state that
the renderer did not produce. Direct sync-back MUST be rejected. Renderer
output MUST NOT flow back into canonical memory except through
attributable writes (001 R9).

R5 [write-back]. The write-back API MUST be the only inbound path for
edits into canonical memory. Annotations, page edits, and filed answers
MUST become attributable writes with a registered policy (001 R9,
011 R2). A write without a registered policy MUST fail closed.

R6 [last-touched]. The renderer MUST write the last-touched date of each
page into its frontmatter. The dates MUST come from canonical fact
metadata. The projection MUST NOT be the authority for these dates.

R7 [review queue]. A worker job MUST flag pages not touched in 30 days.
The job MUST follow the spec 011 worker pattern (jobs and claims,
leases, crash-resumable). The queue MUST be advisory (012 R4, R6). The
surface informs the agent; it does not process the queue.

R8 [open questions]. Open questions MUST be facts with a registered
write policy (001 R9, 011 R2). The lint job MUST generate new open
questions. An answered question MUST be filed back through an
attributable write.

R9 [lint]. The lint pass MUST be an operation, not state. A periodic
worker job MUST check contradictions, orphans, stale claims, and
provenance gaps. The job MUST write lint state to a governed fact
namespace. The job MUST generate new open questions.

R10 [index]. The hierarchical index MUST be a generated read-only
surface (012 R4, R6). The server MUST render the catalog from the
semantic layer. The surface MUST be bounded (item and token caps),
idempotent per request, and content-free in logs.

R11 [schema]. The schema configuration MUST be tenant-owned and
versioned. A schema amendment MUST be governed. The schema MUST NOT
impose a framework bias.

## Acceptance criteria

AC1 — vault rebuild.
Given a tenant with the wiki workspace enabled
when the vault renders from canonical records
then every vault page rebuilds byte for byte from the canonical layer
and the manifest is immutable.

AC2 — export boundary.
Given the new vault export kind
when the suite compares it with spec 004 canonical history
then the vault kind renders derived pages
and the 004 packages and their digests do not change.

AC3 — one-way sync.
Given a rendered vault with push-only git sync
when the suite simulates a direct sync-back
then the sync-back is rejected (no inbound merge path)
and no renderer output enters canonical memory.

AC4 — attributed write-back.
Given a registered write policy
when a principal writes an annotation or a page edit through the
write-back API
then the write is attributable (001 R9)
and a write without a policy fails closed.

AC5 — filed answers.
Given an agent answer filed through write-back
when the suite verifies the receipt
then the answer is an agent write with attribution
and the receipt records the filing agent as writer and the
provenance kind derived (011 R5).

AC6 — review queue.
Given a page not touched in 30 days
when the review-queue job runs
then the job flags the page in an advisory surface
and the job leaves the canonical layer unchanged.

AC7 — open questions.
Given an open question fact with a registered policy
when the suite answers it through an attributable write
then the old question fact is superseded
and the answer page is linked.

AC8 — lint state.
Given a contradiction between two facts
when the lint job runs
then the job writes a governed lint fact
and the job generates a new open question.

AC9 — index surface.
Given a semantic layer with facts
when the index renders
then the catalog lists every page with a link and a summary
and the surface is bounded and idempotent (012 R6).

AC10 — schema versioning.
Given a tenant schema configuration
when the tenant amends the schema
then the amendment is governed
and the old version stays retrievable.

## Out of scope

- The working set (client concern via the MCP adapter, spec 008).
- Obsidian plugins, graph views, or client-side tooling.
- Direct file edits with sync-back (rejected; they bypass 001 R9).
- Embedding-based RAG infrastructure.
- A workflow engine or a prompt-management UI (001 out of scope).
- External providers (opt-in integrations only, as in 011).

## Phases

The capability lands in four phases. Each phase is a separate issue. Each
phase carries its named conformance scenarios (AC1..AC10 above).

- P1: markdown projection with renderer and git sync. AC1, AC2, AC3.
- P2: annotation write-back with a registered policy. AC4, AC5.
- P3: open questions and review-queue worker jobs. AC6, AC7.
- P4: lint job and index generation. AC8, AC9, AC10.

## Resolved questions

1. Is the vault a spec 004 export? No. Spec 004 freezes canonical history
   as JSON or NDJSON (ADR-0008), and its R2 forbids derived summaries.
   The vault renders derived pages. The new export kind reuses the 004
   package machinery only.
2. How do human edits reach canonical memory? Only through the write-back
   API. Direct sync-back is rejected. The third voice survives through
   governed write-back.
3. What is the review due rule? 30 days without a touch. The last-touched
   dates come from canonical fact metadata, never from the projection.
4. Is the lint pass state? No. It is a periodic worker operation. Lint
   findings are governed facts, and the job generates new open questions.
5. What is the index? A generated, read-only, advisory surface (012 R4,
   R6), rendered by the server from the semantic layer.
6. Who owns the schema? The tenant. Version changes need a governed
   amendment.
7. Which pages are editable through write-back? Fact pages only: a page
   edit is an attributable supersede of the canonical fact behind the
   page, preserving the page's evidence grounding. Episode pages are
   read-only because the raw episode layer is never rewritten (001 R8).
   Implementation notes: annotations land in the `wiki/annotations`
   namespace as facts grounded in the annotated page's evidence; filed
   answers land in the `derived` namespace (provenance kind derived,
   011 R5) with the filing agent recorded as writer (AC4, AC5 — landed
   2026-08-10).

## Open questions

- [V-1] Git sync transport. The recommendation names git sync. The first
  implementation MAY use an operator script (the spec 016 D2 pattern). A
  worker-based sync is a later option. Verified at P1 execution.
- [V-2] Frontmatter vocabulary. The renderer writes last-touched dates and
  status tags. The exact frontmatter fields are an execution detail.
  Verified at P1 execution.

## Links

Pattern: `.hermes/desktop-attachments/llm-wiki-revision-2026-08-08.md`
Recommendation: `.steploop/wiki-workspace-recommendation.md`
Issue: #46
Specs: 001 (memory service, R9, R10) · 002 (retrieval receipts) · 004
(export operations) · 008 (MCP adapter) · 011 (consolidation worker) · 012
(proactive surfacing, R4, R6)
