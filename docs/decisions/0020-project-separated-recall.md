# ADR-0020: Project-separated recall evidence

Status: accepted

Date: 2026-08-03

## Context

Project-aware ingestion stores agent-session memories in exact namespaces, but
callers still have to hand-build one retrieval request per project when they
want to compare decisions or conventions. A single request over several
namespaces would make it easier to confuse one project's evidence with
another's.

## Decision

The Python and TypeScript clients expose `recall_by_project` and
`recallByProject`. Each helper accepts project IDs, creates exactly one
retrieval request per distinct project, forces that request's namespaces filter
to `agent_session:<project-id>` (or the configured prefix), and returns a
project-keyed mapping of separate retrieval responses.

Callers may add other retrieval filters, but they may not supply their own
namespaces filter to this helper. Optional idempotency prefixes are suffixed
with the project ID so retries remain attributable to the same evidence bundle.

## Structural comparison boundary

The Python and TypeScript clients additionally expose `compare_by_project` and
`compareByProject`. These helpers first perform the isolated recalls, then
group the visible items by normalized fact key and compare canonical SHA-256
digests of their JSON values. They report exact matches, project-specific keys,
and same-key/different-value review candidates while returning the original
bundles unchanged. They also return a bounded token-Jaccard review list for
content items whose keys differ but whose visible text overlaps.

The local MCP adapter exposes the same operation as
`palimpsest_compare_by_project`. The summary is explicitly structural: it does
not infer semantic equivalence, explain a conflict, call a model, or write a
consolidated fact. The HTTP API and server retrieval policy remain unchanged.

## Consequences

Agents can compare project-specific evidence without cross-project candidate
mixing and can receive a deterministic review queue before asking a model or
human to interpret it. The durable memory model remains free of unreviewed
cross-project synthesis.
