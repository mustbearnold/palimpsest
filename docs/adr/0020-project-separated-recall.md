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

## Consequences

Agents can compare project-specific evidence without cross-project candidate
mixing. The helper does not summarize, rank, or invent a semantic difference;
that comparison remains an explicit caller/model operation over the returned
evidence. The HTTP API and server retrieval policy remain unchanged.
