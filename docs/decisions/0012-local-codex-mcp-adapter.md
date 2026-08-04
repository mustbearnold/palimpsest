# ADR-0012: Local Codex MCP adapter

## Status

Accepted for the local development integration.

## Context

Palimpsest's canonical public seam is the versioned HTTP/OpenAPI service. Codex can launch local MCP servers over stdio, but Palimpsest did not yet have an adapter that let a Codex session use its authorized memory scope. Making the database or MCP protocol canonical would couple storage to one host and bypass the existing HTTP authorization boundary.

## Decision

Add `scripts/palimpsest_mcp.py` as a thin, local stdio MCP adapter. It talks to the running Palimpsest HTTP service with the configured bearer token, tenant, subject, and case identifiers. It exposes six tools:

- `palimpsest_retrieve` creates an authorized current retrieval receipt and returns its visible fact items.
- `palimpsest_recall_by_project` creates one authorized retrieval receipt per named project and returns project-keyed evidence bundles. It requires at least two distinct project IDs, owns the exact project namespaces, and does not claim to infer a semantic diff or consolidate conflicts.
- `palimpsest_compare_by_project` performs the same isolated retrieval and adds deterministic exact-key/value-digest classifications. A same-key, different-value result is a review candidate only; the adapter performs no model inference and no durable write.
- `palimpsest_validate_project_review` validates a caller-supplied semantic review against the fact/revision and source-episode IDs in a prior authorized comparison result. It is a local, non-writing client-side validator: it does not access the database, widen scope, or replace the HTTP contract.
- `palimpsest_consolidate_project_review` performs explicitly approved, per-claim governed fact writes for a validated review. The adapter derives episode lineage from the validated claims and delegates authorization and write-policy enforcement to the HTTP service; it does not infer semantic truth or provide atomic batch behavior.
- `palimpsest_remember` appends an immutable episode, then creates a governed `direct-evidence` fact that cites that episode. The operation is available to Codex but its tool description requires explicit user approval before a write.

The adapter has no database credentials, no direct database access, no delete or export tool, and no authority to widen the server's tenant, subject, or sensitivity scope. It returns only MCP tool content and does not log memory payloads. The HTTP API remains the contract; future remote MCP support must add the protocol's authorization and resource metadata rather than reuse this local synthetic-token composition.

The development launcher uses Docker Compose when available and otherwise can use a user-owned PostgreSQL cluster with the pinned PostgreSQL 18.4 and pgvector 0.8.5 runtime. The fallback remains local-only and does not touch the system PostgreSQL service or the existing Kaneo cluster.

## Consequences

Codex can use Palimpsest after the local service is started and the MCP server is registered with `codex mcp add`. A write is intentionally two HTTP transactions, so an episode can remain as durable evidence if fact promotion fails; the adapter reports that partial outcome instead of hiding it. Fact supersession, temporal as-of retrieval, export, deletion, remote OAuth, and production credential management remain outside this first local adapter.
