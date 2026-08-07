# 012 — Proactive surfacing

Status: active
Owner: AI CEO

## Purpose

Authorized, timely surfacing of relevant memories to agents and hosts, without the agent needing to query. Surfacing is pull-with-context. A host asks "what is relevant to this session?" and receives a bounded, explained bundle. It is never ambient surveillance.

This is the final step of the autonomous memory lifecycle. Spec 006 ingests raw episodes automatically. Spec 011 turns them into governed facts automatically. This spec surfaces those memories proactively. The explicit review flow (spec 007) remains the conservative default.

## Decisions (2026-08-07 finalization)

- D1. The seam is a synchronous read operation. A surface request returns the bundle in its response. It does not create a governed job. The recall contract is its model. The service stores the response for idempotent replay, exactly like the recall contract. The push extension (R7) stays a MAY. When implemented, it reuses the governed-job machinery of spec 011: durable jobs, claims, leases, and crash resume.
- D2. Surfacing is opt-in through a registered surface policy. `memory.surface_policies` holds one row per (tenant, host, principal). The row carries: enabled flag, item cap, context-token cap, result-token cap, optional sensitivity ceiling, optional temporal window. No row means an empty bundle (fail closed). The registry follows the consolidation policy pattern: RLS FORCE, scope GUCs, registration routes. Both the sync seam and the future push path read the same registry.
- D3. Ranking is lexical-first. Embeddings are optional. The surface applies the authorized-retrieval filter pipeline first: authorization, deletion, retention, sensitivity, temporal. Ranking then uses the lexical policy `retrieval-lexical-v1` over the context terms. A registered embedding provider enables vector candidates. Lexical-only surfacing stays fully functional without one. No default provider exists (neutrality principle 15).
- D4. Session-start context contains identifiers and explicit terms only, by default. A host sends tenant, principal, and project/thread identifiers, plus optional bounded lexical terms. Recent episode text is not included by default. A host may send bounded derived terms under its own policy. This honors the content-bound rule (R5).
- D5. The seam is transport-agnostic. Embedded mode (spec 014) reuses the same domain seam. No framework-specific channel exists (principle 15). The MCP tool comes first. Host-specific session-start hooks (Hermes, Codex, Claude Code) are follow-on integrations, Hermes first.
- D6. Surface metrics are content-free. The metrics surface version bumps with this change (spec 010 R3).

## Requirements

- R1. Surfacing MUST apply authorization, deletion, retention, sensitivity, and temporal filters before any relevance ranking (constitution invariant).
- R2. The surfacing seam MUST be an explicit API and MCP surface. Given a bounded current-context digest, it MUST return a bounded, explained bundle with receipts. The digest carries tenant, principal, and project/thread identifiers, plus optional lexical or embedding context. The receipt shape matches the recall tools.
- R3. Surfacing MUST be opt-in. The default configuration surfaces nothing. A registered surface policy is the only way to enable it.
- R4. Surface results MUST be advisory. They inform the agent. They never override or impersonate agent decisions.
- R5. Context digests MUST be content-bounded and privacy-preserving. Hosts send identifiers and bounded context, not full transcripts, unless policy explicitly allows more.
- R6. Bundles MUST be bounded (item and token caps), content-free in logs, and idempotent per request. Surfacing MUST NOT weaken the pull-based retrieval contract.
- R7. Host-initiated push is a MAY extension (delivery of relevant memories on session start or change). It sits behind the same authorization and opt-in requirements. It MUST be disableable per host and per principal. When implemented, it reuses the spec 011 job machinery.

## Design (v1)

### Policy registry

Migration 0023 adds `memory.surface_policies`. One row per (tenant_id, host_id, principal_id). Columns: enabled, max_items, max_context_tokens, max_result_tokens, optional sensitivity ceiling, optional temporal window, audit columns. RLS FORCE and scope GUCs apply. Registration routes follow the consolidation policy pattern.

### Surface operation

A surface request carries a bounded context digest (identifiers plus optional lexical terms). The service looks up the policy. A missing policy yields an empty bundle. The service then applies the filter pipeline: authorization, deletion, retention, sensitivity, temporal. Ranking follows: lexical over the context terms, or vector candidates when a provider is registered. Caps bound the bundle. Each item carries an explanation of its inclusion and a receipt. The service stores the response for idempotent replay.

### API surface

- POST /v1/tenants/{tenant_id}/surface-policies (register a policy)
- GET /v1/tenants/{tenant_id}/surface-policies/{host_id}/{principal_id}
- POST /v1/tenants/{tenant_id}/subjects/{subject_id}/surfaces (requires Idempotency-Key; bounded digest in; bundle out; key reuse with a different body returns 409)
- GET /v1/tenants/{tenant_id}/subjects/{subject_id}/surfaces/{surface_id}

### MCP surface

The MCP adapter adds a `surface` tool. It returns the same receipt shape as the recall tools. Tool-level caps apply. Logs carry no content.

### Metrics

Content-free counters: requests, items surfaced, caps applied, policy absence. The metrics surface version bumps with this change.

## Acceptance criteria

- [x] A1. Tenant isolation: a surface for tenant A never returns tenant B content (scenario `verify_surface_tenant_isolation`).
- [x] A2. Boundedness: the response obeys the caps; receipts explain inclusion (scenario `verify_surface_caps_and_explained_bundle`).
- [x] A3. Opt-in: with no registered policy, the surface returns an empty bundle (scenario `verify_surface_default_empty`).
- [x] A4. Deletion: bundles never include fenced or purged subjects' content (scenario `verify_surface_respects_fence_and_purge`).
- [x] A5. MCP integration: the `surface` tool returns the recall receipt shape and passes the MCP test suite.
- [x] A6. Authorization revocation: a revoked principal gets an empty bundle, not an error that leaks existence (scenario `verify_surface_revoked_principal_empty`).
- [x] A7. Filters before ranking: sensitivity ceiling and temporal window exclude content before ranking; ranking never surfaces filtered content (scenario `verify_surface_filters_before_ranking`).
- [x] A8. Idempotency: the same key and body return the same bundle (scenario `verify_surface_idempotent_replay`). A different body with the same key returns 409.

## Out of scope

- Push delivery (R7). A later spec defines the MAY extension on the spec 011 machinery.
- Host-specific session-start hooks. Follow-on work; Hermes first.
- Model-driven summarization of what to surface. That is spec 011's boundary. Surfacing ranks existing canonical records.
- Ambient session surveillance, unauthenticated push, or agent decision override.
- A default embedding provider. None exists (neutrality).

## Links

Code: `crates/palimpsest-application` (surface service) · `crates/palimpsest-postgres` (policy and ranking) · `crates/palimpsest-http` (routes) · `tools/palimpsest_mcp.py` (MCP tool)
Tests: `conformance_postgres18.rs` (surface scenarios) · `tools/test_palimpsest_mcp.py`
Specs: 002 (authorized retrieval) · 006 (ingestion) · 008 (MCP adapter) · 011 (consolidation) · 014 (embedded mode)
Decisions: 0005 (authorization-first receipts) · 0020 (project-separated recall) · 0028 (governed consolidation semantics) · 0030 (Hermes memory provider plugin)
Backlog: #45
