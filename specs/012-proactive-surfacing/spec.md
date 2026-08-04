# 012 — Proactive surfacing

Status: draft
Owner: AI CEO

## Purpose

Authorized, timely surfacing of relevant memories to agents and hosts —
without the agent having to know to query. Surfacing is pull-with-context
(a host asks "what is relevant to this session?") with an optional
host-initiated push extension; it is never ambient surveillance.

## Requirements

- R1. Surfacing MUST apply authorization, deletion, retention, sensitivity,
  and temporal filters before any relevance ranking (constitution invariant:
  filters before ranking, always).
- R2. The surfacing seam MUST be an explicit API and MCP surface: given a
  bounded current-context digest (tenant, principal, project/thread
  identifiers, optional lexical or embedding context), it MUST return a
  bounded, explained bundle of relevant memories with receipts, exactly like
  the recall tools.
- R3. Surfacing MUST be opt-in: the default configuration surfaces nothing;
  enabling requires explicit host and principal configuration.
- R4. Surface results MUST be advisory: they inform the agent, never
  override or impersonate agent decisions.
- R5. Context digests MUST be content-bounded and privacy-preserving: hosts
  send identifiers and bounded context, not full transcripts, unless policy
  explicitly allows more.
- R6. Bundles MUST be bounded (item and token caps), content-free in logs,
  and idempotent per request semantics; surfacing MUST NOT weaken the
  pull-based retrieval contract.
- R7. Host-initiated push (delivery of relevant memories on session start or
  change) is a MAY extension behind the same authorization and opt-in
  requirements; it MUST be disableable per host and per principal.

## Acceptance criteria

- [ ] A1. Tenant isolation: surfacing for tenant A never returns tenant B
      memories (scenario test at the API seam).
- [ ] A2. Boundedness: response caps are enforced; receipts explain inclusion.
- [ ] A3. Opt-in: with default configuration, surfacing returns nothing.
- [ ] A4. Deletion: bundles never include fenced or purged subjects' content.
- [ ] A5. MCP integration: a session-start surface tool returns the same
      receipt shape as the recall tools and passes the MCP test suite.
- [ ] A6. Authorization revocation: a revoked principal gets empty bundles,
      not errors that leak existence.

## Out of scope

- Ambient session surveillance, unauthenticated push, or overriding agent
  decision-making.
- Model-driven summarization of what to surface (that is spec 011's
  boundary); surfacing ranks existing canonical records.

## Open questions

- Host integration points (Hermes, Codex, Claude Code session-start hooks)
  and which come first.
- Push vs. poll tradeoffs and cadence for the MAY extension.
- Whether session-start context should include recent episode text by
  default under a bounded policy.

## Links

Code: `crates/palimpsest-server` (API surface) · `tools/palimpsest_mcp.py`
Tests: `tools/test_palimpsest_mcp.py` · `conformance_postgres18.rs`
Specs: 002 (authorized retrieval) · 008 (MCP adapter) · 011 (consolidation)
Decisions: 0005 (authorization-first receipts), 0020 (project-separated
recall)
