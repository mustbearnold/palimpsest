# 008 — Local MCP adapter

Status: active
Owner: AI CEO

## Purpose

A local Model Context Protocol adapter that gives coding agents authorized
access to Palimpsest through the HTTP API: current-memory retrieval,
per-project recall, deterministic comparison, validated review, and governed
consolidation.

## Requirements

- R1. The adapter MUST speak MCP over the HTTP API and MUST keep the
  configured tenant and subject scope; it MUST NEVER connect to PostgreSQL
  directly.
- R2. The adapter MUST expose `palimpsest_retrieve`,
  `palimpsest_recall_by_project`, `palimpsest_compare_by_project`,
  `palimpsest_validate_project_review`, `palimpsest_consolidate_project_review`,
  and `palimpsest_remember`.
- R3. The adapter MUST NOT expose delete or export operations.
- R4. Comparison tools MUST NOT infer semantic conflicts; consolidation tools
  MUST require caller-supplied values, temporal fields, and a registered write
  policy.

## Acceptance criteria

- [ ] A1. `scripts/test_palimpsest_mcp.py` passes (tool discovery, recall,
      compare, validate, consolidate, remember).
- [ ] A2. Registration instructions in the quickstart runbook work with
      `codex mcp add`; `codex mcp list` shows the adapter.

## Out of scope

- Remote or hosted MCP surfaces; provider-specific MCP servers.

## Open questions

- None.

## Links

Code: `scripts/palimpsest_mcp.py`
Tests: `scripts/test_palimpsest_mcp.py`
Decisions: 0012, 0020
