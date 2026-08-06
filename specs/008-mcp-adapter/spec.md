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
- R5. The adapter MUST be distributable as a standard Python package with a
  console entry point (`palimpsest-mcp`), installable by any MCP-capable
  client without vendor-specific registration; packaging MUST NOT change the
  adapter's MCP-over-HTTP behavior or add database access.
- R6. The adapter MUST remain transport-neutral to clients: any MCP client
  (codex, Claude, Cursor, or other) can register it with the same documented
  steps; no client-specific code paths.

## Acceptance criteria

- [ ] A1. `tools/test_palimpsest_mcp.py` passes (tool discovery, recall,
      compare, validate, consolidate, remember).
- [ ] A2. Registration instructions in the quickstart runbook work with
      `codex mcp add`; `codex mcp list` shows the adapter.
- [ ] A3. `python -m pip install tools/` installs the `palimpsest-mcp` console
      entry point; `palimpsest-mcp --help` runs and the installed adapter
      passes A1's discovery checks.
- [ ] A4. The quickstart runbook documents client-neutral registration
      (generic MCP client steps, with codex as one verified example).

## Out of scope

- Remote or hosted MCP surfaces; provider-specific MCP servers.
- A compiled Rust MCP binary (the stdio adapter is process-based either way;
  adopt only if a demonstrated packaging need appears — mirrors the
  conditional pattern of spec 002's vector-DB clause).

## Open questions

- None.

## Links

Code: `tools/palimpsest_mcp.py`
Tests: `tools/test_palimpsest_mcp.py`
Decisions: 0012, 0020
