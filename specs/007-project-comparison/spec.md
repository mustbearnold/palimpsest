# 007 — Project comparison and governed review

Status: active
Owner: AI CEO

## Purpose

Deterministic structural comparison of per-project memory bundles plus a
validated, explicitly approved path for external semantic interpretation and
governed consolidation. Palimpsest never infers semantic truth itself.

## Requirements

- R1. Comparison MUST group exact keys by canonical value digest;
  same-key/different-value groups MUST be review candidates, never semantic
  conflict conclusions.
- R2. Comparison MUST provide bounded lexical-overlap hints connecting
  differently keyed session messages, plus bounded shared/only-in token
  deltas, and MUST NOT make a semantic claim or durable write.
- R3. Comparison results MUST carry observed project-root, branch, source,
  role, and unique-session context labels.
- R4. External semantic review validation MUST require every claim to cite
  returned fact/revision and source-episode identifiers plus reviewer/model
  metadata and a versioned policy digest.
- R5. Consolidation MUST be an explicit, approved plan: Palimpsest derives
  cited episode lineage, applies the caller's registered write policy, and
  uses one deterministic idempotency key per claim; partial completion MUST be
  retryable through a typed result.
- R6. Consolidation MUST NOT claim atomic batch behavior or semantic truth.

## Acceptance criteria

- [ ] A1. Comparison, validation, and consolidation tests pass in the Python,
      TypeScript, and MCP surfaces, including the governed project review
      consolidation conformance scenarios.
- [ ] A2. Consolidation preflight failures are classified (idempotent retry
      paths) rather than conflated with write failures.
- [ ] A3. Unvalidated or uncited claims are rejected.

## Out of scope

- Automatic model-driven semantic diffs, conflict explanations, or promotion
  of unreviewed model output.

## Open questions

- Server-side consolidation jobs with retryable claims are now specced in
  spec 011; the client-coordinated sequence remains the conservative path.

## Links

Code: `crates/palimpsest-postgres` · `crates/palimpsest-server` ·
`clients/python` · `clients/typescript` · `scripts/palimpsest_mcp.py`
Tests: `conformance_postgres18.rs` · client suites
Decisions: 0025, 0026, 0028
