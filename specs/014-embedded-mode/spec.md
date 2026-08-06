# 014 — Embedded/single-user mode

Status: draft
Owner: AI CEO

## Purpose

A future embedded mode that lets a local, offline single-user agent use the
same MemoryService domain semantics without a network service (or with an
embedded local server). Product story (PRODUCT_SPEC user story 27). Issue #40.

## Requirements

- R1. Embedded mode MUST provide the same MemoryService boundary and domain
  semantics as the HTTP service: bitemporal writes, authorized retrieval,
  receipts, supersession, deletion fences, export and restore semantics — no
  weakened authorization or provenance, regardless of transport.
- R2. Embedded mode MUST reuse the deterministic domain crate
  (`palimpsest-domain`) and the retrieval semantics (spec 002); the domain
  logic is the same code, not a reimplementation.
- R3. The embedded storage substrate MUST satisfy the same invariants as the
  server substrate: RLS-equivalent tenant isolation, durable canonical
  records, reproducible derived indexes (constitution principles 7, 12),
  content-free tombstones, and the migration lifecycle.
- R4. Embedded mode MUST be single-user and offline-first: no network service
  required for operation, no ambient telemetry, no vendor or framework
  dependency (constitution principle 15).
- R5. Authorization MUST NOT be weakened in single-user mode: the same
  principal/tenant/subject grants apply; a single-user deployment configures
  them locally.
- R6. Retrieval quality and correctness claims need the same scenario-test or
  benchmark evidence as the server (constitution principle 14).

## Acceptance criteria (draft — to be finalized before implementation)

- [ ] A1. The full authorized-retrieval conformance suite (spec 002 A1–A3)
      passes against the embedded substrate.
- [ ] A2. Deletion and restore semantics (specs 003, 005) pass in embedded
      mode, including the subject lifecycle fence and tombstone invariants.
- [ ] A3. Embedded mode operates with no network listener by default; a local
      embedded server MAY be enabled explicitly.
- [ ] A4. No framework-specific behavior leaks into canonical records or the
      public contract (constitution principle 15) — the embedded surface
      speaks the same contracts.

## Out of scope

- Multi-user or networked embedded mode.
- Weakening any authorization, retention, or provenance invariant for
  convenience.
- A dedicated new storage engine unless the benchmark evidence in the open
  questions demonstrates a named failure of the substrate default.

## Open questions

- Storage substrate: the first vertical slice explicitly excluded SQLite; this
  spec must revisit that with evidence. Default candidate: embedded PostgreSQL
  (single-user local cluster, same migrations, same RLS, same recovery
  semantics — zero semantic divergence, maximum reuse). SQLite would require a
  new storage layer with RLS parity and migration divergence, mirroring spec
  002's conditional pattern (adopt only if a benchmark demonstrates a named
  failure of the embedded-Postgres default).
- Relationship to surfacing (spec 012): offline agents may be the first real
  surfacing consumers; embedded mode MUST NOT couple to a framework-specific
  surfacing channel (principle 15).
- Packaging and distribution boundary: library embedding (crate) vs bundled
  runtime vs local server binary; decide at spec finalization, aligned with
  the MCP packaging decision (spec 008).

## Links

Issue: #40 · Specs: 001 (MemoryService), 002 (retrieval), 003 (lifecycle),
005 (restore), 012 (surfacing) · References: `_attic/PRODUCT_SPEC.md`
(user story 27) · Sequencing: after the release gate (#6, closed 2026-08-07)
