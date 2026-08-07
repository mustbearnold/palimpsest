# 014 — Embedded/single-user mode

Status: active
Owner: AI CEO

## Purpose

Embedded mode lets a local, offline single-user agent use the same
MemoryService domain semantics without a network service (or with an
embedded local server). Product story (PRODUCT_SPEC user story 27). Issue
#40.

## Decisions (2026-08-08 finalization)

- D1. Embedded PostgreSQL is the v1 substrate (ADR-0033). A single-user
  local cluster runs the same migrations, the same RLS, and the same
  recovery semantics as the server. SQLite adoption stays conditional: adopt
  only if a benchmark demonstrates a named failure of the default. Spec 001
  excluded SQLite in the first vertical slice; that evidence stands.
- D2. Embedded mode is a library first. A Rust library exposes the
  MemoryService boundary. It reuses `palimpsest-domain`,
  `palimpsest-application`, and `palimpsest-postgres` unchanged. An optional
  loopback HTTP server reuses `palimpsest-http` routes. A host enables it
  explicitly. Packaging follows the spec 008 conditional pattern.
- D3. The surfacing seam (spec 012) applies unchanged. The surface policy
  registry is the same code. No framework-specific channel exists
  (principle 15). Offline agents are a first-class surfacing consumer.
- D4. Authorization does not weaken. Local configuration supplies the same
  tenant, principal, and grant model. RLS FORCE still enforces at the
  substrate. No ambient credentials exist.
- D5. No ambient telemetry exists. The metrics surface (spec 010 R3) does
  not change.

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

## Design (v1)

### Runtime shape

A new library crate (`crates/palimpsest-embedded`) wires the application and
postgres crates against a private local cluster. The cluster starts offline.
No network listener exists by default. A host may enable a loopback-only
HTTP server that reuses `palimpsest-http` routes. The MCP adapter (spec 008)
connects when that server runs.

### Substrate

The substrate is a single-user local PostgreSQL cluster. It applies the same
SQLx migrations (ADR-0015 lifecycle). It runs the same RLS policies. Derived
indexes rebuild from canonical records (principle 12; the ADR-0032
precomputed structure applies).

### Identity and authorization

Local configuration supplies tenant, principal, and grant records at
startup. The application applies the same grant checks. RLS FORCE still
enforces row isolation. No ambient credentials exist.

### API surface

- Library functions expose the write, recall, surface, export, and delete
  operations.
- An optional loopback HTTP server exposes the same `/v1` contract.
- The surface policy registry (spec 012) applies unchanged.

### Telemetry

No ambient telemetry exists. No metrics surface exists by default. A host
may enable the standard metrics surface with the local server (spec 010 R3).

## Acceptance criteria

- [x] A1. The authorized-retrieval conformance suite (spec 002 A1–A3) passes
      unchanged against the embedded substrate (scenario
      `verify_embedded_retrieval_conformance`).
- [x] A2. Deletion fences and restore suppression pass in embedded mode,
      including the subject lifecycle fence and tombstone invariants
      (scenario `verify_embedded_lifecycle_fence_and_restore`).
- [x] A3. Embedded mode operates with no network listener by default; a local
      embedded server MAY be enabled explicitly (scenario
      `verify_embedded_no_listener_default`).
- [x] A4. Canonical records and receipts match the HTTP surface; no
      framework-specific behavior leaks in (constitution principle 15;
      scenario `verify_embedded_contract_parity`).
- [x] A5. RLS-equivalent tenant isolation holds with multiple local tenants
      configured (scenario `verify_embedded_tenant_isolation`).
- [x] A6. Derived indexes rebuild from canonical records on the embedded
      substrate (constitution principle 12; scenario
      `verify_embedded_index_reproducible`).
- [x] A7. With a registered surface policy, the embedded surface returns the
      same bounded bundle as the HTTP seam (spec 012; scenario
      `verify_embedded_surface_policy`).

## Out of scope

- Multi-user or networked embedded mode.
- Weakening any authorization, retention, or provenance invariant for
  convenience.
- A dedicated new storage engine unless the ADR-0033 conditional fires.
- A default embedding provider (neutrality).
- Ambient session surveillance or telemetry.

## Links

Specs: 001 (MemoryService) · 002 (retrieval) · 003 (lifecycle) · 005
(restore) · 008 (MCP adapter) · 010 (operations) · 012 (surfacing)
Decisions: 0015 (migration lifecycle) · 0032 (precomputed structure) · 0033
(substrate)
References: `_attic/PRODUCT_SPEC.md` (user story 27)
Backlog: #40
