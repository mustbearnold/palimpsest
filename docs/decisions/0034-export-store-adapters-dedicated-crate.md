# ADR-0034: Export-store adapters in a dedicated crate

Date: 2026-08-09 · Status: accepted

## Context

The export surface grew two package profiles (spec 017 P1). The application
crate owns the `ExportPackageStore` interface and the export package types.
It also owned the concrete adapters behind that interface: an
S3-compatible object store (path-style, SigV4-signed over `reqwest`), a
filesystem store, and an in-memory store. That put HTTP signing, S3 URL
policy, and filesystem layout inside the same module as the domain
orchestration. The SigV4 helpers are shared with the backup module
(ADR-0002 S3 backup upload), so the signing code was already a cross-module
infrastructure seam.

The repository-layout rule (ADR-0029) consolidates related code into
single crates; the embedded substrate (ADR-0033) and the server
composition root both construct adapters directly. Neither should import
signing machinery from the domain module.

## Decision

Move the three export-store adapters and their configuration into a new
`palimpsest-stores` crate. The application crate keeps the
`ExportPackageStore` interface, the `ExportStoreError` contract, the
package types, and the SigV4 helpers (shared with backup). The stores crate
depends on the application crate; the application crate does not depend on
the stores crate. The server composition root, the embedded substrate, and
the conformance tests import the adapters from `palimpsest-stores`.

Adapter unit tests move with the adapters and build packages against the
`ExportPackage` interface only (a test double replaces the real packages).

## Consequences

- The application crate loses its `reqwest`-adjacent signing surface; the
  stores crate owns HTTP and object-store behavior.
- Adding a future adapter (for example an artifact store for backups) does
  not touch the domain module.
- The dependency direction stays acyclic: stores → application.
- The SigV4 helpers remain in the application because backup.rs shares
  them; a future extraction of a shared signing module can happen without
  changing the public contract.
