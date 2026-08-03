# ADR-0024: S3-compatible export package store

Status: accepted

Date: 2026-08-03

## Context

Canonical-history exports were durable only on the local filesystem. That is a
useful development default, but it does not exercise the object-store failure
and recovery boundary that a self-hosted multi-process deployment needs.

## Decision

Add `S3ExportPackageStore` as an optional implementation of the existing
`ExportPackageStore` port. It uses path-style S3-compatible URLs and AWS
Signature Version 4 request signing. The store stages at an export-specific
`.staging` object and publishes to an export-specific `.zip` object. Publication
uses conditional `If-None-Match: *` writes, compares an already-published
object before accepting a retry, and deletes staging only after the published
bytes match. Deletion treats an already-absent object as success, and reads
map a missing object to the existing content-free `NotFound` store result.

The server keeps the private filesystem store by default. Setting all of
`PALIMPSEST_EXPORT_S3_ENDPOINT`, `PALIMPSEST_EXPORT_S3_BUCKET`,
`PALIMPSEST_EXPORT_S3_REGION`, `PALIMPSEST_EXPORT_S3_ACCESS_KEY_ID`, and
`PALIMPSEST_EXPORT_S3_SECRET_ACCESS_KEY` selects the S3-compatible store;
`PALIMPSEST_EXPORT_S3_PREFIX` and `PALIMPSEST_EXPORT_S3_SESSION_TOKEN` are
optional. A partial or malformed configuration fails startup rather than
silently reverting to the filesystem. Credentials are held in memory only and
are never included in store errors or status output.

The adapter is contract-tested against a local HTTP object-shaped fixture for
signing headers, stage/publish idempotency, different-object conflicts,
staging cleanup, and delete/read absence. That proves the Palimpsest object
port and failure semantics, not availability, durability, deletion, or
recovery behavior of a particular provider.

## Consequences

Self-hosters can move export packages to an S3-compatible endpoint without
changing the HTTP API or canonical package format. The path-style and
conditional-write requirements are explicit portability constraints. Valkey/
Redis cache, artifact-reference storage, provider-specific fault injection,
and production RPO/RTO evidence remain separate work.
