# ADR-0001: PostgreSQL is the temporal source of truth

Status: accepted

Date: 2026-07-28

## Context

Palimpsest must support crash-safe checkpoints, immutable episodes, versioned
facts and procedures, authorization, current and historical queries, flexible
metadata, and semantic retrieval. A vector-only store does not provide the
transactional and temporal authority these records require.

## Decision

Use PostgreSQL plus pgvector as the canonical durable store. Represent observed,
recorded, and valid time explicitly. Preserve episodes as immutable evidence and
facts/procedures as revision chains. Use PostgreSQL full-text search and pgvector
as derived retrieval indexes behind exact authorization and validity filters.

Use Valkey/Redis only as an optional cache. Use S3-compatible storage only for
large artifacts whose authoritative metadata remains in PostgreSQL.

## Consequences

- One transaction can keep records, provenance, authorization, audit, and index
  receipts consistent.
- Embeddings can be regenerated without losing memory truth.
- Temporal behavior is testable with ordinary queries and constraints.
- A dedicated vector or graph database requires a future ADR backed by measured
  failure of this design.
- PostgreSQL operations, migrations, backup, restore, and partition retention
  become release-critical responsibilities.
