# 015 — Optional hot cache (Valkey/Redis)

## Purpose

Provide an optional Valkey/Redis hot cache for checkpoints, locks, and recent retrieval receipts. The cache accelerates measured hot paths only. The cache is never a source of truth. Cache loss must never erase memory.

## Decisions (2026-08-08 draft)

1. The cache is optional and off by default.
2. Canonical records in PostgreSQL remain the only source of truth.
3. The cache must be provably rebuildable from canonical records.
4. The cache stores receipts and markers. It does not store raw private memory content.
5. Cache keys are tenant-scoped. A shared cache cannot leak data between tenants.
6. Every cache read falls back to a canonical path when the cache is unavailable.
7. The cache targets only hot paths with measured bottleneck evidence (issue #37 scale evidence).

## Requirements

- R1. Checkpoint receipts cached. The cache stores the latest committed checkpoint markers (ingestion, consolidation) keyed by subject and scope.
- R2. Lock records cached. Projection leases and short-lived lock records are cached with a TTL matching their canonical lifetime.
- R3. Recent retrieval receipts cached. Authorized retrieval receipts are cached for a bounded window with an explicit TTL.
- R4. Loss safety. Eviction, restart, or total cache wipe must leave retrieval correct. Every cache hit is validated against the canonical coverage marker before it is trusted.
- R5. Rebuild. A full rebuild repopulates every cache entry from canonical records and restores cache correctness.
- R6. Content-free. No raw private memory appears as a routine cache field. Cache values are receipts, hashes, and markers only.
- R7. Tenancy. Every cache key embeds the tenant id. One tenant cannot read or overwrite another tenant's entries.
- R8. Failure injection. With the cache unavailable during writes and reads, canonical state stays correct and reads return correct results via fallback.
- R9. Invalidation. Cache entries invalidate when the projection coverage marker changes. The invalidation protocol is versioned.
- R10. Optional runtime. With caching disabled, the service behaves exactly as today. The cache client is a trait with a no-op implementation as the default.

## Design (v1)

- Cache client. A `HotCache` trait in the domain crate. Implementations: `NoopHotCache` (default) and `ValkeyHotCache` (Redis-compatible protocol client). The application crate selects the implementation from configuration.
- Key schema. `palimpsest:{tenant_id}:{kind}:{scope}` where kind is one of `checkpoint`, `lock`, `receipt`.
- Checkpoints. Values are the checkpoint receipt produced by the write path (`palimpsest-postgres/src/write_path.rs`), stored with the canonical coverage marker version.
- Locks. Values are lease tokens with a TTL at or below the canonical lease expiry.
- Receipts. Values are retrieval receipt identifiers (`palimpsest-postgres/src/receipt_write.rs`, `hybrid_receipt.rs`) for the bounded recent window.
- Coverage validation. Every hit is checked against the current projection coverage marker version. A version mismatch is a miss.
- Rebuild. Enumerate canonical checkpoint, lock, and receipt records; write each entry; verify counts against canonical totals.
- Invalidation. Bump the coverage marker version. Dependent entries fail validation and are lazily refreshed.

## Acceptance criteria

- A1. `verify_cache_optional_off` — default configuration opens no cache connection. All retrieval, checkpoint, and lock paths pass with caching disabled.
- A2. `verify_cache_loss_safe` — simulate eviction, restart, and total wipe. Retrieval results stay correct after each scenario.
- A3. `verify_cache_rebuildable` — full rebuild repopulates all cache entries from canonical records. Rebuild counts match canonical totals.
- A4. `verify_cache_tenant_isolation` — tenant A and tenant B share one cache. A cannot read or overwrite B's entries.
- A5. `verify_cache_content_free` — an inspection gate asserts no cache value contains raw private memory fields.
- A6. `verify_cache_failure_injection` — the cache is down during reads and writes. Canonical state is unchanged and reads return correct results.
- A7. `verify_cache_invalidation` — a coverage-marker version bump invalidates dependent entries; reads fall back and refresh.

## Out of scope

- Caching raw private memory content.
- Any cache entry as a source of truth.
- Replacing PostgreSQL.
- Non-Redis-compatible cache backends.

## Links

- Issue #39 · specs/002-authorized-retrieval · specs/010-operations · specs/011-governed-consolidation · docs/decisions/0001-postgres-temporal-source-of-truth.md · specs/BACKLOG.md
