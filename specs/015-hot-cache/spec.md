# 015 — Optional hot cache (Valkey/Redis)

## Status

Draft. Spec review round 1 FAIL; fixes applied 2026-08-08 (per-kind validation, cache-wrong rule, at-or-below TTL, RFC 2119 tags).

## Owner

Agent lane: palimpsest-application, palimpsest-cache, palimpsest-domain.

## Purpose

Provide an optional Valkey/Redis hot cache for checkpoints, locks, and recent retrieval receipts. The cache accelerates measured hot paths only. The cache is never a source of truth. Cache loss MUST never erase memory.

## Decisions (2026-08-08)

1. The cache is optional and off by default.
2. Canonical records in PostgreSQL remain the only source of truth.
3. The cache MUST be provably rebuildable from canonical records.
4. The cache stores receipts and markers. It MUST NOT store raw private memory content.
5. Cache keys MUST embed the tenant id. A shared cache cannot leak data between tenants.
6. Every cache read MUST fall back to a canonical path when the cache is unavailable.
7. The cache MUST target only hot paths with measured bottleneck evidence (issue #37 scale evidence).
8. Cache reads MUST never gate canonical writes. A cache entry is always advisory.
9. Validation is per kind. Each kind validates a hit against its own canonical state. There is no shared projection coverage marker.
10. Lock entries are advisory only. The canonical lease row decides. A stale lock entry MUST NOT extend a lease.

## Requirements

- R1. Checkpoint receipts cached. The service MUST cache the latest committed checkpoint markers (ingestion, consolidation) keyed by subject and scope.
- R2. Lock records cached. Projection leases and short-lived lock records MUST be cached with a TTL at or below the canonical lease lifetime. An early-released lease MUST NOT outlive its cache entry.
- R3. Recent retrieval receipts cached. Authorized retrieval receipts MUST be cached for a bounded window with an explicit TTL.
- R4. Loss safety. Eviction, restart, or total cache wipe MUST leave retrieval correct. Every cache hit MUST be validated against canonical state before it is trusted.
- R5. Rebuild. A full rebuild MUST repopulate every cache entry from canonical records and restore cache correctness.
- R6. Content-free. No raw private memory MAY appear as a routine cache field. Cache values are receipts, hashes, and markers only.
- R7. Tenancy. Every cache key MUST embed the tenant id. One tenant MUST NOT read or overwrite another tenant's entries.
- R8. Failure injection. With the cache unavailable during writes and reads, canonical state MUST stay correct and reads MUST return correct results via fallback.
- R9. Invalidation. Validation MUST use per-kind canonical state:
  - Checkpoint: the checkpoint entry is valid iff it names the current head revision (ADR-0004 head-CAS).
  - Receipt: the receipt entry is valid iff the canonical receipt row still exists and is current.
  - Lock: the lock entry is advisory only; it has no validity gate.
- R10. Optional runtime. With caching disabled, the service MUST behave exactly as today. The cache client MUST be a trait with a no-op implementation as the default.

## Design (v1)

- Cache client. A `HotCache` trait in the domain crate. Implementations: `NoopHotCache` (default) and `ValkeyHotCache` (Redis-compatible protocol client). The application crate selects the implementation from configuration.
- Key schema. `palimpsest:{tenant_id}:{kind}:{scope}` where kind is one of `checkpoint`, `lock`, `receipt`.
- Checkpoints. Values are the checkpoint receipt produced by the write path (`palimpsest-postgres/src/write_path.rs`), stored with the head revision id they name.
- Locks. Values are lease tokens with a TTL at or below the canonical lease expiry.
- Receipts. Values are retrieval receipt identifiers (`palimpsest-postgres/src/receipt_write.rs`, `hybrid_receipt.rs`) for the bounded recent window.
- Versioned envelope. Values carry an 8-byte little-endian version prefix. The caller supplies the per-kind canonical validator. A version mismatch is a miss.
- Rebuild. Enumerate canonical checkpoint, lock, and receipt records; write each entry; verify counts against canonical totals.
- Cache-wrong rule. A hit that passes validation but is stale in another dimension is harmless: the canonical write path never consults the cache.

## Acceptance criteria

- A1. `verify_cache_optional_off` — Given the default no-op cache, When the service writes and reads cache entries, Then every read is a miss and every path falls back to the canonical store.
- A2. `verify_cache_loss_safe` — Given a cache with entries, When the cache is wiped, restarted, or evicted, Then reads return misses and retrieval results stay correct via fallback.
- A3. `verify_cache_rebuildable` — Given a wiped cache, When a full rebuild runs from canonical records, Then every entry is repopulated and reads return correct values.
- A4. `verify_cache_tenant_isolation` — Given tenants A and B share one cache, When A writes an entry, Then B cannot read or overwrite it, and the reverse holds.
- A5. `verify_cache_content_free` — Given an inspection gate over cache values, When any routine entry is stored, Then no value contains raw private memory fields.
- A6. `verify_cache_failure_injection` — Given the cache is down, When the service reads and writes, Then canonical state is unchanged and reads return correct results.
- A7. `verify_cache_invalidation` — Given a versioned entry with a per-kind validator, When the canonical state changes (head revision bump or receipt supersession), Then the old entry fails validation, reads fall back, and a refresh restores correctness.

## Out of scope

- Caching raw private memory content.
- Any cache entry as a source of truth.
- Replacing PostgreSQL.
- Non-Redis-compatible cache backends.

## Open questions

- Which hot path receives the first wiring: checkpoints, locks, or receipts (needs #37 scale evidence review).

## Links

- Issue #39 · specs/002-authorized-retrieval · specs/010-operations · specs/011-governed-consolidation · docs/decisions/0001-postgres-temporal-source-of-truth.md · docs/decisions/0004-checkpoint-resume-and-effect-recovery.md · docs/decisions/0010-bounded-projection-leases.md · specs/BACKLOG.md
