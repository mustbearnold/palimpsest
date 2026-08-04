# ADR-0027: Derived current fact-revision projection

Status: accepted

Date: 2026-08-03

## Context

The authorized lexical and hybrid retrieval paths previously selected the latest valid revision by sorting the complete fact-revision history on every request. A rollback-only 100,000-revision probe measured p95 3.857 seconds and identified current-revision selection as a material part of the plan. A facts-driven pointer lookup was tested and was slower because it still joined back to the canonical rows.

## Decision

Add `memory.fact_revision_current` as a derived, repairable projection of the latest recorded revision for each `(tenant_id, subject_id, fact_id)`. Migration 0017 backfills it and an insert trigger advances it monotonically when a new immutable revision is written. Migrations 0018 and 0019 add the matching lifecycle, deletion-worker, restore, and owner-only repair paths. Migration 0020 adds a scope-local coverage marker that records whether the projection has one row per canonical fact and whether all projected rows are currently valid. The row carries the fields needed by current retrieval, including validity, provenance-independent value data, sensitivity, and content digest; the canonical revision chain remains the source of truth.

Current lexical and hybrid retrieval first use this projection for rows recorded and valid in the requested current perspective. If a current row is missing, future-recorded, or not valid at the requested time, a bounded fallback selects that fact from canonical revision history. As-of retrieval always uses that fallback, so historical semantics do not depend on a wall-clock current projection. A complete marker lets current retrieval skip the missing-row anti-join; a repair-required marker preserves current rows and runs the anti-join fallback only for facts absent from the projection. The request path does not independently compare every projection row with the canonical latest revision: a measured guard made the hot path slower, so the monotonic insert trigger, coverage marker, and owner-only scope rebuild are the repair boundary. A complete marker is accepted only until the earliest finite valid-time upper bound. Authorization, retention, sensitivity, and deletion-state checks still run before candidate scoring. The projection has the same forced row-level scope, subject-lifecycle, deletion-worker, and restore policies as the canonical content tables. An owner-only `memory.rebuild_fact_revision_current` function can reconstruct one active scope from canonical history. The projection is never exposed as a public memory record.

## Consequences

The current retrieval path maintains one derived row per fact and a repair backfill during migration. The original completeness-preserving fallback check measured p95 4.264 seconds and p99 4.312 seconds at 100,000 revisions; the coverage-gated complete path most recently measured p95 1.747 seconds and p99 1.821 seconds on the same local profile. That is development evidence, not an SLA: the proposed million-revision gate is p95 <= 200 ms and p99 <= 400 ms, and the 100,000-revision run still misses it. A per-request canonical stale guard was slower still at p95 4.609 seconds and p99 4.610 seconds. Concurrent, cold-cache, million-revision, HTTP, and production recovery evidence remain required.
