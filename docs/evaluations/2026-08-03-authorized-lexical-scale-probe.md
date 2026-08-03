# Authorized lexical scale probe — 2026-08-03

Status: development evidence; the profile misses the proposed release latency
target and makes no production-readiness claim.

## Method

`scripts/palimpsest-scale-probe.sh` seeded a reserved synthetic tenant/subject
scope inside one PostgreSQL transaction, generated one attributable episode and
100,000 fact revisions plus their governed lexical projections, analyzed the
five relevant relations, ran the authorization/temporal/current-revision
lexical retrieval core 20 times serially, captured an `EXPLAIN (ANALYZE,
BUFFERS, FORMAT JSON)` plan, and rolled the transaction back. A post-run scope
check found zero retained revisions.

The probe is deliberately narrower than the public release gate: it measures a
direct database lexical query, not HTTP overhead, vector retrieval, embedding
generation, concurrency, cost, cold-start behavior, provider durability, or a
million-revision run.

## Rejected index experiment

An exploratory 100,000-revision run tested a temporary covering index intended
to match current-revision selection:

```sql
CREATE INDEX fact_revisions_scale_current_probe_idx
ON memory.fact_revisions (
    tenant_id, subject_id, fact_id, revision_no DESC, revision_id
)
INCLUDE (case_id, recorded_at, valid_during, sensitivity, content_sha256);
```

The index was dropped after the run and the reserved scope was verified empty.
With five serial queries, the indexed profile measured p50 3,954.626 ms,
p95 4,075.814 ms, p99 4,080.489 ms, and mean 3,975.363 ms, versus the
baseline's p50 3,684.607 ms and p95 3,857.059 ms. It was therefore rejected;
the result is exploratory evidence, not a replacement release profile. The
next remediation must use the captured plan and node timings rather than add
another unmeasured index.

## Rejected lateral current-revision experiment

A same-transaction 10,000-revision experiment compared the existing
`DISTINCT ON` current-revision selection with a `facts`-driven lateral lookup
that selected one latest revision per fact. The one-run `EXPLAIN (ANALYZE)`
execution time was 340.843 ms for the existing shape and 359.744 ms for the
lateral shape. The candidate introduced a more expensive hash/join path, so it
was rejected as a hot-path replacement. The checked-in query uses a lateral
lookup only inside the missing-pointer fallback branch; these are exploratory
single-run timings, not a replacement release profile for that branch.

## Implemented current-row projection

Migration 0017 adds a derived current-revision row per fact and keeps the
canonical revision history as the fallback for as-of retrieval and missing or
not-yet-valid current pointers. A denormalized rollback-only experiment was
implemented in the lexical and hybrid current paths. The checked-in query also
preserves a canonical fallback for a missing, future-recorded, or not-yet-valid
pointer. The scale probe exercises that complete path and still rolls back all
synthetic data.

A later 100,000-revision local run of the checked-in path measured p50 4,136.054
ms, p95 4,263.767 ms, p99 4,311.692 ms, mean 4,134.520 ms, and max 4,323.673
ms across 20 serial queries. Its plan digest is
`9099b7ff19d2e1e993f95139a5f842ba919b0bf6e9f957e6d4c9418a19f14bbf`; the
bounded plan shows the current projection scan and completeness-preserving
facts anti-join alongside the authorized document join. This is slower than
the earlier history-sort baseline, so the projection is retained as a
correctness/repairability slice, not as a claimed latency improvement. The
earlier 1.014-second projection-only result is not accepted as evidence for
the current query shape.

## Rejected per-request stale validation

An exploratory 100,000-revision run added a per-request canonical `NOT EXISTS`
check to ensure every current projection row still matched the latest
immutable revision. It measured p50 4,356.463 ms, p95 4,608.795 ms, p99
4,610.091 ms, mean 4,375.596 ms, and max 4,610.415 ms across 20 serial
queries; its plan digest was
`83ac70c15aa0879f674d403c9d0480a0ae0ed2642093fcab2047e7c4260e48a8`. The
guard was rejected because it made the hot path materially worse. The current
invariant is the monotonic insert trigger plus an owner-only scope rebuild;
arbitrary out-of-band projection corruption is an operational repair case,
not an automatic per-request detection claim.

## Bounded plan profile

The probe now emits a content-free `plan_summary` alongside the digest. It keeps
planning and execution time plus the twelve slowest inclusive plan nodes, with
node type, relation name, actual rows/loops, and shared/temp block counts. The
summary is deliberately not a replacement for the full private `EXPLAIN` plan;
it is enough to compare candidate query changes without exposing synthetic
values or raw SQL in routine output.

A 10,000-revision, five-query diagnostic run on the same local PostgreSQL
profile produced p50 376.156 ms, p95 399.280 ms, p99 400.616 ms, and a plan
execution time of 372.586 ms. The largest inclusive nodes were an aggregate
(372.535 ms), a limit (372.525 ms), a sort (372.512 ms), the authorization
nested loop (370.517 ms), and current-revision selection's unique/sort path
(142.228/139.712 ms). The `fact_revisions` sequential scan read 10,000 rows in
61.643 ms and accounted for 5,433 shared reads. Inclusive node times overlap;
they must not be summed. This points the next experiment toward current-revision
selection and authorization joins, while retaining the existing correctness
checks.

## Profile and result

| Field | Value |
| --- | --- |
| Profile | `authorized-lexical-retrieval-scale-v1` |
| Scope | 100,000 synthetic revisions and 100,000 lexical projections |
| Queries | 20 serial queries |
| Host | x86_64 CachyOS Linux, 16 CPUs, 31 GiB RAM |
| PostgreSQL / pgvector | 18.4 / 0.8.5 |
| PostgreSQL shared buffers | 128 MB |
| p50 | 3,684.607 ms |
| p95 | 3,857.059 ms |
| p99 | 3,922.989 ms |
| Mean / max | 3,705.870 ms / 3,939.472 ms |
| Plan digest | `bc47bb5f1a9d3f70bd130e55d6b7feb94f920d9ae801ab536be8f4024b9392ea` |
| Retention | none; transaction rolled back |

The proposed release gate is p95 <= 200 ms and p99 <= 400 ms on a published
1-million-revision single-node profile. This 100,000-revision result misses
both thresholds, so it is a falsification of the current profile's scale
readiness, not evidence for an extrapolated million-row claim. The 1-million
profile remains unmeasured until query planning and indexing improve.

## Reuse and next work

The probe is a repeatable baseline for index/query-plan remediation and later
warm/cold, concurrent, HTTP, vector, and million-revision measurements. Its
synthetic values and reserved identifiers never leave the rolled-back
transaction, and its output contains only counts, timings, a plan digest, and a
bounded plan summary.
