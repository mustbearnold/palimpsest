# Authorized lexical scale probe — 2026-08-05 (million revisions)

Status: development evidence; the first 1,000,000-revision profile misses the
proposed release latency gate by a large margin and makes no
production-readiness claim.

## Method

`scripts/palimpsest-scale-probe.sh` seeded a reserved synthetic tenant/subject
scope inside one PostgreSQL transaction, generated one attributable episode
and 1,000,000 fact revisions plus their governed lexical projections,
analyzed the five relevant relations, ran the
authorization/temporal/current-revision lexical retrieval core 20 times
serially, captured an `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` plan, and
rolled the transaction back. A post-run scope check found zero retained
revisions (`transaction_rolled_back: true`).

The probe is deliberately narrower than the public release gate: it measures
a direct database lexical query, not HTTP overhead, vector retrieval,
embedding generation, concurrency, cost, cold-start behavior, provider
durability, or a warm-cache million-revision run.

## Profile and result

| Field | Value |
| --- | --- |
| Profile | `authorized-lexical-retrieval-scale-v1` |
| Scope | 1,000,000 synthetic revisions and 1,000,000 lexical projections |
| Queries | 20 serial queries |
| Host | x86_64 CachyOS Linux, 16 CPUs, 31 GiB RAM |
| PostgreSQL / pgvector | 18.4 / 0.8.5 |
| p50 | 11,034.935 ms |
| p95 | 11,301.817 ms |
| p99 | 11,311.869 ms |
| Mean / max | 11,085.416 ms / 11,314.382 ms |
| Plan digest | `8499ae8547697dbe4605c0dcddbc430bf26ca31ecb10e12deb806eedce826d1f` |
| Retention | none; transaction rolled back |

## Plan profile

The bounded plan summary shows the ranking sort dominating the hot path:
Aggregate 10,831.593 ms, Limit 10,831.579 ms, Sort 10,667.713 ms (writing
70,269 temp blocks), the authorization Nested Loop 10,520.485 ms, and the
current-projection CTE joins below it. The `fact_revision_current` sequential
scan itself is only 203.977 ms. Inclusive node times overlap; they must not
be summed. The dominant cost is ordering the full authorized candidate set
(1,000,000 rows) before the limit, with temp spill; projection scan cost is
not the bottleneck.

## Gate assessment

The proposed release gate is p95 <= 200 ms and p99 <= 400 ms on a published
1,000,000-revision single-node profile. This first million-revision result
measures p95 11,301.817 ms and p99 11,311.869 ms — roughly 56x over the p95
gate — so it is a falsification of the current query shape's scale readiness,
not evidence for any extrapolated claim.

Scaling context: the 100,000-revision coverage-gated profile measured p95
1,747.183 ms (2026-08-03). Ten times the data produced roughly 6.5x the p95
latency; the absolute gap to the gate is decisive regardless of scaling
exponent, and no superlinearity or capacity claim is made.

## Remediation experiments (measured 2026-08-05)

The plan pointed at the ranking sort; targeted experiments disproved that
hypothesis and located the real cost:

1. **Sort mechanics are not the bottleneck.** An isolated 1,000,000-row
   microbenchmark (scratch tables, same ranking semantics) measured
   ts_rank_cd computation at ~37 ms, top-N heapsort at ~360 ms, and a
   hash-join pipeline at ~0.6-1.5 s — the planner uses in-memory top-N
   heapsort (26 kB, zero spill) when nothing else constrains it.
2. **The probe's 11.3 s is the documents join.** The measured plan is a
   nested loop producing 1,000,000 rows: per-row index probes into
   `fact_revision_search_documents` (~10 us each). The planner chooses it
   because the `@@` filter's selectivity estimate (and the materialized-CTE
   shape, which blocks parallelism) makes the hash-join alternative look
   more expensive.
3. **Variant A (documents-first join, identical semantics):** no improvement
   at 1M (p95 13.47 s) — the planner re-flipped the join order.
4. **Variant B (documents-first + inline the single-use CTEs):** 100k p95
   1,178.6 ms vs. the 1,747.2 ms baseline (-33%, a hash join appeared on the
   governance join); at 1M, p50 9,664.4 ms / p95 10,458.5 ms / p99 10,965.9
   ms — an ~8% improvement. The documents nested loop (9,776.8 ms) still
   dominates.

Conclusion: with the current query architecture, a 100%-match 1,000,000-row
corpus, and PostgreSQL's cost model, the ~10 s join is inherent to the shape.
The remaining levers are architectural, not query-tuning:

- **(a) Selectivity model for the gate.** The all-match corpus is the
  pathological worst case; a realistic query mix (e.g., 1-10% match rates,
  where the GIN index prunes) is the honest gate shape. The proposed
  p95 <= 200 ms / p99 <= 400 ms gate was defined without a selectivity
  assumption and is not reachable by query shape alone on the all-match
  profile.
- **(b) Join elimination.** Folding `search_vector` and the projection
  integrity fields into the current-projection table would remove the
  documents join from the hot path entirely (the per-row `projection_ready`
  verification is a correctness feature and would need a cheaper home).
- **(c) A different ranking architecture** (e.g., approximate top-N) is the
  last resort and contradicts the exactness invariant.

## Selectivity-mixed profile (measured 2026-08-05)

The probe was extended (corpus gains a 32-group category token; the query
mix alternates four selectivity bands) and re-run at 1,000,000 revisions, 20
queries (5 per band), rollback-only:

| Band (match rate) | p50 | p95 | p99 | mean | max |
| --- | --- | --- | --- | --- | --- |
| all (100%) | 13,764.4 ms | 22,387.8 ms | 24,014.5 ms | 15,448.7 ms | 24,421.2 ms |
| quarter (25%) | 17,606.2 ms | 23,188.4 ms | 24,262.1 ms | 18,187.5 ms | 24,530.5 ms |
| sixteenth (6.25%) | 12,705.6 ms | 12,935.5 ms | 12,965.3 ms | 12,455.4 ms | 12,972.8 ms |
| thirtysecond (3.125%) | 12,194.3 ms | 20,825.5 ms | 22,529.0 ms | 14,048.1 ms | 22,954.8 ms |

The selective documents-side access works exactly as designed: the
thirtysecond-band plan is a Bitmap Index Scan over the GIN index returning
31,250 rows in 43.0 ms (Bitmap Heap Scan 81.2 ms, aggregate 82.4 ms).
Yet the full-pipeline latency is selectivity-independent — every band sits in
the 12-24 s range, with high within-band variance (5 samples each). The cost
is per-query and corpus-wide: materializing the full authorized set (~550 MB
temp spill per query, repeated for all 20 queries) plus the governance join
(5.1 s nested loop). Even a 3% match query pays the entire 1M-row pipeline.

This sharpens the earlier conclusion: the proposed p95 <= 200 ms gate at
1,000,000 revisions is falsified for every selectivity band with the current
query architecture, and a selectivity model alone would not have saved it.
The remaining levers are architectural:

- **(b') Pipeline restructuring**: eliminate the per-query full-set
  materialization and governance join — e.g., a precomputed, incrementally
  maintained authorized-current structure, or a hot cache with loss-safety
  evidence (issue #39). This is now the primary lever; the documents join
  (GIN-accelerated, 43 ms) is no longer the problem.
- **(a) Selectivity model for the gate**: still needed for an honest gate,
  but now secondary — the measured floor is selectivity-independent.
- **(c) A different ranking architecture** remains the last resort and
  contradicts the exactness invariant.

## Concurrent and cold-cache profiles (prepared-seed methodology, measured 2026-08-05)

The rollback-only probe cannot measure these profiles: concurrent sessions
cannot see an uncommitted seed, and a cold-cache profile requires a server
restart. The prepared-seed methodology was executed instead: the seed ran
inside one transaction that was prepared
(`PREPARE TRANSACTION 'palimpsest-scale-seed'`), the dev cluster was
restarted (the prepared transaction survived — two-phase durability
exercised), the seed was committed (`COMMIT PREPARED`), and the reserved
scope (1,000,000 revisions plus their governed lexical projections,
coverage `complete`) was measured: a 20-iteration all-match latency loop
starting from a truly cold cache (restart + `drop_caches`; the first query
is the cold one), then an 8-session x 5-query concurrent run. The scope was
then purged and zero retention verified across all 30 `memory.*` tables
carrying the scope columns (0 rows retained); `max_prepared_transactions`
was reverted to 0 and the cluster restarted.

### Cold-cache profile

| Field | Value |
| --- | --- |
| Cold first-query latency | 19,287.786 ms |
| Disk blocks read by the cold query | 1,655,838 (12.63 GiB) |
| Warm p50 / p95 / p99 (iterations 2-20) | 12,654.650 / 13,111.351 / 13,146.138 ms |
| Warm mean / max | 12,704.728 / 13,154.835 ms |
| Warm EXPLAIN execution / planning | 12,464.277 / 0.518 ms |
| Plan digest | `07fa3f652a59f214980a2a94e0b8095383681d4fd9a9b24f6ea6e65abfc244f1` |

Warm-up curve (ms, iterations 1-20): 19,287.8 -> 12,609.1 -> 12,531.2 ->
12,458.6 -> 12,515.3 -> 12,582.2 -> 12,455.3 -> 12,537.6 -> 12,344.7 ->
12,654.7 -> 12,548.4 -> 12,667.5 -> 12,784.6 -> 12,882.7 -> 12,869.1 ->
12,805.7 -> 13,052.8 -> 12,828.9 -> 13,154.8 -> 13,106.5. Warm-up completes
after the second query; the plateau is the pipeline cost (full-set
materialization + governance join + ranking sort), not disk access.

The cold delta is the physical read of the query's ~12.6 GiB buffer
footprint: the documents nested loop re-probes ~720,896 blocks and the
current-projection scan is 52,150 blocks (~407 MB). The warm EXPLAIN reports
nearly the same `Shared Read Blocks` (1,654,152, with 8,397,999 hits) — the
working set exceeds `shared_buffers` (128 MB), so warm queries re-read it
from the OS page cache; cold vs warm is 19.3 s vs ~12.6 s.

### Concurrent profile (8 parallel sessions x 5 all-match queries)

| Field | Value |
| --- | --- |
| Aggregate (40 samples) p50 / p95 / p99 | 16,292.175 / 16,791.907 / 16,909.329 ms |
| Aggregate mean / max | 16,396.041 / 16,956.198 ms |
| Per-session p50 spread | 16,223.987-16,699.620 ms |
| Per-session mean / max spread | 16,266.110-16,511.950 / 16,545.899-16,956.198 ms |

Per-session percentile lines (p50 / p95 / p99 / mean / max, ms):

| Session | p50 | p95 | p99 | mean | max |
| --- | --- | --- | --- | --- | --- |
| 1 | 16,240.474 | 16,709.341 | 16,725.534 | 16,364.731 | 16,729.582 |
| 2 | 16,699.620 | 16,822.369 | 16,833.291 | 16,502.058 | 16,836.022 |
| 3 | 16,239.590 | 16,712.156 | 16,727.365 | 16,372.529 | 16,731.167 |
| 4 | 16,580.989 | 16,920.649 | 16,949.088 | 16,511.950 | 16,956.198 |
| 5 | 16,240.439 | 16,619.908 | 16,623.517 | 16,321.847 | 16,624.419 |
| 6 | 16,335.581 | 16,767.978 | 16,783.418 | 16,411.791 | 16,787.278 |
| 7 | 16,223.987 | 16,530.766 | 16,542.872 | 16,266.110 | 16,545.899 |
| 8 | 16,312.344 | 16,772.283 | 16,786.125 | 16,417.310 | 16,789.585 |

Eight-way contention costs ~+29% over the same-run serial warm baseline
(p50 16.29 s vs 12.65 s): 8 concurrent x ~550 MB temp spill plus buffer
thrash, with a tight per-session spread (< 0.5 s).

### Methodology findings (deviations from the issue recipe)

1. **Seed timing.** The trigger-maintained 1,000,000-revision insert alone
   exceeds the recipe's 15-minute `statement_timeout`; the seed script's
   timeout was raised to 60 minutes. Total seed ~19 minutes.
2. **PREPARE fires the deferred evidence-requirement constraint trigger.**
   `PREPARE TRANSACTION` fires the deferred `fact_revision_requires_evidence`
   constraint trigger once per inserted revision (1,000,000 checks). The
   recipe's ANALYZE list omitted `fact_revision_evidence`, so each check
   planned as a sequential scan (~59.7 ms each, ~16.7 h total). Adding
   `ANALYZE memory.fact_revision_evidence` makes the check an index probe and
   PREPARE completes in ~2 minutes. The rollback-only probe never exposed
   this because deferred triggers do not fire on rollback.
3. **Cold-cache methodology.** A restart alone leaves the OS page cache
   warm; the profile uses restart + `drop_caches`, with the first data query
   after that being the cold query (scope verification counts run
   afterward).
4. **pg_stat_database reads inside PL/pgSQL return stale values** (delta 0
   observed); the cold query's disk reads were captured with plain-SQL reads
   around the first query.
5. **Cleanup.** The canonical tables are append-only outside the fenced
   deletion workflow (triggers raise `55000`); the reserved synthetic scope
   was purged with `session_replication_role = replica` (superuser trigger
   bypass) and verified at zero rows across all 30 scope tables.

### Gate assessment (updated)

Every measured profile falsifies the proposed p95 <= 200 ms / p99 <= 400 ms
gate at 1,000,000 revisions: cold first query 19.3 s, warm serial p95
13.1 s, concurrent p95 16.8 s. Concurrency and cold start widen the gap;
they do not change the architectural conclusion (the per-query full-set
pipeline is the floor).

## Reuse

The probe is a repeatable baseline for index/query-plan remediation. Its
synthetic values and reserved identifiers never leave the rolled-back
transaction, and its output contains only counts, timings, a plan digest, and
a bounded plan summary.
