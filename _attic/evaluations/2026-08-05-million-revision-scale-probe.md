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

Concurrent and cold-cache profiles remain unmeasured (issue #37).

## Reuse

The probe is a repeatable baseline for index/query-plan remediation. Its
synthetic values and reserved identifiers never leave the rolled-back
transaction, and its output contains only counts, timings, a plan digest, and
a bounded plan summary.
