# Authorized lexical scale probe — 2026-08-03

Status: development evidence; the profile misses the proposed release latency
target and makes no production-readiness claim.

## Method

`scripts/palimpsest-scale-probe.sh` seeded a reserved synthetic tenant/subject
scope inside one PostgreSQL transaction, generated one attributable episode and
100,000 fact revisions plus their governed lexical projections, analyzed the
four relevant relations, ran the authorization/temporal/current-revision
lexical retrieval core 20 times serially, captured an `EXPLAIN (ANALYZE,
BUFFERS, FORMAT JSON)` plan, and rolled the transaction back. A post-run scope
check found zero retained revisions.

The probe is deliberately narrower than the public release gate: it measures a
direct database lexical query, not HTTP overhead, vector retrieval, embedding
generation, concurrency, cost, cold-start behavior, provider durability, or a
million-revision run.

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
transaction, and its output contains only counts, timings, and a plan digest.
