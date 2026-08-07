# Release gate

First evidence-backed release gate (issue #6). One reproducible procedure that
proves temporal correctness, tenant isolation, recovery, and retrieval quality
before any production-readiness claim. Runs locally with the same commands the
CI workflow runs; a release is only "gate-green" when every item below has a
fresh, cited result.

## Gate criteria

1. **Conformance**: the complete public MemoryService conformance suite passes
   against PostgreSQL 18 + pgvector 0.8.5 (CI runs it; locally:
   `cargo test --locked --workspace` with the env recipe in the
   `palimpsest-development` skill). This covers bitemporal lifecycle,
   authorized retrieval, subject deletion, export, restore, crash recovery,
   and governed writes.
2. **Migration hygiene**: migrations must run as the runtime role — the owner
   of the `memory` objects (migrations contain no GRANT statements; a role
   that does not own the objects it migrated cannot be migrated over). A
   drifted database (any `memory` object owned by another role, e.g. an
   early migration once run as a superuser) breaks later migrations:
   `CREATE OR REPLACE` flips SECURITY DEFINER triggers to the migration
   role, whose writes into RLS-FORCE tables then fail with `permission
   denied`. Before migrating an existing database, run:
   `SELECT c.relname FROM pg_class c WHERE c.relnamespace='memory'::regnamespace AND c.relowner::regrole::text != 'palimpsest_runtime'`
   (tables/functions/indexes/sequences alike; zero rows expected) and
   re-own any drift to the runtime role. The live-stack deploy that hit
   this: `docs/decisions/0032` + the `palimpsest-development` skill's
   live-ops reference (2026-08-07).
3. **Retrieval quality report**: a versioned report records recall, precision,
   latency percentiles, and cost for the retrieval evaluation corpus
   (`evaluations/retrieval-corpus-v1/`, digest-pinned in its manifest).
   Latest: `_attic/evaluations/2026-07-29-retrieval-conformance-corpus.md`;
   scale evidence in spec 002 A4 (`_attic/evaluations/2026-08-03-authorized-lexical-scale-probe.md`).
4. **Backup and restore verification**: the current and as-of views survive
   recovery — covered by the restore conformance scenarios
   (`verify_restore_replay_is_hidden_over_http`,
   `verify_restore_corpus_is_visible_over_http`, logical-backup rehearsal
   script `scripts/palimpsest-logical-backup-rehearsal.sh`) and spec 005.
5. **Failure injection**: cache loss (restore/recovery), embedding-provider
   failure (provider-call cardinality conformance), process termination
   (crash-recovery scenario `recovers_a_committed_effect_after_response_loss`),
   and retryable consolidation failure (export/delete worker patterns). Each
   failure class has a named conformance scenario; none may be skipped for a
   release.
6. **Non-claims stated**: every unproven boundary is written into the release
   report. Known today (2026-08-07):
   - Million-revision retrieval latency: the A5 band-separated gate (amended
     2026-08-07, ADR-0032) is MET — 1/32 band p95 179.2 ms / p99 179.3 ms,
     1/16 band p95 391.0 ms / p99 391.4 ms at 1,000,000 revisions
     (`_attic/evaluations/2026-08-07-authorized-current-structure-scale-probe.md`);
     the all-match band is a documented bounded characteristic
     (p95 2.89 s, 6.7x faster than the 19.288 s cold baseline), not an SLA.
   - No throughput, cost, capacity, availability, or SLA evidence at the
     million-revision profile (non-claims, per #37 closing evidence).
   - Provider-managed backup/PITR orchestration: not yet (issue #38).
   - Multi-region active-active: deferred (consistency ADR first).
   - External identity/credential rotation: deferred until first production
     deployment.

## Bypass protection

- The gate cannot be bypassed by weakening tests or workflow conditions:
  failing acceptance criteria are reported as blockers, never edited out of a
  spec to complete a task (constitution: "Never weaken or delete a failing
  test to complete a task").
- A release report must cite the exact scenario/test names and measurement
  digests for every claimed item; an uncited claim is a non-claim.
- Independent review is an additional mandatory gate for first production
  deployment, a major release, or any security-sensitive or high-risk release
  (constitution, authority model).

## Procedure

1. Run the complete local gate (9 commands, skill `palimpsest-development`).
2. Re-run the conformance suite against the pinned PostgreSQL/pgvector image
   (CI does this on push; a release re-runs it explicitly).
3. Collect the retrieval evaluation report and scale digests from
   `_attic/evaluations/`.
4. Write the versioned release report (this document's criteria + measured
   numbers + non-claims), name it `_attic/evaluations/YYYY-MM-DD-release-gate.md`.
5. State every non-claim in the report. No SLA claim without A5 evidence.

## Links

Issue: #6 · Specs: 001 (service), 002 (retrieval + A5 gate), 005 (restore),
010 (operations) · Decisions: 0032 (latency remediation)
