# 018 — Accelerated temporal conformance

## Status

Active. Finalized 2026-08-08 (GitHub issue #48). Founder approved
2026-08-09.

## Owner

Agent lane: palimpsest-conformance, palimpsest-server tests.

## Purpose

The temporal conformance gate proves bounded projection leases, deletion
retry backoff, and receipt and checkpoint expiry. These proofs wait on
wall-clock time today. The lifecycle test floor is about 85 seconds. This
spec defines acceleration techniques for verification. The techniques keep
every correctness claim. The product does not change.

## Requirements

R1 [scratch scope]. Acceleration MUST apply only to scratch databases that
the tests create and destroy. Acceleration MUST NOT change production
migrations, seeded defaults, CHECK bounds, or the HTTP contract.

R2 [policy shrink]. The conformance suite MAY set the projection lease
policy to any values that the schema CHECK bounds allow. The minimum is a 5
second lease with a 1 second renewal interval. The suite MUST disable the
immutability trigger, update the policy row, and re-enable the trigger.

R3 [seed proof]. The suite MUST verify the seeded policy values of 60 and 20
seconds on a clean migrated database. The suite MUST verify that the
immutability trigger rejects policy mutation. These two checks MUST complete
without wall-clock waits.

R4 [deadline rewind]. Some claims have this shape: nothing happens before a
deadline, and the expected action happens after it. For these claims the
suite MAY rewind the stored deadline instead of waiting. Allowed rewind
targets are the projection generation lease expiry, the deletion retry_at
timestamp, and receipt or checkpoint expiry timestamps. The suite MUST
assert both sides of the deadline. The probe before the deadline MUST show
no action. The probe after the deadline MUST show the expected action.

R5 [minimal test durations]. The tests pass some durations themselves, such
as deletion lease seconds. These durations MUST be the smallest values that
keep the ordering margins of the scenario.

R6 [frozen evidence]. The accelerated suite MUST produce the frozen evidence
artifacts byte for byte. The retrieval corpus predictions MUST verify
unchanged.

R7 [content-free]. Acceleration MUST NOT add memory content to logs,
metrics, probes, or reports.

R8 [safety margin]. A rewind-based check MUST keep a positive margin between
the rewound deadline and the probe. The margin MUST be at least 100
milliseconds.

## Acceptance criteria

AC1 — seed and immutability without waits.
Given a fresh migrated scratch database
when the suite checks the projection lease policy
then the row reads 60 lease seconds and 20 renewal seconds
and an update of the policy row fails with the immutability error
and both checks complete without wall-clock waits.

AC2 — accelerated renewal observation.
Given a scratch database with the policy set to 5 and 1 seconds
when a projection provider holds a claim across one renewal interval
then the suite observes a lease renewal within 3 seconds of wall-clock time
and a second coordinator makes no claim while the lease holds.

AC3 — expiry handoff by rewind.
Given a projection claim with its lease expiry rewound into the past
when the coordinator rebuilds pending projections
then the expired claim is reclaimed
and the row reaches the ready state with a new lease.

AC4 — deletion backoff by rewind.
Given a deletion operation in retry_wait with retry_at rewound into the past
when the deletion worker runs once
then the operation advances
and the same worker run before the rewind makes no progress.

AC5 — receipt expiry by rewind.
Given a retrieval receipt with its expiry rewound into the past
when the suite reads the receipt
then the receipt reports the abstained state with no items
and the response does not contain the hidden probe value.

AC6 — gate budget.
Given the accelerated suite
when the bitemporal lifecycle conformance test runs
then the explicit test sleeps sum to at most 10 seconds
and all frozen evidence artifacts verify unchanged
and the test passes.

AC7 — production invariants.
Given the merged change that implements this spec
when the repository is compared with the state before the change
then migrations/, api/openapi.yaml, and the runtime crates show no change to
seeded values, CHECK bounds, trigger definitions, or the HTTP contract.

## Out of scope

- Virtual clocks or time simulation inside PostgreSQL.
- Runtime configuration knobs for lease durations or backoff values.
- Changes to the frozen retrieval corpus digests.
- Changes to deletion fence semantics or subject lifecycle states.
- Sub-second retention policy vocabulary.

## Resolved questions

1. Receipt expiry reads a stored timestamp. Retention lives in
   `memory.fact_revision_governance.retention_expires_at`. Rewind works.
2. Checkpoints store `expires_at` in `memory.checkpoint_revisions`. Rewind
   works. The retention interval CHECK bound stays untouched.
3. The deletion lease scenario keeps one short real-time wait as live expiry
   evidence. The retry backoff uses rewind.

## Links

- GitHub issue #48
- [ADR 0010 — bounded projection leases](../../docs/decisions/0010-bounded-projection-leases.md)
- [ADR 0007 — deterministic temporal retrieval](../../docs/decisions/0007-deterministic-temporal-retrieval.md)
- [Migration 0014 — bounded projection leases](../../migrations/0014_bounded_projection_leases.sql)
- [Migration 0010 — deletion operations](../../migrations/0010_deletion_operations.sql)
- [Spec 010 — operations and evidence tooling](../010-operations/spec.md)
