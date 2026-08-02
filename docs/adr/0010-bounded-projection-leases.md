# ADR-0010: Observable bounded projection leases

Status: accepted

Date: 2026-08-02

## Context

Embedding projection generation calls an external provider outside the
PostgreSQL transaction. A fixed age check on `generation_started_at` could
reclaim a live provider call, duplicate work, and potentially create duplicate
billable requests. The claim must be bounded without relying on a hidden SQL
interval.

## Decision

Projection reclamation uses the migration-owned immutable
`memory.embedding_projection_lease_policies` registry. Version `v1` exposes a
60-second claim lease and a 20-second renewal interval. Each `generating`
projection stores `generation_lease_expires_at`; a worker may return a claim to
`pending` only when that timestamp is absent or expired. Claiming assigns a
fresh attempt token and lease expiry, and terminal success or failure clears
the expiry. The attempt token remains the finalization fence, so an abandoned
worker cannot overwrite a later claim.

The policy row is readable by runtime workers and immutable after migration.
While provider I/O is active, the coordinator renews the matching attempt at
the recorded renewal interval. Renewal stops when the attempt no longer owns
the claim or when the subject fence prevents a new scoped transaction.

## Consequences

- Reclamation timing is explicit, queryable, and reviewable rather than hidden
  in a hard-coded age comparison.
- A live claim is not reclaimed before its configured expiry, while an expired
  claim remains recoverable without waiting fifteen minutes.
- Lease expiry is operational metadata only; embeddings remain derived and
  source/profile/input digests remain the authority.
- Provider work still has the existing bounded subject content lease; this ADR
  does not claim provider latency, cost, or production readiness.
