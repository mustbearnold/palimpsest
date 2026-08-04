# Backlog

Known-but-unspecced capabilities and gaps. One line each. Promote to a
`specs/NNN-<slug>/spec.md` when a capability is scheduled. Status markers:
`scheduled → #N` (tracked in the issue frontier) or `deferred: reason`
(intentionally parked; not a claim).

- [scheduled → #37] Million-revision latency, throughput, cost, capacity,
  availability, and SLA evidence (the 100,000-revision coverage-gated profile
  still misses the proposed p95 ≤ 200 ms / p99 ≤ 400 ms gate).
- [scheduled → #37] Concurrent and cold-cache retrieval evidence at scale.
- [scheduled → #38] Provider-managed backup/PITR orchestration, independent
  backup disposition, and full restore suppression against a real backup
  provider.
- [scheduled → #39] Valkey/Redis optional hot cache (checkpoints, locks,
  recent retrievals) with loss-safety evidence; cache loss must never erase
  memory.
- [scheduled → #40] Embedded/single-user mode for offline agents with the
  same domain semantics.
- [deferred: needs a consistency ADR first] Multi-region active-active writes
  and a hosted control plane.
- [deferred: until after the first production deployment] External identity
  and credential rotation; public procedure/artifact APIs.
- [deferred: until a live provider contract exists] Provider-specific
  artifact/object deletion revocation, outage, and recovery evidence.
