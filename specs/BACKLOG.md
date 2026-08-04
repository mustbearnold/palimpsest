# Backlog

Known-but-unspecced capabilities and gaps. One line each. Promote to a
`specs/NNN-<slug>/spec.md` when a capability is scheduled.

- Valkey/Redis optional hot cache (checkpoints, locks, recent retrievals) with
  loss-safety evidence; cache loss must never erase memory.
- Provider-managed backup/PITR orchestration, independent backup disposition,
  and full restore suppression against a real backup provider.
- External identity and credential rotation; public procedure/artifact APIs.
- Multi-region active-active writes and a hosted control plane (needs a
  consistency decision).
- Million-revision latency, throughput, cost, capacity, availability, and SLA
  evidence (the 100,000-revision coverage-gated profile still misses the
  proposed p95 ≤ 200 ms / p99 ≤ 400 ms gate).
- Concurrent and cold-cache retrieval evidence at scale.
- Durable server-side consolidation jobs with retryable claims (today the
  client coordinates per-claim sequences).
- Automatic model-driven semantic project diffs, conflict explanations, and
  promotion of raw session messages into higher-level facts (external
  interpretation only today).
- Provider-specific artifact/object deletion revocation, outage, and recovery
  evidence.
- Embedded/single-user mode for offline agents with the same domain semantics.
