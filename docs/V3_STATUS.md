# Palimpsest v3 development status

Date: 2026-08-03

Status: active v3 development on the sole local and remote `main` branch. This
is not an official release or a production-readiness claim.

## Working

- PostgreSQL 18 plus pgvector is the canonical temporal memory store. Episodes,
  fact-revision chains, checkpoints, provenance, retention, sensitivity, and
  authorization are durable and tested.
- Current and as-of retrieval applies authorization and temporal filters before
  lexical or exact-vector candidate generation. Durable receipts preserve the
  policy and provenance needed to explain a result.
- Checkpoints, export operations, scoped deletion, projection leases, and
  restore-fence replay have HTTP/PostgreSQL conformance over the local
  development profile.
- The dependency-free Python and TypeScript clients use the governed HTTP
  boundary, including checkpoint, export, deletion, per-project recall, and
  structural comparison helpers.
- Codex, Claude Code, and Hermes user/assistant text can be ingested with
  resumable cursors, idempotent writes, common credential redaction, stable
  project identities, and exact project namespaces.
- The local MCP adapter exposes `palimpsest_recall_by_project` and
  `palimpsest_compare_by_project`, which give an agent separate authorized
  bundles plus deterministic exact-key/value-digest review candidates.
- `watch --discover` checks the conventional current-user stores, and the
  optional Linux systemd user service can supervise that watcher continuously.
- A guarded PostgreSQL custom-format logical backup rehearsal can restore into
  an isolated empty database and compare content-free schema, extension,
  migration, and selected row-count probes.
- An optional S3-compatible export package store is wired behind the same
  contract as the private filesystem store, with SigV4 signing, conditional
  publication, retry comparison, and delete-already-absent semantics.

## Somewhat working

- Multiple-project understanding now has a deterministic structural layer:
  each project gets its own namespace, the Python, TypeScript, and MCP helpers
  return one retrieval bundle per project, and comparison groups exact keys by
  canonical value digest. Same-key/different-value groups are review
  candidates, not semantic conflict conclusions; bounded token-overlap hints
  also connect differently keyed session messages for agent review and include
  a bounded shared/only-in token delta. The result also carries observed
  project-root, branch, source, role, and unique-session context labels.
- The ingestion adapters handle the observed local Codex, Claude Code, and
  Hermes seams, but they are not provider APIs, native hooks, or a universal
  transcript parser. Tool rows, private thinking, system prompts, and tool
  results are deliberately excluded.
- Export packages use a private local filesystem store by default, while the
  S3-compatible adapter is contract-tested against a local object-shaped
  fixture. A live provider's durability, deletion, outage, and recovery
  behavior is not yet evidenced.
- A rollback-only scale probe is repeatable and content-free. Its first
  100,000-revision local profile measured p95 3.857 seconds and p99 3.923
  seconds, so it is a useful baseline but currently misses the proposed
  release latency target.
- Restore work proves database-copy replay and logical dump/restore. It does
  not prove base-backup/WAL/PITR recovery, backup expiry, or production RPO/RTO.
- The default server embedding provider is unavailable; exact and lexical
  retrieval remain the correctness path, while an external embedding provider
  is an integration boundary rather than a hidden fallback.

## Not working yet

- Automatic semantic project diffs, conflict explanations, or model-driven
  consolidation of raw session messages into higher-level facts. The current
  key and lexical comparison deliberately stops before those claims.
- Valkey/Redis cache plus provider-specific artifact/object deletion,
  revocation, outage, and recovery evidence.
- Provider-managed backup/PITR orchestration, independent backup disposition,
  and full restore suppression against a real backup provider.
- Million-revision latency, throughput, cost, capacity, availability, and SLA
  evidence; the measured 100,000-revision baseline currently misses the
  proposed release latency target.
- External identity and credential rotation, public procedure/artifact APIs,
  multi-region writes, hosted control plane, and official production release
  gates.

## Next v3 frontier

The next high-value slices are deterministic object/cache fault injection,
query-plan/index remediation followed by concurrent million-revision evidence,
and a governed semantic interpretation/consolidation boundary that keeps
every durable write attributable. v3 is only honest when those remaining
boundaries are either implemented with evidence or clearly retained as
non-claims.
