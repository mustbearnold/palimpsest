# Product Specification: Palimpsest Temporal Memory Service

Status: ready for issue publication

Date: 2026-07-28

Decision owner: human founder

Operating owner: AI CEO

## Problem Statement

AI agents lose useful context across conversations, overfill their model context
with stale transcripts, and often treat embedding similarity as memory truth.
Flat memory systems cannot reliably answer which fact was current at a past
date, distinguish when an event happened from when it was learned, explain why
a memory exists, or replace a contradicted fact without erasing history.

Developers need one trustworthy memory boundary that supports crash-safe
short-term checkpoints and durable long-term episodic, semantic, and procedural
memory. It must favor relevant recent information without discarding months-old
evidence, enforce tenant and subject isolation before retrieval, and remain
operable by a small team or autonomous engineering agent.

## Solution

Palimpsest will be a self-hostable temporal memory service for AI agents. It will
store canonical memory in PostgreSQL, add pgvector and PostgreSQL full-text
indexes for hybrid retrieval, and expose a versioned HTTP API. Every durable
memory will carry provenance, authorization scope, sensitivity, observed and
recorded times, valid-time bounds, retention state, and schema version.

The service will separate immutable episodes from versioned facts and
procedures. New revisions supersede old revisions without destroying them.
Current queries will favor current, relevant information; as-of queries will
reconstruct what was valid or known at a historical instant. Type-specific
retrieval policies will combine exact authorization and validity filters with
lexical relevance, vector similarity, importance, confidence, and temporal
decay. Large artifacts will remain in optional object storage with integrity
metadata in PostgreSQL.

The project will be operated by an AI CEO under the founder's explicit charter.
GitHub Issues will be the work source of truth, official Matt Pocock skills will
drive specification, decomposition, implementation, and independent review,
and direct linear commits on `main` plus executable gates will control delivery.

## User Stories

1. As an agent developer, I want to save thread checkpoints, so that an interrupted run can resume without repeating successful side effects.
2. As an agent developer, I want checkpoints scoped to tenant, subject, agent, and thread, so that unrelated conversations cannot leak into one another.
3. As an agent developer, I want to append immutable episodes, so that the original evidence remains auditable.
4. As an agent developer, I want to distinguish when an event occurred from when it was recorded, so that delayed observations do not corrupt chronology.
5. As an agent developer, I want facts to have valid-time intervals, so that the service can answer what was true at a particular date.
6. As an agent developer, I want recorded-time history, so that I can reconstruct what the system believed before later evidence arrived.
7. As an agent developer, I want a newer fact to supersede an older fact without deleting it, so that current retrieval and historical audit both remain correct.
8. As an agent developer, I want stable facts to decay slowly or not at all, so that identity and policy information is not displaced merely because it is old.
9. As an agent developer, I want active-task memories to decay quickly, so that stale plans do not dominate a new task.
10. As an agent developer, I want hybrid lexical and semantic retrieval, so that exact names and conceptually related memories are both discoverable.
11. As an agent developer, I want authorization and validity filters applied before similarity search, so that forbidden memories never become candidates.
12. As an agent developer, I want every retrieval result to include provenance and scoring explanations, so that I can understand why it was selected.
13. As an agent developer, I want retrieval policies to be versioned, so that behavior changes are reproducible and comparable.
14. As an agent developer, I want embeddings to record model and dimension versions, so that indexes can be rebuilt safely after model changes.
15. As an agent developer, I want raw canonical text preserved independently of embeddings, so that a failed or obsolete embedding model cannot erase memory.
16. As an agent developer, I want consolidation to be idempotent and attributable, so that retries do not create duplicate facts or unverifiable summaries.
17. As an agent developer, I want procedures and security rules selected by exact policy, so that vector similarity cannot choose which authority executes.
18. As an agent developer, I want large tool outputs stored as integrity-checked artifact references, so that the relational database stays bounded.
19. As an end user, I want to export my memories and their provenance, so that I can inspect or move my data.
20. As an end user, I want scoped deletion that also removes derived indexes, so that deletion is effective rather than cosmetic.
21. As a security engineer, I want tenant-isolation tests at the public API seam, so that a query cannot cross authorization boundaries through an index.
22. As an operator, I want retention policies and partition lifecycle controls, so that short-lived checkpoints do not grow without bound.
23. As an operator, I want backup and restore verification, so that durability claims are proven rather than assumed.
24. As an operator, I want retrieval latency, recall, and cost benchmarks, so that adding a dedicated vector database is an evidence-based decision.
25. As an operator, I want structured audit events for memory mutations and policy decisions, so that incidents can be investigated without exposing private content in logs.
26. As an SDK author, I want an OpenAPI-defined contract, so that Python and TypeScript clients remain behaviorally consistent.
27. As a local-agent developer, I want a future embedded mode, so that an offline single-user agent can use the same domain semantics without a server.
28. As the founder, I want the AI CEO to work from an explicit autonomy frontier, so that unattended work stays within specified, unblocked issues.
29. As the founder, I want credentials, spending, legal commitments, destructive production changes, and high-risk releases reserved for human approval, so that autonomy does not silently expand authority.
30. As a contributor, I want issue forms, contribution guidance, security reporting, and deterministic CI, so that public collaboration is safe and efficient.

## Implementation Decisions

- The system will have one public behavioral seam named `MemoryService`. Its
  versioned HTTP contract covers checkpoint, episode, fact, procedure, artifact,
  retrieval, history, export, and deletion operations.
- Rust will own deterministic domain rules, persistence coordination, temporal
  validation, authorization ordering, retrieval-policy execution, and the HTTP
  service. SDKs remain thin clients.
- PostgreSQL plus pgvector is canonical. PostgreSQL transactions keep a memory
  revision, provenance, authorization, audit record, and index-enqueue receipt
  consistent.
- A dedicated vector database is not part of the initial architecture. It may be
  introduced only after a benchmark demonstrates a named pgvector recall,
  latency, throughput, recovery, or cost failure.
- Valkey/Redis is an optional cache for active checkpoints, locks, and recent
  retrievals. Losing or evicting the cache must not lose durable memory.
- S3-compatible storage is optional for large immutable artifacts. PostgreSQL
  stores the URI, content hash, media type, byte size, encryption/access policy,
  provenance, and retention state.
- Episodes are append-only. Corrections create new attributable events rather
  than mutating the original evidence.
- Facts and procedures are revision chains. Each revision records valid time,
  recorded time, last confirmation, confidence, sensitivity, provenance, and an
  optional superseded revision.
- Overlapping active validity intervals for the same scoped fact key are rejected
  unless the domain explicitly allows multiple simultaneous values.
- Current retrieval first applies tenant, subject, namespace, kind, sensitivity,
  deletion, retention, and valid-time filters. Candidate generation then combines
  PostgreSQL full-text and pgvector search. Ranking may incorporate importance,
  confidence, type-specific temporal decay, access context, and a versioned
  reranker.
- Temporal decay affects ranking, not historical retention. Memory kinds have
  explicit decay policies; stable identity, contractual, and security facts may
  have no recency decay.
- As-of queries support valid-time and recorded-time cutoffs. The API makes the
  requested temporal perspective explicit instead of inferring it from prose.
- Embeddings are derived indexes. Each vector records the embedding provider,
  model, dimensions, normalization, content hash, and generation time.
- Consolidation runs asynchronously from durable jobs. Each result records its
  source episodes, prompt/policy version, model identity, content hash, and
  idempotency key.
- Memory deletion is a stateful workflow covering canonical records, derived
  indexes, caches, artifact references, and audit-safe tombstones. Backups follow
  documented retention and erasure limits.
- Logs and metrics use identifiers and redacted metadata by default. Raw private
  memory content is never a routine log field.
- The initial deployment is single-region and self-hostable. Multi-region
  active-active writes require a later consistency ADR.
- The AI CEO uses GitHub Issues as the source of truth, official Matt Pocock
  skills as the procedure layer, and direct linear commits on the sole `main`
  branch as the delivery seam. Local gates and push-triggered CI are mandatory;
  independent Standards and Spec review remains required for the risk and
  release classes that call for it.
- The remote `main` ruleset permits ordinary direct pushes while prohibiting
  force-pushes and branch deletion. The AI CEO does not create feature branches,
  extra worktrees, or pull requests for routine product work.

## Testing Decisions

- The highest test seam is one implementation-neutral MemoryService conformance
  suite executed through the public API. Internal modules may have focused unit
  tests, but release claims come from externally observable behavior.
- The first tracer-bullet scenario writes an episode, derives a fact, supersedes
  it with newer evidence, retrieves the current revision, and reconstructs both
  valid-time and recorded-time history.
- Conformance scenarios cover checkpoint interruption/resume, idempotent retry,
  tenant isolation, valid-time boundaries, late-arriving evidence, supersession,
  stable versus fast-decay memory, hybrid retrieval, deletion/export, artifact
  integrity, and embedding rebuilds.
- Property tests cover interval invariants, revision chains, monotonic recorded
  time, idempotency keys, and authorization-filter ordering.
- PostgreSQL integration tests run against the supported PostgreSQL and pgvector
  versions rather than mocks. Migration tests cover empty install, forward
  upgrade, rollback posture, and representative data volume.
- Retrieval evaluations use a versioned corpus with relevance judgments,
  temporal contradictions, exact-name queries, multi-tenant traps, and stale
  distractors. Reports include recall, precision, latency percentiles, and cost.
- Failure-injection tests cover process termination between side effects,
  consolidation retries, cache loss, embedding-provider failure, object-store
  unavailability, and restore from backup.
- Security tests prove that unauthorized memories do not enter candidate sets,
  response bodies, logs, traces, exports, or error details.
- Good tests assert public behavior and durable invariants. They do not freeze
  private function structure, incidental SQL formatting, or a particular ANN
  query plan.

## Out of Scope

- Training or hosting foundation models.
- Acting as a general workflow engine, chat application, prompt-management UI,
  or autonomous computer-use platform.
- Treating vector similarity, graph inference, or model confidence as authority.
- A dedicated graph database, dedicated vector database, or Kafka deployment in
  the initial release.
- Multi-region active-active writes, unlimited transcript retention, and
  unbounded automatic consolidation.
- Storing credentials, identity documents, private keys, or unrestricted raw
  production payloads as ordinary memories.
- An embedded SQLite implementation in the first vertical slice.
- Unsupervised modification of the AI CEO charter or release gates.

## Further Notes

- “Palimpsest” is a provisional project name, not a completed trademark
  clearance.
- The first release should prove correctness on one narrow vertical slice before
  adding SDK breadth, a UI, specialized stores, or advanced graph retrieval.
- The repository is public, but operational credentials, private evaluation
  corpora, customer memory, and security evidence remain outside Git.
- The initial GitHub specification issue will be labelled `ready-for-agent`; the
  first implementation tickets must preserve dependency edges and keep human
  authority work labelled `ready-for-human`.
