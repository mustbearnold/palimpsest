# 001 — Memory Service core

Status: active
Owner: AI CEO (operating) · human founder (constitutional)

## Purpose

The public behavioral boundary (`MemoryService`) for writing, reading,
retrieving, superseding, exporting, and deleting memory. Durable canonical
memory is structured data in PostgreSQL plus pgvector; embeddings and derived
indexes are reproducible representations, never the source of truth.

## Requirements

- R1. Every durable memory MUST carry tenant/subject scope, provenance,
  temporal metadata, sensitivity, retention state, and schema version.
- R2. Episodes MUST be immutable, timestamped observations (messages, tool
  results, actions, outcomes); corrections append new attributable events.
- R3. Facts and procedures MUST be revision chains recording observed time,
  recorded time, valid-time interval, confidence, sensitivity, provenance, and
  an optional superseded revision.
- R4. The service MUST support current view and bitemporal as-of queries by
  valid time and recorded time; as-of queries MUST NOT return revisions that
  were invalid at the requested time.
- R5. Supersession MUST link a newer revision to the older revision it
  replaces without deleting history.
- R6. Overlapping active validity intervals for the same scoped fact key MUST
  be rejected unless the domain explicitly allows multiple simultaneous
  values.
- R7. Checkpoints MUST be durable snapshots or deltas that allow an
  interrupted thread to resume without replaying successful side effects, and
  MUST be scoped by tenant, subject, agent, and thread.
- R8. Raw episodes MUST remain distinguishable from derived facts and
  summaries.
- R9. No model output MAY become durable memory without an attributable write
  policy.
- R10. Large tool outputs MUST be storable as integrity-checked artifact
  references; PostgreSQL stores URI, content hash, media type, byte size,
  policy, provenance, and retention state.
- R11. The versioned HTTP contract MUST cover checkpoint, episode, fact,
  procedure, artifact, retrieval, history, export, and deletion operations and
  MUST be described by `api/openapi.yaml`.

## Acceptance criteria

- [ ] A1. The tracer-bullet conformance scenario passes: write an episode,
      derive a fact, supersede it with newer evidence, retrieve the current
      revision, and reconstruct both valid-time and recorded-time history.
- [ ] A2. The tenant-isolation conformance scenarios pass: records from
      different tenants never share an authorization or retrieval candidate
      set through the public API.
- [ ] A3. The checkpoint interruption/resume and idempotent retry conformance
      scenarios pass, including crash between side effects.
- [ ] A4. The full MemoryService conformance suite (`conformance_postgres18.rs`)
      passes against PostgreSQL 18 with pgvector 0.8.5.

## Out of scope

- Training or hosting foundation models; workflow engines, chat applications,
  or prompt-management UIs.
- Treating vector similarity, graph inference, or model confidence as
  authority.
- A dedicated graph or vector database, or Kafka, in the initial release.
- Multi-region active-active writes and unlimited transcript retention.
- An embedded SQLite implementation in the first vertical slice.

## Open questions

- None. Domain vocabulary is defined in `specs/constitution.md`.

## Links

Code: `crates/palimpsest-domain` · `crates/palimpsest-postgres` ·
`crates/palimpsest-http` · `crates/palimpsest-server`
Tests: `crates/palimpsest-server/tests/conformance_postgres18.rs`
Decisions: 0001, 0004, 0007, 0009
Contract: `api/openapi.yaml`
