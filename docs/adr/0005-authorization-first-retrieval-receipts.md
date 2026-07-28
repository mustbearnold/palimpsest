# ADR-0005: Lexical retrieval persists manifests and reauthorizes content

Status: accepted

Date: 2026-07-29

## Context

Palimpsest needs a first retrieval resource that finds exact identities and
lexically related fact revisions without allowing forbidden records to enter a
candidate set. The resource must remain auditable and idempotent while current
authorization, sensitivity, retention, or deletion state can become stricter
after a receipt is created. Replaying serialized result content would resurrect
data that the current principal can no longer access.

Fact revisions already carry immutable tenant, subject, case, sensitivity,
retention-policy, bitemporal, provenance, and content-digest fields. They do not
yet carry resolved expiry, lifecycle, recency, or importance state, and there is
no reproducible lexical projection or durable retrieval receipt. The first
lexical slice must add those seams without making a derived index canonical or
freezing the later vector and reciprocal-rank-fusion implementation into the
public interface.

## Decision

Each fact revision has a canonical companion governance envelope. The envelope
records its retention policy and resolved expiry, lifecycle state, recency
profile, importance, and schema version. Existing and new revisions default to
the immutable `standard` indefinite-retention policy, `active` lifecycle,
`stable-v1` recency, and neutral importance. Previously used retention-policy
identifiers are registered during migration with their historical indefinite
behavior; the migration does not fabricate expiration dates. New unregistered
retention policies fail closed.

Lifecycle state has the monotonic seam `active` to `deletion_pending` to
`deleted`. Only `active`, unexpired revisions may be retrieved. Issue #5 owns
the public deletion workflow and completion proof; this ADR defines only the
state that retrieval must obey. Governance identity, retention, recency,
importance, and schema fields are immutable after creation.

Lexical search documents are derived rows keyed to immutable fact revisions.
Projection schema v1 uses PostgreSQL's explicit `pg_catalog.simple`
configuration, weights namespace and key lexemes as `A`, weights serialized
JSON values as `B`, and stores only the resulting `tsvector` plus source,
projection, and projection-schema digests. A GIN index accelerates full-text
matching. The projection is populated atomically by the fact-revision insert
trigger and backfilled for existing revisions. It can be deleted and rebuilt
from canonical facts without changing memory truth.

The immutable `retrieval-lexical-v1` policy fixes candidate and page limits,
the full-text configuration, rank function and normalization, fixed score
precision, exact identity precedence, and deterministic tie-breaking. Callers cannot supply
weights, candidate limits, rank functions, vectors, or authorization rules.
Adding vector candidates and reciprocal-rank fusion extends the implementation
and score-component representation without changing the retrieval resource.

A retrieval is a durable receipt resource. Its receipt stores scope and policy
identifiers, request and query digests, explicit valid-time and recorded-time
coordinates, an authorization-scope digest, stage timings, outcome, manifest
digest, and schema versions. Its ordered manifest stores only fact and revision
identifiers, opaque cursor tokens, deterministic ranks and fixed-point scores,
and content/projection/item digests. Neither table stores query text, fact
values, episode payloads, embeddings, credentials, or serialized HTTP response
bodies.

A separate content-free idempotency reservation is keyed by tenant, principal,
and caller key. It records only subject, fingerprint, and retrieval identity,
so cross-subject reuse is rejected consistently without weakening receipt RLS.
Repeatable-read serialization conflicts are retried a bounded number of times;
identical concurrent calls converge on the committed receipt.

Version 1 receipt and manifest records are immutable and retained indefinitely.
There is no receipt-expiry field, cleanup job, or receipt-deletion operation in
this version. Introducing finite retention later requires an explicit policy
migration that defines eligibility, preserves required audit evidence, and does
not silently reinterpret or partially remove existing manifests.

Receipt creation uses one repeatable-read transaction. Within that snapshot,
the repository first selects the effective bitemporal revision for each fact.
It then applies trusted tenant, subject, exact-filter, current sensitivity,
lifecycle, and retention predicates to that effective relation. Exact-identity
and full-text candidate generation run only against the resulting eligible
relation. Filtering revisions before bitemporal selection is forbidden because
it could make a hidden successor disappear and resurrect an older revision.

Every receipt read, idempotent POST replay, and cursor page reauthorizes the
receipt and each manifest item. PostgreSQL forced row-level security checks the
current tenant, subject, and principal. A security-barrier, security-invoker
manifest view additionally checks the current sensitivity allowlist, lifecycle,
retention expiry, and source-content digest. The adapter uses that view and
rehydrates canonical content in one read-only repeatable-read transaction.
A newly inaccessible item is omitted without replacement; it is never swapped
for an older or newer revision. Responses do not reveal how many manifest rows
were hidden. The response authorization digest is recalculated from the current
principal scope, while the durable receipt retains the creation-time digest for
audit.

Opaque cursors identify a manifest position, not a query or authorization
claim. The server may resolve a cursor even when its item has become hidden,
then returns only later items that still pass current authorization. Invalid,
cross-receipt, or cross-scope cursors fail with the same redacted not-found
behavior.

## Consequences

- Durable receipts remain useful audit evidence without becoming a private-data
  shadow store or bypassing later deletion, expiry, or revocation.
- Exact authorization and bitemporal ordering remain visible in one PostgreSQL
  query plan, while the public MemoryService interface stays small.
- Lexical projection failures can be repaired by rebuilding derived rows from
  canonical revisions and the pinned projection schema.
- The companion governance row adds one trigger-controlled insert to every fact
  revision transaction; a missing registered retention policy rejects the whole
  write rather than creating partially governed memory.
- Existing nonstandard retention identifiers retain indefinite behavior during
  backfill. Assigning them a finite duration later requires an explicit policy
  migration and must not retroactively invent an earlier expiry.
- Receipt and manifest rows are immutable and have indefinite retention in v1.
  Deletion removes their ability to rehydrate content through governance state;
  any later finite-retention policy requires an explicit migration rather than
  an undocumented cleanup path.
- The regular conformance gate must run PostgreSQL 18 with pgvector 0.8.5 under
  a `NOSUPERUSER NOBYPASSRLS` role and prove isolation, expiry, deletion without
  resurrection, temporal selection, pagination, replay, and deterministic
  ordering through the versioned HTTP interface.
