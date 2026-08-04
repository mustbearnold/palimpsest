# ADR-0008: Scoped export and deletion use durable operations and a monotonic fence

Status: accepted

Date: 2026-07-29

## Context

Palimpsest must let an authorized principal export a subject's canonical memory
history and request deletion of that subject scope. Both operations cross more
than one storage class and can outlive an HTTP request. Export must preserve
temporal history and provenance without leaking another scope. Deletion must
stop new disclosure immediately, survive partial failure, remove canonical and
derived data, revoke external access, and retain only the minimum evidence
needed for idempotency and safe restore.

The current service has tenant- and subject-scoped episodes, checkpoints, fact
revisions, retrieval projections, and durable receipts. ADR-0005 already makes
fact lifecycle monotonic from `active` through `deletion_pending` to `deleted`
and requires receipt replay to reauthorize canonical content. It also makes v1
retrieval receipts and manifests immutable and indefinitely retained until an
explicit policy migration defines otherwise. That fact-level state and
retention rule are not a subject-wide deletion boundary: they do not coordinate
in-flight responses, checkpoints, episodes, export objects, caches, or a
restore from a pre-deletion backup.

This ADR is the explicit subject-deletion policy migration anticipated by
ADR-0005. It preserves indefinite receipt retention for every other lifecycle
and deletes receipts and manifests only after an authorized subject-wide fence
is durable. The deletion tombstone preserves evidence that the scope was
purged, not the subject's individual historical retrieval activity.

PostgreSQL deletion is logical before vacuum and point-in-time recovery can
restore an earlier cluster state. HTTP `202 Accepted` also means work may later
fail; it cannot represent completion. The public contract therefore cannot
claim immediate physical media overwrite, synchronous completion, or deletion
from isolated backups. The supporting primary-source analysis is recorded in
[`2026-07-29-export-deletion-lifecycle.md`](../research/2026-07-29-export-deletion-lifecycle.md).

MemoryService executes a controller-approved operation. It does not determine
lawful basis, verify real-world identity, decide legal holds or rights-of-others
redaction, or declare that a general canonical-history package satisfies a
particular legal request.

## Decision

### Keep one deep subject-lifecycle boundary

MemoryService owns export and deletion through one subject-lifecycle
application capability. HTTP handlers authenticate, parse the versioned
request, and delegate; they do not issue deletion SQL, reconstruct grants, or
coordinate storage effects.

The application capability depends on:

- a `SubjectLifecycleRepository` port that owns lifecycle locking, operation
  receipts, idempotency, content leases, manifests, target ledgers, tombstones,
  and the transactions that join them;
- an `ExportPackageStore` port that stages, atomically publishes, opens,
  revokes, probes, and expires immutable packages; and
- closed cache and artifact target capabilities whose configured adapters can
  revoke, delete, and prove absence idempotently.

PostgreSQL is the mandatory implementation of the lifecycle repository. Every
subject lifecycle, operation, manifest, target, status, and tombstone relation
uses forced row-level security and the normal `NOSUPERUSER NOBYPASSRLS` runtime
role. Migration and owner roles never serve requests. External adapters receive
opaque target handles from the application capability, not memory payloads or
authorization policy.

This boundary replaces endpoint-specific lifecycle logic. A new storage class
cannot participate in export or deletion by adding a handler branch; it must
implement the relevant target capability and its conformance contract.

### Resolve operation grants before existence disclosure

Trusted authentication and controller policy resolve closed operation grants:

- `canonical_history_export`; and
- `subject_delete` under a versioned deletion policy.

Tenant and subject path values select a scope but grant no authority. Request
bodies may select only an allowed export profile or server-registered deletion
policy; they cannot assert lawful basis, waive a hold, widen sensitivity, or
mint a grant. Authorization occurs before the service reveals whether the
subject, operation, manifest, or object exists.

Absent, hidden, and cross-tenant resources use the same redacted `404`
contract. Rejected requests disclose no hidden counts or state. Every worker
claim re-establishes the trusted tenant, subject, principal or worker-policy,
and target context before forced RLS permits access.

### Expose durable asynchronous resources

Version 1 adds these resources:

```text
POST /v1/tenants/{tenant_id}/subjects/{subject_id}/exports
GET  /v1/tenants/{tenant_id}/subjects/{subject_id}/exports/{export_id}
GET  /v1/tenants/{tenant_id}/subjects/{subject_id}/exports/{export_id}/content

POST /v1/tenants/{tenant_id}/subjects/{subject_id}/deletions
GET  /v1/tenants/{tenant_id}/subjects/{subject_id}/deletions/{deletion_id}
```

Both POST requests require `Idempotency-Key`. A content-free reservation binds
the caller-key digest to tenant, principal, subject, operation kind, profile or
policy, schema version, and request fingerprint. An identical retry returns the
same operation. Reuse with any different bound field returns `409` and performs
no mutation.

Creation commits the operation before returning `202 Accepted`. The response
contains `Location`, a small status representation, and `Cache-Control:
private, no-store`. Status resources have strong ETags and support
`If-None-Match`; an unchanged authorized representation may return `304`.
Closed OpenAPI `oneOf` variants own each state and reject unknown fields.

A ready export status returns `303 See Other` to the separately authorized
content resource. An expired authorized content resource returns `410 Gone`.
Hidden content remains the redacted `404`, including after expiration.
Deletion status stays a `200` representation in terminal states because its
content-free tombstone is the result.

Unimplemented paths are not added to the public OpenAPI. Each implementation
child adds its operation and every reachable success and error response only
when runtime conformance exists.

### Capture an immutable authorized export membership

The first and only version 1 profile is
`palimpsest-canonical-history-v1`. It is a portable engineering export, not an
alias for a GDPR Article 15 access response or Article 20 portability response.
Future legal-rights profiles require separate controller policy and names.

Export creation authenticates and authorizes the principal, sets forced-RLS
context, and locks the subject lifecycle. The subject must be `active`. In one
short serializable transaction, the repository selects authorized canonical
members and commits their deterministic ordered identifiers and digests with
the operation and idempotency reservation. The manifest is forced-RLS,
subject-scoped data and contains no memory payload.

A worker streams immutable members in `(record_kind, recorded_at, id)` order to
private staging while computing byte lengths and SHA-256 digests. A missing
member, changed source digest, lost authorization, or non-active lifecycle fails
closed and publishes nothing. The worker reauthorizes and rechecks lifecycle
immediately before atomic publication. Every content request repeats those
checks and holds a subject content lease for the response.

If the worker's trusted authorization lookup no longer grants the persisted
operation, or the subject fence prevents a content lease, the worker records a
sanitized terminal failure (`authorization_revoked` or `lifecycle_revoked`),
clears its worker lease, and publishes no package. Lease-cleanup failures are
reported through a stable service-unavailable class rather than embedding
repository or provider error text in an `Invalid` response.

The package is an integrity-checked ZIP whose semantic files are UTF-8 JSON or
NDJSON:

```text
manifest.json
schema/palimpsest-canonical-history-v1.schema.json
records/episodes.ndjson
records/checkpoints.ndjson
records/fact-revisions.ndjson
records/procedures.ndjson
records/artifact-references.ndjson
artifacts/...
processing-context.json
README.txt
```

The manifest distinguishes supported-empty, unsupported, and omitted-by-policy
record classes. It records the format and schema, exact scope, profile,
snapshot coordinates, generation time, policy versions, ordered files, byte
lengths, SHA-256 digests, and record counts. Each record is a self-describing
envelope with origin classification (`provided`, `observed`, `derived`, or
`system`), scope, temporal coordinates, governance, provenance, relations, and
semantic payload.

The package does not contain embeddings, `tsvector` values, cache entries,
internal leases, authorization secrets, raw audit logs, credentials, opaque
database metadata, or unrelated scopes. A deletion fence revokes the operation
and removes staging and ready objects. A revoked operation cannot materialize
again under another identifier through idempotent replay.

### Fence new disclosure before purge

Subject lifecycle is monotonic:

```text
active -> deletion_pending -> deleted
```

Every content-producing current, as-of, history, receipt-replay, cursor,
retrieval, export, or artifact path obtains a bounded subject-scoped content
lease under the same lifecycle lock. A lease identifies the operation and
expiry but stores no response content. An `active` subject can grant a lease;
`deletion_pending` and `deleted` cannot.

Deletion creation first authenticates, resolves the trusted delete grant and
policy, and locks the exact subject. One serializable transaction commits:

- `deletion_pending` subject lifecycle;
- the `draining` operation;
- the content-free idempotency reservation;
- the durable target ledger;
- the tombstone seed;
- the privacy-safe audit seed; and
- the worker outbox intent.

Only then may the server return `202`. `draining` blocks new leases and waits
for existing leases to end or be durably revoked. Bytes delivered before the
request cannot be recalled. A signed external capability that cannot be
revoked has a bounded lifetime; deletion cannot pass `fenced` until it expires
and an access probe fails.

The deletion operation is monotonic:

```text
draining -> fenced -> purging -> verifying -> completed
    |          |         |           |
    +----------+---------+-----------+-> retry_wait -> purging
    |                                         \
    +-----------------------------------------> failed
```

- `draining` proves no new content lease can start.
- `fenced` proves no new or in-flight lease can return subject content.
- `purging` runs idempotent target deletion while the fence remains durable.
- `retry_wait` records a sanitized transient class, attempt, and retry time.
- `verifying` runs independent negative queries and target probes.
- `completed` proves every configured live target and negative postcondition.
- `failed` means retries were exhausted or an invariant failed; the subject
  remains fenced and attributable operator repair resumes the same operation.

There is no public cancellation, no transition to `active`, and no replacement
operation that hides attempts. Policy refusal, legal hold, absent authority, or
invalid scope is decided before an operation exists.

### Purge in a fixed, capability-aware order

After `fenced`, workers:

1. revoke export downloads, signed artifact capabilities, and other access;
2. delete caches, retrieval projections, embeddings, export manifests and
   packages, and other derived state;
3. delete the subject's immutable retrieval manifests and receipts under this
   ADR's explicit exception to ADR-0005; no per-retrieval replacement remains;
4. hard-delete canonical and provenance rows in foreign-key-safe order;
5. delete prior subject-scoped write-operation, idempotency, and audit rows,
   then reduce only the deletion operation's rows to the tombstone allowlist;
6. independently query every current, as-of, history, exact, lexical, vector,
   replay, cursor, cache, export, and artifact surface before completion.

Targets use a unique `(deletion_id, target_class, target_key_digest)` identity
and monotonic `pending -> leased -> done` state. Workers use renewable leases
and record a completed external effect before claiming another. Repeating an
already absent delete or revoke is success. Provider error text is never
persisted; errors are classified as `retryable_dependency`,
`retryable_serialization`, `permanent_configuration`, or
`invariant_violation`.

When an external target effect fails after its target failure is durably
recorded, the worker releases its operation lease before returning the error.
The subject remains fenced and the target remains retryable, so another worker
can reclaim the same operation immediately rather than waiting for lease
expiry. If lease release also fails, the worker reports both failures and the
operation remains recoverable through the normal lease-expiry path.

Every supported target capability reports `verified`; unavailable optional
adapters report `not_configured`. `completed` is capability-scoped and cannot
claim an unimplemented provider. A failed configured target prevents
completion even when public access was already revoked.

### Retain a minimum content-free tombstone

The terminal tombstone may retain only:

- opaque deletion operation ID, tenant ID, and versioned HMAC scope digest;
- idempotency-key digest and request fingerprint;
- accepted, fenced, and completed or failed timestamps and state version;
- deletion-policy, contract-schema, worker-release, and scope-digest-key
  versions;
- coarse target completion bits, verification digest, and sanitized outcome or
  error class; and
- backup policy ID, deletion watermark, and earliest declared backup expiry.

It never retains the raw subject ID, memory IDs, values, provenance, queries,
vectors, object paths, signed URLs, credentials, per-table row lists, request
body, or deleted-content hashes. The HMAC digest remains sensitive linkable data
with an explicit purpose, authorization policy, and retention period. It is not
treated as anonymous.

The tombstone's aggregate negative-verification digest proves the named target
classes passed absence checks under the pinned verifier and policy versions. It
does not preserve or reconstruct deleted retrieval receipts, manifests,
queries, result identifiers, or event-level audit history. This loss of
subject-specific historical retrieval evidence is intentional: retaining it
would violate the scoped deletion outcome and the tombstone allowlist.

Logs, traces, metrics, problem responses, and worker errors contain only
operation IDs, target-class enums, attempt counts, durations, and sanitized
classes. Ordinary observability output is never the audit tombstone.

### Separate live purge from isolated backup disposition

`completed` has two explicit dispositions:

- `live_disposition: purged_and_verified`; and
- `backup_disposition: isolated_until_expiry` with the backup policy, deletion
  watermark, earliest declared expiry, and restore-gate version.

Completion does not claim immediate physical overwrite of heap pages, WAL,
replicas, snapshots, or backup media. Backups and WAL are encrypted,
access-restricted, not directly queryable, and expire under a documented
operator policy.

A current authenticated deletion-fence ledger exists independently of any PITR
target time. Each backup records its included deletion watermark. Restore
always enters a network-isolated quarantine role, verifies the current ledger,
applies every later tombstone, reruns purge and negative conformance, rebuilds
derived indexes, and advances to the current watermark before it may serve.
Missing, stale, corrupt, or unverifiable ledger input fails readiness closed.

Issue #5 proves this restore-suppression behavior. Issue #6 reuses the evidence
but still owns broader recovery, scale, latency, cost, release, and first
production-deployment gates. Key custody, backup retention, and controller
policy are deployment inputs and cannot be guessed by the service.

## Consequences

- A deletion request stops new disclosure when the durable `draining` fence
  commits, while completion remains honest about in-flight leases, external
  effects, PostgreSQL MVCC, and isolated backups.
- Export preserves canonical temporal history and provenance in a documented
  format without turning a database dump or derived index into the public
  contract.
- One lifecycle application boundary localizes authorization, transactions,
  retries, leases, tombstones, and adapter capabilities. The deeper interface
  costs more initial implementation but prevents five endpoint-specific policy
  paths from diverging.
- Durable manifests, target ledgers, and tombstones add protected metadata.
  Their schemas and retention are therefore privacy and security boundaries,
  not general observability tables.
- Content leases bound what deletion can prove about concurrent downloads.
  Providers without revocation must expose bounded capabilities and absence
  probes or remain unsupported.
- A PostgreSQL-only deployment may prove cache and artifact targets
  `not_configured`; it cannot claim provider deletion it did not execute.
- The complete conformance gate must run through the versioned HTTP API over
  real TCP and PostgreSQL under forced RLS. It covers cross-tenant
  non-disclosure, grant and lifecycle revocation, deterministic export,
  concurrency, fault injection after every durable transition and external
  effect, same-operation retry, stale receipt non-resurrection, tombstone
  allowlisting, privacy canaries, and pre-deletion restore suppression.
- Passing that gate establishes the named development profile only. It does
  not establish legal compliance, physical or cryptographic erasure, external
  provider guarantees, production readiness, or a production release.
