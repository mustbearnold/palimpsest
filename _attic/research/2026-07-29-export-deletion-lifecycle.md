# Export and scoped-deletion lifecycle

Status: research recommendation for GitHub issue #5

Research date: 2026-07-29
Decision horizon: first public development release through 2027

## Recommendation

Build export and deletion as durable, independently authorized operation
resources, not synchronous database commands.

An export first commits an immutable, authorized membership manifest, then
materializes a short-lived machine-readable package. A deletion first commits a
monotonic subject fence that immediately removes the scope from every live read,
historical read, retrieval candidate set, export, and artifact access path. A
worker then purges canonical rows, derived projections, caches, manifests, and
external objects, verifies negative postconditions, and leaves only a
content-free, retention-governed tombstone.

This is a product and engineering recommendation, not a legal-compliance
opinion. Palimpsest cannot determine whether a particular controller is subject
to the GDPR, whether an Article 17 exception applies, or which records qualify
for Article 20 without controller-supplied legal-basis and data-origin policy.
The public contract must describe what it does and must not label a general
canonical archive as a GDPR-compliant response.

## Scope and non-goals

This report covers the tenant-and-subject scope required by
[issue #5](https://github.com/mustbearnold/palimpsest/issues/5): canonical
episodes, checkpoints, fact revisions and their provenance; derived lexical and
vector projections; retrieval and idempotency artifacts; caches; export and
artifact objects; logs; and PostgreSQL backup/restore behavior. It assumes the
authorization-first and lifecycle invariants already accepted in
[`ADR-0005`](../../docs/decisions/0005-authorization-first-retrieval-receipts.md) and
[`ADR-0007`](../../docs/decisions/0007-deterministic-temporal-retrieval.md).

The first slice does not decide a controller's lawful basis, verify a data
subject's real-world identity, contact downstream controllers, implement legal
holds, guarantee physical media overwrite, or promise erasure from an old
backup before that backup's declared expiry. Those are explicit deployment or
controller-policy boundaries.

## Research method and confidence

Statements labeled **Source fact** report primary law, regulator guidance, or
maintainer specifications. **Recommendation** and **Inference** translate those
facts into testable Palimpsest behavior. Every external factual claim links to
the source that owns it.

Confidence is high for the live lifecycle, authorization ordering, HTTP status
resource, export encoding, PostgreSQL MVCC, and PITR boundaries. Confidence is
medium for the exact minimum tombstone and backup-retention wording because
applicable law, controller purpose, sector rules, and deployment topology vary.
Those policies must remain explicit inputs rather than hidden product guesses.

## Source findings

### Access, portability, erasure, minimisation, and accountability

**Source fact — access.** GDPR Article 15 distinguishes the right to obtain
confirmation and a copy of personal data from the accompanying information
about purposes, categories, recipients, retention, source, and certain
automated decision-making. The copy must not adversely affect the rights and
freedoms of others
([GDPR Article 15](https://eur-lex.europa.eu/eli/reg/2016/679/art_15/oj/eng)).
The EDPB says access concerns personal data being processed, including archived
data. Where a backup has more or different subject data than the live system,
the controller should be transparent and, where technically feasible, provide
the requested backup data
([EDPB Guidelines 01/2022, section 4.2.2](https://www.edpb.europa.eu/system/files/documents/2023-04/edpb_guidelines_202201_data_subject_rights_access_v2_en.pdf)).

**Inference.** A raw database dump is neither a safe access response nor a
Palimpsest export. It contains unrelated tenants, storage internals, and data
whose disclosure can affect other people. An Article-15-oriented integration
needs both an authorized data package and controller-provided processing
information; MemoryService alone does not own all of the latter.

**Source fact — portability.** Article 20 applies to personal data concerning
the subject and provided by the subject when processing is automated and based
on consent or contract. It requires a structured, commonly used,
machine-readable format and direct controller-to-controller transmission where
technically feasible; it does not erase the source data and must not adversely
affect others
([GDPR Article 20](https://eur-lex.europa.eu/eli/reg/2016/679/art_20/oj/eng)).
The EDPB-endorsed WP29 guidance includes actively supplied and observed data,
but excludes inferred and derived data from Article 20's scope. It recommends
granular metadata that preserves meaning, explains that interoperability is the
goal rather than mandatory system compatibility, and warns that PDF versions
of structured records are insufficient
([WP242 rev.01](https://ec.europa.eu/information_society/newsroom/image/document/2016-51/wp242_en_40852.pdf),
[EDPB endorsement](https://www.edpb.europa.eu/documents/guideline/guidelines-on-the-right-to-data-portability-under-regulation-2016679-wp242_en)).
The EDPB's current small-business guidance names JSON, XML, and CSV as common
machine-readable formats and says useful metadata must accompany the data
([EDPB rights guidance](https://www.edpb.europa.eu/sme/be-compliant/respect-individuals-rights_en)).

**Inference.** Palimpsest fact revisions are often inferred or derived. A full
canonical-history export can be portable in the ordinary engineering sense but
cannot truthfully be called an Article 20 portability response. The manifest
therefore needs an explicit export profile and an origin classification for
each record: `provided`, `observed`, `derived`, or `system`. Controller policy,
not the caller's request body, selects what a legal-rights workflow releases.

**Source fact — erasure.** Article 17 requires erasure without undue delay when
one of its grounds applies and also lists exceptions, including freedom of
expression and information, legal obligations or public tasks, public health,
certain archiving/research/statistical purposes, and legal claims
([GDPR Article 17](https://eur-lex.europa.eu/eli/reg/2016/679/art_17/oj/eng)).
The European Commission likewise describes erasure as a qualified rather than
absolute right and notes that appropriately anonymised data may be retained
([Commission erasure guidance](https://commission.europa.eu/law/law-topic/data-protection/information-business-and-organisations/dealing-requests-individuals/do-we-always-have-delete-personal-data-if-person-asks_en)).

**Inference.** MemoryService should execute an already-authorized deletion
policy, not infer whether an Article 17 ground or exception exists. A policy
refusal occurs before any deletion operation is created. Once a deletion fence
is committed, transient failures must never reactivate the subject.

**Source fact — minimisation and accountability.** Article 5 requires data to
be adequate, relevant, and limited to what is necessary; kept identifiable no
longer than necessary; protected appropriately; and processed so the controller
can demonstrate compliance
([GDPR Article 5](https://eur-lex.europa.eu/eli/reg/2016/679/art_5/oj/eng)).
Article 30's processing records include purposes, subject/data categories,
recipients, transfers, envisaged erasure time limits, and a general description
of security measures
([GDPR Article 30](https://eur-lex.europa.eu/eli/reg/2016/679/art_30/oj/eng)).
Pseudonymised data that can be re-associated remains personal data; it is not
equivalent to irreversible anonymisation
([Commission definition guidance](https://commission.europa.eu/law/law-topic/data-protection/information-business-and-organisations/application-gdpr_en)).

**Inference.** A keyed scope digest reduces disclosure in a tombstone but is
still treated as sensitive, linkable data. It receives explicit purpose,
authorization, and retention. Palimpsest should maintain its processing and
deletion inventory as an operational control regardless of whether a deployer
might qualify for an Article 30(5) derogation. The EU legislative procedure to
change that derogation was still active on the research date, so employee-count
thresholds must not be encoded into the product
([procedure 2025/0130(COD)](https://oeil.secure.europarl.europa.eu/oeil/en/procedure-file?reference=2025%2F0130%28COD%29)).

### PostgreSQL deletion, isolation, export, and recovery

**Source fact — MVCC and vacuum.** PostgreSQL statements see MVCC snapshots.
`UPDATE` and `DELETE` do not immediately remove old row versions because an old
version can remain visible to another transaction. Standard `VACUUM` later
removes dead versions and makes their space reusable, but generally does not
return it to the operating system; `VACUUM FULL` rewrites the table and requires
an `ACCESS EXCLUSIVE` lock
([MVCC introduction](https://www.postgresql.org/docs/18/mvcc-intro.html),
[routine vacuuming](https://www.postgresql.org/docs/18/routine-vacuuming.html#VACUUM-FOR-SPACE-RECOVERY)).

**Decision consequence.** `completed` proves that live queries cannot return the
scope and that governed rows and external objects have been logically purged.
It does not prove immediate physical overwrite of heap pages, WAL, storage
snapshots, or backup media. `VACUUM FULL` is not part of per-subject completion:
it is disruptive and still is not a general secure-media-erasure primitive.

**Source fact — transactions.** At PostgreSQL's default Read Committed level,
successive statements in one transaction can see different committed states.
Repeatable Read gives successive reads one stable snapshot but applications
must retry serialization failures; Serializable additionally rejects
non-serializable executions
([transaction isolation](https://www.postgresql.org/docs/18/transaction-iso.html)).

**Recommendation.** The short authorization, lifecycle-fence, operation,
idempotency-receipt, target-ledger, and outbox writes commit in one serializable
transaction with bounded whole-transaction retry. Export membership is captured
and committed in one short serializable transaction while holding the subject
lifecycle lock. Package generation happens later from immutable member
identifiers, so a large export does not hold an MVCC snapshot open for its
entire encoding and upload time.

**Source fact — row security.** Enabled RLS is default-deny without a policy,
but superusers, `BYPASSRLS` roles, and normally table owners bypass it; table
owners can opt into it with `FORCE ROW LEVEL SECURITY`. RLS applies to normal
row selection and modification, while operations such as `TRUNCATE` are outside
RLS
([PostgreSQL row security](https://www.postgresql.org/docs/18/ddl-rowsecurity.html)).

**Recommendation.** Every export, deletion target, tombstone, status read, and
worker claim runs under the existing `NOSUPERUSER NOBYPASSRLS` runtime role with
forced RLS and explicit trusted tenant/subject predicates. Migration or owner
roles never serve requests. No deletion implementation may use `TRUNCATE`.

**Source fact — streaming.** `COPY (query) TO STDOUT` transmits query output over
the client connection and applies relevant `SELECT` RLS policies. PostgreSQL's
native `COPY` formats are text, CSV, and binary, not JSON
([PostgreSQL `COPY`](https://www.postgresql.org/docs/18/sql-copy.html)).

**Recommendation.** Encode versioned NDJSON in the application adapter from an
explicit authorized query or `COPY` of already shaped fields; never expose a
PostgreSQL binary dump. Stream to a private staging object while calculating
byte length and SHA-256, then publish atomically only after the final lifecycle
and authorization recheck succeeds.

**Source fact — PITR.** Continuous archiving combines a base backup with WAL,
can restore the cluster to a selected prior time, and restores an entire
database cluster rather than a subset. WAL contains effectively all database
changes and must be protected
([PostgreSQL continuous archiving and PITR](https://www.postgresql.org/docs/18/continuous-archiving.html)).

**Decision consequence.** A PITR restore to a time before deletion can
reintroduce deleted rows. A backup is never a subject export and is never
restored directly into a serving role. Backup expiry, a current deletion-fence
ledger, and a gated restore procedure are part of the deletion contract, not
operator folklore.

### HTTP and OpenAPI constraints

**Source fact — asynchronous status.** HTTP `202 Accepted` means processing has
not completed and might still fail; its representation ought to describe the
current status and point to a status monitor. HTTP has no later callback status
on the original response
([RFC 9110 section 15.3.3](https://www.rfc-editor.org/rfc/rfc9110.html#name-202-accepted)).
`303 See Other` identifies another resource that can supply an indirect result,
and `410 Gone` says access is no longer available and is likely permanently so
([303](https://www.rfc-editor.org/rfc/rfc9110.html#name-303-see-other),
[410](https://www.rfc-editor.org/rfc/rfc9110.html#name-410-gone)).

**Source fact — retries and conditions.** HTTP defines PUT, DELETE, and safe
methods as idempotent, but POST is not inherently idempotent. Conditional
requests can prevent lost updates; `If-None-Match: *` can prevent accidental
creation over an existing representation, while entity tags support efficient
conditional GET polling
([idempotent methods](https://www.rfc-editor.org/rfc/rfc9110.html#name-idempotent-methods),
[conditional requests](https://www.rfc-editor.org/rfc/rfc9110.html#name-conditional-requests),
[`If-None-Match`](https://www.rfc-editor.org/rfc/rfc9110.html#name-if-none-match)).

**Recommendation.** Preserve Palimpsest's existing POST plus mandatory
`Idempotency-Key` convention. Commit the operation and its content-free
fingerprint before returning `202`, return `Location` for the operation
resource, and replay the same operation identity for an identical retry. Reuse
of the key with a different fingerprint is `409` and performs no mutation.
Status GETs return strong ETags and accept `If-None-Match`; an unchanged status
may return `304`. All responses remain `Cache-Control: private, no-store`.

When an export is ready, GET of its operation resource returns `303` with
`Location` pointing to the separately authorized download resource. Once the
short download-retention window expires, that result returns `410` only after
the caller passes the same scope authorization; hidden and cross-tenant
resources retain Palimpsest's indistinguishable `404`. Deletion status remains
a `200` representation in terminal `completed` or `failed` states because the
content-free tombstone is the result.

**Source fact — OpenAPI.** OpenAPI 3.1.2 requires a Responses Object to contain
at least one response and expects known successful and error responses to be
documented. Link Objects can express a design-time relationship to another
operation but do not guarantee runtime access. Schema Objects use the OpenAPI
3.1 dialect over JSON Schema Draft 2020-12
([Responses Object](https://spec.openapis.org/oas/v3.1.2.html#responses-object),
[Link Object](https://spec.openapis.org/oas/v3.1.2.html#link-object),
[Schema Object](https://spec.openapis.org/oas/v3.1.2.html#schema-object)).

**Recommendation.** Describe `202`, `200`, `303`, `304`, `404`, `409`, `410`,
`412`, `422`, `429`, `500`, and `503` explicitly where reachable. Model each
operation state as a closed `oneOf` variant with a required `state` constant and
`unevaluatedProperties: false`; do not rely on prose or a discriminator alone.
Add OpenAPI Links from `202` to status GET and from ready export status to the
download operation, while still returning runtime `Location` headers.

### Privacy-safe audit and logging

**Source fact.** NIST SP 800-53 Rev. 5 control AU-3(3) says PII in audit records
should be limited to the elements identified by a privacy risk assessment, and
AU-9 requires audit information and logging tools to be protected from
unauthorized access, modification, and deletion
([NIST SP 800-53 Rev. 5](https://csrc.nist.gov/pubs/sp/800/53/r5/upd1/final)).

**Recommendation.** Application logs, traces, metrics, problem responses, and
worker errors contain operation IDs, target-class enums, attempt counts,
durations, and sanitized error classes only. They never contain request bodies,
memory values, provenance payloads, queries, vectors, object URIs, credentials,
raw idempotency keys, raw subject identifiers, or lists of deleted item IDs.
The durable audit tombstone is access-controlled data with its own retention;
ordinary observability output is not the tombstone.

## Recommended public resources

Use two collection resources under the existing trusted scope:

```text
POST /v1/tenants/{tenant_id}/subjects/{subject_id}/exports
GET  /v1/tenants/{tenant_id}/subjects/{subject_id}/exports/{export_id}
GET  /v1/tenants/{tenant_id}/subjects/{subject_id}/exports/{export_id}/content

POST /v1/tenants/{tenant_id}/subjects/{subject_id}/deletions
GET  /v1/tenants/{tenant_id}/subjects/{subject_id}/deletions/{deletion_id}
```

The path selects scope but grants no authority. Both POSTs require the standard
`Idempotency-Key`. Export accepts a versioned `profile` and optional controller
context references, never free-form authorization claims. Deletion accepts a
versioned policy identifier and scope version chosen from trusted server-side
configuration, not a caller-authored exception or grant.

The `202` body is small and content-free:

```json
{
  "operation_id": "019...",
  "operation_kind": "subject_deletion",
  "state": "draining",
  "submitted_at": "2026-07-29T08:00:00.000000Z",
  "status_uri": "/v1/tenants/.../subjects/.../deletions/019...",
  "schema_version": 1
}
```

Do not report hidden row counts or the existence of other-tenant resources in a
failure response. Status representations may show coarse target classes and
sanitized state to an authorized caller; detailed identifiers remain internal.

## Export snapshot and portable shape

### Snapshot algorithm

1. Authenticate the principal, resolve grants from trusted policy, set the
   transaction's RLS context, and acquire the exact tenant/subject lifecycle
   lock before revealing whether records exist.
2. Require lifecycle `active`. In one short serializable transaction while
   holding the lifecycle lock, select only currently authorized canonical
   records and commit an ordered membership manifest, export operation, and
   idempotency receipt.
3. A worker streams immutable members in `(record_kind, recorded_at, id)` order
   into a private staging package. Missing members or a changed lifecycle fail
   closed; no partial package becomes downloadable.
4. Reauthorize the scope and recheck lifecycle immediately before atomic
   publication and again on every download. A deletion fence revokes the
   export, deletes staging/ready objects, and makes the content unavailable.

The membership manifest is itself subject-scoped, forced-RLS data. It contains
record IDs and digests but no payloads and is deleted when the export expires or
when subject deletion begins.

### Package `palimpsest-canonical-history-v1`

Return a ZIP container for convenient integrity-checked transfer, with all
semantic data in UTF-8 JSON or NDJSON:

```text
manifest.json
schema/palimpsest-canonical-history-v1.schema.json
records/episodes.ndjson
records/checkpoints.ndjson
records/fact-revisions.ndjson
records/procedures.ndjson          # empty until supported
records/artifact-references.ndjson # empty until supported
artifacts/...                      # only authorized immutable bytes, if present
processing-context.json            # controller-supplied fields, if requested
README.txt
```

`manifest.json` contains `format`, `schema_version`, `export_id`, exact scope,
`profile`, snapshot valid/recorded coordinates, generation time, ordered file
entries, byte lengths, SHA-256 digests, record counts, and the policy versions
used. The whole downloadable representation receives a strong ETag. Empty
future record classes remain declared so readers can distinguish unsupported,
empty, and omitted-by-policy data.

Every NDJSON record is a self-describing envelope:

```json
{
  "schema_version": 1,
  "record_kind": "fact_revision",
  "origin_class": "derived",
  "id": "019...",
  "scope": {"tenant_id": "...", "subject_id": "...", "case_id": "..."},
  "temporal": {
    "observed_at": "...",
    "recorded_at": "...",
    "valid_from": "...",
    "valid_to": null
  },
  "governance": {
    "sensitivity": "...",
    "retention_policy_id": "...",
    "schema_version": 1
  },
  "provenance": {"source_episode_ids": ["..."], "write_policy_id": "..."},
  "relations": {"supersedes_id": null},
  "payload": {}
}
```

Export semantic values, not PostgreSQL column dumps. Preserve every revision,
supersession/conflict link, observed/recorded/valid time, provenance, and
governance field needed to understand history. Do not export embeddings,
`tsvector` values, cache entries, internal leases, authorization secrets, raw
audit logs, or opaque database tuple metadata. Derived indexes are
reproducible; their omission is declared in the manifest.

`canonical_history` is the issue #5 product profile. A future
`data_subject_access` profile can add controller processing information and
rights-of-others redaction. A future `data_portability` profile must filter on
controller-validated legal basis and `provided`/`observed` origin. The profiles
must never silently alias one another.

## Deletion state machine

```text
draining -> fenced -> purging -> verifying -> completed
    |          |         |           |
    +----------+---------+-----------+-> retry_wait -> purging
    |                                         \
    +-----------------------------------------> failed
```

| State | Required invariant |
| --- | --- |
| `draining` | The operation, tombstone seed, target ledger, outbox intent, and monotonic subject `deletion_pending` fence committed atomically. No new content lease can start; existing leases are draining or being revoked. |
| `fenced` | No new or in-flight content lease can return the subject. The durable fence remains active. |
| `purging` | Idempotent target workers are deleting live canonical, derived, cache, export, and artifact classes. The fence remains active across crashes and lease expiry. |
| `retry_wait` | A transient target failed; the sanitized class, attempt number, and next availability are durable. The subject remains fenced. |
| `verifying` | All target tasks report done; independent negative queries and external-object probes are running under the runtime role. |
| `completed` | Every live target and verification postcondition passed. Only the minimum tombstone and declared isolated-backup boundary remain. |
| `failed` | Bounded automated retries were exhausted or an invariant failed. Operator action is required; the fence is permanent and the same operation can resume. |

Every content-producing read or download holds a bounded, subject-scoped lease
obtained under the same lifecycle lock. Deletion commits `draining` to prevent
new leases, then reaches `fenced` only after existing leases end or are revoked.
Bytes already delivered before the request cannot be recalled. Providers whose
signed download capabilities cannot be revoked must use a bounded capability
TTL, and deletion cannot pass `fenced` until it expires and access probes fail.

There is no public cancellation or transition back to `active`. Policy refusal,
legal hold, absent authority, and invalid scope are decided before `fenced` and
do not create a deletion operation. Retrying the original POST never creates a
second operation. A privileged repair resumes the same operation and target
ledger; it does not mint a replacement that obscures the failure history.

## Authorization and purge ordering

Apply this order at every seam:

1. Authenticate; resolve trusted grants and deletion policy; set tenant,
   subject, principal, sensitivity, and policy context for forced RLS.
2. Authorize the exact operation before existence disclosure; return the same
   redacted `404` for absent and hidden cross-scope resources.
3. Lock the subject lifecycle row and atomically commit `deletion_pending`, the
   `draining` operation, idempotency receipt, durable target ledger, audit seed,
   and outbox. Prevent new content leases and drain or revoke existing leases.
4. Commit `fenced`, revoke downloads and artifact capabilities, purge caches and
   derived search rows, hard-delete canonical/provenance data in
   foreign-key-safe order, then reduce operation/idempotency/audit rows to the
   minimum tombstone.
5. Verify all current/as-of/history reads, exact/lexical/vector candidates,
   retrieval manifests, caches, export objects, and artifact access return no
   subject content before committing `completed`.

The immediate fence is the safety boundary; background purge is the
completion boundary. Candidate generation, historical selection, export
membership, and content rehydration all check the subject fence before touching
payload rows. A failed external object deletion stays retryable and prevents
`completed`, even though access credentials were already revoked.

## Minimum tombstone

Retain only fields justified by idempotency, restore suppression, failure
diagnosis, and evidence that the workflow completed:

- opaque deletion operation ID, tenant ID, and a versioned HMAC scope digest;
- idempotency-key digest and request fingerprint, never the raw key or body;
- accepted, fenced, and completed/failed timestamps plus monotonic state version;
- deletion-policy, contract-schema, worker-release, and scope-digest-key versions;
- coarse target-class completion bits, verification digest, and sanitized final
  outcome/error class;
- backup policy ID, deletion watermark, and earliest declared backup expiry.

Do not retain the raw subject ID, memory IDs, values, queries, vectors,
provenance, object paths, signed URLs, credentials, per-table row lists, request
payload, or deleted-content hashes. Even the HMAC digest and operation metadata
remain protected and retention-limited because pseudonymisation is not presumed
anonymisation. A future purge of tombstones must preserve restore suppression by
advancing a compact deletion watermark or rotating/retiring the affected backup
sets first.

## Backup and restore boundary

`completed` has two explicit components:

- `live_disposition: purged_and_verified` means no serving primary, replica,
  cache, export object, derived index, or artifact endpoint can return content;
- `backup_disposition: isolated_until_expiry` names the backup policy, deletion
  watermark, earliest expiry, and restore gate. It does not claim that old
  backup bytes were individually modified.

Deployment requirements:

1. Backups and WAL are encrypted, access-restricted, not directly queryable,
   and expire on a documented schedule. Deletion status exposes the schedule's
   boundary without revealing storage locations.
2. A current, authenticated deletion-fence ledger is retained independently of
   any PITR target time. Each backup set records the latest included deletion
   watermark.
3. Restore always enters a network-isolated quarantine role. Before serving,
   it applies every later tombstone, reruns target purge and negative
   conformance, rebuilds derived indexes, and advances to the current watermark.
4. If the current fence ledger is unavailable or its integrity cannot be
   verified, the restored cluster fails closed and cannot enter a serving role.
5. Backup retirement is tested: expired sets and dependent incremental/WAL
   chains become unrestorable according to policy, and the evidence contains no
   memory content.

The EDPB access guidance makes backup divergence observable: a content-free
deletion ledger can show that a backup may contain recently deleted data, but
does not itself expose that data. Whether an operator must recover specific
backup content for a particular access request remains a controller decision
subject to technical feasibility and the rights of others.

## Failure, retry, and recovery semantics

Each deletion target is keyed by `(deletion_id, target_class, target_key_digest)`
with a unique constraint and monotonic `pending -> leased -> done` lifecycle.
Workers use renewable leases and commit each completed external effect before
claiming another. A crash after an external delete but before its receipt is
safe because repeating delete/revoke must treat already-absent as success.

Classify errors as `retryable_dependency`, `retryable_serialization`,
`permanent_configuration`, or `invariant_violation`; never persist provider
messages that can contain object names or payloads. Retry transient errors with
bounded exponential backoff and jitter. Exhaustion moves the operation to
`failed` without removing the fence. Operator repair records an attributable
reason code and resumes the same incomplete targets.

Export generation follows the same lease discipline but failure deletes all
staging bytes and leaves no downloadable partial result. An export whose
subject becomes fenced is terminal `revoked`, even if materialization had
finished. A client retry can observe only the same revoked operation, not
regenerate data under a new ID.

## Required black-box conformance scenarios

### Authorization and export

- A principal granted tenant A/subject X exports every authorized revision,
  temporal coordinate, and provenance link exactly once in deterministic order;
  the archive and per-file digests reproduce from the same snapshot.
- Tenant A credentials cannot create, poll, redirect to, or download an export
  for tenant B. Absent and hidden resources have identical status, media type,
  body shape, and no count/timing detail intended to reveal existence.
- A grant or lifecycle revocation after membership capture but before publish
  prevents publication; revocation after publish prevents download and removes
  the ready object.
- The canonical profile classifies provided, observed, derived, and system data
  and does not claim Article 20 scope. No vector, lexical projection, secret,
  raw log, or unrelated subject appears.

### Deletion safety and recovery

- The successful POST response is emitted only after the subject fence is
  durable. From that point no new current, as-of, historical, retrieval-page,
  idempotent-replay, cursor, export, or artifact content lease can start. The
  operation reaches `fenced` only after every earlier lease drains or is
  revoked.
- Fault injection after every target transition, transaction commit, cache
  eviction, and external delete proves lease recovery and same-operation retry
  without reactivation or duplicate completion.
- Exact, lexical, vector, fusion, and rebuilt candidate sets contain zero
  deleted IDs before `completed`; stale retrieval receipts cannot rehydrate or
  reveal hidden-row counts.
- Reusing one idempotency key with an identical fingerprint returns the same
  operation. Reusing it across subject, tenant, or request fingerprint returns
  `409` and performs no mutation.
- A worker that exhausts retries exposes only a sanitized `failed` state while
  the fence remains active; authorized repair resumes and preserves attempts.

### Tombstone, logging, and backup

- Structured logs, traces, metrics, problems, and audit receipts pass a canary
  scan for memory values, subject IDs, record IDs, queries, vectors, object
  paths, credentials, and raw idempotency keys during success and every injected
  failure.
- Tombstone schema inspection proves the allowlist above and rejects payload,
  provenance, raw subject, and per-record identifiers at the database boundary.
- A PITR restore chosen before deletion cannot serve traffic. Applying the
  current fence ledger and purge produces zero live/historical/candidate/object
  results before readiness succeeds.
- Missing, stale, corrupt, or unverifiable deletion ledgers make restore
  readiness fail closed. Backup expiry evidence proves the declared set and its
  dependencies are no longer usable without containing private content.

### HTTP and contract

- `202` contains `Location` and a status representation; status polling honors
  ETag/`If-None-Match`; ready export status redirects with `303`; expired
  authorized content yields `410`; hidden content remains redacted `404`.
- OpenAPI examples and generated fixtures validate every state variant and
  reachable status. Unknown states/fields fail the closed schemas, and runtime
  responses conform byte-for-byte on required headers and problem types.

## Explicit uncertainties and decisions still required

1. **Controller policy and legal scope.** Palimpsest lacks authoritative lawful
   basis, recipient, legal-hold, and rights-of-others metadata. Decide whether
   issue #5 ships only `canonical_history` or also introduces a controller-policy
   adapter; do not market the first as Article 15/20 compliance.
2. **Tombstone retention.** The product needs a documented default and operator
   override bounded by purpose. Indefinite retention is not justified by
   “audit” alone; too-short retention can break restore suppression and
   idempotency.
3. **Independent fence ledger.** PostgreSQL is the only mandatory data service,
   but safe PITR to a pre-deletion time needs a current ledger outside that
   target timeline. The release gate must specify the portable signed ledger,
   integrity key custody, and fail-closed restore input.
4. **Artifact providers and caches.** The first implementation can prove the
   PostgreSQL/no-cache profile. Completion claims must be capability-scoped
   until concrete object-store and cache adapters have delete, revoke, probe,
   and failure-injection conformance.
5. **Physical and cryptographic erasure.** PostgreSQL logical deletion, vacuum,
   WAL, snapshots, replicas, storage remanence, and provider retention differ.
   Per-subject envelope encryption and key destruction may strengthen a future
   profile, but key sharing, deduplication, rotations, and backup recovery need
   a separate threat model before any crypto-erasure claim.

## Implementation gate derived from this research

Issue #5 is ready to specify once one ADR fixes the public resources, operation
states, fence transaction, `canonical_history` package, tombstone allowlist,
and backup restore gate above. Implementation is not ready for direct main
delivery until the HTTP
conformance suite exercises authorization, cross-tenant non-disclosure, partial
failure, retry, stale receipt reauthorization, log canaries, and a pre-deletion
PITR restore. Any narrower test proves only a component, not end-to-end scoped
deletion.
