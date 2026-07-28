# Architecture and operations baseline through 2027

Status: research recommendation
Research date: 2026-07-29
Resolves: [GitHub issue #9](https://github.com/mustbearnold/palimpsest/issues/9)

## Decision

Build Palimpsest as a modular Rust service with two process roles, `api` and
`worker`, over one PostgreSQL 18 system of record. Keep the deterministic domain
and `MemoryService` application boundary free of HTTP, async-runtime, database,
vector, and telemetry types. Put PostgreSQL/pgvector, HTTP/OpenAPI, object
storage, authentication, and telemetry behind adapters.

Ship a pinned Rust binary and multi-architecture OCI image. Make PostgreSQL the
only mandatory external service. Use its transaction log, job table, lexical
search, and exact pgvector search before adding a broker, cache, approximate
index, or orchestration platform. Publish a design-first OpenAPI 3.1.2 contract
at `/v1`; treat generated clients and server bindings as conformance aids, not
the behavioral authority.

This is the strongest 2027 baseline because it makes temporal correctness,
authorization, recovery, and portability architectural commitments while
keeping high-churn libraries and hosting choices replaceable.

## Research method and confidence

This report uses specifications and maintainer documentation current on the
research date. Statements labeled **Source fact** report what a source says.
Statements labeled **Recommendation** or **Inference** are Palimpsest decisions
derived from those facts and the accepted product invariants in
[`ADR-0001`](../adr/0001-postgres-temporal-source-of-truth.md).

Confidence is high for the durable boundaries, PostgreSQL posture, migration
rules, API compatibility policy, and packaging format. Confidence is medium for
the precise Rust adapter crates because their APIs will continue to evolve.
Approximate-vector indexing and production topology deliberately remain
evaluation-gated rather than being guessed before representative workloads
exist.

## Exact supported-version posture

| Surface | Baseline on 2026-07-29 | Policy through 2027 |
| --- | --- | --- |
| Rust | Stable 1.97.1, edition 2024, Cargo resolver 3 | Pin the exact stable patch in `rust-toolchain.toml`; update and run the full gate within 14 days of each stable or security release. Rust only fixes the latest stable release, so do not promise an old compiler for the deployable service. Declare `rust-version` if a reusable library is published. |
| PostgreSQL | 18.4 | Require major 18 initially and always require its current minor. After a new major is GA, pgvector supports it, restore succeeds, and conformance/benchmark matrices pass, support the current and previous two GA majors; keep 18 as the minimum through 2027. Never support beta/RC databases in a release. |
| pgvector | 0.8.5 | Pin the packaged extension to 0.8.5; accept only tested patch releases in the same minor. Upgrade minor versions only after exact/ANN recall, filtered retrieval, migration, backup, and restore evaluations pass. |
| OpenAPI | 3.1.2 | Keep 3.1.2 as the public description format until validators and generators used in CI pass the corpus on 3.2.0. Changing OAS syntax is not an API version change. Review 3.2 adoption by 2026-Q4. |
| HTTP | HTTP semantics from RFC 9110; JSON request/response bodies | `/v1` remains additive through 2027. Breaking behavior requires `/v2`; security fixes may override the deprecation window with an explicit advisory. |
| Telemetry | Stable OpenTelemetry signals over OTLP; Prometheus scrape endpoint | Pin SDK and semantic-convention versions. OTLP is the vendor-neutral export boundary; no hosted backend is mandatory. |
| Packaging | OCI Image Specification 1.1 image for `linux/amd64` and `linux/arm64`, plus checksummed Linux binaries | Publish immutable digests, SBOM, provenance, and signatures. Compose and generic OCI are supported deployment seams; Kubernetes remains an adapter. |

**Source facts.** Rust 1.97.1 was the current stable patch on July 16, 2026,
and Cargo documents that the Rust Project provides fixes only for the latest
Rust version ([Rust releases](https://blog.rust-lang.org/releases/),
[`rust-version` support guidance](https://doc.rust-lang.org/stable/cargo/reference/rust-version.html)).
Edition 2024 implies resolver 3, which is aware of package Rust-version
compatibility ([Rust 2024 resolver](https://doc.rust-lang.org/stable/edition-guide/rust-2024/cargo-resolver.html)).

PostgreSQL supports each major for five years and recommends always running the
current minor. On the research date, 18.4 was current and supported until
2030-11-14; 19 was beta and therefore not a production candidate
([PostgreSQL versioning policy](https://www.postgresql.org/support/versioning/)).
pgvector 0.8.5 was released on 2026-07-08, supports PostgreSQL 13+, and follows
several 2026 correctness fixes affecting HNSW vacuuming, index corruption, and
parallel builds ([pgvector 0.8.5 README](https://github.com/pgvector/pgvector/blob/v0.8.5/README.md),
[changelog](https://github.com/pgvector/pgvector/blob/v0.8.5/CHANGELOG.md)).

OpenAPI 3.2.0 is the latest specification, while 3.1.2 is a maintained published
version ([OAS 3.2.0 revision history](https://spec.openapis.org/oas/v3.2.0.html),
[published versions](https://spec.openapis.org/oas/)). Choosing 3.1.2 is an
interoperability recommendation, not a claim that 3.2.0 is unstable.

## Durable commitments versus adapters

| Commit now: public or data invariant | Keep replaceable behind a port |
| --- | --- |
| `MemoryService` behavior and domain vocabulary | Axum, Tower, Tokio, SQLx, and the OpenAPI rendering library |
| PostgreSQL as canonical temporal and authorization store | Pool implementation, migration runner, backup product, managed PostgreSQL vendor |
| Immutable episodes; revision chains; explicit observed, recorded, and valid time | Physical tables, index parameters, partition layout, query-plan hints |
| Transactional provenance, authorization, audit receipt, and durable work intent | Worker scheduler, optional Valkey cache, wake-up mechanism |
| Exact authorization/deletion/validity filtering before retrieval ranking | Embedding provider/model, HNSW or IVFFlat, reranker, cache |
| Versioned `/v1` HTTP behavior and OpenAPI conformance | HTTP framework, code generator, reverse proxy, ingress |
| OTLP/Prometheus and structured-log output contracts | Collector, metrics database, tracing backend, dashboard stack |
| OCI image, release manifest, SBOM, signatures, checksums | Registry, Compose, systemd, Kubernetes, Nomad, cloud platform |

Do not leak adapter types across the inward boundary. In particular, domain
identifiers are domain newtypes rather than `sqlx` rows, API errors are mapped
from domain errors rather than created inside the core, and spans/metrics wrap
operations instead of changing their return values.

## Rust service shape

Use one Cargo workspace with these dependency directions:

```text
palimpsest-domain       <- no I/O, framework, async runtime, or database types
palimpsest-application  <- MemoryService use cases and explicit ports
palimpsest-postgres     <- SQLx implementation, migrations, job repository
palimpsest-http         <- Axum/Tower adapter and OpenAPI conformance
palimpsest-telemetry    <- tracing/OpenTelemetry setup and metric definitions
palimpsest-server       <- composition root; `serve`, `worker`, `migrate`, `doctor`
```

**Recommendation.** Use Tokio + Axum + Tower for the HTTP adapter, SQLx for
explicit PostgreSQL queries and checked migrations, Serde for wire DTOs, and
`tracing` plus OpenTelemetry for observability. Keep `Cargo.lock` committed and
pin direct dependencies to compatible release lines. Ban wildcard dependencies,
run license/advisory/source checks, and update dependencies in small attributable
pull requests.

Do not introduce an ORM. Palimpsest's bitemporal, authorization-first, range,
full-text, and pgvector queries are part of correctness and must remain visible
SQL with scenario tests and inspected plans. Do not split into networked
microservices: `api` and `worker` can scale independently while sharing the same
release artifact and database transaction boundary.

## Canonical PostgreSQL model

1. Store `tenant_id`, `subject_id`, provenance, sensitivity, retention policy,
   schema version, `observed_at`, `recorded_at`, and explicit validity on every
   durable record where the invariant applies. Use `timestamptz` and half-open
   `tstzrange` intervals. UUIDv7 may improve locality, but its timestamp is never
   temporal authority.
2. Keep episodes append-only. Keep facts and procedures as immutable revision
   rows linked by `supersedes_id`; make current and as-of projections queries or
   views, not destructive updates to history.
3. Commit the canonical record, provenance, authorization metadata, audit
   receipt, idempotency receipt, and outbox/job intent in one transaction.
4. Put embeddings in versioned derived rows containing model/provider identity,
   dimension, normalization/distance policy, source revision, content digest,
   generation time, and status. They can be invalidated and rebuilt.
5. Use separate owner/migrator and least-privilege runtime roles. Revoke unsafe
   `PUBLIC` privileges, remove writable schemas from `search_path`, enable and
   `FORCE ROW LEVEL SECURITY` as defense in depth, and still express tenant,
   deletion, and temporal predicates in every repository query.

**Source facts.** PostgreSQL 18 added temporal `WITHOUT OVERLAPS` primary/unique
constraints and `PERIOD` foreign keys
([PostgreSQL 18 release notes](https://www.postgresql.org/docs/18/release-18.html)).
Use them where they directly express a tested invariant, but retain service-level
conformance tests because PostgreSQL `CHECK` constraints cannot safely reference
other table data
([constraint limits](https://www.postgresql.org/docs/18/ddl-constraints.html)).

RLS defaults to deny when enabled without a policy, but superusers, `BYPASSRLS`
roles, and normally table owners bypass it unless `FORCE ROW LEVEL SECURITY` is
used ([row-security policies](https://www.postgresql.org/docs/18/ddl-rowsecurity.html)).
PostgreSQL also warns that untrusted writable schemas in `search_path` can enable
Trojan-horse functions
([function security](https://www.postgresql.org/docs/18/perm-functions.html)).
These facts are why RLS is a second barrier rather than Palimpsest's only
authorization implementation.

### Retrieval and pgvector

Start with exact hybrid retrieval:

1. Materialize the authorized, non-deleted, valid-at-query-time candidate set.
2. Generate PostgreSQL full-text and exact vector candidates only inside that
   set.
3. Fuse candidates under a versioned retrieval policy.
4. Rerank deterministically, retaining component scores and policy version in a
   retrieval receipt.

**Source fact.** pgvector exact search provides perfect recall. Its approximate
HNSW and IVFFlat indexes trade recall for speed; with approximate indexes,
ordinary filters are applied after index scanning, and iterative scans can search
farther when filtering leaves too few results
([pgvector indexing and filtering](https://github.com/pgvector/pgvector/blob/v0.8.5/README.md#indexing),
[iterative scans](https://github.com/pgvector/pgvector/blob/v0.8.5/README.md#iterative-index-scans)).

**Inference.** A shared-table ANN index is not the safe baseline for the product
invariant that authorization and deletion filters precede vector candidate
generation. Enable HNSW only when a scenario evaluation proves isolation and
recall for the intended physical scope, such as partition-local indexes over an
already-authorized cohort. HNSW is the first ANN candidate because pgvector
documents a better speed/recall trade-off than IVFFlat, despite slower builds and
higher memory use. Keep exact search as a conformance oracle and fallback.

Do not partition on day one. PostgreSQL says partitioning usually pays when a
table is very large relative to memory, and warns that poor or excessive
partitioning raises planning and memory costs
([partitioning guidance](https://www.postgresql.org/docs/18/ddl-partitioning.html)).
Add range partitions for retention-heavy episodes only after measured table,
vacuum, deletion, or query-plan thresholds are crossed. Never create one
partition per tenant.

## Migrations and compatible rollout

Use forward-only, numbered SQL migrations embedded in the release and recorded
in a checksum-protected ledger. The release artifact must expose:

- `palimpsest migrate plan` to show pending version, locks, and transaction mode;
- `palimpsest migrate apply` to acquire one PostgreSQL advisory lock and apply;
- `palimpsest migrate status` for automation and readiness diagnostics;
- `palimpsest doctor` to check server, extension, schema, roles, storage, and
  recovery prerequisites without exposing memory contents.

Application startup must never apply migrations. It must refuse a schema newer
than it understands and report whether an older schema is migratable. Deployment
automation runs migrations as the privileged migrator identity, then starts the
least-privilege runtime.

Normal migrations are transactional and idempotent. Operations that PostgreSQL
forbids in a transaction, including `CREATE INDEX CONCURRENTLY`, are explicitly
marked non-transactional and have preflight, progress, failure-repair, and
postcondition checks. PostgreSQL documents that concurrent indexes cannot run in
a transaction and can leave an invalid index after failure
([`CREATE INDEX`](https://www.postgresql.org/docs/18/sql-createindex.html)).

Use expand/backfill/cutover/contract for every incompatible data change:

1. Expand with nullable/additive schema that supports release N-1 and N.
2. Backfill through a resumable, rate-limited durable job with progress receipts.
3. Cut reads/writes to the new representation after shadow comparison passes.
4. Contract no earlier than the next stable release and after rollback support
   is explicitly retired.

Production rollback means rolling back application code while keeping a
backward-compatible expanded schema. Never promise down-migrations for durable
data. Rehearse major PostgreSQL upgrades using a restored copy; PostgreSQL major
upgrades require dump/restore, `pg_upgrade`, or logical replication, while minor
updates do not require dump/restore
([versioning and upgrades](https://www.postgresql.org/support/versioning/)).

## Durable jobs, consolidation, and side effects

Store jobs and transactional outbox entries in PostgreSQL. Each job has a stable
type and schema version, tenant/subject scope, idempotency key, state, priority,
`available_at`, lease owner/expiry, attempt count, bounded retry policy, last
sanitized error class, and result receipt.

Workers claim small ordered batches using `FOR UPDATE SKIP LOCKED`, commit the
lease before work, heartbeat long jobs, and make completion conditional on the
current lease token. Expired leases recover abandoned work. PostgreSQL explicitly
identifies `SKIP LOCKED` as appropriate for avoiding contention among consumers
of a queue-like table, while warning that it is not a general consistent view
([locking clause](https://www.postgresql.org/docs/18/sql-select.html)).

Use `LISTEN/NOTIFY` only as a replaceable low-latency wake-up; bounded polling of
the durable table remains authoritative. Use Valkey only for disposable caches.
Do not add Kafka, NATS, RabbitMQ, or a workflow engine until measured throughput,
isolation, or scheduling requirements exceed this design. External model and
object-store calls use idempotency keys and a state machine because no database
transaction can make those side effects atomic.

## Public HTTP and OpenAPI contract

The repository-owned, design-first OpenAPI document is the public contract.
Server conformance tests and client fixtures are generated from it, but generated
code is not committed as domain authority.

Required conventions:

- `/v1` path versioning; UTF-8 JSON; RFC 3339 UTC timestamps with explicit
  precision; canonical domain IDs encoded as strings;
- UUIDv7 identifiers for new public resources, while retaining explicit time
  fields; RFC 9562 defines UUIDv7 using Unix-epoch milliseconds
  ([RFC 9562](https://www.rfc-editor.org/rfc/rfc9562.html));
- `Idempotency-Key` on every durable mutation, scoped to tenant, operation, and
  authenticated principal, with request-digest conflict detection and a durable
  response receipt;
- opaque, signed or integrity-protected cursor pagination with a unique stable
  order; never offset pagination for changing memory streams;
- RFC 9457 `application/problem+json` errors with stable Palimpsest problem
  types and trace IDs, but no raw memory or tool payloads
  ([RFC 9457](https://www.rfc-editor.org/rfc/rfc9457.html));
- conditional requests for mutable policy/configuration resources, explicit
  request size/time limits, and cancellation propagation.

Within `/v1`, add optional fields and endpoints; do not change meaning, required
fields, defaults, enum closure, ordering, or error semantics. Announce ordinary
deprecations for at least one stable release and 180 days. Publish contract diffs
in CI. A breaking change gets `/v2` with an overlap window and migration guide.
Security and privacy fixes may shorten the window only with a published advisory.

OpenAPI 3.2.0 is newer, but adopting its document syntax before the selected
validator, mock server, diff checker, Rust renderer, and TypeScript/Python client
generators pass the same fixture corpus would reduce interoperability. That is
why 3.1.2 is the baseline and 3.2.0 is an evidence-gated adapter upgrade.

## Observability and auditability

Emit structured JSON logs to stdout/stderr, metrics at `/metrics`, and OTLP for
traces, metrics, and log correlation. Accept W3C trace context at the HTTP edge,
propagate context across jobs via stored trace links, and assign every write,
retrieval, migration, and background derivation an attributable operation ID.

OpenTelemetry marks tracing API/SDK/protocol stable, metrics API and protocol
stable, and logging bridge/SDK/protocol stable; it also notes that individual
language implementations have their own feature status
([OpenTelemetry status](https://opentelemetry.io/docs/specs/status/)). Therefore
OTLP is the durable export seam, while the Rust SDK and collector configuration
remain replaceable and pinned.

Instrument at minimum:

- HTTP rate, duration, inflight, response class, cancellation, and body rejection;
- pool saturation, transaction duration/retry, query fingerprints, locks,
  replica lag, WAL/archive health, vacuum debt, and migration state;
- job depth/age/lease expiry/attempt/outcome by bounded job type;
- retrieval candidate counts, stage latency, exact/ANN mode, policy version,
  and evaluation cohort, without memory content;
- backup age, restore-rehearsal result, object-orphan count, and deletion backlog.

Never put tenant IDs, subject IDs, memory IDs, prompts, raw text, embeddings,
tokens, or unbounded problem strings in metric labels or default logs. Hashing a
private value does not automatically make it safe. Audit records are durable
domain data with retention and access policy; telemetry is operational and may
be sampled or discarded. Outbound telemetry is off by default for self-hosters.

Expose three distinct probes: liveness checks only process health; readiness
checks database connectivity, compatible schema, and ability to serve; startup
allows migrations/recovery to finish without triggering restart loops. Optional
cache, object, and telemetry outages degrade only the features that require them.

## Backup, restore, artifacts, and deletion

For any production-like deployment, require PostgreSQL base backups plus
continuous WAL archiving for point-in-time recovery, encrypted in transit and at
rest, with a documented operator-selected RPO/RTO. PostgreSQL identifies SQL
dumps, file-system backups, and continuous archiving as distinct approaches
([backup and restore](https://www.postgresql.org/docs/18/backup.html)). A logical
export is portability tooling, not the sole backup.

Run automated backup verification and a scheduled restore rehearsal into an
isolated database, then execute temporal, tenant-isolation, extension, and
retrieval conformance checks. Monitor replication slots because PostgreSQL warns
that unbounded retained WAL can fill `pg_wal`
([replication slots](https://www.postgresql.org/docs/18/warm-standby.html#WARM-STANDBY-SLOTS)).
PostgreSQL provides replication but not the external failure detector/fencing
system, so any HA topology must prove split-brain prevention and failover rather
than claiming HA from a standby alone
([failover](https://www.postgresql.org/docs/18/warm-standby-failover.html)).

Large immutable artifacts live in an S3-compatible adapter under content-derived
keys. PostgreSQL stores authoritative metadata, digest, size, media type,
sensitivity, retention, encryption/key reference, upload state, and deletion
state. Upload to a temporary key, verify digest, then finalize metadata; a
reconciler cleans abandoned objects. Deletion is an attributable state machine
with retryable object erasure and a completion receipt. Backups, replicas, and
retention holds are part of deletion semantics, not hidden exceptions.

## Packaging and deployment

Publish the same release as checksummed Linux binaries and OCI images for
`linux/amd64` and `linux/arm64`. The OCI Image Format is explicitly designed as
a portable, long-lived format paired with the OCI runtime and distribution
specifications
([OCI Image Specification](https://github.com/opencontainers/image-spec),
[OCI Distribution Specification](https://github.com/opencontainers/distribution-spec/blob/main/spec.md)).

The production image must:

- run as a fixed non-root UID/GID with no privilege escalation;
- contain no package manager or compiler and support a read-only root filesystem;
- handle `SIGTERM`, stop accepting work, drain bounded requests/jobs, and exit;
- expose only the HTTP port and write mutable state only to declared temporary
  paths; and
- carry source revision, version, license inventory, SPDX or CycloneDX SBOM,
  build provenance, signature, and immutable digest.

Support a generic OCI invocation and a Compose reference topology with
Palimpsest, PostgreSQL 18 + pgvector 0.8.5, and optional object storage and
OpenTelemetry Collector profiles. Compose is the reproducible single-node
evaluation and small-install path, not an HA claim. Provide systemd guidance for
the binary. Add Helm/Kubernetes only after the generic image, probes, graceful
shutdown, migration job, security context, backup contract, and multi-replica
conformance are proven; Kubernetes is not part of the core architecture.

Production uses an external PostgreSQL endpoint and object store. Do not embed
PostgreSQL, silently run a sidecar database, or persist canonical data in the
application container. Terminate public TLS in an operator-selected proxy or
ingress, require TLS to remote PostgreSQL/object storage, and support secrets via
mounted files as well as environment references. Configuration is validated and
redacted at startup; unknown keys fail fast.

## Release and compatibility gates

A release is not architecture-complete until all applicable gates pass:

1. Rust format, lint, unit/property/integration tests, dependency policy, and
   reproducible release build on pinned stable Rust.
2. PostgreSQL 18 current-minor conformance including bitemporal boundaries,
   contradiction/supersession, tenant isolation, deletion, crash recovery,
   migration from N-1, backup, and restored-copy tests.
3. OpenAPI validation, breaking-change diff, server conformance, and generated
   TypeScript/Python client fixture tests.
4. Retrieval evaluation against exact-search truth: recall, temporal correctness,
   isolation, latency distribution, and explain-plan evidence under realistic
   filtered workloads.
5. OCI smoke on amd64 and arm64, non-root/read-only execution, graceful shutdown,
   SBOM/provenance/signature verification, upgrade, rollback, and restore report.

These gates prove specific properties; they do not prove production readiness,
HA, an RPO/RTO, or superior retrieval without the corresponding deployment and
evaluation evidence. The first production deployment and security-sensitive
release still require founder approval under `AGENTS.md`.

## 2027 decision triggers

Revisit an adapter only when its threshold is observed and recorded:

| Trigger | Evaluate next; do not pre-commit |
| --- | --- |
| Exact vector retrieval misses latency SLO at representative authorized-set sizes | Partition-local HNSW, iterative scans, quantization/reranking; preserve exact oracle |
| PostgreSQL job queue cannot meet measured claim latency/throughput or isolation | Dedicated broker/workflow adapter with transactional-outbox continuity |
| Cacheable reads dominate database load and staleness rules are explicit | Valkey read-through cache; database remains authority |
| Episode retention/vacuum/index size crosses measured thresholds | Time-range partitioning with automated future partitions and restore tests |
| Single-primary recovery cannot meet approved RPO/RTO | Managed HA or an operator stack with fencing, WAL archive, failover, and restore proof |
| OpenAPI 3.2 toolchain passes the contract corpus | Upgrade document syntax without changing `/v1` behavior |
| PostgreSQL 19/20 GA plus pgvector and conformance matrices pass | Add the major under the current-plus-two policy; never release against beta/RC |

## Strongest conclusions

1. PostgreSQL 18 current-minor is the correct greenfield floor through 2027; its
   temporal constraints are useful, but Palimpsest conformance tests remain the
   authority for bitemporal behavior.
2. Exact authorization-first hybrid retrieval is the safe baseline. Shared ANN
   is an optimization gated by isolation and recall evidence, not an architectural
   assumption.
3. A PostgreSQL job/outbox design keeps durable state and side-effect intent in
   one transaction and avoids a premature broker without preventing one later.
4. Design-first OpenAPI 3.1.2 plus additive `/v1` compatibility is more
   interoperable than coupling the contract to a Rust framework or generator.
5. One modular binary, generic OCI packaging, external PostgreSQL, OTLP, and
   evidence-gated deployment adapters provide the strongest self-hostable path
   without mistaking Compose, Kubernetes, a standby, or an image build for
   production proof.
