# ADR-0003: First-release architecture and contract baseline

Status: accepted

Date: 2026-07-29

## Context

The first-release Wayfinder map resolved Palimpsest's remaining product, contract, temporal-data, retrieval, security, operations, evaluation, and delivery decisions. The choices are hard to reverse, cross several adapters, and deliberately trade early compatibility and correctness against premature scale or framework coupling. They therefore need one authoritative decision record rather than competing research recommendations.

Primary evidence lives in the dated ecosystem, architecture, and product-wedge reports under `_attic/research/`. The closed Wayfinder tickets retain the detailed questions and resolutions.

## Decision

Palimpsest's first release is a governed, bitemporal case-memory service for teams running multi-tenant operational agents. Its winning behavior is the complete authorized lifecycle from immutable episode through attributable fact revision, explicit conflict or supersession, current/as-of retrieval, and completed scoped deletion. It is not a generic agent runtime, vector store, or temporal graph product.

The canonical public seam is additive `/v1` HTTP/JSON described initially with OpenAPI 3.1.2. The description uses explicit JSON Schema dialects and avoids implementation-defined constructs. OpenAPI 3.2 is a syntax promotion, not a behavioral API version: adopt it only after the selected validator, mock server, diff checker, Rust renderer, and Python/TypeScript generators pass the same contract corpus. Breaking behavior requires `/v2`; ordinary `/v1` deprecations receive at least one stable release and 180 days.

The service is a modular Rust 2024-edition workspace with deterministic domain and application layers isolated from HTTP, async-runtime, database, embedding, and telemetry types. One release artifact composes `api`, `worker`, `migrate`, and `doctor` roles. PostgreSQL 18 current-minor is the minimum canonical store through 2027, with pgvector pinned to a conformance-tested patch. PostgreSQL 19 or later is supported only after GA, extension support, restore, conformance, and benchmark matrices pass.

Canonical episodes are append-only; facts and procedures are immutable revision chains with explicit observed, recorded, and valid time, provenance, policy identity, and conflict/supersession links. One transaction commits the canonical record, authorization metadata, provenance, audit receipt, idempotency receipt, and durable outbox/job intent. Forced PostgreSQL row-level security is defense in depth; application queries still apply trusted tenant, subject, sensitivity, deletion, retention, and temporal predicates before candidate generation.

Retrieval begins with exact authorized PostgreSQL full-text and pgvector candidates, reciprocal-rank fusion, and a versioned deterministic temporal policy. Exact retrieval remains the conformance oracle. ANN, learned reranking, graph expansion, a dedicated vector store, partitions, caches, and a broker are introduced only after a named measured failure and must preserve isolation, temporal correctness, provenance, recovery, and rebuildability.

Durable jobs and transactional outbox entries remain in PostgreSQL initially. The release ships checksummed Linux binaries and signed non-root OCI images for amd64 and arm64, requires external PostgreSQL for production-like use, exports vendor-neutral OTLP/Prometheus telemetry without private content by default, and treats Compose as a reproducible development/small-install profile rather than production or HA proof.

MCP, A2A, agent frameworks, SDKs, object stores, embedding providers, observability backends, and deployment orchestrators are versioned adapters. MCP/A2A task, session, context, or message identifiers may be retained as provenance but never become canonical memory or authorization authority.

## Consequences

- PostgreSQL 18 narrows the initial compatibility matrix but provides a supported greenfield floor through 2027 and native temporal constraints.
- OpenAPI 3.1.2 maximizes current tool interoperability while a deterministic gate prevents 3.2 support from becoming an indefinite subjective delay.
- Exact authorized retrieval limits early scale but establishes a trustworthy oracle and prevents approximate-index architecture from preceding evidence.
- The first release carries strong temporal, isolation, deletion, recovery, provenance, and evaluation obligations before any production-readiness claim.
- Protocol, framework, model, index, and hosting changes should require adapter updates or derived-index rebuilds, not canonical-memory migrations.
