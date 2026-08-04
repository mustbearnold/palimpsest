# ADR-0013: Dependency-free Python client boundary

Status: accepted

## Context

Palimpsest's first-party HTTP service already implements the temporal memory
lifecycle, but an agent developer still has to hand-write authorization headers,
idempotency handling, temporal request bodies, and conditional supersession.
The product wedge calls for a Python client while keeping the versioned HTTP
contract canonical and adapters replaceable.

## Decision

Ship `clients/python` as a thin, dependency-free Python client for the existing
`/v1` API. It fixes one tenant and subject scope at construction and requires an
explicit bearer token; request path values never grant authority. It exposes
low-level episode, fact, correction, retrieval, temporal as-of, export,
deletion, and conditional checkpoint operations plus four adoption-facing helpers:

- `remember` appends an immutable episode and then promotes a governed fact;
- `recall` creates an authorized current or explicit as-of retrieval receipt;
- `correct` appends a fact revision with the caller's strong ETag and evidence;
- `forget` starts the server-owned subject deletion state machine, and
  `wait_for_deletion` follows it with ETag-aware conditional polling until a
  terminal result.

Checkpoint reads and writes use the same exact-one-precondition rule as the
HTTP contract: creation requires `If-None-Match: *`, while an advance requires
the current strong `If-Match` ETag.

Export status and content preserve the HTTP contract's `ETag`, `Location`,
`303`, and binary package response instead of silently following a ready
redirect or decoding an export as JSON.

The client generates idempotency keys when omitted, but callers must provide a
stable key for a retry. `remember` derives distinct episode and fact keys. If
the second request fails, it raises a typed `PartialRememberError` containing
the committed episode and the typed server/transport cause. It never retries a
mutation implicitly, parses retrieval policy behavior locally, or accesses the
database.

## Consequences

Python agents can adopt the governed lifecycle without coupling to Rust,
PostgreSQL, MCP, or a particular framework. The standard-library-only runtime
keeps installation simple for self-hosted agents, while the narrow client means
the server remains the source of truth. TypeScript, framework adapters, and
generated clients remain separate future adapters rather than being smuggled
into this boundary.
