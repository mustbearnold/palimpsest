# ADR-0018: Dependency-free TypeScript client boundary

Status: accepted

Date: 2026-08-03

## Context

Palimpsest already has a dependency-free Python client, but a JavaScript or
TypeScript agent still has to hand-build requests, conditional headers, and
the two-step governed `remember` operation. That duplicates policy-adjacent
HTTP behavior and makes SDK parity harder to test.

## Decision

Ship a first-party `@palimpsest/client` package under `clients/typescript`.
The package contains dependency-free ESM runtime JavaScript and TypeScript
declarations rather than a generated or bundled runtime. It uses the host
platform's `fetch` implementation and supports Node 18 or newer.

The client mirrors the stable Python adoption boundary: episodes, facts and
corrections, temporal reads, retrieval, checkpoints, exports, deletion, and
the high-level `remember`/`recall`/`correct`/`forget` helpers. The server stays
the authority for authorization, temporal rules, write policies, and lifecycle
state.

Client behavior has these fixed safety properties:

- mutation helpers generate an idempotency key when omitted and preserve a
  caller-supplied key across retries;
- `remember` performs the episode and fact writes separately and reports a
  committed episode with a typed `PartialRememberError` if promotion fails;
- strong ETags and checkpoint preconditions are passed explicitly;
- export `303` responses are returned to the caller with their `Location`
  rather than followed implicitly;
- request timeouts, transport failures, malformed JSON, and problem responses
  have distinct typed errors;
- the base URL rejects credentials, queries, and fragments, and all requests
  use the configured bearer token without logging it.

## Consequences

JavaScript and TypeScript agents can adopt the same governed HTTP behavior as
Python without adding a runtime dependency or a code-generation toolchain.
The package is intentionally not a second service implementation: it does not
connect to PostgreSQL, interpret memory policy, or expose unimplemented
procedure/artifact operations. The Node contract tests are part of the normal
CI gate, while the OpenAPI document remains the public contract authority.
