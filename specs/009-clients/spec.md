# 009 — Governed clients

Status: active
Owner: AI CEO

## Purpose

Dependency-free Python and TypeScript clients that follow the stable,
OpenAPI-defined HTTP contract. Clients are thin: they expose the governed
lifecycle and never own memory policy.

## Requirements

- R1. The Python client MUST be dependency-free and the TypeScript client MUST
  be a dependency-free ESM module with TypeScript declarations.
- R2. Both clients MUST expose `remember`, `recall`, `correct`, and `forget`,
  plus lower-level episode, fact, temporal as-of, checkpoint, retrieval-page,
  export, and deletion-status helpers.
- R3. Both clients MUST expose per-project recall helpers returning isolated
  evidence bundles, and comparison helpers returning deterministic
  key/value-digest review candidates plus bounded lexical-overlap and token
  deltas without making semantic claims or durable writes.
- R4. Both clients MUST support `validate_project_review` and
  `consolidate_project_review` with per-claim deterministic idempotency and
  retryable partial completion.
- R5. Clients MUST connect only through the authorized HTTP boundary; they
  MUST NOT connect to PostgreSQL or carry memory policy.

## Acceptance criteria

- [ ] A1. The Python suite passes: `python3 -m unittest discover -s
      clients/python/tests -p 'test_*.py'` (35 tests).
- [ ] A2. The TypeScript suite passes: `node --test
      clients/typescript/test/*.test.mjs`.
- [ ] A3. Client behavior matches the OpenAPI contract used by the server.

## Out of scope

- Client-side memory policy, direct database access, embedded mode.

## Open questions

- None.

## Links

Code: `clients/python/src/palimpsest` · `clients/typescript/src`
Tests: `clients/python/tests` · `clients/typescript/test`
Decisions: 0013, 0018
Contract: `api/openapi.yaml`
