# Subject lifecycle fence evaluation

Date: 2026-07-29 Issue: #29 Status: review remediation under validation

## Claim under evaluation

An active subject may admit bounded content work. Once its monotonic lifecycle commits `deletion_pending`, no new HTTP response, idempotent replay, projection worker, or restricted database query may begin returning subject content. Work admitted before the transition remains durably visible as a content lease until it drains or is released.

This evaluation does not claim that subject deletion is implemented. Durable deletion operations, lease revocation, purge workers, and absence verification remain issue #31.

## Environment

- PostgreSQL server version: 18 or newer, asserted by each integration test.
- pgvector extension: exactly 0.8.5, asserted by each integration test.
- Isolation authority: separate fresh `NOSUPERUSER NOBYPASSRLS` runtime and least-privilege lifecycle-controller roles with forced row-level security.
- Public seam: real TCP HTTP requests through the Axum adapter and PostgreSQL repository.

## Scenarios and results

| Scenario | Result |
| --- | --- |
| Closed trusted grant vocabulary rejects unknown names and defaults empty | Pass |
| Missing delete grant and wrong subject scope both fail before the repository with the same not-found result | Pass |
| Domain lifecycle permits only active to deletion-pending to deleted | Pass |
| Authorized application seam safely initializes a missing row and retries concurrent serializable transitions | Pass |
| Ordinary runtime role cannot invoke transition functions or update lifecycle rows | Pass |
| Database trigger rejects reactivation and refuses deleted while any lease remains | Pass |
| Every public content-producing MemoryService method requires an unforgeable matching lease permit | Pass |
| Active HTTP response retains one UUIDv7 lease through body delivery | Pass |
| Pending fence rejects all episode, fact, as-of, checkpoint, retrieval, and replay handlers with redacted 404 responses | Pass |
| Response admitted before the fence drains its already-authorized body, then releases its lease | Pass |
| Expired permits, HTTP bodies, and provider work fail closed; release cleanup retries | Pass |
| Embedding projection retains one worker lease through its provider call and releases it afterward | Pass |
| Pending fence rejects a new projection before the provider is called | Pass |
| Exclusive lifecycle transition waits for an in-flight shared subject transaction | Pass |
| Restricted forced-RLS queries expose zero canonical, receipt, checkpoint, retrieval, and projection rows after the fence | Pass |
| Pre-vector lexical receipts survive migrations 0007 through 0009 and still replay | Pass |

The cross-tenant and cross-subject conformance suite also remained green under the new restrictive policies.

## Commands

```bash
bash scripts/check-repo.sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

All commands passed. The workspace test includes the PostgreSQL 18 conformance, legacy receipt upgrade, and focused subject lifecycle fence scenarios.

## Residual boundaries

- Content delivery and provider work stop at the 30-second lease deadline. Lease rows remain durable until retrying cleanup succeeds, and terminal lifecycle transition requires zero rows. Issue #31 owns explicit revocation, target purge, and absence verification.
- No public export or deletion endpoint exists in this change.
- No production deployment, destructive purge, recovery exercise, or release claim was performed.

## Verdict

The implementation is undergoing independent Standards and Spec re-review. This report authorizes neither a security-sensitive release nor a first production deployment.
