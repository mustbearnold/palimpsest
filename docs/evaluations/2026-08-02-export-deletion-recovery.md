# Export and deletion recovery evaluation

Date: 2026-08-02
Product profile: Palimpsest development HTTP service over PostgreSQL 18 plus
pgvector 0.8.5, with forced RLS and the configured private filesystem export
store
Schema profile: migrations `0010_deletion_operations`,
`0011_canonical_history_exports`, `0012_deletion_rls_worker_paths`,
`0013_deletion_terminal_outcomes`, `0014_bounded_projection_leases`,
`0015_restore_fence_replay`, and `0016_release_deletion_operation_lease`
Worker profile: `palimpsest-deletion-worker/v1`
Configured deletion targets: canonical memory, PostgreSQL projections, and
the private filesystem export target
Backup policy: `not_configured` in this development profile

## Evidence

The fixed-seed HTTP corpus writes an episode containing a private marker,
creates a full-history export, verifies idempotent export replay, waits for a
ready package, checks conditional status, and reads the authorized content.
Cross-tenant access is redacted. A same-scope principal without the export
grant cannot create or read the export, and a same-scope principal without the
deletion grant cannot read the deletion operation.

The deletion request immediately fences new exports, replays idempotently,
completes through the worker, verifies the configured target ledger and
content-free verification digest, supports conditional status, and leaves the
episode, export content, and export status unavailable. The retained
tombstone is checked for the episode identifier, private marker, external
identifier, and raw idempotency key; none are present.

The export-worker lease corpus claims a fixed-seed operation with a one-second
lease, proves that a live lease is not reclaimed, then reclaims it after
expiry. The original worker cannot finalize the operation after reclamation;
only the recovered worker can advance the operation to `ready`.

The same corpus forces the configured filesystem store to reject staging. The
worker returns an unavailable result, records the sanitized
`package_store_failed` terminal state, and leaves package metadata absent.

A separate worker fixture denies the persisted export grant after the operation
is queued. The worker records `authorization_revoked`, clears its claim, and
returns without materializing content; the operation does not remain stranded
in `materializing` until lease expiry.

The deletion worker is also run with a queued export and the same unavailable
store after the subject fence is committed. It records `target_effect_failed`,
keeps the export target pending and the operation in `purging`, and reports no
verification or completion claim. A new content lease is rejected by the
fence, and a mismatched-scope lease release is denied by forced RLS. Replacing
the faulting file with a directory lets the same operation resume; it reaches
`completed`, marks the export target `done` and `verified`, and clears the
transient error.

The complete local suite, PostgreSQL conformance, repository contract, denied
warnings lint, and OpenAPI lint are the required gates for this evidence.

## Boundary

This is development recovery evidence, not proof of backup/PITR, object-store
or cache recovery, external-effect fault injection after every durable
transition, or a production release. Those adapters and release gates remain
open work.
