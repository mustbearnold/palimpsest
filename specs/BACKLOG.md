# Backlog

Known-but-unspecced capabilities and gaps. One line each. Promote to a
`specs/NNN-<slug>/spec.md` when a capability is scheduled. Status markers:
`scheduled → #N` (tracked in the issue frontier) or `deferred: reason`
(intentionally parked; not a claim).

- [scheduled → #38] Provider-managed backup/PITR orchestration, independent
  backup disposition, and full restore suppression against a real backup
  provider.
- [deferred: needs a consistency ADR first] Multi-region active-active writes
  and a hosted control plane.
- [deferred: until after the first production deployment] External identity
  and credential rotation; public procedure/artifact APIs.
- [deferred: until a live provider contract exists] Provider-specific
  artifact/object deletion revocation, outage, and recovery evidence.
