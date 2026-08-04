# AGENTS.md — agent entry point

Read the repository law before any work:

1. **`specs/constitution.md`** — highest authority: principles, the SDD loop,
   authority model, GitHub workflow, quality bar, and domain vocabulary.
2. **`specs/conventions.md`** — formatting and style law.

Then follow the SDD loop: no code without a spec; update the spec in the same
change when implementation diverges; a bug is a failing acceptance criterion.
New documents follow the decision tree in the constitution (spec / ADR /
runbook / architecture — no fourth kind).

Operational guidance: `docs/runbooks/issue-tracker.md` (GitHub Issues as the
planning source of truth) and `docs/runbooks/triage-labels.md` (five canonical
triage roles). Capability specs: `specs/` (`001`–`010`); known gaps:
`specs/BACKLOG.md`.

Also read `docs/architecture.md` before changing architecture, governance,
security, storage, or the public contract, and the relevant records under
`docs/decisions/`.
