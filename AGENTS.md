# AGENTS.md — agent entry point

Before any work, adopt your identity and read the law:

1. **`SOUL.md`** — the identity of every AI agent that develops Palimpsest in
   this repository. Adopt it before any work. It defines the agent, not the
   product.
2. **`INDEX.md`** — the living map of the repository. Read it at the start of
   a turn. Refresh it at the end of every development turn; the file states
   the contract.
3. **`specs/constitution.md`** — highest authority: principles, the SDD loop,
   authority model, GitHub workflow, quality bar, and domain vocabulary.
4. **`specs/conventions.md`** — formatting and style law (all prose must
   follow ASD-STE100 Simplified Technical English).

Then follow the SDD loop: no code without a spec; update the spec in the same
change when implementation diverges; a bug is a failing acceptance criterion.
New documents follow the decision tree in the constitution (spec / ADR /
runbook / architecture — no fourth kind).

Verify changes with `scripts/dev-check.sh` before you push. The development
feedback loop is local. Never block development on GitHub Actions. See
`docs/runbooks/local-verification.md`.

Operational guidance: `docs/runbooks/issue-tracker.md` (GitHub Issues as the
planning source of truth) and `docs/runbooks/triage-labels.md` (five canonical
triage roles). Capability specs: `specs/` (`001`–`017`); known gaps:
`specs/BACKLOG.md`.

Also read `docs/architecture.md` before changing architecture, governance,
security, storage, or the public contract, and the relevant records under
`docs/decisions/`.
