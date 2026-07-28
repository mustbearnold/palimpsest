# ADR-0002: AI CEO operates through bounded GitHub governance

Status: accepted

Date: 2026-07-28

## Context

The founder wants the project to develop autonomously while remaining safe,
auditable, and recognizably expert in public GitHub. Unlimited agent authority
would allow the system to weaken its own controls or create external commitments
the founder did not authorize.

## Decision

The human founder retains constitutional authority. The AI agent acts as
operating CEO inside `AGENTS.md`. GitHub Issues define work, dependency-aware
`ready-for-agent` issues define the autonomy frontier, and pull requests plus
required checks and independent two-axis review define the delivery gate.

The CEO may merge low-risk, fully specified work after gates pass. Governance,
credentials, spending, legal terms, destructive production operations, first
production deployment, major releases, and every security-sensitive or high-risk
release remain human-controlled. Independent review is still mandatory for those
releases and does not replace founder approval.

## Consequences

- Autonomous work can proceed without repeated approval for ordinary reversible
  engineering actions.
- Every material decision and implementation has a durable public trail.
- A running agent or scheduled job is still required; governance documentation
  does not itself create a daemon.
- The CEO cannot expand its own authority or call self-review independent.
