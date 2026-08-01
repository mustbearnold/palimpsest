# ADR-0009: Main-only direct-commit delivery

Status: accepted

Date: 2026-08-01

## Context

The founder has explicitly chosen a single-branch delivery workflow for
Palimpsest. The existing PR-only ruleset and documentation added ceremony and
made the repository's actual operating mode differ from the founder's intent.
Routine work must be attributable, checked, and delivered from `main` both in
the local checkout and on `origin/main`.

## Decision

- `main` is the sole development and delivery branch, with one ordinary working
  checkout.
- The AI CEO makes coherent commits directly on local `main` and pushes those
  commits to `origin/main`.
- The remote ruleset permits direct pushes to the default branch while retaining
  linear-history, no-force-push, and no-branch-deletion protections.
- Local checks run before commit and push-triggered CI runs after push. Delivery
  is not claimed until the intended SHA is verified on `origin/main` and the
  relevant CI is green.
- GitHub Issues remain the planning and autonomy frontier. Independent review,
  founder approval, security gates, and release gates retain their existing
  scope; this decision changes the delivery mechanism, not those authorities.
- Feature branches, extra worktrees, and pull requests are not used for routine
  maintainer or AI CEO work. External contributors may still submit pull
  requests when appropriate.

## Consequences

- The repository has one current line of development and no merge queue or PR
  merge step for routine work.
- A coherent commit message, local validation evidence, the remote SHA, and CI
  result are the delivery record.
- Direct main commits increase the importance of pre-push checks and make
  recovery discipline essential; force-push and branch-deletion protections stay
  enabled.
- The PR template remains only for external contributions and is not a normal
  maintainer workflow.
