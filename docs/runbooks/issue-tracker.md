# Issue tracker: GitHub

Specifications and tickets live as GitHub Issues in the repository identified by `git remote -v`. Use the `gh` CLI for issue operations.

## Conventions

- Create specifications and tickets as issues, not pull-request descriptions.
- Read an issue with its comments and labels before acting.
- Preserve native sub-issue and blocking relationships when available.
- An unassigned, unblocked `ready-for-agent` issue is on the autonomy frontier.
- Claim an issue by assigning it before the first implementation write.
- Record validation evidence and remaining uncertainty before closing.
- GitHub shares numbering between issues and pull requests; resolve ambiguous references before mutating either.

## Pull requests as an optional external-contribution surface

Routine maintainer and AI CEO work is committed directly to `main`; it does not use pull requests. An external contributor may submit a pull request, but it must still implement an already-specified issue rather than become a substitute for the issue queue.

## Skill mappings

- “Publish to the issue tracker” means create a GitHub issue.
- “Fetch the relevant ticket” means read the GitHub issue and its comments.
- `to-spec` creates one `ready-for-agent` issue containing the full specification.
- `to-tickets` creates dependency-aware child issues and identifies the frontier.
- `triage` applies the labels configured in `triage-labels.md`.
