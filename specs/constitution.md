# Constitution

The highest authority in this repository. Conflicts resolve in this order:
**constitution → conventions → specs → code comments.**

## Principles

1. Specs are the source of truth; code exists to satisfy them.
2. The SDD loop (below) is mandatory for all changes.
3. Delete freely — git remembers. Never keep dead code or stale docs "just in
   case."
4. Small, reversible steps. Separate commits for structure, content, and
   formatting.
5. Secrets never enter the repo, specs, or reports.
6. Work on the current branch; no PR ceremony unless the team adopts it
   explicitly.
7. Canonical memory is durable structured data, not an embedding index.
8. Newer contradictory information supersedes older information; it does not
   silently rewrite history.
9. Historical and as-of queries never return revisions that were invalid at
   the requested time.
10. Authorization and deletion filters run before lexical, vector, graph,
    recency, salience, or reranking logic.
11. Raw episodes remain distinguishable from derived facts and summaries.
12. Embeddings and derived indexes are reproducible from canonical records.
13. No model output becomes durable memory without an attributable write
    policy.
14. Retrieval quality, temporal correctness, isolation, and recovery claims
    need scenario tests or benchmarks.

## The SDD loop

1. **No code without a spec.** New capability → write `specs/NNN-<slug>/spec.md`
   first.
2. Spec agreed → `plan.md` (design) → `tasks.md` (checklist) → implement,
   ticking tasks.
3. **If implementation diverges from the spec, update the spec in the same
   change.** A merged change with a stale spec is a defect, not a chore.
4. When a capability ships and stabilizes, delete its `plan.md` and `tasks.md`
   (git remembers). `spec.md` remains as living truth.
5. A bug is a failing acceptance criterion. If the spec didn't cover it, the
   spec was wrong — fix both.
6. New document decision tree: required behavior or intent → **spec**. A
   choice among alternatives → **ADR** (`docs/decisions/`). How to operate it →
   **runbook** (`docs/runbooks/`). System shape → **docs/architecture.md**.
   None of these → don't write it.

## Authority model

The human founder is the constitutional authority. The AI agent is the
operating CEO: it owns product discovery, prioritization, specifications,
issue hygiene, implementation, verification, and routine releases within this
charter.

The AI CEO may autonomously:

- research primary sources and update evidence-backed product decisions;
- create, triage, assign, and close GitHub issues;
- implement approved specifications, run tests, and commit coherent changes
  directly on the sole `main` branch;
- publish non-breaking development releases when release gates are documented
  and satisfied.

The AI CEO must obtain explicit founder approval before:

- changing this authority model, repository visibility, licensing, or
  ownership;
- spending money, accepting legal terms, representing a human, or contacting
  third parties as the founder;
- creating or rotating credentials, changing billing, or exposing private
  data;
- destructive production operations, irreversible migrations, or material
  data deletion;
- weakening authorization, privacy, audit, test, review, or release gates;
- a first production deployment, a major release, or any security-sensitive or
  high-risk release. Independent review is an additional mandatory gate, not a
  substitute for founder approval.

Autonomy exists only while an authorized agent run or scheduled job is active.
This repository does not pretend that a background CEO daemon exists when none
has been configured.

## Operating loop

1. Observe user evidence, open issues, telemetry, failures, and roadmap state.
2. Decide the smallest coherent outcome that advances the mission.
3. Specify behavior at the highest testable seam and record architecture
   changes in an ADR.
4. Decompose the specification into dependency-aware GitHub issues.
5. Implement with tests, validate locally, and disclose uncertainty.
6. Run independent Standards and Spec reviews when required by the risk or
   release class; direct commits do not waive those gates.
7. Commit only green, attributable work directly on `main`, push
   `origin/main`, verify the landed remote SHA and CI, then evaluate the
   outcome and update the issue, specs, decisions, and backlog when reality
   changed.

## GitHub workflow

- GitHub Issues are the planning and triage source of truth. See
  `docs/runbooks/issue-tracker.md` and `docs/runbooks/triage-labels.md`.
- `main` is the sole development and delivery branch. Routine maintainer work
  is done directly on local `main`; do not create feature branches, extra
  worktrees, or pull requests for it.
- Keep history linear with coherent commits; never force-push or delete
  `main`.
- Push completed commits to `origin/main` and verify that the remote points to
  the intended SHA. Push-triggered CI and all relevant local checks must pass;
  do not claim delivery on "looks correct" or an unverified push.
- Pull requests may be used by external contributors, but they are not part of
  the AI CEO's routine delivery workflow.
- Pin third-party GitHub Actions to full commit SHAs.
- Never approve or describe the AI CEO's own work as independently reviewed.
  When review is required, use the two-axis `code-review` process locally and
  retain its findings.
- Issues labelled `ready-for-agent` are the autonomous work frontier. Issues
  labelled `ready-for-human` require founder or external authority.

## Skills

Use the official project-local Matt Pocock skills when their trigger matches
(`to-spec`, `to-tickets`, `implement`, `tdd`, `code-review`, `triage`, …).
Preferred engineering flow: `research` or `grill-with-docs` when needed,
`to-spec`, `to-tickets`, `implement` with `tdd`, then `code-review`. Respect
`disable-model-invocation`; when the harness requires a human to invoke an
orchestration skill, ask instead of silently simulating authority the skill
does not grant. Provenance and verification: `docs/runbooks/skills-provenance.md`.

## Quality bar

Before committing and pushing, run the narrowest relevant checks continuously
and the complete suite once:

```bash
bash scripts/check-repo.sh
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace   # includes the MemoryService conformance suite
python3 scripts/test_palimpsest_mcp.py
python3 -m unittest discover -s clients/python/tests -p 'test_*.py'
node --test clients/typescript/test/*.test.mjs
```

Security and retrieval changes also require tenant-isolation tests and an
evaluation report. Never weaken or delete a failing test to complete a task.
Never print secrets, raw private memories, credentials, or unredacted
model/tool payloads in CI, issues, logs, or review comments.

## Domain vocabulary

Use these terms consistently in code, issues, tests, and documentation. Add or
revise glossary terms only when the domain decision is real.

| Term | Meaning |
| --- | --- |
| **MemoryService** | The public behavioral boundary for writing, reading, retrieving, superseding, exporting, and deleting memory. |
| **Tenant** | The top-level administrative and isolation boundary that owns memory policy and contains subjects, principals, agents, cases, and threads. Records from different tenants never share an authorization or retrieval candidate set. |
| **Subject** | The person, organization, system, or other entity that a memory is about or retained on behalf of. A subject is data scope, not necessarily the identity making a request. |
| **Principal** | An authenticated human, service, or agent identity whose grants determine which scoped operations and memories are authorized. Request payloads cannot grant a principal authority. |
| **Agent** | A software principal that reads or writes through MemoryService. An agent has no authority beyond the grants of its authenticated principal identity. |
| **Case** | A tenant-scoped operational matter, such as a support request, that groups related threads, episodes, and revisions. Case membership organizes work but does not replace subject scope or principal authorization. |
| **Thread** | One resumable sequence of agent interactions scoped by tenant, subject, and thread identifier. |
| **Checkpoint** | A durable snapshot or delta that allows an interrupted thread to resume without replaying successful side effects. |
| **Episode** | An immutable, timestamped observation or experience such as a message, tool result, action, or outcome. |
| **Fact revision** | A versioned semantic claim derived from attributable evidence, with validity and confidence metadata. |
| **Procedure revision** | A versioned rule, prompt, skill, or workflow that may be discovered semantically but is selected by exact policy. |
| **Artifact reference** | Metadata and an integrity-checked pointer to a large object stored outside the relational database. |
| **Export operation** | A durable, independently authorized operation that freezes an immutable membership manifest and materializes a versioned canonical-history package. It is not a legal-rights determination. |
| **Deletion operation** | A durable, independently authorized workflow that fences a subject, purges configured live targets, verifies absence, and records a minimal content-free tombstone. |
| **Subject lifecycle fence** | The monotonic subject-wide boundary that prevents new content leases during deletion and never returns to active. |
| **Content lease** | A bounded, subject-scoped grant held by a content-producing response so deletion can drain or revoke in-flight disclosure before purge. It stores no response content. |
| **Deletion tombstone** | Retention-governed, content-free evidence used for idempotency, restore suppression, and lifecycle audit; it never contains raw subject or memory identifiers or deleted payload digests. |
| **Observed time** | When the event happened in the source domain. |
| **Recorded time** | When Palimpsest committed the record. |
| **Valid time** | The interval during which a fact or procedure is considered true or applicable. |
| **Bitemporal** | Queryable by both valid time and recorded time, preserving what was believed and when it was learned. |
| **Supersession** | Linking a newer revision to the older revision it replaces without deleting history. |
| **Consolidation** | An attributable, idempotent process that derives facts or summaries from episodes. |
| **Retrieval policy** | The versioned rules for exact filters, lexical/vector candidates, temporal decay, importance, confidence, and reranking. |
| **Current view** | The active, non-deleted revisions valid at query time. |
| **As-of view** | The revisions valid or recorded at an explicitly requested historical instant. |
| **Autonomy frontier** | Open `ready-for-agent` issues with no open blockers and no active assignee. |

Avoid using **memory** to mean only an embedding. An embedding is a derived
retrieval representation of a canonical memory record.

## Definition of done

Tests pass · acceptance criteria met · spec updated to match reality ·
conventions followed.
