# ADR-0031: Token-efficiency initiative (flat-file splits, skill slimming, stale-reference fixes)

Date: 2026-08-07 · Status: accepted

## Context

AI-agent sessions operating this repo pay a recurring context chain and a
flat-file targeted-read cost. Measured on the live tree (2026-08-07):

- The mandatory per-session chain (AGENTS.md → palimpsest-development skill
  body → constitution → conventions → issue-tracker/triage runbooks) was
  **9,758 tokens**, of which the skill body was 5,775 (59%).
- Three flat Rust files dominate targeted reads: `palimpsest-postgres/src/
  lib.rs` (7,111 lines, ~70k tok, one `mod tests`), `palimpsest-server/tests/
  conformance_postgres18.rs` (6,578 lines, ~62k tok), `palimpsest-conformance/
  src/lib.rs` (5,024 lines, ~43k tok, 40 `pub async fn` scenario entry
  points). A session touching persistence pays a full-file read of the
  70k-token `lib.rs`.
- Three stale references were found: AGENTS.md and README.md both advertise
  capability specs `001–010` (thirteen exist), and `specs/constitution.md`
  line 139 lists `python3 scripts/test_palimpsest_mcp.py` — the script lives
  at `tools/test_palimpsest_mcp.py`; `scripts/…` does not exist.

## Decision

Adopt the approved ASTRONOMICAL-PLAN "Token Efficiency for Palimpsest"
(`.hermes/plans/2026-08-07_token-efficiency-PLAN.md`, single-reviewer gate:
APPROVED 2026-08-07, same-session re-review):

1. **Skill slimming (profile-side, no repo commit)**: relocate bulk
   (frontier snapshot, tooling pitfalls, server-contract/conformance facts,
   break-test loop) from the `palimpsest-development` SKILL.md body into its
   existing `references/` (four new files); keep the full local gate and env
   recipe verbatim. Body target ≤ 2,800 tok (5,955 → 2,796 achieved).
   Nothing deleted — every relocated section is greppable in `references/`.
2. **Module splits (repo, structure-only commits)**: split the three flat
   Rust files into trait/domain-scoped modules with `pub use` re-exports so
   the public crate surface is unchanged. Targets: postgres `lib.rs` facade
   ≤ 12k tok, every new module ≤ 15k tok; conformance `lib.rs` facade
   ≤ 3k tok; no file under `tests/conformance_postgres18/` > 15k tok.
   Zero behavior change — the conformance suite is the contract.
3. **Stale-reference fixes (repo, docs commits)**: AGENTS.md + README.md
   spec range → `001–013`; constitution gate path `scripts/` → `tools/`.
   (Scope note: the constitution's gate block may still omit the
   integrations/hermes unittest and scale-probe steps — aligning block
   membership is a constitutional change for the founder, not part of this
   decision.)
4. **Protected surfaces untouched**: SHA-pinned `.agents/skills`,
   digest-pinned `evaluations/retrieval-corpus-v1/`, `migrations/`,
   `_attic/`, `api/openapi.yaml`, client package formats, gitignored
   caches.

## Consequences

- Easier: targeted reads cost 5–15k tok instead of 43–70k; the mandatory
  chain drops ~3.1k tok per session (−32%); entry docs state reality.
- Harder: module splits change `src/lib.rs` layouts; any external reference
  to old internal paths must be re-pointed (sweep done in the same commits;
  `check-repo.sh` pins none of the moved paths).
- Locked in: trait/domain-scoped modules as the layout convention for
  `palimpsest-postgres` and the conformance suite; skill body stays an index
  over references.
