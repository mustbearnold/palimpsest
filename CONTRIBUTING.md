# Contributing

## Workflow

1. Start with a GitHub issue that states the user-visible outcome and validation
   seam.
2. Use a short-lived branch named `codex/<outcome>` for agent work or an equally
   descriptive branch for human contributions.
3. Add tests at the highest stable seam and run focused checks while working.
4. Open a pull request that links the issue, states risks, and includes exact
   validation evidence.
5. Address Standards and Spec review findings separately. Merge only after
   required checks pass.

## Commit and pull-request quality

- Keep commits coherent and attributable.
- Do not commit secrets, customer data, private evaluation corpora, generated
  credentials, or raw production memory.
- Pin GitHub Actions to full commit SHAs.
- Update `CONTEXT.md` or an ADR when behavior changes their truth.
- Do not weaken tests or authorization to complete a change.

## First check

```bash
bash scripts/check-repo.sh
```
