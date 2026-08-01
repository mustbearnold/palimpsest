# Contributing

## Workflow

1. Start with a GitHub issue that states the user-visible outcome and validation
   seam.
2. Maintainer and AI CEO work is performed directly on the sole local `main`
   branch. Do not create feature branches, extra worktrees, or pull requests for
   routine changes.
3. Add tests at the highest stable seam and run focused checks while working.
4. Commit a coherent, attributable change on `main`, then push it to
   `origin/main` and verify the remote SHA and push-triggered CI.
5. External contributors may use pull requests when needed, but that is not the
   maintainer delivery path. Required review and release gates still apply to
   the work that falls under them.

## Commit quality

- Keep commits coherent and attributable.
- Do not commit secrets, customer data, private evaluation corpora, generated
  credentials, or raw production memory.
- Pin GitHub Actions to full commit SHAs.
- Update `CONTEXT.md` or an ADR when behavior changes their truth.
- Do not weaken tests or authorization to complete a change.
- Never force-push or delete `main`.

## First check

```bash
bash scripts/check-repo.sh
```
