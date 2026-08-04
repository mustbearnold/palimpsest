# Official Matt Pocock skills provenance

Palimpsest installs the complete editable skill inventory from [`mattpocock/skills`](https://github.com/mattpocock/skills) at project scope.

- Installed inventory: 41 skills
- Upstream observed commit: `2ab958093e83e0ec752e6c1c5932da465bf23e0c`
- Observed at: 2026-07-28
- Installer: `skills@latest`
- Mode: copied, full depth, Codex agent target
- Per-skill source paths and computed hashes: `skills-lock.json`
- Pinned installed-tree digest: `skills-tree.sha256`

Reproducible install command:

```bash
npx --yes skills@latest add mattpocock/skills \
  --agent codex --skill '*' --yes --copy --full-depth
```

After updates, verify discovery, the 41-name inventory, lock entries, and byte-for-byte parity with the pinned upstream commit before committing. Static installer risk labels are review signals, not security guarantees.

`scripts/check-repo.sh` also verifies the exact installed names, lock provenance, and a deterministic SHA-256 digest over every installed skill file and relative path. Regenerate `skills-tree.sha256` only after reviewing and verifying an intentional upstream update.
