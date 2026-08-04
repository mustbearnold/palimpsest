#!/usr/bin/env bash
set -euo pipefail

required_files=(
    AGENTS.md
    README.md
    SECURITY.md
    docs/architecture.md
    docs/decisions/0001-postgres-temporal-source-of-truth.md
    docs/decisions/0002-ai-ceo-and-github-governance.md
    docs/runbooks/issue-tracker.md
    docs/runbooks/triage-labels.md
    specs/constitution.md
    specs/conventions.md
    specs/001-memory-service/spec.md
    skills-tree.sha256
    skills-lock.json
)

for required_file in "${required_files[@]}"; do
    if [[ ! -s "$required_file" ]]; then
        echo "missing or empty required file: $required_file" >&2
        exit 1
    fi
done

skill_count="$(find .agents/skills -mindepth 2 -maxdepth 2 -name SKILL.md -type f | wc -l | tr -d ' ')"
lock_count="$(node -e "const lock=require('./skills-lock.json'); process.stdout.write(String(Object.keys(lock.skills ?? {}).length))")"

if [[ "$skill_count" != "41" ]]; then
    echo "expected 41 installed skills, found $skill_count" >&2
    exit 1
fi

if [[ "$lock_count" != "$skill_count" ]]; then
    echo "skills-lock.json has $lock_count entries for $skill_count installed skills" >&2
    exit 1
fi

installed_names="$(find .agents/skills -mindepth 1 -maxdepth 1 -type d -printf '%f\n' | LC_ALL=C sort)"
locked_names="$(node -e "const lock=require('./skills-lock.json'); process.stdout.write(Object.keys(lock.skills ?? {}).sort().join('\\n'))")"

if [[ "$installed_names" != "$locked_names" ]]; then
    echo "installed skill names do not match skills-lock.json" >&2
    diff -u <(printf '%s\n' "$locked_names") <(printf '%s\n' "$installed_names") || true
    exit 1
fi

node <<'NODE'
const lock = require('./skills-lock.json');
for (const [name, skill] of Object.entries(lock.skills ?? {})) {
  if (skill.source !== 'mattpocock/skills' || skill.sourceType !== 'github') {
    throw new Error(`${name}: unexpected skill source`);
  }
  if (!/^skills\/.+\/SKILL\.md$/.test(skill.skillPath ?? '')) {
    throw new Error(`${name}: invalid upstream skill path`);
  }
  if (!/^[0-9a-f]{64}$/.test(skill.computedHash ?? '')) {
    throw new Error(`${name}: invalid computed hash`);
  }
}
NODE

expected_skills_tree="$(tr -d '[:space:]' <skills-tree.sha256)"
actual_skills_tree="$({
    find .agents/skills -type f -print0 |
        LC_ALL=C sort -z |
        while IFS= read -r -d '' skill_file; do
            file_hash="$(sha256sum "$skill_file" | cut -d' ' -f1)"
            printf '%s  %s\n' "$file_hash" "$skill_file"
        done
} | sha256sum | cut -d' ' -f1)"

if [[ ! "$expected_skills_tree" =~ ^[0-9a-f]{64}$ ]]; then
    echo "skills-tree.sha256 does not contain one SHA-256 digest" >&2
    exit 1
fi

if [[ "$actual_skills_tree" != "$expected_skills_tree" ]]; then
    echo "installed skill tree does not match skills-tree.sha256" >&2
    exit 1
fi

git diff --check
empty_tree="$(git hash-object -t tree /dev/null)"
git diff --check "$empty_tree" HEAD
echo "repository contract valid: $skill_count pinned official skills verified"
