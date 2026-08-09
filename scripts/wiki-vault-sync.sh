#!/usr/bin/env bash
# Wiki vault push-only git sync (spec 017, P1, [V-1] operator script).
#
# The vault is a derived markdown projection. The sync rebuilds the vault
# directory from an export ZIP, commits it, and pushes it. The sync is
# PUSH-ONLY: this script never pulls, merges, rebases, fetches, or clones.
# A rebuild discards any non-renderer file state in the vault directory.
#
# Usage:
#   wiki-vault-sync.sh <vault-dir> <export-zip> [remote-url] [branch]
#
#   vault-dir   the working directory that holds the vault (may exist)
#   export-zip  path to a materialized palimpsest-wiki-vault-v1 ZIP
#   remote-url  optional git remote; when given, the script pushes to it
#   branch      branch to commit on and push (default: main)
#
# Exit codes: 0 on success, 1 on usage error, 2 on a failed push.

set -euo pipefail

if [[ $# -lt 2 ]]; then
    echo "usage: wiki-vault-sync.sh <vault-dir> <export-zip> [remote-url] [branch]" >&2
    exit 1
fi

VAULT_DIR=$1
EXPORT_ZIP=$2
REMOTE_URL=${3:-}
BRANCH=${4:-main}

if [[ ! -f "$EXPORT_ZIP" ]]; then
    echo "error: export ZIP not found: $EXPORT_ZIP" >&2
    exit 1
fi

# Rebuild the vault from the package. A rebuild discards every previous
# file: the package is the only source of truth for the directory.
rm -rf "$VAULT_DIR"
mkdir -p "$VAULT_DIR"

if ! unzip -q -o "$EXPORT_ZIP" -d "$VAULT_DIR"; then
    echo "error: failed to extract $EXPORT_ZIP" >&2
    exit 1
fi

# The manifest freezes the export identity; the commit message names it so
# an export maps to a commit one-to-one.
EXPORT_ID=$(sed -n 's/.*"export_id": *"\([^"]*\)".*/\1/p' \
    "$VAULT_DIR/manifest.json" | head -n 1)
if [[ -z "$EXPORT_ID" ]]; then
    echo "error: manifest.json carries no export_id" >&2
    exit 1
fi

cd "$VAULT_DIR"

if [[ ! -d .git ]]; then
    git init -q -b "$BRANCH"
fi

git add -A
if ! git diff --cached --quiet; then
    git -c user.name="palimpsest-vault-sync" \
        -c user.email="vault-sync@palimpsest.local" \
        commit -q -m "sync palimpsest-wiki-vault-v1 $EXPORT_ID"
fi

if [[ -n "$REMOTE_URL" ]]; then
    if git remote | grep -qx "palimpsest"; then
        git remote set-url palimpsest "$REMOTE_URL"
    else
        git remote add palimpsest "$REMOTE_URL"
    fi
    # Fast-forward push only: a diverged remote rejects the push. The sync
    # never rewrites remote history.
    if ! git push -q palimpsest "HEAD:$BRANCH"; then
        echo "error: push rejected; remote history diverged" >&2
        exit 2
    fi
fi

echo "vault synced: $EXPORT_ID on $BRANCH"
