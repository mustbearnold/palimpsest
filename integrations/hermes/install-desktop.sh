#!/usr/bin/env bash
# Install the Palimpsest Hermes plugin (memory provider + desktop pane) into a
# Hermes home. Idempotent: safe to re-run after updates. Uses symlinks so a
# local checkout stays live, or run it from an installed copy to materialize.
#
# Usage: bash install-desktop.sh [HERMES_HOME]
# Default HERMES_HOME: $HERMES_HOME env, else ~/.hermes (or
# ~/.hermes/profiles/<name> for named profiles — pass it explicitly).
set -euo pipefail

HERMES_HOME="${1:-${HERMES_HOME:-$HOME/.hermes}}"
PLUGIN_SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

mkdir -p "$HERMES_HOME/plugins" "$HERMES_HOME/desktop-plugins/palimpsest"

if [[ ! -e "$HERMES_HOME/plugins/palimpsest" ]]; then
    ln -s "$PLUGIN_SRC" "$HERMES_HOME/plugins/palimpsest"
    echo "linked $HERMES_HOME/plugins/palimpsest -> $PLUGIN_SRC"
fi

if [[ ! -e "$HERMES_HOME/desktop-plugins/palimpsest/plugin.js" ]]; then
    ln -s "$PLUGIN_SRC/desktop/plugin.js" "$HERMES_HOME/desktop-plugins/palimpsest/plugin.js"
    echo "linked $HERMES_HOME/desktop-plugins/palimpsest/plugin.js"
fi

echo
echo "Next steps:"
echo "  hermes plugins enable palimpsest    # enables the /api/plugins/palimpsest backend"
echo "  hermes memory setup                 # select palimpsest (bearer token stays in .env)"
echo "  Desktop app: ⌘K → Reload desktop plugins"
