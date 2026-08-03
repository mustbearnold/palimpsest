#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
project_root="$(cd -- "$script_dir/.." && pwd)"
user_home="$(getent passwd "$(id -u)" | cut -d: -f6)"
expected_root="${user_home}/Projects/Palimpsest"

if [[ "$project_root" != "$expected_root" ]]; then
  echo "the checked-in service template expects this checkout at $expected_root" >&2
  echo "use an explicit --source watcher when the checkout lives elsewhere" >&2
  exit 2
fi

if ! command -v systemctl >/dev/null 2>&1 || ! systemctl --user status >/dev/null 2>&1; then
  echo "a running systemd user manager is required" >&2
  exit 2
fi

service_dir="${XDG_CONFIG_HOME:-${user_home}/.config}/systemd/user"
install -d -m 700 "$service_dir"
install -m 600 "$project_root/scripts/palimpsest-ingest.service" "$service_dir/palimpsest-ingest.service"

systemctl --user daemon-reload
systemctl --user enable --now palimpsest-ingest.service
systemctl --user --no-pager --full status palimpsest-ingest.service
