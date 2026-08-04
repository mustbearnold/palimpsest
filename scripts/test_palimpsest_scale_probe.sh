#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fixture_dir="$repo_root/scripts/test-fixtures/scale-probe"

probe_output="$(
    PATH="$fixture_dir:$PATH" \
        PALIMPSEST_SCALE_DATABASE_URL='postgresql://scale-probe-fixture' \
        PALIMPSEST_SCALE_REVISIONS=1000 \
        PALIMPSEST_SCALE_QUERIES=5 \
        bash "$repo_root/scripts/palimpsest-scale-probe.sh"
)"

PROBE_OUTPUT="$probe_output" python3 - <<'PY'
import json
import os

report = json.loads(os.environ["PROBE_OUTPUT"])
assert report["transaction_rolled_back"] is True
assert report["plan_summary"] == {
    "planning_time_ms": 1.25,
    "execution_time_ms": 13.75,
    "top_nodes": [
        {
            "node_type": "Aggregate",
            "relation": None,
            "actual_total_time_ms": 12.5,
            "actual_rows": 1,
            "actual_loops": 1,
            "shared_hit_blocks": 4,
            "shared_read_blocks": 2,
            "temp_read_blocks": 0,
            "temp_written_blocks": 0,
        },
        {
            "node_type": "Sort",
            "relation": "memory.fact_revisions",
            "actual_total_time_ms": 9.25,
            "actual_rows": 1000,
            "actual_loops": 1,
            "shared_hit_blocks": 2,
            "shared_read_blocks": 2,
            "temp_read_blocks": 1,
            "temp_written_blocks": 3,
        },
    ],
}
PY
