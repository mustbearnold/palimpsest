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
import re

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
assert report["selective_plan_summary"]["top_nodes"][1]["node_type"] == "Bitmap Heap Scan"
assert report["selective_plan_summary"]["top_nodes"][2]["node_type"] == "Bitmap Index Scan"
assert re.fullmatch(r"[0-9a-f]{64}", report["selective_plan_sha256"])
assert report["bands"] == [
    {
        "band": "all",
        "measured_queries": 2,
        "p50_ms": 1.0,
        "p95_ms": 2.0,
        "p99_ms": 3.0,
        "mean_ms": 1.5,
        "max_ms": 4.0,
    },
    {
        "band": "quarter",
        "measured_queries": 1,
        "p50_ms": 0.5,
        "p95_ms": 0.6,
        "p99_ms": 0.7,
        "mean_ms": 0.55,
        "max_ms": 0.7,
    },
    {
        "band": "sixteenth",
        "measured_queries": 1,
        "p50_ms": 0.3,
        "p95_ms": 0.35,
        "p99_ms": 0.4,
        "mean_ms": 0.32,
        "max_ms": 0.4,
    },
    {
        "band": "thirtysecond",
        "measured_queries": 1,
        "p50_ms": 0.2,
        "p95_ms": 0.25,
        "p99_ms": 0.3,
        "mean_ms": 0.22,
        "max_ms": 0.3,
    },
]
PY
