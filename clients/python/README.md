# Palimpsest Python client

This is a small, dependency-free client for Palimpsest's versioned `/v1` HTTP
API. The server remains responsible for authorization, temporal correctness,
write-policy validation, and deletion state.

Install it from a checkout:

```bash
python3 -m pip install ./clients/python
```

Use a stable `idempotency_key` when a mutation may be retried. The four
high-level operations mirror the first adoption wedge:

```python
from palimpsest import PalimpsestClient

client = PalimpsestClient(
    base_url="http://127.0.0.1:8080",
    bearer_token="operator-issued-token",
    tenant_id="019be000-0000-7000-8000-000000000010",
    subject_id="019be000-0000-7000-8000-000000000020",
    case_id="019be000-0000-7000-8000-000000000030",
)

saved = client.remember(
    "The customer moved to 20 New Street.",
    key="shipping-address",
    idempotency_key="case-123-address-1",
)
current = client.recall("shipping address", idempotency_key="case-123-recall-1")
fact = client.get_fact_response(saved["fact"]["fact_id"])
client.correct(
    saved["fact"]["fact_id"],
    supersedes_revision_id=fact.data["revision"]["revision_id"],
    value={"address": "22 New Street"},
    observed_at="2026-08-03T00:00:00Z",
    valid_time={"from": "2026-08-03T00:00:00Z"},
    evidence_episode_ids=[saved["episode"]["episode_id"]],
    write_policy={"id": "direct-evidence", "version": "1"},
    confidence=1.0,
    sensitivity="internal",
    retention_policy_id="standard",
    if_match=fact.etag,
    idempotency_key="case-123-address-2",
)
deletion = client.forget(idempotency_key="case-123-forget-1")
# Wait for the server-owned deletion worker when the caller needs completion:
client.wait_for_deletion(deletion["operation_id"], timeout_seconds=60)
```

The same client also exposes conditional checkpoint read/save methods for
resumable agent threads, plus `start_export`, `get_export_response`, and
`download_export` for authorized canonical-history packages. Ready export
status is represented as a `303` response with its download `Location`; the
client does not follow that redirect implicitly.

`remember` intentionally performs two durable requests: the immutable episode
is committed first, then the governed fact cites it. If promotion fails,
`PartialRememberError.episode` exposes the saved evidence and the original
typed cause so callers can retry or investigate without pretending the write
was atomic.

## Ingest coding-agent sessions

The checkout includes an opt-in polling bridge for Codex, Claude Code, and
Hermes. It accepts source paths explicitly, or can check the exact conventional
locations for the current user with `--discover`; it never scans a home tree or
another user's Hermes data.

Set the authorized Palimpsest connection in environment variables, then run a
long-lived poller:

```bash
export PALIMPSEST_INGEST_BASE_URL=http://127.0.0.1:8080
export PALIMPSEST_INGEST_BEARER_TOKEN=operator-issued-token
export PALIMPSEST_INGEST_TENANT_ID=019be000-0000-7000-8000-000000000010
export PALIMPSEST_INGEST_SUBJECT_ID=019be000-0000-7000-8000-000000000020
export PALIMPSEST_INGEST_CASE_ID=019be000-0000-7000-8000-000000000030

python3 scripts/palimpsest_ingest.py watch \
  --source codex="$HOME/.codex/sessions" \
  --source claude="$HOME/.claude/projects" \
  --source hermes="$HOME/.hermes/state.db"
```

For the conventional locations above, the equivalent discovery mode is:

```bash
python3 scripts/palimpsest_ingest.py watch --discover
```

On this Linux checkout, install that watcher as a user service when it should
run continuously with the desktop session:

```bash
bash scripts/install-palimpsest-ingest-service.sh
systemctl --user status palimpsest-ingest.service
```

The service reads the current user's exact conventional stores, keeps its
owner-only cursor under `~/.local/state/palimpsest`, and uses the same local
development HTTP defaults as the foreground command. Put remote credentials
or non-default scope in the owner-only
`~/.config/palimpsest/ingest.env`; do not put them in the unit file. Remove it
with `systemctl --user disable --now palimpsest-ingest.service` when continuous
ingestion is no longer wanted.

Discovery is recalculated on every watch pass, so a provider store created
after the watcher starts is picked up. Use `PALIMPSEST_INGEST_CODEX_SESSIONS`,
`PALIMPSEST_INGEST_CLAUDE_PROJECTS`, or
`PALIMPSEST_INGEST_HERMES_STATE_DB` for a non-standard location, or keep using
`--source` when another account has separately authorized a path.

The first pass baselines existing history and ingests only later events. Add
`--backfill` when importing existing history is deliberate. Use
`--project-root /path/to/repository` to limit a poller to one project. Every
event receives a stable `project-...` identity and an exact
`agent_session:project-...` namespace, so retrieval can keep projects
separate:

```python
from palimpsest import project_namespace

project_facts = client.recall(
    "release decision",
    filters={"namespaces": [project_namespace("project-0123456789abcdef")]},
)
# Or ask for separate evidence bundles for several projects at once:
project_bundles = client.recall_by_project(
    "release decision",
    ["project-0123456789abcdef", "project-fedcba9876543210"],
)
comparison = client.compare_by_project(
    "release decision",
    ["project-0123456789abcdef", "project-fedcba9876543210"],
)
```

`recall_by_project` intentionally returns one retrieval response per project;
it gives a caller or model clean evidence bundles to compare. The
`compare_by_project` helper returns those bundles plus a structural summary:
exact key/value matches, project-specific keys, and same-key/different-value
review candidates, plus bounded token-overlap candidates for differently keyed
session messages. Each lexical candidate includes a bounded shared/only-in
token delta to make wording changes visible without pretending to understand
intent. It uses canonical value digests and lexical Jaccard only, performs no
model inference, and writes no memory; callers must inspect the returned
bundles before treating any candidate as a real semantic conflict.

The local MCP adapter exposes the same operations as
`palimpsest_recall_by_project` and `palimpsest_compare_by_project`. Both
require at least two distinct project IDs, so an agent can ask one question and
receive explicitly keyed bundles without cross-project candidate mixing; the
comparison tool adds only the deterministic structural summary described
above.

The bridge ingests user and assistant text only. It excludes tool rows,
thinking blocks, system prompts, and tool results; common credential-shaped
values are redacted before upload. This is a supervised local process rather
than a hidden background daemon, and the HTTP service remains the authority
for tenant scope, policy, retention, and deletion.
