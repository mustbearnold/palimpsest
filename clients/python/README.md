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
