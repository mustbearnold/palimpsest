//! surface — proactive surfacing conformance (spec 012, issue #45).
//!
//! Scenarios A1–A8 mirror the consolidation scenario style. A5 (the MCP
//! surface tool) is covered by tools/test_palimpsest_mcp.py and is not an
//! HTTP scenario; this module documents the mapping in each verify_* name.

use anyhow::{Context, Result, ensure};
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use sqlx::PgPool;
use uuid::Uuid;

use palimpsest_conformance::Target;

pub(crate) const SURFACE_HOST_ID: &str = "hermes-desktop-conformance";
pub(crate) const SURFACE_PRINCIPAL_ID: &str = "principal-a-surface";

const RETENTION_POLICY_ID: &str = "standard";

fn surface_url(target: &Target) -> String {
    format!(
        "{}/v1/tenants/{}/subjects/{}/surfaces",
        target.base_url.trim_end_matches('/'),
        target.tenant_id,
        target.subject_id
    )
}

fn surface_policies_url(target: &Target) -> String {
    format!(
        "{}/v1/tenants/{}/surface-policies",
        target.base_url.trim_end_matches('/'),
        target.tenant_id
    )
}

async fn register_surface_policy(
    client: &Client,
    target: &Target,
    host_id: &str,
    principal_id: &str,
    body: Value,
) -> Result<Value> {
    let mut policy_body = body;
    let policy_object = policy_body
        .as_object_mut()
        .context("surface policy body must be a JSON object")?;
    policy_object.insert("host_id".to_owned(), json!(host_id));
    policy_object.insert("principal_id".to_owned(), json!(principal_id));
    let response = client
        .post(surface_policies_url(target))
        .bearer_auth(&target.bearer_token)
        .json(&policy_body)
        .send()
        .await
        .context("register surface policy request failed")?;
    ensure!(
        response.status() == StatusCode::CREATED,
        "register surface policy returned {}, expected 201",
        response.status()
    );
    response.json().await.context("policy view missing")
}

async fn post_surface(
    client: &Client,
    target: &Target,
    host_id: &str,
    principal_id: &str,
    context_terms: &[&str],
    idempotency_key: &str,
) -> Result<(StatusCode, Value)> {
    let (status, body) = post_surface_unchecked(
        client,
        target,
        host_id,
        principal_id,
        context_terms,
        idempotency_key,
    )
    .await?;
    ensure!(
        status == StatusCode::CREATED,
        "surface returned {status}, expected 201: {body}"
    );
    Ok((status, body))
}

async fn post_surface_unchecked(
    client: &Client,
    target: &Target,
    host_id: &str,
    principal_id: &str,
    context_terms: &[&str],
    idempotency_key: &str,
) -> Result<(StatusCode, Value)> {
    let response = client
        .post(surface_url(target))
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", idempotency_key)
        .json(&json!({
            "host_id": host_id,
            "principal_id": principal_id,
            "context_terms": context_terms,
        }))
        .send()
        .await
        .context("surface request failed")?;
    let status = response.status();
    let body = response
        .json()
        .await
        .context("surface response was not json")?;
    Ok((status, body))
}

#[allow(clippy::too_many_arguments)]
async fn append_surface_fact(
    client: &Client,
    target: &Target,
    case_id: Uuid,
    namespace: &str,
    key: &str,
    message: &str,
    sensitivity: &str,
    observed_at: &str,
    valid_from: &str,
    valid_until: Option<&str>,
    idempotency_key: &str,
) -> Result<()> {
    let episode_url = format!(
        "{}/v1/tenants/{}/subjects/{}/episodes",
        target.base_url.trim_end_matches('/'),
        target.tenant_id,
        target.subject_id
    );
    let episode_response = client
        .post(&episode_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", format!("{idempotency_key}-episode"))
        .json(&json!({
            "case_id": case_id,
            "kind": "message",
            "observed_at": observed_at,
            "provenance": {
                "source_type": "conformance",
                "source_uri": null,
                "external_id": format!("{idempotency_key}-episode"),
            },
            "sensitivity": sensitivity,
            "retention_policy_id": RETENTION_POLICY_ID,
            "payload": {"message": message},
        }))
        .send()
        .await
        .context("surface fact episode request failed")?;
    ensure!(
        episode_response.status() == StatusCode::CREATED,
        "surface fact episode returned {}, expected 201",
        episode_response.status()
    );
    let episode: Value = episode_response.json().await?;
    let facts_url = format!(
        "{}/v1/tenants/{}/subjects/{}/facts",
        target.base_url.trim_end_matches('/'),
        target.tenant_id,
        target.subject_id
    );
    let fact_response = client
        .post(&facts_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", format!("{idempotency_key}-fact"))
        .json(&json!({
            "case_id": case_id,
            "namespace": namespace,
            "key": key,
            "value": {"message": message},
            "observed_at": observed_at,
            "valid_time": {"from": valid_from, "until": valid_until},
            "evidence_episode_ids": [episode["episode_id"]],
            "write_policy": {"id": "direct-evidence", "version": "1"},
            "confidence": 0.95,
            "sensitivity": sensitivity,
            "retention_policy_id": RETENTION_POLICY_ID,
        }))
        .send()
        .await
        .context("surface fact request failed")?;
    ensure!(
        fact_response.status() == StatusCode::CREATED,
        "surface fact returned {}, expected 201",
        fact_response.status()
    );
    Ok(())
}

/// A1. Tenant isolation: a surface for tenant A never returns tenant B
/// content (spec scenario `verify_surface_tenant_isolation`).
pub(crate) async fn verify_surface_tenant_isolation(target: &Target) -> Result<()> {
    let client = Client::new();
    let case_id = Uuid::parse_str("019be000-0000-7000-8000-000000000701")?;
    register_surface_policy(
        &client,
        target,
        SURFACE_HOST_ID,
        SURFACE_PRINCIPAL_ID,
        json!({}),
    )
    .await?;
    append_surface_fact(
        &client,
        target,
        case_id,
        "case.profile",
        "apollo_plan",
        "Apollo quasar plan targets the lunar south pole.",
        "internal",
        "2026-01-10T09:00:00Z",
        "2026-01-10T00:00:00Z",
        None,
        "surface-a1-apollo",
    )
    .await?;

    // Tenant B content with the same distinctive term. The surface for
    // tenant A must never surface it.
    let tenant_b_url = format!(
        "{}/v1/tenants/{}/subjects/{}/facts",
        target.base_url.trim_end_matches('/'),
        target.principal_b_tenant_id,
        target.principal_b_subject_id
    );
    let tenant_b_episode_url = format!(
        "{}/v1/tenants/{}/subjects/{}/episodes",
        target.base_url.trim_end_matches('/'),
        target.principal_b_tenant_id,
        target.principal_b_subject_id
    );
    let episode_response = client
        .post(&tenant_b_episode_url)
        .bearer_auth(&target.principal_b_bearer_token)
        .header("Idempotency-Key", "surface-a1-tenant-b-episode")
        .json(&json!({
            "case_id": Uuid::parse_str("019be000-0000-7000-8000-000000000402")?,
            "kind": "message",
            "observed_at": "2026-01-10T09:00:00Z",
            "provenance": {
                "source_type": "conformance",
                "source_uri": null,
                "external_id": "surface-a1-tenant-b-episode",
            },
            "sensitivity": "restricted",
            "retention_policy_id": RETENTION_POLICY_ID,
            "payload": {"message": "Apollo quasar launch manifest for tenant B."},
        }))
        .send()
        .await
        .context("tenant b episode request failed")?;
    ensure!(
        episode_response.status() == StatusCode::CREATED,
        "tenant b episode returned {}, expected 201",
        episode_response.status()
    );
    let tenant_b_episode: Value = episode_response.json().await?;
    let tenant_b_response = client
        .post(&tenant_b_url)
        .bearer_auth(&target.principal_b_bearer_token)
        .header("Idempotency-Key", "surface-a1-tenant-b-fact")
        .json(&json!({
            "case_id": Uuid::parse_str("019be000-0000-7000-8000-000000000402")?,
            "namespace": "case.profile",
            "key": "apollo_manifest_tenant_b",
            "value": {"message": "Apollo quasar launch manifest for tenant B."},
            "observed_at": "2026-01-10T09:00:00Z",
            "valid_time": {"from": "2026-01-10T00:00:00Z", "until": null},
            "evidence_episode_ids": [tenant_b_episode["episode_id"]],
            "write_policy": {"id": "direct-evidence", "version": "1"},
            "confidence": 0.95,
            "sensitivity": "restricted",
            "retention_policy_id": RETENTION_POLICY_ID,
        }))
        .send()
        .await
        .context("tenant b fact request failed")?;
    ensure!(
        tenant_b_response.status() == StatusCode::CREATED,
        "tenant b fact returned {}, expected 201",
        tenant_b_response.status()
    );

    let (status, bundle) = post_surface(
        &client,
        target,
        SURFACE_HOST_ID,
        SURFACE_PRINCIPAL_ID,
        &["quasar"],
        "surface-a1",
    )
    .await?;
    ensure!(
        status == StatusCode::CREATED,
        "surface returned {}, expected 201",
        status
    );
    let items = bundle["items"].as_array().context("bundle items missing")?;
    ensure!(!items.is_empty(), "tenant a surface is unexpectedly empty");
    for item in items {
        let key = item["fact_key"].as_str().context("item fact_key missing")?;
        ensure!(
            key != "apollo_manifest_tenant_b",
            "tenant b content leaked into tenant a surface"
        );
    }
    let keys: Vec<&str> = items
        .iter()
        .filter_map(|item| item["fact_key"].as_str())
        .collect();
    ensure!(
        keys.contains(&"apollo_plan"),
        "tenant a surface omitted its own content: {keys:?}"
    );
    Ok(())
}

/// A2. Boundedness: the response obeys the caps, and receipts explain
/// inclusion (spec scenario `verify_surface_caps_and_explained_bundle`).
pub(crate) async fn verify_surface_caps_and_explained_bundle(target: &Target) -> Result<()> {
    let client = Client::new();
    let case_id = Uuid::parse_str("019be000-0000-7000-8000-000000000702")?;
    let host_id = "hermes-desktop-caps";
    register_surface_policy(
        &client,
        target,
        host_id,
        SURFACE_PRINCIPAL_ID,
        json!({"max_items": 2, "max_context_tokens": 256, "max_result_tokens": 4096}),
    )
    .await?;
    for (index, (key, message)) in [
        ("cap_mercury", "Mercury quasar flyby confirmed."),
        ("cap_venus", "Venus quasar sample collected."),
        ("cap_mars", "Mars quasar baseline approved."),
    ]
    .iter()
    .enumerate()
    {
        append_surface_fact(
            &client,
            target,
            case_id,
            "case.profile",
            key,
            message,
            "internal",
            &format!("2026-01-10T09:{:02}:00Z", index),
            "2026-01-10T00:00:00Z",
            None,
            &format!("surface-a2-{key}"),
        )
        .await?;
    }
    let (status, bundle) = post_surface(
        &client,
        target,
        host_id,
        SURFACE_PRINCIPAL_ID,
        &["quasar"],
        "surface-a2",
    )
    .await?;
    ensure!(
        status == StatusCode::CREATED,
        "surface returned {}, expected 201",
        status
    );
    let items = bundle["items"].as_array().context("bundle items missing")?;
    ensure!(
        items.len() == 2,
        "max_items cap violated: {} items surfaced",
        items.len()
    );
    ensure!(
        bundle["truncated"].as_bool() == Some(true),
        "surface did not report truncation"
    );
    for item in items {
        let lexical_score = item["lexical_score"]
            .as_f64()
            .context("item lexical_score missing")?;
        ensure!(
            lexical_score > 0.0,
            "item has no ranking explanation: {lexical_score}"
        );
        let item_sha256 = item["item_sha256"]
            .as_str()
            .context("item receipt missing")?;
        ensure!(
            item_sha256.len() == 64,
            "item receipt is not a sha256 digest"
        );
        let content_sha256 = item["content_sha256"]
            .as_str()
            .context("item content digest missing")?;
        ensure!(
            content_sha256.len() == 64,
            "item content digest is not a sha256 digest"
        );
    }
    // The result cap bounds the payload: a tiny cap must drop items.
    let host_id_result = "hermes-desktop-result-cap";
    register_surface_policy(
        &client,
        target,
        host_id_result,
        SURFACE_PRINCIPAL_ID,
        json!({"max_items": 2, "max_context_tokens": 256, "max_result_tokens": 4}),
    )
    .await?;
    let (_status, capped) = post_surface(
        &client,
        target,
        host_id_result,
        SURFACE_PRINCIPAL_ID,
        &["quasar"],
        "surface-a2-result-cap",
    )
    .await?;
    let capped_items = capped["items"].as_array().context("capped items missing")?;
    ensure!(
        capped_items.is_empty(),
        "max_result_tokens cap was not enforced: {} items",
        capped_items.len()
    );
    Ok(())
}

/// A3. Opt-in: with no registered policy, the surface returns an empty
/// bundle (spec scenario `verify_surface_default_empty`).
pub(crate) async fn verify_surface_default_empty(target: &Target) -> Result<()> {
    let client = Client::new();
    let (status, bundle) = post_surface(
        &client,
        target,
        "hermes-desktop-unregistered",
        "principal-unregistered-surface",
        &["quasar"],
        "surface-a3",
    )
    .await?;
    ensure!(
        status == StatusCode::CREATED,
        "surface returned {}, expected 201",
        status
    );
    ensure!(
        bundle["item_count"].as_i64() == Some(0),
        "missing policy did not yield an empty bundle"
    );
    ensure!(
        bundle["items"].as_array().map(Vec::is_empty) == Some(true),
        "missing policy bundle contains items"
    );
    ensure!(
        bundle["policy_sha256"].is_null(),
        "missing policy bundle claims a policy"
    );
    Ok(())
}

async fn set_subject_lifecycle_state(
    pool: &PgPool,
    target: &Target,
    from: &str,
    to: &str,
) -> Result<()> {
    // The conformance subject has no lifecycle row (missing rows are
    // implicitly active). Seed the row so the monotonic fence trigger
    // applies, then transition with the required version advance. The
    // scope GUCs are transaction-local so the returned connection stays
    // clean for the pool.
    let mut transaction = pool.begin().await.context("begin lifecycle transition")?;
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
        .bind(target.tenant_id.to_string())
        .execute(&mut *transaction)
        .await
        .context("set tenant scope")?;
    sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
        .bind(target.subject_id.to_string())
        .execute(&mut *transaction)
        .await
        .context("set subject scope")?;
    sqlx::query(
        "INSERT INTO memory.subject_lifecycles (tenant_id, subject_id)
         VALUES ($1, $2)
         ON CONFLICT (tenant_id, subject_id) DO NOTHING",
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .execute(&mut *transaction)
    .await
    .context("seed subject lifecycle row")?;
    sqlx::query(
        "UPDATE memory.subject_lifecycles
         SET lifecycle_state = $3, state_version = state_version + 1
         WHERE tenant_id = $1 AND subject_id = $2
           AND lifecycle_state = $4",
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(to)
    .bind(from)
    .execute(&mut *transaction)
    .await
    .map_err(|error| anyhow::anyhow!("transition subject lifecycle state: {error}"))?;
    transaction
        .commit()
        .await
        .context("commit lifecycle transition")?;
    Ok(())
}

/// A4. Deletion: bundles never include fenced or purged subjects' content
/// (spec scenario `verify_surface_respects_fence_and_purge`).
pub(crate) async fn verify_surface_respects_fence_and_purge(
    pool: &PgPool,
    migration_pool: &PgPool,
    target: &Target,
) -> Result<()> {
    let client = Client::new();
    let case_id = Uuid::parse_str("019be000-0000-7000-8000-000000000703")?;
    let host_id = "hermes-desktop-fence";
    register_surface_policy(&client, target, host_id, SURFACE_PRINCIPAL_ID, json!({})).await?;
    append_surface_fact(
        &client,
        target,
        case_id,
        "case.profile",
        "fence_kepler",
        "Kepler quasar transits recorded.",
        "internal",
        "2026-01-10T09:00:00Z",
        "2026-01-10T00:00:00Z",
        None,
        "surface-a4-kepler",
    )
    .await?;
    let (_status, before) = post_surface(
        &client,
        target,
        host_id,
        SURFACE_PRINCIPAL_ID,
        &["quasar"],
        "surface-a4-before",
    )
    .await?;
    ensure!(
        before["items"].as_array().map(|items| !items.is_empty()) == Some(true),
        "fence fixture surface is unexpectedly empty before fencing"
    );

    set_subject_lifecycle_state(pool, target, "active", "deletion_pending")
        .await
        .context("fence to pending")?;
    let (pending_status, _pending) = post_surface_unchecked(
        &client,
        target,
        host_id,
        SURFACE_PRINCIPAL_ID,
        &["quasar"],
        "surface-a4-pending",
    )
    .await?;
    ensure!(
        pending_status == StatusCode::NOT_FOUND,
        "fenced subject surface returned {pending_status}, expected 404"
    );

    set_subject_lifecycle_state(pool, target, "deletion_pending", "deleted")
        .await
        .context("fence to deleted")?;
    let (deleted_status, _deleted) = post_surface_unchecked(
        &client,
        target,
        host_id,
        SURFACE_PRINCIPAL_ID,
        &["quasar"],
        "surface-a4-deleted",
    )
    .await?;
    ensure!(
        deleted_status == StatusCode::NOT_FOUND,
        "purged subject surface returned {deleted_status}, expected 404"
    );

    // Restore the implicit active lifecycle (missing rows are active) so
    // later scenarios reuse the subject. The mutation trigger guards
    // UPDATEs only, so deleting the row is the supported reset path. The
    // subject_lifecycles table has no DELETE policy (RLS FORCE), so the
    // reset runs on the migration pool, which bypasses RLS.
    sqlx::query(
        "DELETE FROM memory.subject_lifecycles
         WHERE tenant_id = $1 AND subject_id = $2",
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .execute(migration_pool)
    .await
    .context("restore active lifecycle")?;
    Ok(())
}

/// A6. Authorization revocation: a revoked principal gets an empty bundle,
/// not an error that leaks existence (spec scenario
/// `verify_surface_revoked_principal_empty`).
pub(crate) async fn verify_surface_revoked_principal_empty(
    pool: &PgPool,
    target: &Target,
) -> Result<()> {
    let client = Client::new();
    let host_id = "hermes-desktop-revoked";
    register_surface_policy(&client, target, host_id, SURFACE_PRINCIPAL_ID, json!({})).await?;
    let (_status, before) = post_surface(
        &client,
        target,
        host_id,
        SURFACE_PRINCIPAL_ID,
        &["quasar"],
        "surface-a6-before",
    )
    .await?;
    let before_items = before["items"].as_array().context("items missing")?;
    ensure!(
        !before_items.is_empty(),
        "revocation fixture surface is unexpectedly empty before revocation"
    );

    // Revocation is modeled as the policy being disabled (there is no
    // separate principal revocation registry in the memory model). The
    // policy table is RLS FORCEd on the tenant scope, so the UPDATE runs
    // with the tenant GUC set transaction-locally.
    let mut transaction = pool.begin().await.context("begin policy revocation")?;
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
        .bind(target.tenant_id.to_string())
        .execute(&mut *transaction)
        .await
        .context("set tenant scope for revocation")?;
    let revoked = sqlx::query(
        "UPDATE memory.surface_policies
         SET enabled = false
         WHERE tenant_id = $1 AND host_id = $2 AND principal_id = $3",
    )
    .bind(target.tenant_id)
    .bind(host_id)
    .bind(SURFACE_PRINCIPAL_ID)
    .execute(&mut *transaction)
    .await?;
    transaction
        .commit()
        .await
        .context("commit policy revocation")?;
    ensure!(
        revoked.rows_affected() == 1,
        "surface policy revocation did not touch a row"
    );

    let (status, after) = post_surface(
        &client,
        target,
        host_id,
        SURFACE_PRINCIPAL_ID,
        &["quasar"],
        "surface-a6-after",
    )
    .await?;
    ensure!(
        status.is_success(),
        "revoked principal surface errored: {status}"
    );
    ensure!(
        after["items"].as_array().map(|items| items.is_empty()) == Some(true),
        "revoked principal still received content"
    );
    Ok(())
}

/// A7. Filters before ranking: sensitivity ceiling and temporal window
/// exclude content before ranking (spec scenario
/// `verify_surface_filters_before_ranking`).
pub(crate) async fn verify_surface_filters_before_ranking(target: &Target) -> Result<()> {
    let client = Client::new();
    let case_id = Uuid::parse_str("019be000-0000-7000-8000-000000000704")?;
    let host_id = "hermes-desktop-filters";
    register_surface_policy(
        &client,
        target,
        host_id,
        SURFACE_PRINCIPAL_ID,
        json!({
            "sensitivity_ceiling": "internal",
            "window_from": "2026-01-01T00:00:00Z",
            "window_until": "2026-02-01T00:00:00Z",
        }),
    )
    .await?;
    // Inside the window, at or under the ceiling: must surface.
    append_surface_fact(
        &client,
        target,
        case_id,
        "case.profile",
        "filter_europa",
        "Europa quasar hypothesis filed.",
        "internal",
        "2026-01-10T09:00:00Z",
        "2026-01-10T00:00:00Z",
        None,
        "surface-a7-europa",
    )
    .await?;
    // Inside the window but above the ceiling: must not surface.
    append_surface_fact(
        &client,
        target,
        case_id,
        "case.profile",
        "filter_titan",
        "Titan quasar radar mapping restricted.",
        "restricted",
        "2026-01-11T09:00:00Z",
        "2026-01-11T00:00:00Z",
        None,
        "surface-a7-titan",
    )
    .await?;
    // Inside the ceiling but outside the window: must not surface.
    append_surface_fact(
        &client,
        target,
        case_id,
        "case.profile",
        "filter_ganymede",
        "Ganymede quasar ocean model updated.",
        "internal",
        "2025-12-01T09:00:00Z",
        "2025-12-01T00:00:00Z",
        Some("2025-12-02T00:00:00Z"),
        "surface-a7-ganymede",
    )
    .await?;

    let (status, bundle) = post_surface(
        &client,
        target,
        host_id,
        SURFACE_PRINCIPAL_ID,
        &["quasar"],
        "surface-a7",
    )
    .await?;
    ensure!(
        status == StatusCode::CREATED,
        "surface returned {}, expected 201",
        status
    );
    let items = bundle["items"].as_array().context("bundle items missing")?;
    let keys: Vec<&str> = items
        .iter()
        .filter_map(|item| item["fact_key"].as_str())
        .collect();
    ensure!(
        keys.contains(&"filter_europa"),
        "in-window in-ceiling content was not surfaced: {keys:?}"
    );
    ensure!(
        !keys.contains(&"filter_titan"),
        "above-ceiling content was ranked into the surface: {keys:?}"
    );
    ensure!(
        !keys.contains(&"filter_ganymede"),
        "outside-window content was ranked into the surface: {keys:?}"
    );
    for item in items {
        let sensitivity = item["sensitivity"]
            .as_str()
            .context("sensitivity missing")?;
        ensure!(
            sensitivity == "internal",
            "ceiling was violated by surfaced item: {sensitivity}"
        );
    }
    Ok(())
}

/// A8. Idempotency: the same key and body return the same bundle; a
/// different body with the same key returns 409 (spec scenario
/// `verify_surface_idempotent_replay`).
pub(crate) async fn verify_surface_idempotent_replay(target: &Target) -> Result<()> {
    let client = Client::new();
    let case_id = Uuid::parse_str("019be000-0000-7000-8000-000000000705")?;
    let host_id = "hermes-desktop-replay";
    register_surface_policy(&client, target, host_id, SURFACE_PRINCIPAL_ID, json!({})).await?;
    append_surface_fact(
        &client,
        target,
        case_id,
        "case.profile",
        "replay_io",
        "Io quasar activity monitored.",
        "internal",
        "2026-01-10T09:00:00Z",
        "2026-01-10T00:00:00Z",
        None,
        "surface-a8-io",
    )
    .await?;

    let (first_status, first) = post_surface(
        &client,
        target,
        host_id,
        SURFACE_PRINCIPAL_ID,
        &["quasar"],
        "surface-a8-key",
    )
    .await?;
    ensure!(
        first_status == StatusCode::CREATED,
        "first surface returned {first_status}"
    );
    let first_surface_id = first["surface_id"]
        .as_str()
        .context("first surface_id missing")?;
    let first_item_sha256 = first["items"][0]["item_sha256"]
        .as_str()
        .context("first item receipt missing")?;

    let (replay_status, replay) = post_surface(
        &client,
        target,
        host_id,
        SURFACE_PRINCIPAL_ID,
        &["quasar"],
        "surface-a8-key",
    )
    .await?;
    ensure!(
        replay_status == StatusCode::CREATED,
        "replay returned {replay_status}"
    );
    ensure!(
        replay["surface_id"].as_str() == Some(first_surface_id),
        "replay returned a different surface_id"
    );
    ensure!(
        replay["items"][0]["item_sha256"].as_str() == Some(first_item_sha256),
        "replay returned a different bundle"
    );

    // The same key with a different body must be a 409.
    let (conflict_status, _conflict) = post_surface_unchecked(
        &client,
        target,
        host_id,
        SURFACE_PRINCIPAL_ID,
        &["quasar", "conflict-probe"],
        "surface-a8-key",
    )
    .await?;
    ensure!(
        conflict_status == StatusCode::CONFLICT,
        "different body with a reused key returned {conflict_status}, expected 409"
    );
    Ok(())
}
