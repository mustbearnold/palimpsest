//! consolidation — governed consolidation conformance (spec 011 A1-A6).
//! Mirrors the deletion_ops crash pattern: a child test crashes mid-job, the
//! parent forces lease expiry, and a production server resumes the job.

use anyhow::{Context, Result, ensure};
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use sqlx::{PgPool, Row};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

fn sha256_bytes(input: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(input);
    hasher.finalize().into()
}

use palimpsest_application::{
    ClaimedConsolidationJob, ConsolidationRepository, PendingConsolidationClaim,
};
use palimpsest_conformance::Target;
use palimpsest_domain::{CaseId, EpisodeId, FactId, PrincipalId, RevisionId, SubjectId, TenantId};
use palimpsest_postgres::PostgresMemoryRepository;

use super::crash::{reserve_local_address, spawn_production_server, wait_for_listener};

const SOURCE_KIND: &str = "conversation";
const POLICY_ID: &str = "derive-summaries-v1";
const PROVIDER_KIND: &str = "fixture-deterministic-v1";
const WRITE_POLICY_ID: &str = "direct-evidence";
const WRITE_POLICY_VERSION: &str = "1";
const RETENTION_POLICY_ID: &str = "standard";
const WORKER_PRINCIPAL: &str = "palimpsest-consolidation-worker";

fn consolidation_url(target: &Target) -> String {
    format!(
        "{}/v1/tenants/{}/subjects/{}/consolidations",
        target.base_url.trim_end_matches('/'),
        target.tenant_id,
        target.subject_id
    )
}

fn policy_collection_url(target: &Target) -> String {
    format!(
        "{}/v1/tenants/{}/consolidation-policies",
        target.base_url.trim_end_matches('/'),
        target.tenant_id
    )
}

fn job_status_url(target: &Target, job_id: &str) -> String {
    format!("{}/{}", consolidation_url(target), job_id)
}

fn interpreter_config_url(target: &Target) -> String {
    format!(
        "{}/v1/tenants/{}/consolidation-interpreter-configs",
        target.base_url.trim_end_matches('/'),
        target.tenant_id
    )
}

async fn register_interpreter_config(
    client: &Client,
    target: &Target,
    provider_kind: &str,
) -> Result<Uuid> {
    let response = client
        .post(interpreter_config_url(target))
        .bearer_auth(&target.bearer_token)
        .json(&json!({
            "provider_kind": provider_kind,
            "prompt_policy_version": "fixture-prompt-v1",
        }))
        .send()
        .await
        .context("register interpreter config request failed")?;
    ensure!(
        response.status() == StatusCode::CREATED,
        "register interpreter config returned {}, expected 201",
        response.status()
    );
    let body: Value = response.json().await?;
    let config_id = Uuid::parse_str(
        body["interpreter_config_id"]
            .as_str()
            .context("interpreter config id is missing")?,
    )?;
    ensure!(
        body["provider_kind"] == PROVIDER_KIND
            && body["config_digest"]
                .as_str()
                .is_some_and(|digest| digest.len() == 64),
        "interpreter config view is wrong"
    );
    Ok(config_id)
}

async fn register_policy(
    client: &Client,
    target: &Target,
    config_id: Uuid,
    source_kind: &str,
    policy_id: &str,
) -> Result<()> {
    let response = client
        .post(policy_collection_url(target))
        .bearer_auth(&target.bearer_token)
        .json(&json!({
            "source_kind": source_kind,
            "policy_id": policy_id,
            "interpreter_config_id": config_id,
            "write_policy_id": WRITE_POLICY_ID,
            "write_policy_version": WRITE_POLICY_VERSION,
            "retention_policy_id": RETENTION_POLICY_ID,
            "confidence_auto_promote_min": 0.8,
        }))
        .send()
        .await
        .context("register policy request failed")?;
    ensure!(
        response.status() == StatusCode::CREATED,
        "register policy returned {}, expected 201",
        response.status()
    );
    Ok(())
}

async fn append_conversation_episode(
    client: &Client,
    target: &Target,
    case_id: Uuid,
    index: u32,
    idempotency_key: &str,
    source_kind: &str,
) -> Result<Uuid> {
    let url = format!(
        "{}/v1/tenants/{}/subjects/{}/episodes",
        target.base_url.trim_end_matches('/'),
        target.tenant_id,
        target.subject_id
    );
    let response = client
        .post(&url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", idempotency_key)
        .json(&json!({
            "case_id": case_id,
            "kind": "message",
            "observed_at": format!("2026-01-10T09:{:02}:00Z", index % 60),
            "provenance": {
                "source_type": source_kind,
                "source_uri": null,
                "external_id": format!("consolidation-episode-{index}"),
            },
            "sensitivity": "internal",
            "retention_policy_id": RETENTION_POLICY_ID,
            "payload": {"message": format!("Consolidation source episode {index}.")},
        }))
        .send()
        .await
        .context("append episode request failed")?;
    ensure!(
        response.status() == StatusCode::CREATED,
        "append episode returned {}, expected 201",
        response.status()
    );
    let body: Value = response.json().await?;
    Uuid::parse_str(
        body["episode_id"]
            .as_str()
            .context("episode id is missing")?,
    )
    .context("episode id is not a uuid")
}

fn window_from_now() -> (String, String) {
    use time::format_description::well_known::Rfc3339;
    let truncate = |instant: time::OffsetDateTime| {
        instant
            .replace_nanosecond(instant.nanosecond() / 1000 * 1000)
            .expect("nanosecond truncation stays in range")
    };
    let now = time::OffsetDateTime::now_utc();
    let from = truncate(now - Duration::from_secs(3600));
    let until = truncate(now + Duration::from_secs(3600));
    (
        from.format(&Rfc3339).expect("window from formats"),
        until.format(&Rfc3339).expect("window until formats"),
    )
}

async fn create_consolidation_job(
    client: &Client,
    target: &Target,
    idempotency_key: &str,
    source_kind: &str,
    policy_id: &str,
) -> Result<Value> {
    let (window_from, window_until) = window_from_now();
    let response = client
        .post(consolidation_url(target))
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", idempotency_key)
        .json(&json!({
            "source_kind": source_kind,
            "policy_id": policy_id,
            "window_from": window_from,
            "window_until": window_until,
        }))
        .send()
        .await
        .context("create consolidation job request failed")?;
    let status = response.status();
    let body_text = response
        .text()
        .await
        .context("read consolidation job response")?;
    ensure!(
        status == StatusCode::CREATED,
        "create consolidation job returned {status}, expected 201: {body_text}"
    );
    let body: Value =
        serde_json::from_str(&body_text).context("consolidation job response was not JSON")?;
    ensure!(body["replayed"] == false, "fresh job must not be a replay");
    Ok(body)
}

async fn poll_until_complete(client: &Client, target: &Target, job_id: &str) -> Result<Value> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let response = client
            .get(job_status_url(target, job_id))
            .bearer_auth(&target.bearer_token)
            .send()
            .await
            .context("poll consolidation job request failed")?;
        ensure!(
            response.status() == StatusCode::OK,
            "poll consolidation job returned {}, expected 200",
            response.status()
        );
        let body: Value = response.json().await?;
        let state = body["lifecycle_state"].as_str().context("state missing")?;
        if state == "complete" {
            return Ok(body);
        }
        ensure!(
            state != "failed",
            "consolidation job failed: {} (claims_done = {}, claims_total = {}, state = {})",
            body["failure_reason"],
            body["claims_done"],
            body["claims_total"],
            body["lifecycle_state"]
        );
        crate::sleep_budget::poll_sleep(Duration::from_millis(200)).await;
        ensure!(
            tokio::time::Instant::now() < deadline,
            "consolidation job did not complete in time"
        );
    }
}

async fn derived_fact_count(pool: &PgPool, target: &Target) -> Result<i64> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
        .bind(target.tenant_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
        .bind(target.subject_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.principal_id', 'principal-a', true)")
        .execute(&mut *transaction)
        .await?;
    let count = sqlx::query_scalar::<_, i64>(
        "SELECT count(*)
         FROM memory.fact_revisions
         WHERE tenant_id = $1 AND subject_id = $2
           AND writer_principal_id = $3",
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(WORKER_PRINCIPAL)
    .fetch_one(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(count)
}

async fn assert_claims_attribution(
    pool: &PgPool,
    target: &Target,
    episode_ids: &[Uuid],
) -> Result<()> {
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
        .bind(target.tenant_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
        .bind(target.subject_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.principal_id', 'principal-a', true)")
        .execute(&mut *transaction)
        .await?;
    let rows = sqlx::query(
        "SELECT claim_id, model_identity, episode_ids, content_hash, sensitivity,
                confidence, lifecycle_state
         FROM memory.consolidation_claims
         WHERE tenant_id = $1 AND subject_id = $2
         ORDER BY created_at",
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .fetch_all(&mut *transaction)
    .await?;
    transaction.commit().await?;
    ensure!(rows.len() == episode_ids.len(), "claim count mismatch");
    for (row, episode_id) in rows.iter().zip(episode_ids) {
        let model_identity: String = row.try_get("model_identity")?;
        let claim_episodes: Vec<Uuid> = row.try_get("episode_ids")?;
        let content_hash: String = row.try_get("content_hash")?;
        let sensitivity: String = row.try_get("sensitivity")?;
        let confidence: f64 = row.try_get("confidence")?;
        let state: String = row.try_get("lifecycle_state")?;
        ensure!(model_identity.starts_with("fixture-deterministic-v1:"));
        ensure!(
            claim_episodes == vec![*episode_id],
            "claim lineage mismatch"
        );
        ensure!(
            content_hash.len() == 64,
            "claim content hash is not a digest"
        );
        ensure!(sensitivity == "internal");
        ensure!(confidence == 0.9);
        ensure!(state == "done", "claim did not materialize");
    }
    Ok(())
}

pub(crate) async fn consolidation_worker_materializes_attributable_facts(
    pool: &PgPool,
    target: &Target,
) -> Result<()> {
    let client = Client::new();
    let case_id = Uuid::parse_str("019be000-0000-7000-8000-000000000002")?;
    let config_id = register_interpreter_config(&client, target, PROVIDER_KIND).await?;
    register_policy(&client, target, config_id, SOURCE_KIND, POLICY_ID).await?;
    let mut episode_ids = Vec::new();
    for index in 0..3u32 {
        let episode_id = append_conversation_episode(
            &client,
            target,
            case_id,
            index,
            &format!("consolidation-episode-{index}"),
            SOURCE_KIND,
        )
        .await?;
        episode_ids.push(episode_id);
    }
    let job = create_consolidation_job(
        &client,
        target,
        "consolidation-job-run-1",
        SOURCE_KIND,
        POLICY_ID,
    )
    .await?;
    let job_id = job["job_id"].as_str().context("job id is missing")?;
    let completed = poll_until_complete(&client, target, job_id).await?;
    ensure!(completed["claims_total"] == 3, "claims_total mismatch");
    ensure!(completed["claims_done"] == 3, "claims_done mismatch");
    ensure!(completed["claim_cap"] == 100_000, "claim_cap mismatch");
    ensure!(
        derived_fact_count(pool, target).await? == 3,
        "fact count mismatch"
    );
    assert_claims_attribution(pool, target, &episode_ids).await?;
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
        .bind(target.tenant_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
        .bind(target.subject_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.principal_id', 'principal-a', true)")
        .execute(&mut *transaction)
        .await?;
    let facts = sqlx::query(
        "SELECT writer_principal_id, write_policy_id
         FROM memory.fact_revisions
         WHERE tenant_id = $1 AND subject_id = $2
           AND writer_principal_id = $3",
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(WORKER_PRINCIPAL)
    .fetch_all(&mut *transaction)
    .await?;
    transaction.commit().await?;
    ensure!(facts.len() == 3, "derived fact count mismatch");
    for row in facts {
        let writer_principal_id: String = row.try_get("writer_principal_id")?;
        let write_policy_id: String = row.try_get("write_policy_id")?;
        ensure!(
            writer_principal_id == WORKER_PRINCIPAL,
            "derived fact writer mismatch"
        );
        ensure!(
            write_policy_id == WRITE_POLICY_ID,
            "derived fact policy mismatch"
        );
    }
    // Replay the same window: the claims are deterministic per episode, so
    // the materialization replays and no new facts appear (A2).
    let replay = create_consolidation_job(
        &client,
        target,
        "consolidation-job-run-2",
        SOURCE_KIND,
        POLICY_ID,
    )
    .await?;
    let replay_job_id = replay["job_id"].as_str().context("job id is missing")?;
    let replay_completed = poll_until_complete(&client, target, replay_job_id).await?;
    ensure!(
        replay_completed["claims_total"] == 3,
        "replay claims_total mismatch"
    );
    ensure!(
        derived_fact_count(pool, target).await? == 3,
        "replay duplicated derived facts"
    );
    Ok(())
}

pub(crate) async fn consolidation_fails_closed_without_registered_policy(
    _pool: &PgPool,
    target: &Target,
) -> Result<()> {
    let client = Client::new();
    // No policy registered for this policy id: the job fails closed (A4).
    let (window_from, window_until) = window_from_now();
    let response = client
        .post(consolidation_url(target))
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "consolidation-job-no-policy")
        .json(&json!({
            "source_kind": SOURCE_KIND,
            "policy_id": "no-such-policy-v1",
            "window_from": window_from,
            "window_until": window_until,
        }))
        .send()
        .await?;
    ensure!(
        response.status() == StatusCode::NOT_FOUND,
        "job without a policy returned {}, expected 404",
        response.status()
    );
    // A policy that references an unknown interpreter config fails too.
    let response = client
        .post(policy_collection_url(target))
        .bearer_auth(&target.bearer_token)
        .json(&json!({
            "source_kind": SOURCE_KIND,
            "policy_id": POLICY_ID,
            "interpreter_config_id": Uuid::now_v7(),
            "write_policy_id": WRITE_POLICY_ID,
            "write_policy_version": WRITE_POLICY_VERSION,
            "retention_policy_id": RETENTION_POLICY_ID,
            "confidence_auto_promote_min": 0.8,
        }))
        .send()
        .await?;
    ensure!(
        response.status() == StatusCode::NOT_FOUND,
        "policy with an unknown interpreter returned {}, expected 404",
        response.status()
    );
    // An unknown provider kind is rejected at registration.
    let response = client
        .post(interpreter_config_url(target))
        .bearer_auth(&target.bearer_token)
        .json(&json!({
            "provider_kind": "not-a-provider",
            "prompt_policy_version": "v1",
        }))
        .send()
        .await?;
    ensure!(
        response.status() == StatusCode::UNPROCESSABLE_ENTITY,
        "unknown provider returned {}, expected 422",
        response.status()
    );
    // An out-of-range confidence threshold is rejected.
    let config_id = register_interpreter_config(&client, target, PROVIDER_KIND).await?;
    let response = client
        .post(policy_collection_url(target))
        .bearer_auth(&target.bearer_token)
        .json(&json!({
            "source_kind": SOURCE_KIND,
            "policy_id": POLICY_ID,
            "interpreter_config_id": config_id,
            "write_policy_id": WRITE_POLICY_ID,
            "write_policy_version": WRITE_POLICY_VERSION,
            "retention_policy_id": RETENTION_POLICY_ID,
            "confidence_auto_promote_min": 1.5,
        }))
        .send()
        .await?;
    ensure!(
        response.status() == StatusCode::BAD_REQUEST,
        "out-of-range confidence returned {}, expected 400",
        response.status()
    );
    Ok(())
}

pub(crate) async fn consolidation_jobs_are_isolated_by_scope(
    pool: &PgPool,
    target: &Target,
) -> Result<()> {
    let client = Client::new();
    let case_id = Uuid::parse_str("019be000-0000-7000-8000-000000000003")?;
    let config_id = register_interpreter_config(&client, target, PROVIDER_KIND).await?;
    register_policy(
        &client,
        target,
        config_id,
        "conversation.isolation",
        "isolation-policy-v1",
    )
    .await?;
    append_conversation_episode(
        &client,
        target,
        case_id,
        0,
        "isolation-episode-0",
        "conversation.isolation",
    )
    .await?;
    let job = create_consolidation_job(
        &client,
        target,
        "isolation-job-a",
        "conversation.isolation",
        "isolation-policy-v1",
    )
    .await?;
    let job_id = job["job_id"].as_str().context("job id is missing")?;
    let completed = poll_until_complete(&client, target, job_id).await?;
    ensure!(completed["claims_done"] == 1);
    // Principal B (another tenant) cannot see A's job: RLS hides it.
    let response = client
        .get(job_status_url(target, job_id))
        .bearer_auth(&target.principal_b_bearer_token)
        .send()
        .await?;
    ensure!(
        response.status() == StatusCode::NOT_FOUND,
        "cross-tenant poll returned {}, expected 404",
        response.status()
    );
    // The secondary-subject scope sees no consolidation rows at the SQL level.
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
        .bind(target.tenant_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
        .bind(target.principal_a_secondary_subject_id.to_string())
        .execute(&mut *transaction)
        .await?;
    let visible: i64 =
        sqlx::query_scalar("SELECT count(*) FROM memory.consolidation_claims WHERE tenant_id = $1")
            .bind(target.tenant_id)
            .fetch_one(&mut *transaction)
            .await?;
    ensure!(
        visible == 0,
        "secondary subject scope sees consolidation claims"
    );
    transaction.rollback().await?;
    // Principal B runs its own job on its own tenant: its facts stay in its
    // own scope, and A's facts are untouched.
    let response = client
        .post(format!(
            "{}/v1/tenants/{}/consolidation-interpreter-configs",
            target.base_url.trim_end_matches('/'),
            target.principal_b_tenant_id
        ))
        .bearer_auth(&target.principal_b_bearer_token)
        .json(&json!({
            "provider_kind": PROVIDER_KIND,
            "prompt_policy_version": "fixture-prompt-v1",
        }))
        .send()
        .await?;
    ensure!(
        response.status() == StatusCode::CREATED,
        "principal B could not register an interpreter config"
    );
    let config_b: Value = response.json().await?;
    let response = client
        .post(format!(
            "{}/v1/tenants/{}/consolidation-policies",
            target.base_url.trim_end_matches('/'),
            target.principal_b_tenant_id
        ))
        .bearer_auth(&target.principal_b_bearer_token)
        .json(&json!({
            "source_kind": SOURCE_KIND,
            "policy_id": POLICY_ID,
            "interpreter_config_id": config_b["interpreter_config_id"],
            "write_policy_id": WRITE_POLICY_ID,
            "write_policy_version": WRITE_POLICY_VERSION,
            "retention_policy_id": RETENTION_POLICY_ID,
            "confidence_auto_promote_min": 0.8,
        }))
        .send()
        .await?;
    ensure!(
        response.status() == StatusCode::CREATED,
        "principal B could not register a policy"
    );
    let b_url = format!(
        "{}/v1/tenants/{}/subjects/{}/consolidations",
        target.base_url.trim_end_matches('/'),
        target.principal_b_tenant_id,
        target.principal_b_subject_id
    );
    let (window_from, window_until) = window_from_now();
    let response = client
        .post(&b_url)
        .bearer_auth(&target.principal_b_bearer_token)
        .header("Idempotency-Key", "isolation-job-b")
        .json(&json!({
            "source_kind": SOURCE_KIND,
            "policy_id": POLICY_ID,
            "window_from": window_from,
            "window_until": window_until,
        }))
        .send()
        .await?;
    ensure!(
        response.status() == StatusCode::CREATED,
        "principal B could not create a job"
    );
    let job_b: Value = response.json().await?;
    let job_b_id = job_b["job_id"].as_str().context("job id is missing")?;
    let completed_b = poll_until_complete(&client, &target_with_b(target), job_b_id).await?;
    ensure!(completed_b["claims_total"] == 0, "B's job saw A's episodes");
    ensure!(
        derived_fact_count(pool, target).await? == 4,
        "B's job touched A's facts"
    );
    Ok(())
}

fn target_with_b(target: &Target) -> Target {
    Target {
        base_url: target.base_url.clone(),
        bearer_token: target.principal_b_bearer_token.clone(),
        tenant_id: target.principal_b_tenant_id,
        subject_id: target.principal_b_subject_id,
        principal_a_secondary_subject_id: target.principal_a_secondary_subject_id,
        principal_a_internal_bearer_token: target.principal_a_internal_bearer_token.clone(),
        principal_b_bearer_token: target.principal_b_bearer_token.clone(),
        principal_b_tenant_id: target.principal_b_tenant_id,
        principal_b_subject_id: target.principal_b_subject_id,
        principal_c_bearer_token: target.principal_c_bearer_token.clone(),
        principal_c_subject_id: target.principal_c_subject_id,
        principal_d_same_scope_bearer_token: target.principal_d_same_scope_bearer_token.clone(),
    }
}

pub(crate) async fn consolidation_jobs_enforce_bounded_queues(
    pool: &PgPool,
    target: &Target,
) -> Result<()> {
    let client = Client::new();
    let config_id = register_interpreter_config(&client, target, PROVIDER_KIND).await?;
    register_policy(&client, target, config_id, SOURCE_KIND, POLICY_ID).await?;
    let job =
        create_consolidation_job(&client, target, "bounded-job-1", SOURCE_KIND, POLICY_ID).await?;
    let job_id = job["job_id"].as_str().context("job id is missing")?;
    let completed = poll_until_complete(&client, target, job_id).await?;
    ensure!(
        completed["claim_cap"] == 100_000,
        "server claim cap mismatch"
    );
    // The database rejects a claim cap outside the bounded range.
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
        .bind(target.tenant_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
        .bind(target.subject_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.principal_id', 'principal-a', true)")
        .execute(&mut *transaction)
        .await?;
    let result = sqlx::query(
        "INSERT INTO memory.consolidation_jobs (
            tenant_id, subject_id, job_id, source_kind, policy_id, policy_version,
            window_from, window_until, lifecycle_state, claim_cap, principal_id,
            idempotency_key_digest, request_fingerprint
         ) VALUES ($1, $2, $3, $4, $5, $6, now() - interval '1 hour',
            now(), 'pending', 100001, $7, repeat('a', 64), repeat('b', 64))",
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(Uuid::now_v7())
    .bind(SOURCE_KIND)
    .bind(POLICY_ID)
    .bind("1")
    .bind("principal-a")
    .execute(&mut *transaction)
    .await;
    ensure!(result.is_err(), "claim cap above the bound was accepted");
    transaction.rollback().await?;
    Ok(())
}

pub(crate) async fn consolidation_crash_resume_yields_no_duplicates_or_loss(
    database_url: &str,
    target: &Target,
) -> Result<()> {
    let client = Client::new();
    let case_id = Uuid::parse_str("019be000-0000-7000-8000-000000000004")?;
    // The crash child sets up the policy and job and aborts after the first
    // claim materializes, while the remaining claims stay leased.
    let crash_address = reserve_local_address().await?;
    let mut crash_child = spawn_consolidation_crash_child(database_url, target, crash_address)?;
    let crash_status = crash_child.wait().await.context("crash child failed")?;
    ensure!(
        !crash_status.success(),
        "crash child must exit with a crash"
    );
    // Force-expire the leases the dead worker held.
    let mut transaction = sqlx::PgPool::connect(database_url).await?.begin().await?;
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
        .bind(target.tenant_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
        .bind(target.subject_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.worker_claim', 'palimpsest-worker-v1', true)")
        .execute(&mut *transaction)
        .await?;
    let expired = sqlx::query(
        "UPDATE memory.consolidation_claims
         SET lease_expires_at = now() - interval '1 second'
         WHERE tenant_id = $1 AND subject_id = $2
           AND lifecycle_state = 'leased'",
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .execute(&mut *transaction)
    .await?;
    // The crash may land mid-claim (a leased claim) or in the gap between
    // claims (only pending remain). Both states resume: the expiry covers
    // leased claims, and a fresh worker claims pending claims normally.
    let _ = expired.rows_affected();
    sqlx::query(
        "UPDATE memory.consolidation_jobs
         SET worker_lease_expires_at = now() - interval '1 second'
         WHERE tenant_id = $1 AND subject_id = $2
           AND lifecycle_state = 'running'",
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    // A fresh production server picks the expired leases up and completes
    // the job without duplicating the already-materialized fact.
    let restart_address = reserve_local_address().await?;
    let mut restart_server = spawn_production_server(database_url, target, restart_address)?;
    wait_for_listener(restart_address).await?;
    let restarted_target = Target {
        base_url: format!("http://{restart_address}"),
        ..target.clone()
    };
    let result = async {
        let job_id = crash_job_id(database_url, target).await?;
        let completed = poll_until_complete(&client, &restarted_target, &job_id).await?;
        let claims_total = completed["claims_total"].as_i64().context("claims_total")?;
        ensure!(
            completed["claims_done"] == claims_total,
            "crash resume did not finish all claims"
        );
        let mut transaction = sqlx::PgPool::connect(database_url).await?.begin().await?;
        sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
            .bind(target.tenant_id.to_string())
            .execute(&mut *transaction)
            .await?;
        sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
            .bind(target.subject_id.to_string())
            .execute(&mut *transaction)
            .await?;
        let facts: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM memory.fact_revisions
             WHERE tenant_id = $1 AND subject_id = $2
               AND writer_principal_id = $3",
        )
        .bind(target.tenant_id)
        .bind(target.subject_id)
        .bind(WORKER_PRINCIPAL)
        .fetch_one(&mut *transaction)
        .await?;
        let unlinked: Vec<Uuid> = sqlx::query(
            "SELECT f.fact_id
             FROM memory.fact_revisions AS f
             LEFT JOIN memory.consolidation_claims AS c
               ON c.fact_id = f.fact_id
              AND c.tenant_id = f.tenant_id
              AND c.subject_id = f.subject_id
             WHERE f.tenant_id = $1 AND f.subject_id = $2
               AND f.writer_principal_id = $3
               AND c.claim_id IS NULL",
        )
        .bind(target.tenant_id)
        .bind(target.subject_id)
        .bind(WORKER_PRINCIPAL)
        .fetch_all(&mut *transaction)
        .await?
        .into_iter()
        .map(|row| row.try_get::<Uuid, _>(0))
        .collect::<std::result::Result<Vec<_>, _>>()?;
        ensure!(
            unlinked.is_empty(),
            "crash resume left unlinked worker facts (duplicates or orphans): {unlinked:?} \
             (facts = {facts}, claims_total = {claims_total})"
        );
        let undone: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM memory.consolidation_claims
             WHERE tenant_id = $1 AND subject_id = $2 AND lifecycle_state <> 'done'",
        )
        .bind(target.tenant_id)
        .bind(target.subject_id)
        .fetch_one(&mut *transaction)
        .await?;
        ensure!(undone == 0, "crash resume left {undone} claims unfinished");
        transaction.commit().await?;
        Ok(())
    }
    .await;
    let _ = restart_server.kill().await;
    let _ = restart_server.wait().await;
    let _ = case_id;
    result
}

/// Regression for issue #47: a job must not be failed while another worker
/// pass still holds leased claims. Deterministic simulation of the
/// two-pass interleaving that flaked CI: pass A leases the job, its lease
/// expires mid-run, pass B takes the job over and leases the remaining
/// claim; pass A must not fail the job, and pass B completes it.
///
/// The job is inserted directly as a running job leased by pass A, so the
/// lifecycle server's ambient worker can never claim it: a running job
/// with an unexpired lease is invisible to claim_next_job. Pass B takes
/// the job over with a single atomic UPDATE, exactly as claim_next_job
/// would after the lease expiry, so the scenario has no timing windows.
pub(crate) async fn consolidation_job_not_failed_while_claims_in_flight(
    pool: &PgPool,
    target: &Target,
) -> Result<()> {
    let job_id = Uuid::now_v7();
    let worker_a = Uuid::now_v7();
    let worker_b = Uuid::now_v7();
    let now = time::OffsetDateTime::now_utc();
    let window_from = now - time::Duration::hours(1);
    let window_until = now + time::Duration::hours(1);

    let mut transaction = pool.begin().await?;
    for (guc, value) in [
        ("palimpsest.tenant_id", target.tenant_id.to_string()),
        ("palimpsest.subject_id", target.subject_id.to_string()),
        ("palimpsest.worker_claim", "palimpsest-worker-v1".to_owned()),
    ] {
        sqlx::query("SELECT set_config($1, $2, true)")
            .bind(guc)
            .bind(value)
            .execute(&mut *transaction)
            .await?;
    }
    sqlx::query(
        r#"
        INSERT INTO memory.consolidation_jobs (
            tenant_id, subject_id, job_id, source_kind, policy_id, policy_version,
            window_from, window_until, lifecycle_state, worker_lease_id,
            worker_lease_expires_at, claim_cap, claims_total, claims_done,
            principal_id, idempotency_key_digest, request_fingerprint
        )
        VALUES ($1, $2, $3, 'conversation', 'derive-summaries-v1', '1',
                $4, $5, 'running', $6,
                clock_timestamp() + interval '30 seconds', 100000, 0, 0,
                'palimpsest-consolidation-worker', $7, $8)
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(job_id)
    .bind(window_from)
    .bind(window_until)
    .bind(worker_a)
    .bind("1".repeat(64))
    .bind("2".repeat(64))
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;

    let repository = Arc::new(PostgresMemoryRepository::new(pool.clone()));
    let job = ClaimedConsolidationJob {
        tenant_id: TenantId(target.tenant_id),
        subject_id: SubjectId(target.subject_id),
        job_id,
        source_kind: "conversation".to_owned(),
        policy_id: "derive-summaries-v1".to_owned(),
        policy_version: "1".to_owned(),
        window_from,
        window_until,
        claim_cap: 100_000,
        principal_id: PrincipalId("palimpsest-consolidation-worker".to_owned()),
    };
    let claims: Vec<PendingConsolidationClaim> = [1u8, 2]
        .into_iter()
        .map(|index| PendingConsolidationClaim {
            claim_id: Uuid::from_u128(u128::from(index)),
            case_id: CaseId(Uuid::now_v7()),
            episode_ids: vec![EpisodeId(Uuid::now_v7())],
            content_hash: format!("{:064x}", index),
            confidence: 0.95,
            sensitivity: "internal".to_owned(),
            observed_at: now,
            valid_from: now,
            valid_until: None,
            model_identity: "fixture-deterministic-v1:config-digest".to_owned(),
            prompt_policy_version: "fixture-prompt-v1".to_owned(),
            value: json!({"kind": "fixture-summary", "index": index}),
        })
        .collect();
    repository.insert_claims(&job, &claims).await?;

    // Pass A completes the first claim; the second stays pending.
    let claimed_1 = repository
        .claim_next_claim(&job, worker_a, 30)
        .await?
        .context("pass A could not lease claim 1")?;
    ensure!(
        repository
            .complete_claim(
                &claimed_1,
                FactId(Uuid::from_u128(101)),
                RevisionId(Uuid::from_u128(102)),
            )
            .await?,
        "pass A could not complete claim 1"
    );

    // Pass A's job lease expires mid-run (the slow-run interleaving), and
    // pass B takes the job over atomically, exactly as claim_next_job
    // would after the expiry.
    let mut transaction = pool.begin().await?;
    for (guc, value) in [
        ("palimpsest.tenant_id", target.tenant_id.to_string()),
        ("palimpsest.subject_id", target.subject_id.to_string()),
        ("palimpsest.worker_claim", "palimpsest-worker-v1".to_owned()),
    ] {
        sqlx::query("SELECT set_config($1, $2, true)")
            .bind(guc)
            .bind(value)
            .execute(&mut *transaction)
            .await?;
    }
    let taken = sqlx::query(
        r#"
        UPDATE memory.consolidation_jobs
        SET worker_lease_id = $4,
            worker_lease_expires_at = clock_timestamp() + interval '30 seconds',
            state_version = state_version + 1,
            updated_at = clock_timestamp()
        WHERE tenant_id = $1 AND subject_id = $2 AND job_id = $3
          AND lifecycle_state = 'running'
        "#,
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(job_id)
    .bind(worker_b)
    .execute(&mut *transaction)
    .await?;
    ensure!(
        taken.rows_affected() == 1,
        "pass B could not take the job over"
    );
    transaction.commit().await?;

    // Pass B leases the second claim; its lease is unexpired.
    let claimed_2 = repository
        .claim_next_claim(&job, worker_b, 30)
        .await?
        .context("pass B could not lease claim 2")?;

    // Pass A's claim loop drains to none while pass B holds an unexpired
    // lease, so complete_job cannot finish the job from A's view...
    ensure!(
        repository
            .claim_next_claim(&job, worker_a, 30)
            .await?
            .is_none(),
        "pass A reclaimed a claim leased by pass B"
    );
    ensure!(
        !repository.complete_job(&job).await?,
        "complete_job finished a job with a leased claim"
    );
    // ...and the worker must not fail the job while a claim is still in
    // flight: it defers and leaves the job running for pass B.
    ensure!(
        repository.has_in_flight_claims(&job).await?,
        "has_in_flight_claims missed the claim leased by pass B"
    );
    let deferred = repository
        .poll_job(
            TenantId(target.tenant_id),
            SubjectId(target.subject_id),
            job_id,
        )
        .await?;
    ensure!(
        deferred.lifecycle_state == "running" && deferred.failure_reason.is_none(),
        "the job was failed while a claim was in flight"
    );

    // Pass B finishes the last claim and completes the job.
    ensure!(
        repository
            .complete_claim(
                &claimed_2,
                FactId(Uuid::from_u128(103)),
                RevisionId(Uuid::from_u128(104)),
            )
            .await?,
        "pass B could not complete claim 2"
    );
    ensure!(
        repository.complete_job(&job).await?,
        "pass B could not complete the job"
    );
    let completed = repository
        .poll_job(
            TenantId(target.tenant_id),
            SubjectId(target.subject_id),
            job_id,
        )
        .await?;
    ensure!(
        completed.lifecycle_state == "complete" && completed.failure_reason.is_none(),
        "the job did not complete cleanly: {completed:?}"
    );
    ensure!(
        completed.claims_done == 2,
        "claims_done mismatch: {}",
        completed.claims_done
    );
    Ok(())
}

fn spawn_consolidation_crash_child(
    database_url: &str,
    target: &Target,
    address: std::net::SocketAddr,
) -> Result<tokio::process::Child> {
    let mut command = tokio::process::Command::new(std::env::current_exe()?);
    command
        .arg("--ignored")
        .arg("--exact")
        .arg("consolidation::crash_after_first_claim_child")
        .arg("--test-threads=1")
        .env("PALIMPSEST_CRASH_CHILD", "1")
        .env("PALIMPSEST_TEST_CHILD_DATABASE_URL", database_url)
        .env(
            "PALIMPSEST_TEST_CHILD_TENANT_ID",
            target.tenant_id.to_string(),
        )
        .env(
            "PALIMPSEST_TEST_CHILD_SUBJECT_ID",
            target.subject_id.to_string(),
        )
        .env("PALIMPSEST_TEST_CHILD_BEARER_TOKEN", &target.bearer_token)
        .env("PALIMPSEST_TEST_CHILD_BIND", address.to_string())
        .kill_on_drop(true)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
    command.spawn().context("spawn consolidation crash child")
}

async fn crash_job_id(database_url: &str, target: &Target) -> Result<String> {
    let pool = sqlx::PgPool::connect(database_url).await?;
    let mut transaction = pool.begin().await?;
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
        .bind(target.tenant_id.to_string())
        .execute(&mut *transaction)
        .await?;
    sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
        .bind(target.subject_id.to_string())
        .execute(&mut *transaction)
        .await?;
    let job_id: Uuid = sqlx::query_scalar(
        "SELECT job_id
         FROM memory.consolidation_jobs
         WHERE tenant_id = $1 AND subject_id = $2
           AND idempotency_key_digest = $3
         ORDER BY created_at
         LIMIT 1",
    )
    .bind(target.tenant_id)
    .bind(target.subject_id)
    .bind(hex::encode(sha256_bytes(b"crash-job-1")))
    .fetch_one(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(job_id.to_string())
}

#[test]
#[ignore]
fn crash_after_first_claim_child() {
    let database_url = std::env::var("PALIMPSEST_TEST_CHILD_DATABASE_URL")
        .expect("crash child needs the database url");
    let tenant_id = Uuid::parse_str(
        &std::env::var("PALIMPSEST_TEST_CHILD_TENANT_ID").expect("crash child needs the tenant id"),
    )
    .expect("tenant id is not a uuid");
    let subject_id = Uuid::parse_str(
        &std::env::var("PALIMPSEST_TEST_CHILD_SUBJECT_ID")
            .expect("crash child needs the subject id"),
    )
    .expect("subject id is not a uuid");
    let bearer_token = std::env::var("PALIMPSEST_TEST_CHILD_BEARER_TOKEN")
        .expect("crash child needs the bearer token");
    let bind = std::env::var("PALIMPSEST_TEST_CHILD_BIND").expect("crash child needs the bind");
    let target = Target {
        base_url: format!("http://{bind}"),
        bearer_token,
        tenant_id,
        subject_id,
        principal_a_secondary_subject_id: Uuid::nil(),
        principal_a_internal_bearer_token: String::new(),
        principal_b_bearer_token: String::new(),
        principal_b_tenant_id: Uuid::nil(),
        principal_b_subject_id: Uuid::nil(),
        principal_c_bearer_token: String::new(),
        principal_c_subject_id: Uuid::nil(),
        principal_d_same_scope_bearer_token: String::new(),
    };
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    runtime.block_on(async {
        let listener = tokio::net::TcpListener::bind(&bind).await.expect("bind");
        let pool = sqlx::PgPool::connect(&database_url).await.expect("pool");
        let server_pool = pool.clone();
        let bearer = target.bearer_token.clone();
        let tenant_id = target.tenant_id;
        let subject_id = target.subject_id;
        let _server = tokio::spawn(async move {
            axum::serve(
                listener,
                palimpsest_server::app(server_pool.clone(), server_pool, {
                    use palimpsest_http::StaticAuthenticator;
                    use std::collections::HashMap;
                    let mut principals = HashMap::new();
                    principals.insert(
                        bearer,
                        palimpsest_domain::PrincipalScope {
                            principal_id: palimpsest_domain::PrincipalId("principal-a".to_owned()),
                            tenant_id: palimpsest_domain::TenantId(tenant_id),
                            subject_ids: vec![palimpsest_domain::SubjectId(subject_id)],
                            allowed_sensitivities: vec![
                                palimpsest_domain::Sensitivity::try_from("internal".to_owned())
                                    .expect("sensitivity"),
                            ],
                            operation_grants: vec![],
                        },
                    );
                    Arc::new(StaticAuthenticator::new(principals))
                }),
            )
            .await
            .expect("serve");
        });
        wait_for_listener(bind.parse().expect("bind address"))
            .await
            .expect("listener");
        let client = Client::new();
        let case_id = Uuid::parse_str("019be000-0000-7000-8000-000000000004").expect("case id");
        let config_id = register_interpreter_config(&client, &target, PROVIDER_KIND)
            .await
            .expect("interpreter config");
        register_policy(&client, &target, config_id, SOURCE_KIND, POLICY_ID)
            .await
            .expect("policy");
        for index in 0..50u32 {
            let episode = append_conversation_episode(
                &client,
                &target,
                case_id,
                index,
                &format!("crash-episode-{index}"),
                SOURCE_KIND,
            )
            .await
            .expect("episode");
            let _ = episode;
        }
        let job = create_consolidation_job(&client, &target, "crash-job-1", SOURCE_KIND, POLICY_ID)
            .await
            .expect("job");
        let job_id = job["job_id"].as_str().expect("job id").to_owned();
        // Wait until the worker materialized some claims, then crash
        // mid-job while the rest are still leased.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        loop {
            let response = client
                .get(job_status_url(&target, &job_id))
                .bearer_auth(&target.bearer_token)
                .send()
                .await
                .expect("poll");
            let body: Value = response.json().await.expect("json");
            let done = body["claims_done"].as_i64().expect("claims_done");
            if (1..50).contains(&done) {
                break;
            }
            crate::sleep_budget::poll_sleep(Duration::from_millis(100)).await;
            assert!(
                tokio::time::Instant::now() < deadline,
                "worker finished before the crash point"
            );
        }
        // Crash immediately: the abort keeps the in-flight claim's lease
        // and the remaining claims pending, which is exactly the state the
        // resume must recover from.
        std::process::abort();
    });
}
