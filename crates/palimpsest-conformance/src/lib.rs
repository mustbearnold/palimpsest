use anyhow::{Context, Result, ensure};
use reqwest::{Client, StatusCode, header};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, Deserialize, Serialize)]
struct Provenance {
    source_type: String,
    source_uri: Option<String>,
    external_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct Target {
    pub base_url: String,
    pub bearer_token: String,
    pub tenant_id: Uuid,
    pub subject_id: Uuid,
    pub principal_a_secondary_subject_id: Uuid,
    pub principal_b_bearer_token: String,
    pub principal_b_tenant_id: Uuid,
    pub principal_b_subject_id: Uuid,
    pub principal_c_bearer_token: String,
    pub principal_c_subject_id: Uuid,
}

#[derive(Debug, Deserialize, Serialize)]
struct Episode {
    tenant_id: Uuid,
    subject_id: Uuid,
    case_id: Uuid,
    episode_id: Uuid,
    kind: String,
    observed_at: String,
    recorded_at: String,
    writer_principal_id: String,
    provenance: Provenance,
    sensitivity: String,
    retention_policy_id: String,
    schema_version: u32,
    payload: Value,
    payload_sha256: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct ValidTime {
    from: String,
    until: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct WritePolicy {
    id: String,
    version: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct FactRevision {
    tenant_id: Uuid,
    subject_id: Uuid,
    case_id: Uuid,
    fact_id: Uuid,
    revision_id: Uuid,
    revision_number: u64,
    supersedes_revision_id: Option<Uuid>,
    namespace: String,
    key: String,
    value: Value,
    observed_at: String,
    recorded_at: String,
    valid_time: ValidTime,
    evidence_episode_ids: Vec<Uuid>,
    write_policy: WritePolicy,
    confidence: f64,
    sensitivity: String,
    retention_policy_id: String,
    writer_principal_id: String,
    schema_version: u32,
}

#[derive(Debug, Deserialize)]
struct FactView {
    tenant_id: Uuid,
    subject_id: Uuid,
    case_id: Uuid,
    fact_id: Uuid,
    namespace: String,
    key: String,
    head_revision_id: Uuid,
    evaluated_at: String,
    valid_at: String,
    recorded_at: String,
    revision: Option<FactRevision>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Checkpoint {
    tenant_id: Uuid,
    subject_id: Uuid,
    case_id: Uuid,
    agent_id: Uuid,
    thread_id: Uuid,
    checkpoint_id: Uuid,
    checkpoint_revision_id: Uuid,
    revision_number: u64,
    parent_revision_id: Option<Uuid>,
    recorded_at: String,
    state: Value,
    state_schema_version: u32,
    state_sha256: String,
    effects: Vec<CheckpointEffect>,
    provenance: Provenance,
    sensitivity: String,
    retention_policy_id: String,
    expires_at: String,
    writer_principal_id: String,
    schema_version: u32,
}

#[derive(Debug, Deserialize, Serialize)]
struct CheckpointEffect {
    effect_id: Uuid,
    effect_key: String,
    kind: String,
    recovery_mode: String,
    status: String,
    prepared_at: String,
    completed_at: Option<String>,
    receipt: Option<EffectReceipt>,
}

#[derive(Debug, Deserialize, Serialize)]
struct EffectReceipt {
    observed_at: String,
    external_reference: Option<String>,
    outcome_sha256: String,
}

pub async fn records_and_reads_an_immutable_episode(target: &Target) -> Result<()> {
    let client = Client::new();
    let case_id = Uuid::parse_str("019be000-0000-7000-8000-000000000001")?;
    let collection_url = format!(
        "{}/v1/tenants/{}/subjects/{}/episodes",
        target.base_url.trim_end_matches('/'),
        target.tenant_id,
        target.subject_id
    );
    let body = json!({
        "case_id": case_id,
        "kind": "message",
        "observed_at": "2026-01-10T09:00:00Z",
        "provenance": {
            "source_type": "conformance",
            "source_uri": null,
            "external_id": "episode-a"
        },
        "sensitivity": "internal",
        "retention_policy_id": "standard",
        "payload": {"message": "Customer supplied the first shipping address."}
    });

    let create_response = client
        .post(&collection_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "episode-a-create")
        .json(&body)
        .send()
        .await
        .context("append episode request failed")?;

    ensure!(
        create_response.status() == StatusCode::CREATED,
        "append episode returned {}, expected 201",
        create_response.status()
    );
    ensure!(
        create_response
            .headers()
            .get(header::CONTENT_TYPE)
            .is_some_and(|value| {
                value
                    .to_str()
                    .is_ok_and(|text| text.starts_with("application/json"))
            }),
        "append episode did not return JSON"
    );
    let location = create_response
        .headers()
        .get(header::LOCATION)
        .context("append episode omitted Location")?
        .to_str()
        .context("Location was not valid text")?
        .to_owned();
    let created: Episode = create_response
        .json()
        .await
        .context("append episode response was not an Episode")?;

    ensure!(
        created.tenant_id == target.tenant_id,
        "tenant scope changed"
    );
    ensure!(
        created.subject_id == target.subject_id,
        "subject scope changed"
    );
    ensure!(created.case_id == case_id, "case scope changed");
    ensure!(
        created.observed_at == "2026-01-10T09:00:00Z",
        "observed time changed"
    );
    ensure!(
        created.provenance.source_type == "conformance",
        "source type changed"
    );
    ensure!(
        created.provenance.external_id.as_deref() == Some("episode-a"),
        "external source ID changed"
    );
    ensure!(created.payload == body["payload"], "payload changed");
    ensure!(created.schema_version == 1, "schema version changed");
    ensure!(
        created.episode_id.get_version_num() == 7,
        "episode ID is not UUIDv7"
    );
    ensure!(
        created.payload_sha256.len() == 64,
        "payload digest is not SHA-256 hex"
    );
    ensure!(!created.recorded_at.is_empty(), "recorded time is absent");

    let replay_response = client
        .post(&collection_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "episode-a-create")
        .json(&body)
        .send()
        .await
        .context("idempotent episode replay failed")?;
    ensure!(
        replay_response.status() == StatusCode::CREATED,
        "episode replay returned {}, expected 201",
        replay_response.status()
    );
    ensure!(
        replay_response
            .headers()
            .get("idempotency-replayed")
            .is_some_and(|value| value == "true"),
        "episode replay did not identify itself"
    );
    let replayed: Episode = replay_response
        .json()
        .await
        .context("episode replay response was not an Episode")?;
    ensure!(
        serde_json::to_value(&replayed)? == serde_json::to_value(&created)?,
        "episode replay did not return the original representation"
    );

    let resource_url = if location.starts_with("http://") || location.starts_with("https://") {
        location
    } else {
        format!("{}{}", target.base_url.trim_end_matches('/'), location)
    };
    let read_response = client
        .get(resource_url)
        .bearer_auth(&target.bearer_token)
        .send()
        .await
        .context("read episode request failed")?;
    ensure!(
        read_response.status() == StatusCode::OK,
        "read episode returned {}, expected 200",
        read_response.status()
    );
    let read: Episode = read_response
        .json()
        .await
        .context("read episode response was not an Episode")?;
    ensure!(
        serde_json::to_value(&read)? == serde_json::to_value(&created)?,
        "read episode differs from appended episode"
    );

    let null_payload_response = client
        .post(&collection_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "episode-null-payload-create")
        .json(&json!({
            "case_id": case_id,
            "kind": "signal",
            "observed_at": "2026-01-11T09:00:00Z",
            "provenance": {
                "source_type": "conformance",
                "source_uri": null,
                "external_id": "episode-null-payload"
            },
            "sensitivity": "internal",
            "retention_policy_id": "standard",
            "payload": null
        }))
        .send()
        .await?;
    ensure!(
        null_payload_response.status() == StatusCode::CREATED,
        "JSON null episode payload returned {}",
        null_payload_response.status()
    );
    let null_payload: Episode = null_payload_response.json().await?;
    ensure!(null_payload.payload.is_null(), "JSON null payload changed");

    Ok(())
}

pub async fn saves_and_reads_a_resumable_checkpoint(target: &Target) -> Result<()> {
    let client = Client::new();
    let case_id = Uuid::parse_str("019be000-0000-7000-8000-000000000301")?;
    let agent_id = Uuid::parse_str("019be000-0000-7000-8000-000000000302")?;
    let thread_id = Uuid::parse_str("019be000-0000-7000-8000-000000000303")?;
    let checkpoint_path = format!(
        "/v1/tenants/{}/subjects/{}/agents/{agent_id}/threads/{thread_id}/checkpoint",
        target.tenant_id, target.subject_id
    );
    let checkpoint_url = format!(
        "{}{}",
        target.base_url.trim_end_matches('/'),
        checkpoint_path
    );
    let body = json!({
        "case_id": case_id,
        "parent_revision_id": null,
        "state": {
            "step": "awaiting-provider",
            "work_item": "case-301"
        },
        "state_schema_version": 1,
        "effect_transitions": [],
        "provenance": {
            "source_type": "agent.runtime",
            "source_uri": null,
            "external_id": "checkpoint-run-301"
        },
        "sensitivity": "internal",
        "retention_policy_id": "checkpoint-active-30d-v1"
    });

    let create_response = client
        .put(&checkpoint_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "checkpoint-run-301-create")
        .header(header::IF_NONE_MATCH, "*")
        .json(&body)
        .send()
        .await?;
    ensure!(
        create_response.status() == StatusCode::CREATED,
        "checkpoint create returned {}, expected 201",
        create_response.status()
    );
    let etag = create_response
        .headers()
        .get(header::ETAG)
        .context("checkpoint create omitted ETag")?
        .to_str()?
        .to_owned();
    ensure!(
        etag.starts_with('"') && etag.ends_with('"'),
        "checkpoint ETag is not strong"
    );
    ensure!(
        create_response
            .headers()
            .get(header::LOCATION)
            .is_some_and(|value| value
                .to_str()
                .is_ok_and(|value| value == checkpoint_path || value == checkpoint_url)),
        "checkpoint create Location does not identify the logical head"
    );
    let created: Checkpoint = create_response.json().await?;
    ensure!(created.tenant_id == target.tenant_id);
    ensure!(created.subject_id == target.subject_id);
    ensure!(created.case_id == case_id);
    ensure!(created.agent_id == agent_id);
    ensure!(created.thread_id == thread_id);
    ensure!(created.checkpoint_id.get_version_num() == 7);
    ensure!(created.checkpoint_revision_id.get_version_num() == 7);
    ensure!(created.revision_number == 1);
    ensure!(created.parent_revision_id.is_none());
    ensure!(created.state == body["state"]);
    ensure!(created.state_schema_version == 1);
    ensure!(created.state_sha256.len() == 64);
    ensure!(created.effects.is_empty());
    ensure!(created.provenance.source_type == "agent.runtime");
    ensure!(created.retention_policy_id == "checkpoint-active-30d-v1");
    ensure!(created.expires_at > created.recorded_at);
    ensure!(created.writer_principal_id == "principal-a");
    ensure!(created.schema_version == 1);

    let replay_response = client
        .put(&checkpoint_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "checkpoint-run-301-create")
        .header(header::IF_NONE_MATCH, "*")
        .json(&body)
        .send()
        .await?;
    ensure!(replay_response.status() == StatusCode::CREATED);
    ensure!(
        replay_response
            .headers()
            .get("idempotency-replayed")
            .is_some_and(|value| value == "true")
    );
    ensure!(
        replay_response.headers().get(header::ETAG) == Some(&header::HeaderValue::from_str(&etag)?)
    );
    let replayed: Checkpoint = replay_response.json().await?;
    ensure!(
        serde_json::to_value(&replayed)? == serde_json::to_value(&created)?,
        "checkpoint creation replay did not return the committed representation"
    );

    let read_response = client
        .get(&checkpoint_url)
        .bearer_auth(&target.bearer_token)
        .send()
        .await?;
    ensure!(read_response.status() == StatusCode::OK);
    ensure!(
        read_response.headers().get(header::ETAG) == Some(&header::HeaderValue::from_str(&etag)?),
        "checkpoint read ETag differs from create"
    );
    let read: Checkpoint = read_response.json().await?;
    ensure!(
        serde_json::to_value(&read)? == serde_json::to_value(&created)?,
        "checkpoint read differs from the committed head"
    );

    let prepare_body = json!({
        "case_id": case_id,
        "parent_revision_id": created.checkpoint_revision_id,
        "state": {
            "step": "provider-call-prepared",
            "work_item": "case-301"
        },
        "state_schema_version": 1,
        "effect_transitions": [{
            "type": "prepare",
            "effect_key": "notify-case-301",
            "kind": "notification.send",
            "recovery_mode": "idempotency_key"
        }],
        "provenance": body["provenance"],
        "sensitivity": "internal",
        "retention_policy_id": "checkpoint-active-30d-v1"
    });
    let prepare_response = client
        .put(&checkpoint_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "checkpoint-run-301-prepare")
        .header(header::IF_MATCH, &etag)
        .json(&prepare_body)
        .send()
        .await?;
    ensure!(
        prepare_response.status() == StatusCode::OK,
        "checkpoint effect preparation returned {}",
        prepare_response.status()
    );
    let prepared_etag = prepare_response
        .headers()
        .get(header::ETAG)
        .context("checkpoint preparation omitted ETag")?
        .to_str()?
        .to_owned();
    let prepared: Checkpoint = prepare_response.json().await?;
    ensure!(prepared.checkpoint_id == created.checkpoint_id);
    ensure!(prepared.checkpoint_revision_id != created.checkpoint_revision_id);
    ensure!(prepared.parent_revision_id == Some(created.checkpoint_revision_id));
    ensure!(prepared.revision_number == 2);
    ensure!(prepared.effects.len() == 1);
    let prepared_effect = &prepared.effects[0];
    ensure!(prepared_effect.effect_id.get_version_num() == 7);
    ensure!(prepared_effect.effect_key == "notify-case-301");
    ensure!(prepared_effect.kind == "notification.send");
    ensure!(prepared_effect.recovery_mode == "idempotency_key");
    ensure!(prepared_effect.status == "prepared");
    ensure!(prepared_effect.completed_at.is_none());
    ensure!(prepared_effect.receipt.is_none());

    let stale_response = client
        .put(&checkpoint_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "checkpoint-run-301-stale")
        .header(header::IF_MATCH, &etag)
        .json(&json!({
            "case_id": case_id,
            "parent_revision_id": created.checkpoint_revision_id,
            "state": {"step": "stale-writer"},
            "state_schema_version": 1,
            "effect_transitions": [],
            "provenance": body["provenance"],
            "sensitivity": "internal",
            "retention_policy_id": "checkpoint-active-30d-v1"
        }))
        .send()
        .await?;
    assert_problem(
        stale_response,
        StatusCode::PRECONDITION_FAILED,
        "stale-checkpoint",
    )
    .await?;

    let receipt_digest = "a".repeat(64);
    let complete_body = json!({
        "case_id": case_id,
        "parent_revision_id": prepared.checkpoint_revision_id,
        "state": {
            "step": "provider-call-completed",
            "work_item": "case-301"
        },
        "state_schema_version": 1,
        "effect_transitions": [{
            "type": "complete",
            "effect_id": prepared_effect.effect_id,
            "receipt": {
                "observed_at": "2026-07-29T01:30:00Z",
                "external_reference": "provider-result-301",
                "outcome_sha256": receipt_digest
            }
        }],
        "provenance": body["provenance"],
        "sensitivity": "internal",
        "retention_policy_id": "checkpoint-active-30d-v1"
    });
    let complete_response = client
        .put(&checkpoint_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "checkpoint-run-301-complete")
        .header(header::IF_MATCH, &prepared_etag)
        .json(&complete_body)
        .send()
        .await?;
    ensure!(complete_response.status() == StatusCode::OK);
    let completed_etag = complete_response
        .headers()
        .get(header::ETAG)
        .context("checkpoint completion omitted ETag")?
        .to_str()?
        .to_owned();
    let completed: Checkpoint = complete_response.json().await?;
    ensure!(completed.checkpoint_id == created.checkpoint_id);
    ensure!(completed.parent_revision_id == Some(prepared.checkpoint_revision_id));
    ensure!(completed.revision_number == 3);
    ensure!(completed.effects.len() == 1);
    let completed_effect = &completed.effects[0];
    ensure!(completed_effect.effect_id == prepared_effect.effect_id);
    ensure!(completed_effect.status == "completed");
    ensure!(completed_effect.completed_at.is_some());
    ensure!(completed_effect.receipt.as_ref().is_some_and(|receipt| {
        receipt.external_reference.as_deref() == Some("provider-result-301")
    }));

    let completion_replay_response = client
        .put(&checkpoint_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "checkpoint-run-301-complete")
        .header(header::IF_MATCH, &prepared_etag)
        .json(&complete_body)
        .send()
        .await?;
    ensure!(completion_replay_response.status() == StatusCode::OK);
    ensure!(
        completion_replay_response
            .headers()
            .get("idempotency-replayed")
            .is_some_and(|value| value == "true")
    );
    ensure!(
        completion_replay_response.headers().get(header::ETAG)
            == Some(&header::HeaderValue::from_str(&completed_etag)?)
    );
    let completion_replay: Checkpoint = completion_replay_response.json().await?;
    ensure!(
        serde_json::to_value(&completion_replay)? == serde_json::to_value(&completed)?,
        "completion replay did not return the original committed representation"
    );

    let duplicate_prepare_response = client
        .put(&checkpoint_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "checkpoint-run-301-duplicate-effect")
        .header(header::IF_MATCH, &completed_etag)
        .json(&json!({
            "case_id": case_id,
            "parent_revision_id": completed.checkpoint_revision_id,
            "state": {"step": "must-not-advance"},
            "state_schema_version": 1,
            "effect_transitions": [{
                "type": "prepare",
                "effect_key": "notify-case-301",
                "kind": "notification.send",
                "recovery_mode": "idempotency_key"
            }],
            "provenance": body["provenance"],
            "sensitivity": "internal",
            "retention_policy_id": "checkpoint-active-30d-v1"
        }))
        .send()
        .await?;
    assert_problem(
        duplicate_prepare_response,
        StatusCode::CONFLICT,
        "effect-key-conflict",
    )
    .await?;

    let duplicate_complete_response = client
        .put(&checkpoint_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "checkpoint-run-301-duplicate-completion")
        .header(header::IF_MATCH, &completed_etag)
        .json(&json!({
            "case_id": case_id,
            "parent_revision_id": completed.checkpoint_revision_id,
            "state": {"step": "must-not-advance"},
            "state_schema_version": 1,
            "effect_transitions": complete_body["effect_transitions"],
            "provenance": body["provenance"],
            "sensitivity": "internal",
            "retention_policy_id": "checkpoint-active-30d-v1"
        }))
        .send()
        .await?;
    assert_problem(
        duplicate_complete_response,
        StatusCode::CONFLICT,
        "invalid-effect-transition",
    )
    .await?;

    let missing_precondition_response = client
        .put(&checkpoint_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "checkpoint-run-301-missing-precondition")
        .json(&complete_body)
        .send()
        .await?;
    assert_problem(
        missing_precondition_response,
        StatusCode::PRECONDITION_REQUIRED,
        "checkpoint-precondition-required",
    )
    .await?;

    let rejected_policy_url = format!(
        "{}/v1/tenants/{}/subjects/{}/agents/019be000-0000-7000-8000-000000000332/threads/019be000-0000-7000-8000-000000000333/checkpoint",
        target.base_url.trim_end_matches('/'),
        target.tenant_id,
        target.subject_id
    );
    let rejected_policy_response = client
        .put(rejected_policy_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "checkpoint-run-301-rejected-policy")
        .header(header::IF_NONE_MATCH, "*")
        .json(&json!({
            "case_id": case_id,
            "parent_revision_id": null,
            "state": {"step": "must-not-persist"},
            "state_schema_version": 1,
            "effect_transitions": [],
            "provenance": body["provenance"],
            "sensitivity": "internal",
            "retention_policy_id": "checkpoint-unknown-policy-v1"
        }))
        .send()
        .await?;
    assert_problem(
        rejected_policy_response,
        StatusCode::UNPROCESSABLE_ENTITY,
        "retention-policy-rejected",
    )
    .await?;

    let oversized_effects = (0..101)
        .map(|index| {
            json!({
                "type": "prepare",
                "effect_key": format!("oversized-effect-{index}"),
                "kind": "conformance.noop",
                "recovery_mode": "reconcile"
            })
        })
        .collect::<Vec<_>>();
    let oversized_url = format!(
        "{}/v1/tenants/{}/subjects/{}/agents/019be000-0000-7000-8000-000000000342/threads/019be000-0000-7000-8000-000000000343/checkpoint",
        target.base_url.trim_end_matches('/'),
        target.tenant_id,
        target.subject_id
    );
    let oversized_response = client
        .put(oversized_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "checkpoint-run-301-oversized")
        .header(header::IF_NONE_MATCH, "*")
        .json(&json!({
            "case_id": case_id,
            "parent_revision_id": null,
            "state": {"step": "must-not-persist"},
            "state_schema_version": 1,
            "effect_transitions": oversized_effects,
            "provenance": body["provenance"],
            "sensitivity": "internal",
            "retention_policy_id": "checkpoint-active-30d-v1"
        }))
        .send()
        .await?;
    assert_problem(
        oversized_response,
        StatusCode::PAYLOAD_TOO_LARGE,
        "checkpoint-too-large",
    )
    .await?;

    let sibling_subject_url = format!(
        "{}/v1/tenants/{}/subjects/{}/agents/{agent_id}/threads/{thread_id}/checkpoint",
        target.base_url.trim_end_matches('/'),
        target.tenant_id,
        target.principal_a_secondary_subject_id
    );
    let sibling_subject_response = client
        .get(sibling_subject_url)
        .bearer_auth(&target.bearer_token)
        .send()
        .await?;
    assert_problem(
        sibling_subject_response,
        StatusCode::NOT_FOUND,
        "resource-not-found",
    )
    .await?;

    let unauthorized_subject_response = client
        .get(&checkpoint_url)
        .bearer_auth(&target.principal_c_bearer_token)
        .send()
        .await?;
    assert_problem(
        unauthorized_subject_response,
        StatusCode::NOT_FOUND,
        "resource-not-found",
    )
    .await?;
    Ok(())
}

pub async fn expires_only_the_targeted_checkpoint(target: &Target) -> Result<()> {
    let client = Client::new();
    let case_id = Uuid::parse_str("019be000-0000-7000-8000-000000000311")?;
    let agent_id = Uuid::parse_str("019be000-0000-7000-8000-000000000312")?;
    let thread_id = Uuid::parse_str("019be000-0000-7000-8000-000000000313")?;
    let checkpoint_url = format!(
        "{}/v1/tenants/{}/subjects/{}/agents/{agent_id}/threads/{thread_id}/checkpoint",
        target.base_url.trim_end_matches('/'),
        target.tenant_id,
        target.subject_id
    );
    let response = client
        .put(&checkpoint_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "checkpoint-run-311-create")
        .header(header::IF_NONE_MATCH, "*")
        .json(&json!({
            "case_id": case_id,
            "parent_revision_id": null,
            "state": {"step": "short-lived"},
            "state_schema_version": 1,
            "effect_transitions": [],
            "provenance": {"source_type": "conformance", "external_id": "checkpoint-run-311"},
            "sensitivity": "internal",
            "retention_policy_id": "checkpoint-test-1s-v1"
        }))
        .send()
        .await?;
    ensure!(
        response.status() == StatusCode::CREATED,
        "short-lived checkpoint returned {}",
        response.status()
    );
    let short_lived: Checkpoint = response.json().await?;
    ensure!(short_lived.retention_policy_id == "checkpoint-test-1s-v1");

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let expired_response = client
        .get(&checkpoint_url)
        .bearer_auth(&target.bearer_token)
        .send()
        .await?;
    assert_problem(
        expired_response,
        StatusCode::NOT_FOUND,
        "resource-not-found",
    )
    .await?;

    let durable_checkpoint_url = format!(
        "{}/v1/tenants/{}/subjects/{}/agents/019be000-0000-7000-8000-000000000302/threads/019be000-0000-7000-8000-000000000303/checkpoint",
        target.base_url.trim_end_matches('/'),
        target.tenant_id,
        target.subject_id
    );
    let durable_response = client
        .get(durable_checkpoint_url)
        .bearer_auth(&target.bearer_token)
        .send()
        .await?;
    ensure!(
        durable_response.status() == StatusCode::OK,
        "expiring one checkpoint affected a sibling thread"
    );
    Ok(())
}

pub async fn rejects_cross_subject_idempotency_reuse(target: &Target) -> Result<()> {
    let client = Client::new();
    let response = client
        .post(format!(
            "{}/v1/tenants/{}/subjects/{}/episodes",
            target.base_url.trim_end_matches('/'),
            target.tenant_id,
            target.principal_a_secondary_subject_id
        ))
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "episode-a-create")
        .json(&json!({
            "case_id": "019be000-0000-7000-8000-000000000001",
            "kind": "message",
            "observed_at": "2026-01-10T09:00:00Z",
            "provenance": {
                "source_type": "conformance",
                "source_uri": null,
                "external_id": "episode-a"
            },
            "sensitivity": "internal",
            "retention_policy_id": "standard",
            "payload": {"message": "Customer supplied the first shipping address."}
        }))
        .send()
        .await?;
    assert_problem(
        response,
        StatusCode::UNPROCESSABLE_ENTITY,
        "idempotency-key-reused",
    )
    .await
}

pub async fn rejects_invalid_domain_and_timestamp_inputs(target: &Target) -> Result<()> {
    let client = Client::new();
    let episodes_url = format!(
        "{}/v1/tenants/{}/subjects/{}/episodes",
        target.base_url.trim_end_matches('/'),
        target.tenant_id,
        target.subject_id
    );
    for (key, observed_at) in [
        ("invalid-offset-timestamp", "2026-01-10T22:00:00+13:00"),
        (
            "invalid-nanosecond-timestamp",
            "2026-01-10T09:00:00.1234567Z",
        ),
    ] {
        let response = client
            .post(&episodes_url)
            .bearer_auth(&target.bearer_token)
            .header("Idempotency-Key", key)
            .json(&json!({
                "case_id": "019be000-0000-7000-8000-000000000001",
                "kind": "message",
                "observed_at": observed_at,
                "provenance": {"source_type": "conformance"},
                "sensitivity": "internal",
                "retention_policy_id": "standard",
                "payload": {"message": "This request must not be persisted."}
            }))
            .send()
            .await?;
        assert_problem(response, StatusCode::BAD_REQUEST, "invalid-request").await?;
    }

    let response = client
        .post(format!(
            "{}/v1/tenants/{}/subjects/{}/facts",
            target.base_url.trim_end_matches('/'),
            target.tenant_id,
            target.subject_id
        ))
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "invalid-valid-time")
        .json(&json!({
            "case_id": "019be000-0000-7000-8000-000000000001",
            "namespace": "case.profile",
            "key": "invalid_interval",
            "value": {"invalid": true},
            "observed_at": "2026-01-10T09:00:00Z",
            "valid_time": {
                "from": "2026-01-10T00:00:00Z",
                "until": "2026-01-10T00:00:00Z"
            },
            "evidence_episode_ids": ["019be000-0000-7000-8000-000000000099"],
            "write_policy": {"id": "direct-evidence", "version": "1"},
            "confidence": 1.0,
            "sensitivity": "internal",
            "retention_policy_id": "standard"
        }))
        .send()
        .await?;
    assert_problem(
        response,
        StatusCode::UNPROCESSABLE_ENTITY,
        "invalid-valid-time",
    )
    .await
}

async fn assert_problem(response: reqwest::Response, status: StatusCode, kind: &str) -> Result<()> {
    ensure!(
        response.status() == status,
        "{kind} returned {}, expected {status}",
        response.status()
    );
    ensure!(
        response
            .headers()
            .get(header::CONTENT_TYPE)
            .is_some_and(|value| value == "application/problem+json"),
        "{kind} did not return RFC 9457 problem JSON"
    );
    ensure!(
        response
            .headers()
            .get(header::CACHE_CONTROL)
            .is_some_and(|value| value == "private, no-store"),
        "{kind} response can be cached"
    );
    let problem: Value = response.json().await?;
    ensure!(
        problem["type"] == format!("https://palimpsest.dev/problems/{kind}"),
        "{kind} did not use its stable problem type"
    );
    ensure!(
        problem["trace_id"]
            .as_str()
            .is_some_and(|value| Uuid::parse_str(value).is_ok()),
        "{kind} has no valid trace_id"
    );
    Ok(())
}

pub async fn creates_an_attributable_fact_revision(target: &Target) -> Result<()> {
    let client = Client::new();
    let case_id = Uuid::parse_str("019be000-0000-7000-8000-000000000001")?;
    let episode_url = format!(
        "{}/v1/tenants/{}/subjects/{}/episodes",
        target.base_url.trim_end_matches('/'),
        target.tenant_id,
        target.subject_id
    );
    let episode_body = json!({
        "case_id": case_id,
        "kind": "message",
        "observed_at": "2026-01-10T09:00:00Z",
        "provenance": {
            "source_type": "conformance",
            "source_uri": null,
            "external_id": "episode-a"
        },
        "sensitivity": "internal",
        "retention_policy_id": "standard",
        "payload": {"message": "Customer supplied the first shipping address."}
    });
    let episode_response = client
        .post(episode_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "episode-a-create")
        .json(&episode_body)
        .send()
        .await
        .context("fact setup episode request failed")?;
    ensure!(
        episode_response.status() == StatusCode::CREATED,
        "fact setup episode returned {}",
        episode_response.status()
    );
    let episode: Episode = episode_response.json().await?;

    let facts_url = format!(
        "{}/v1/tenants/{}/subjects/{}/facts",
        target.base_url.trim_end_matches('/'),
        target.tenant_id,
        target.subject_id
    );
    let fact_body = json!({
        "case_id": case_id,
        "namespace": "case.profile",
        "key": "shipping_address",
        "value": {"city": "Wellington", "country": "NZ"},
        "observed_at": "2026-01-10T09:00:00Z",
        "valid_time": {
            "from": "2026-01-10T00:00:00Z",
            "until": null
        },
        "evidence_episode_ids": [episode.episode_id],
        "write_policy": {"id": "direct-evidence", "version": "1"},
        "confidence": 0.95,
        "sensitivity": "internal",
        "retention_policy_id": "standard"
    });
    let create_response = client
        .post(&facts_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "fact-shipping-address-create")
        .json(&fact_body)
        .send()
        .await
        .context("create fact request failed")?;
    ensure!(
        create_response.status() == StatusCode::CREATED,
        "create fact returned {}, expected 201",
        create_response.status()
    );
    let etag = create_response
        .headers()
        .get(header::ETAG)
        .context("create fact omitted ETag")?
        .to_str()?
        .to_owned();
    ensure!(
        etag.starts_with('"') && etag.ends_with('"'),
        "fact ETag is not strong"
    );
    let location = create_response
        .headers()
        .get(header::LOCATION)
        .context("create fact omitted Location")?
        .to_str()?
        .to_owned();
    let created_view: FactView = create_response
        .json()
        .await
        .context("create fact response was not a view")?;
    let created = created_view
        .revision
        .as_ref()
        .context("created fact view has no revision")?;

    ensure!(created.tenant_id == target.tenant_id, "fact tenant changed");
    ensure!(
        created.subject_id == target.subject_id,
        "fact subject changed"
    );
    ensure!(created.case_id == case_id, "fact case changed");
    ensure!(created.revision_number == 1, "first revision is not 1");
    ensure!(
        created.supersedes_revision_id.is_none(),
        "first revision has a predecessor"
    );
    ensure!(
        created.evidence_episode_ids == vec![episode.episode_id],
        "fact evidence is not attributable"
    );
    ensure!(
        created.writer_principal_id == "principal-a",
        "fact writer is not the authenticated principal"
    );
    ensure!(
        created.fact_id.get_version_num() == 7,
        "fact ID is not UUIDv7"
    );
    ensure!(
        created.revision_id.get_version_num() == 7,
        "revision ID is not UUIDv7"
    );

    let resource_url = if location.starts_with("http://") || location.starts_with("https://") {
        location
    } else {
        format!("{}{}", target.base_url.trim_end_matches('/'), location)
    };
    let current_response = client
        .get(resource_url)
        .bearer_auth(&target.bearer_token)
        .send()
        .await
        .context("read current fact request failed")?;
    ensure!(
        current_response.status() == StatusCode::OK,
        "current fact returned {}, expected 200",
        current_response.status()
    );
    ensure!(
        current_response.headers().get(header::ETAG)
            == Some(&header::HeaderValue::from_str(&etag)?),
        "current fact ETag differs from creation"
    );
    let current: FactView = current_response
        .json()
        .await
        .context("current fact response was not a view")?;
    ensure!(current.tenant_id == target.tenant_id, "view tenant changed");
    ensure!(
        current.subject_id == target.subject_id,
        "view subject changed"
    );
    ensure!(current.fact_id == created.fact_id, "view fact changed");
    ensure!(current.case_id == case_id, "view case changed");
    ensure!(
        current.head_revision_id == created.revision_id,
        "view head is not the created revision"
    );
    ensure!(
        current.namespace == created.namespace,
        "view namespace changed"
    );
    ensure!(current.key == created.key, "view key changed");
    ensure!(
        !current.evaluated_at.is_empty(),
        "evaluation time is absent"
    );
    ensure!(
        current.valid_at == current.evaluated_at,
        "current valid time diverged"
    );
    ensure!(
        current.recorded_at == current.evaluated_at,
        "current recorded time diverged"
    );
    let revision = current.revision.context("current view has no revision")?;
    ensure!(
        serde_json::to_value(revision)? == serde_json::to_value(created)?,
        "current revision differs from created revision"
    );

    Ok(())
}

pub struct TemporalFixture {
    fact_id: Uuid,
    first_revision_id: Uuid,
    second_revision_id: Uuid,
    first_recorded_at: String,
    second_recorded_at: String,
}

pub async fn supersedes_the_fact_head(target: &Target) -> Result<TemporalFixture> {
    let client = Client::new();
    let case_id = Uuid::parse_str("019be000-0000-7000-8000-000000000001")?;
    let episode_url = format!(
        "{}/v1/tenants/{}/subjects/{}/episodes",
        target.base_url.trim_end_matches('/'),
        target.tenant_id,
        target.subject_id
    );
    let episode_a_response = client
        .post(&episode_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "episode-a-create")
        .json(&json!({
            "case_id": case_id,
            "kind": "message",
            "observed_at": "2026-01-10T09:00:00Z",
            "provenance": {
                "source_type": "conformance",
                "source_uri": null,
                "external_id": "episode-a"
            },
            "sensitivity": "internal",
            "retention_policy_id": "standard",
            "payload": {"message": "Customer supplied the first shipping address."}
        }))
        .send()
        .await?;
    ensure!(episode_a_response.status() == StatusCode::CREATED);
    let episode_a: Episode = episode_a_response.json().await?;

    let facts_url = format!(
        "{}/v1/tenants/{}/subjects/{}/facts",
        target.base_url.trim_end_matches('/'),
        target.tenant_id,
        target.subject_id
    );
    let first_fact_response = client
        .post(&facts_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "fact-shipping-address-create")
        .json(&json!({
            "case_id": case_id,
            "namespace": "case.profile",
            "key": "shipping_address",
            "value": {"city": "Wellington", "country": "NZ"},
            "observed_at": "2026-01-10T09:00:00Z",
            "valid_time": {"from": "2026-01-10T00:00:00Z", "until": null},
            "evidence_episode_ids": [episode_a.episode_id],
            "write_policy": {"id": "direct-evidence", "version": "1"},
            "confidence": 0.95,
            "sensitivity": "internal",
            "retention_policy_id": "standard"
        }))
        .send()
        .await?;
    ensure!(first_fact_response.status() == StatusCode::CREATED);
    let first_etag = first_fact_response
        .headers()
        .get(header::ETAG)
        .context("first fact response omitted ETag")?
        .to_str()?
        .to_owned();
    let first_view: FactView = first_fact_response.json().await?;
    let first_revision = first_view
        .revision
        .as_ref()
        .context("first fact has no revision")?;

    let episode_b_response = client
        .post(&episode_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "episode-b-create")
        .json(&json!({
            "case_id": case_id,
            "kind": "message",
            "observed_at": "2026-02-10T09:00:00Z",
            "provenance": {
                "source_type": "conformance",
                "source_uri": null,
                "external_id": "episode-b"
            },
            "sensitivity": "internal",
            "retention_policy_id": "standard",
            "payload": {"message": "Customer supplied a replacement shipping address."}
        }))
        .send()
        .await?;
    ensure!(episode_b_response.status() == StatusCode::CREATED);
    let episode_b: Episode = episode_b_response.json().await?;

    let fact_url = format!("{facts_url}/{}", first_view.fact_id);
    let supersede_response = client
        .put(&fact_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "fact-shipping-address-supersede")
        .header(header::IF_MATCH, &first_etag)
        .json(&json!({
            "supersedes_revision_id": first_revision.revision_id,
            "value": {"city": "Auckland", "country": "NZ"},
            "observed_at": "2026-02-10T09:00:00Z",
            "valid_time": {"from": "2026-02-10T00:00:00Z", "until": null},
            "evidence_episode_ids": [episode_b.episode_id],
            "write_policy": {"id": "direct-evidence", "version": "1"},
            "confidence": 0.98,
            "sensitivity": "internal",
            "retention_policy_id": "standard"
        }))
        .send()
        .await
        .context("supersede fact request failed")?;
    ensure!(
        supersede_response.status() == StatusCode::OK,
        "supersede fact returned {}, expected 200",
        supersede_response.status()
    );
    let second_etag = supersede_response
        .headers()
        .get(header::ETAG)
        .context("supersede fact omitted ETag")?
        .to_str()?
        .to_owned();
    ensure!(second_etag != first_etag, "fact ETag did not change");
    let second_view: FactView = supersede_response.json().await?;
    let second_revision = second_view
        .revision
        .as_ref()
        .context("superseded fact has no current revision")?;
    ensure!(
        second_revision.revision_number == 2,
        "successor is not revision 2"
    );
    ensure!(
        second_revision.supersedes_revision_id == Some(first_revision.revision_id),
        "successor does not name the former head"
    );
    ensure!(
        second_revision.evidence_episode_ids == vec![episode_b.episode_id],
        "successor is not attributable to its evidence"
    );
    ensure!(
        second_revision.recorded_at > first_revision.recorded_at,
        "recorded time did not advance"
    );
    ensure!(
        second_view.head_revision_id == second_revision.revision_id,
        "logical head did not advance"
    );

    let current_response = client
        .get(&fact_url)
        .bearer_auth(&target.bearer_token)
        .send()
        .await?;
    ensure!(current_response.status() == StatusCode::OK);
    ensure!(
        current_response.headers().get(header::ETAG)
            == Some(&header::HeaderValue::from_str(&second_etag)?),
        "current fact did not expose the successor ETag"
    );
    let current: FactView = current_response.json().await?;
    ensure!(
        current.head_revision_id == second_revision.revision_id,
        "current fact did not expose the successor"
    );

    Ok(TemporalFixture {
        fact_id: second_view.fact_id,
        first_revision_id: first_revision.revision_id,
        second_revision_id: second_revision.revision_id,
        first_recorded_at: first_revision.recorded_at.clone(),
        second_recorded_at: second_revision.recorded_at.clone(),
    })
}

pub async fn reconstructs_both_temporal_axes(target: &Target) -> Result<()> {
    let fixture = supersedes_the_fact_head(target).await?;
    let client = Client::new();
    let as_of_url = format!(
        "{}/v1/tenants/{}/subjects/{}/facts/{}/as-of",
        target.base_url.trim_end_matches('/'),
        target.tenant_id,
        target.subject_id,
        fixture.fact_id
    );

    let cases = [
        (
            "2026-01-15T00:00:00Z",
            fixture.second_recorded_at.as_str(),
            fixture.first_revision_id,
            "new knowledge must not rewrite earlier valid time",
        ),
        (
            "2026-03-01T00:00:00Z",
            fixture.first_recorded_at.as_str(),
            fixture.first_revision_id,
            "later evidence must not appear before it was recorded",
        ),
        (
            "2026-03-01T00:00:00Z",
            fixture.second_recorded_at.as_str(),
            fixture.second_revision_id,
            "newer evidence must appear after it was recorded",
        ),
        (
            "2026-02-10T00:00:00Z",
            fixture.second_recorded_at.as_str(),
            fixture.second_revision_id,
            "half-open valid time must include the successor boundary",
        ),
    ];
    for (valid_at, recorded_at, expected_revision_id, message) in cases {
        let response = client
            .get(&as_of_url)
            .bearer_auth(&target.bearer_token)
            .query(&[("valid_at", valid_at), ("recorded_at", recorded_at)])
            .send()
            .await
            .with_context(|| format!("as-of request failed: {message}"))?;
        ensure!(
            response.status() == StatusCode::OK,
            "as-of returned {}: {message}",
            response.status()
        );
        let view: FactView = response
            .json()
            .await
            .with_context(|| format!("as-of response was invalid: {message}"))?;
        ensure!(
            view.valid_at == valid_at,
            "valid coordinate changed: {message}"
        );
        ensure!(
            view.recorded_at == recorded_at,
            "recorded coordinate changed: {message}"
        );
        ensure!(
            view.revision
                .as_ref()
                .is_some_and(|revision| revision.revision_id == expected_revision_id),
            "wrong revision: {message}"
        );
    }

    Ok(())
}

pub async fn cross_scope_reads_fail_closed(target: &Target) -> Result<()> {
    let client = Client::new();
    let tenant_b_fact = create_private_fact(
        &client,
        target,
        PrivateFactFixture {
            bearer_token: &target.principal_b_bearer_token,
            tenant_id: target.principal_b_tenant_id,
            subject_id: target.principal_b_subject_id,
            case_id: Uuid::parse_str("019be000-0000-7000-8000-000000000101")?,
            name: "tenant-b",
            secret: "tenant-b-only",
        },
    )
    .await?;
    let facts_url = format!(
        "{}/v1/tenants/{}/subjects/{}/facts",
        target.base_url.trim_end_matches('/'),
        target.principal_b_tenant_id,
        target.principal_b_subject_id
    );
    let tenant_b_fact_url = format!("{facts_url}/{}", tenant_b_fact.fact_id);

    let authorized = client
        .get(&tenant_b_fact_url)
        .bearer_auth(&target.principal_b_bearer_token)
        .send()
        .await?;
    ensure!(
        authorized.status() == StatusCode::OK,
        "tenant B cannot read its own fact"
    );

    let hidden = client
        .get(&tenant_b_fact_url)
        .bearer_auth(&target.bearer_token)
        .send()
        .await?;
    ensure!(
        hidden.status() == StatusCode::NOT_FOUND,
        "cross-tenant fact read returned {}, expected cloaked 404",
        hidden.status()
    );
    ensure!(
        hidden
            .headers()
            .get(header::CONTENT_TYPE)
            .is_some_and(|value| value == "application/problem+json"),
        "cross-tenant rejection is not RFC 9457 problem JSON"
    );
    ensure!(
        hidden
            .headers()
            .get(header::CACHE_CONTROL)
            .is_some_and(|value| value == "private, no-store"),
        "cross-tenant problem can be cached"
    );
    let hidden_problem: Value = hidden.json().await?;
    ensure!(
        hidden_problem["type"] == "https://palimpsest.dev/problems/resource-not-found",
        "cross-tenant problem type is not stable"
    );
    ensure!(
        hidden_problem["trace_id"]
            .as_str()
            .is_some_and(|value| Uuid::parse_str(value).is_ok()),
        "cross-tenant problem has no valid trace_id"
    );
    ensure!(
        !hidden_problem.to_string().contains("tenant-b-only"),
        "cross-tenant problem disclosed private value"
    );
    ensure!(
        !hidden_problem
            .to_string()
            .contains(&tenant_b_fact.fact_id.to_string()),
        "cross-tenant problem disclosed hidden identifier"
    );

    let subject_c_fact = create_private_fact(
        &client,
        target,
        PrivateFactFixture {
            bearer_token: &target.principal_c_bearer_token,
            tenant_id: target.tenant_id,
            subject_id: target.principal_c_subject_id,
            case_id: Uuid::parse_str("019be000-0000-7000-8000-000000000201")?,
            name: "subject-c",
            secret: "subject-c-only",
        },
    )
    .await?;
    let subject_c_fact_url = format!(
        "{}/v1/tenants/{}/subjects/{}/facts/{}",
        target.base_url.trim_end_matches('/'),
        target.tenant_id,
        target.principal_c_subject_id,
        subject_c_fact.fact_id
    );
    let authorized = client
        .get(&subject_c_fact_url)
        .bearer_auth(&target.principal_c_bearer_token)
        .send()
        .await?;
    ensure!(
        authorized.status() == StatusCode::OK,
        "subject C cannot read its own fact"
    );
    let hidden = client
        .get(subject_c_fact_url)
        .bearer_auth(&target.bearer_token)
        .send()
        .await?;
    ensure!(
        hidden.status() == StatusCode::NOT_FOUND,
        "cross-subject fact read returned {}, expected cloaked 404",
        hidden.status()
    );
    let hidden_problem: Value = hidden.json().await?;
    ensure!(
        !hidden_problem.to_string().contains("subject-c-only"),
        "cross-subject problem disclosed private value"
    );
    ensure!(
        !hidden_problem
            .to_string()
            .contains(&subject_c_fact.fact_id.to_string()),
        "cross-subject problem disclosed hidden identifier"
    );

    Ok(())
}

struct PrivateFactFixture<'a> {
    bearer_token: &'a str,
    tenant_id: Uuid,
    subject_id: Uuid,
    case_id: Uuid,
    name: &'a str,
    secret: &'a str,
}

async fn create_private_fact(
    client: &Client,
    target: &Target,
    fixture: PrivateFactFixture<'_>,
) -> Result<FactView> {
    let PrivateFactFixture {
        bearer_token,
        tenant_id,
        subject_id,
        case_id,
        name: fixture_name,
        secret,
    } = fixture;
    let episode_url = format!(
        "{}/v1/tenants/{tenant_id}/subjects/{subject_id}/episodes",
        target.base_url.trim_end_matches('/')
    );
    let episode_response = client
        .post(episode_url)
        .bearer_auth(bearer_token)
        .header("Idempotency-Key", format!("{fixture_name}-episode-create"))
        .json(&json!({
            "case_id": case_id,
            "kind": "message",
            "observed_at": "2026-03-10T09:00:00Z",
            "provenance": {
                "source_type": "conformance",
                "source_uri": null,
                "external_id": format!("{fixture_name}-episode")
            },
            "sensitivity": "restricted",
            "retention_policy_id": "standard",
            "payload": {"secret": secret}
        }))
        .send()
        .await?;
    ensure!(
        episode_response.status() == StatusCode::CREATED,
        "{fixture_name} setup episode returned {}",
        episode_response.status()
    );
    let episode: Episode = episode_response.json().await?;
    let facts_url = format!(
        "{}/v1/tenants/{tenant_id}/subjects/{subject_id}/facts",
        target.base_url.trim_end_matches('/')
    );
    let fact_response = client
        .post(facts_url)
        .bearer_auth(bearer_token)
        .header("Idempotency-Key", format!("{fixture_name}-fact-create"))
        .json(&json!({
            "case_id": case_id,
            "namespace": "case.private",
            "key": "scoped_secret",
            "value": secret,
            "observed_at": "2026-03-10T09:00:00Z",
            "valid_time": {"from": "2026-03-10T00:00:00Z", "until": null},
            "evidence_episode_ids": [episode.episode_id],
            "write_policy": {"id": "direct-evidence", "version": "1"},
            "confidence": 1.0,
            "sensitivity": "restricted",
            "retention_policy_id": "standard"
        }))
        .send()
        .await?;
    ensure!(
        fact_response.status() == StatusCode::CREATED,
        "{fixture_name} setup fact returned {}",
        fact_response.status()
    );
    fact_response.json().await.map_err(Into::into)
}
