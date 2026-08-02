use anyhow::{Context, Result, bail, ensure};
use reqwest::{Client, StatusCode, header};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::time::Duration;
use uuid::Uuid;

pub mod retrieval_evaluation;

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
    pub principal_a_internal_bearer_token: String,
    pub principal_b_bearer_token: String,
    pub principal_b_tenant_id: Uuid,
    pub principal_b_subject_id: Uuid,
    pub principal_c_bearer_token: String,
    pub principal_c_subject_id: Uuid,
    pub principal_d_same_scope_bearer_token: String,
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

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct RetrievalPolicy {
    id: String,
    version: String,
    digest: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct RetrievalAuthorization {
    decision: String,
    scope_digest: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct RetrievalScore {
    component: String,
    value: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct RetrievalEmbeddingLineage {
    profile_id: String,
    profile_version: String,
    profile_digest: String,
    projection_sha256: String,
    input_sha256: String,
    vector_sha256: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct RetrievalQueryEmbeddingLineage {
    profile_id: String,
    profile_version: String,
    profile_digest: String,
    projection_profile_id: String,
    projection_profile_version: String,
    projection_profile_digest: String,
    input_sha256: String,
    vector_sha256: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct RetrievalItem {
    memory_kind: String,
    fact_id: Uuid,
    revision_id: Uuid,
    namespace: String,
    key: String,
    value: Value,
    evidence_episode_ids: Vec<Uuid>,
    scores: Vec<RetrievalScore>,
    #[serde(default)]
    embedding: Option<RetrievalEmbeddingLineage>,
}

#[derive(Debug, Deserialize, Serialize)]
struct RetrievalReceipt {
    tenant_id: Uuid,
    subject_id: Uuid,
    retrieval_id: Uuid,
    status: String,
    evaluated_at: String,
    valid_at: String,
    recorded_at: String,
    policy: RetrievalPolicy,
    authorization: RetrievalAuthorization,
    document_schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    query_embedding: Option<RetrievalQueryEmbeddingLineage>,
    items: Vec<RetrievalItem>,
    next_cursor: Option<String>,
}

fn ensure_lexical_only_scores(item: &RetrievalItem) -> Result<()> {
    ensure!(
        item.scores.iter().all(|score| matches!(
            score.component.as_str(),
            "exact_identity_rank" | "lexical_rank" | "lexical_score" | "final_rank" | "final_score"
        )),
        "lexical-only receipt exposed an undeclared fusion or bonus score"
    );
    let lexical_score = item
        .scores
        .iter()
        .find(|score| score.component == "lexical_score")
        .context("lexical-only receipt omitted lexical_score")?;
    let final_score = item
        .scores
        .iter()
        .find(|score| score.component == "final_score")
        .context("lexical-only receipt omitted final_score")?;
    ensure!(
        final_score.value == lexical_score.value,
        "lexical-only final_score contains an undeclared reciprocal-rank or identity bonus"
    );
    ensure!(
        lexical_score
            .value
            .split_once('.')
            .is_some_and(|(_, fraction)| fraction.len() == 12),
        "lexical score was not persisted at policy scale 12"
    );
    Ok(())
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

    for (idempotency_key, observed_at) in [
        (
            "checkpoint-run-301-invalid-receipt-offset",
            "2026-07-29T14:30:00+13:00",
        ),
        (
            "checkpoint-run-301-invalid-receipt-precision",
            "2026-07-29T01:30:00.1234567Z",
        ),
    ] {
        let invalid_receipt_response = client
            .put(&checkpoint_url)
            .bearer_auth(&target.bearer_token)
            .header("Idempotency-Key", idempotency_key)
            .header(header::IF_MATCH, &prepared_etag)
            .json(&json!({
                "case_id": case_id,
                "parent_revision_id": prepared.checkpoint_revision_id,
                "state": {"step": "must-not-complete"},
                "state_schema_version": 1,
                "effect_transitions": [{
                    "type": "complete",
                    "effect_id": prepared_effect.effect_id,
                    "receipt": {
                        "observed_at": observed_at,
                        "external_reference": null,
                        "outcome_sha256": "c".repeat(64)
                    }
                }],
                "provenance": body["provenance"],
                "sensitivity": "internal",
                "retention_policy_id": "checkpoint-active-30d-v1"
            }))
            .send()
            .await?;
        assert_problem(
            invalid_receipt_response,
            StatusCode::BAD_REQUEST,
            "invalid-request",
        )
        .await?;
    }

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

    let wrong_case_response = client
        .put(&checkpoint_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "checkpoint-run-301-wrong-case")
        .header(header::IF_MATCH, &completed_etag)
        .json(&json!({
            "case_id": "019be000-0000-7000-8000-000000000399",
            "parent_revision_id": completed.checkpoint_revision_id,
            "state": {"step": "must-not-change-case"},
            "state_schema_version": 1,
            "effect_transitions": [],
            "provenance": body["provenance"],
            "sensitivity": "internal",
            "retention_policy_id": "checkpoint-active-30d-v1"
        }))
        .send()
        .await?;
    assert_problem(
        wrong_case_response,
        StatusCode::CONFLICT,
        "checkpoint-case-conflict",
    )
    .await?;

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

    let missing_parent_url = format!(
        "{}/v1/tenants/{}/subjects/{}/agents/019be000-0000-7000-8000-000000000352/threads/019be000-0000-7000-8000-000000000353/checkpoint",
        target.base_url.trim_end_matches('/'),
        target.tenant_id,
        target.subject_id
    );
    let missing_parent_response = client
        .put(missing_parent_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "checkpoint-run-301-missing-parent")
        .header(header::IF_NONE_MATCH, "*")
        .json(&json!({
            "case_id": case_id,
            "state": {"step": "must-not-persist"},
            "state_schema_version": 1,
            "effect_transitions": [],
            "provenance": body["provenance"],
            "sensitivity": "internal",
            "retention_policy_id": "checkpoint-active-30d-v1"
        }))
        .send()
        .await?;
    assert_problem(
        missing_parent_response,
        StatusCode::BAD_REQUEST,
        "invalid-request",
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
    let create_body = json!({
        "case_id": case_id,
        "parent_revision_id": null,
        "state": {"step": "long-lived-root"},
        "state_schema_version": 1,
        "effect_transitions": [],
        "provenance": {"source_type": "conformance", "external_id": "checkpoint-run-311"},
        "sensitivity": "internal",
        "retention_policy_id": "checkpoint-active-30d-v1"
    });
    let response = client
        .put(&checkpoint_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "checkpoint-run-311-create")
        .header(header::IF_NONE_MATCH, "*")
        .json(&create_body)
        .send()
        .await?;
    ensure!(
        response.status() == StatusCode::CREATED,
        "retention fixture root returned {}",
        response.status()
    );
    let root_etag = response
        .headers()
        .get(header::ETAG)
        .context("retention fixture root omitted ETag")?
        .to_str()?
        .to_owned();
    let root: Checkpoint = response.json().await?;
    ensure!(root.retention_policy_id == "checkpoint-active-30d-v1");

    let short_lived_body = json!({
        "case_id": case_id,
        "parent_revision_id": root.checkpoint_revision_id,
        "state": {"step": "short-lived-head"},
        "state_schema_version": 1,
        "effect_transitions": [],
        "provenance": {"source_type": "conformance", "external_id": "checkpoint-run-311"},
        "sensitivity": "internal",
        "retention_policy_id": "checkpoint-test-1s-v1"
    });
    let short_lived_response = client
        .put(&checkpoint_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "checkpoint-run-311-shorten")
        .header(header::IF_MATCH, root_etag)
        .json(&short_lived_body)
        .send()
        .await?;
    ensure!(short_lived_response.status() == StatusCode::OK);
    let short_lived: Checkpoint = short_lived_response.json().await?;
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

    let expired_replay_response = client
        .put(&checkpoint_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "checkpoint-run-311-create")
        .header(header::IF_NONE_MATCH, "*")
        .json(&create_body)
        .send()
        .await?;
    assert_problem(
        expired_replay_response,
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

pub async fn checkpoint_scopes_fail_closed(target: &Target) -> Result<()> {
    let client = Client::new();
    let agent_id = Uuid::parse_str("019be000-0000-7000-8000-000000000302")?;
    let thread_id = Uuid::parse_str("019be000-0000-7000-8000-000000000303")?;
    let fixtures = [
        (
            &target.principal_b_bearer_token,
            target.principal_b_tenant_id,
            target.principal_b_subject_id,
            Uuid::parse_str("019be000-0000-7000-8000-000000000371")?,
            "checkpoint-tenant-b-only",
            "checkpoint-scope-tenant-b-create",
        ),
        (
            &target.principal_c_bearer_token,
            target.tenant_id,
            target.principal_c_subject_id,
            Uuid::parse_str("019be000-0000-7000-8000-000000000372")?,
            "checkpoint-subject-c-only",
            "checkpoint-scope-subject-c-create",
        ),
    ];

    for (owner_token, tenant_id, subject_id, case_id, private_marker, key) in fixtures {
        let checkpoint_url = format!(
            "{}/v1/tenants/{tenant_id}/subjects/{subject_id}/agents/{agent_id}/threads/{thread_id}/checkpoint",
            target.base_url.trim_end_matches('/')
        );
        let create_response = client
            .put(&checkpoint_url)
            .bearer_auth(owner_token)
            .header("Idempotency-Key", key)
            .header(header::IF_NONE_MATCH, "*")
            .json(&json!({
                "case_id": case_id,
                "parent_revision_id": null,
                "state": {"private_marker": private_marker},
                "state_schema_version": 1,
                "effect_transitions": [],
                "provenance": {"source_type": "conformance.scope-isolation"},
                "sensitivity": "restricted",
                "retention_policy_id": "checkpoint-active-30d-v1"
            }))
            .send()
            .await?;
        ensure!(create_response.status() == StatusCode::CREATED);

        let owner_read = client
            .get(&checkpoint_url)
            .bearer_auth(owner_token)
            .send()
            .await?;
        ensure!(owner_read.status() == StatusCode::OK);
        let owner_checkpoint: Checkpoint = owner_read.json().await?;
        ensure!(owner_checkpoint.state["private_marker"] == private_marker);

        let hidden_response = client
            .get(checkpoint_url)
            .bearer_auth(&target.bearer_token)
            .send()
            .await?;
        ensure!(hidden_response.status() == StatusCode::NOT_FOUND);
        let hidden_problem: Value = hidden_response.json().await?;
        ensure!(hidden_problem["type"] == "https://palimpsest.dev/problems/resource-not-found");
        ensure!(!hidden_problem.to_string().contains(private_marker));
        ensure!(
            !hidden_problem
                .to_string()
                .contains(&owner_checkpoint.checkpoint_id.to_string())
        );
    }
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
    assert_problem(response, StatusCode::CONFLICT, "idempotency-key-reused").await
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

pub async fn creates_and_replays_a_lexical_retrieval_receipt(target: &Target) -> Result<()> {
    let client = Client::new();
    let retrievals_url = format!(
        "{}/v1/tenants/{}/subjects/{}/retrievals",
        target.base_url.trim_end_matches('/'),
        target.tenant_id,
        target.subject_id
    );
    let body = json!({
        "query": "Wellington",
        "perspective": {"kind": "current"},
        "page_size": 10,
        "filters": {"namespaces": ["case.profile"]}
    });

    let create_response = client
        .post(&retrievals_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "retrieval-shipping-address")
        .json(&body)
        .send()
        .await
        .context("create retrieval receipt request failed")?;
    ensure!(
        create_response.status() == StatusCode::CREATED,
        "create retrieval receipt returned {}, expected 201",
        create_response.status()
    );
    ensure!(
        create_response
            .headers()
            .get(header::CACHE_CONTROL)
            .is_some_and(|value| value == "private, no-store"),
        "retrieval receipt can be cached"
    );
    let location = create_response
        .headers()
        .get(header::LOCATION)
        .context("retrieval receipt omitted Location")?
        .to_str()?
        .to_owned();
    let created: RetrievalReceipt = create_response
        .json()
        .await
        .context("create retrieval response was not a receipt")?;

    ensure!(created.tenant_id == target.tenant_id);
    ensure!(created.subject_id == target.subject_id);
    ensure!(created.retrieval_id.get_version_num() == 7);
    ensure!(created.status == "results");
    ensure!(created.valid_at == created.evaluated_at);
    ensure!(created.recorded_at == created.evaluated_at);
    ensure!(created.policy.id == "retrieval-lexical-v1");
    ensure!(created.policy.version == "1");
    ensure!(created.policy.digest.len() == 64);
    ensure!(created.authorization.decision == "authorized");
    ensure!(created.authorization.scope_digest.len() == 64);
    ensure!(created.document_schema_version == 1);
    ensure!(created.next_cursor.is_none());
    ensure!(created.items.len() == 1);
    let item = &created.items[0];
    ensure!(item.memory_kind == "fact_revision");
    ensure!(item.namespace == "case.profile");
    ensure!(item.key == "shipping_address");
    ensure!(item.value == json!({"city": "Wellington", "country": "NZ"}));
    ensure!(!item.evidence_episode_ids.is_empty());
    ensure!(
        item.scores.iter().any(|score| {
            score.component == "lexical_rank" && score.value.parse::<u32>() == Ok(1)
        })
    );
    ensure!(item.scores.iter().all(|score| !score.value.is_empty()));
    ensure_lexical_only_scores(item)?;

    let replay_response = client
        .post(&retrievals_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "retrieval-shipping-address")
        .json(&body)
        .send()
        .await?;
    ensure!(replay_response.status() == StatusCode::CREATED);
    ensure!(
        replay_response
            .headers()
            .get("Idempotency-Replayed")
            .is_some_and(|value| value == "true"),
        "retrieval replay was not identified"
    );
    let replayed: RetrievalReceipt = replay_response.json().await?;
    ensure!(
        serde_json::to_value(&replayed)? == serde_json::to_value(&created)?,
        "retrieval replay changed the durable receipt"
    );

    let receipt_url = if location.starts_with("http://") || location.starts_with("https://") {
        location
    } else {
        format!("{}{}", target.base_url.trim_end_matches('/'), location)
    };
    let get_response = client
        .get(receipt_url)
        .bearer_auth(&target.bearer_token)
        .send()
        .await?;
    ensure!(get_response.status() == StatusCode::OK);
    ensure!(
        get_response
            .headers()
            .get(header::CACHE_CONTROL)
            .is_some_and(|value| value == "private, no-store")
    );
    let fetched: RetrievalReceipt = get_response.json().await?;
    ensure!(
        serde_json::to_value(fetched)? == serde_json::to_value(created)?,
        "retrieval GET changed the durable receipt"
    );

    Ok(())
}

#[derive(Clone, Debug)]
pub struct HybridFusionFixture {
    pub exact_revision_id: Uuid,
    pub alpha_revision_id: Uuid,
    pub beta_revision_id: Uuid,
    pub gamma_revision_id: Uuid,
    pub delta_revision_id: Uuid,
    pub forbidden_revision_id: Uuid,
}

#[derive(Clone, Debug)]
pub struct HybridReplayFixture {
    pub request_body: Value,
    pub receipt: Value,
}

#[derive(Clone, Debug)]
pub struct TemporalRetrievalFixture {
    pub exact_revision_id: Uuid,
    pub alpha_root_revision_id: Uuid,
    pub alpha_successor_revision_id: Uuid,
    pub beta_revision_id: Uuid,
    pub gamma_revision_id: Uuid,
    pub delta_revision_id: Uuid,
    pub alpha_root_recorded_at: String,
    pub alpha_successor_recorded_at: String,
}

#[derive(Debug)]
pub struct TemporalReplayFixture {
    pub first_retrieval_id: Uuid,
    pub second_retrieval_id: Uuid,
    pub independent_retrieval_ids: Vec<Uuid>,
    pub paginated_retrieval_id: Uuid,
    request_body: Value,
    first_receipt: RetrievalReceipt,
}

#[derive(Clone, Debug)]
pub struct TemporalRuntimeReplayFixture {
    pub retrieval_id: Uuid,
    request_body: Value,
    receipt: Value,
}

#[derive(Clone, Debug)]
pub struct TemporalLifecycleFixture {
    pub deleted_case_id: Uuid,
    pub deleted_root_revision_id: Uuid,
    pub deleted_successor_revision_id: Uuid,
    pub expired_case_id: Uuid,
    pub expired_root_revision_id: Uuid,
    pub expired_successor_revision_id: Uuid,
}

#[derive(Debug)]
pub struct TemporalLifecycleReplayFixture {
    receipts: Vec<TemporalLifecycleReceiptFixture>,
}

#[derive(Debug)]
struct TemporalLifecycleReceiptFixture {
    name: &'static str,
    retrieval_id: Uuid,
    idempotency_key: String,
    request_body: Value,
    root_revision_id: Uuid,
    successor_revision_id: Uuid,
    private_marker: &'static str,
}

pub async fn hybrid_retrieval_requires_an_available_provider(target: &Target) -> Result<()> {
    let response = Client::new()
        .post(retrievals_url(target))
        .bearer_auth(&target.principal_a_internal_bearer_token)
        .header("Idempotency-Key", "hybrid-provider-unavailable-default")
        .json(&hybrid_request_body())
        .send()
        .await?;
    assert_retryable_hybrid_failure(
        response,
        &["fixture-provider-outage", "[1,0,0,0]", "fusiontoken"],
    )
    .await
}

pub async fn hybrid_retrieval_rejects_caller_ranking_internals(target: &Target) -> Result<()> {
    for (name, field) in [
        ("vector", json!([1, 0, 0, 0])),
        ("model", json!("caller-selected-model")),
        ("weights", json!({"vector": 999})),
        ("candidate_limit", json!(999)),
        ("recency_profile", json!("active-case-30d-v1")),
        ("recency_anchor_at", json!("2026-06-30T00:00:00Z")),
        ("importance", json!(1)),
    ] {
        let mut body = hybrid_request_body();
        body.as_object_mut()
            .context("hybrid fixture request was not an object")?
            .insert(name.to_owned(), field);
        let response = Client::new()
            .post(retrievals_url(target))
            .bearer_auth(&target.principal_a_internal_bearer_token)
            .header("Idempotency-Key", format!("hybrid-reject-caller-{name}"))
            .json(&body)
            .send()
            .await?;
        ensure!(
            response.status() == StatusCode::BAD_REQUEST,
            "caller-controlled {name} returned {}, expected 400",
            response.status()
        );
        let problem: Value = response.json().await?;
        let stable_problem_fields = json!({
            "type": problem.get("type"),
            "title": problem.get("title"),
            "status": problem.get("status"),
            "detail": problem.get("detail"),
        })
        .to_string();
        for private_text in ["[1,0,0,0]", "caller-selected-model", "fusiontoken", "999"] {
            ensure!(
                !stable_problem_fields.contains(private_text),
                "rejected hybrid request echoed caller-controlled ranking material"
            );
        }
    }
    Ok(())
}

pub async fn rejects_unregistered_write_policies(target: &Target) -> Result<()> {
    let client = Client::new();
    let episode_url = format!(
        "{}/v1/tenants/{}/subjects/{}/episodes",
        target.base_url.trim_end_matches('/'),
        target.tenant_id,
        target.subject_id
    );
    let facts_url = format!(
        "{}/v1/tenants/{}/subjects/{}/facts",
        target.base_url.trim_end_matches('/'),
        target.tenant_id,
        target.subject_id
    );
    let unknown_case_id = Uuid::parse_str("019be000-0000-7000-8000-000000000590")?;
    let unknown_episode_response = client
        .post(&episode_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "unknown-write-policy-create-episode")
        .json(&json!({
            "case_id": unknown_case_id,
            "kind": "retrieval-fixture",
            "observed_at": "2026-06-01T00:00:00Z",
            "provenance": {
                "source_type": "conformance",
                "source_uri": null,
                "external_id": "unknown-write-policy-create"
            },
            "sensitivity": "internal",
            "retention_policy_id": "standard",
            "payload": {"purpose": "unknown-write-policy-create"}
        }))
        .send()
        .await?;
    ensure!(unknown_episode_response.status() == StatusCode::CREATED);
    let unknown_episode: Episode = unknown_episode_response.json().await?;
    let unknown_create = client
        .post(&facts_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "unknown-write-policy-create-fact")
        .json(&json!({
            "case_id": unknown_case_id,
            "namespace": "case.policy",
            "key": "unknown-create",
            "value": {"state": "should-not-persist"},
            "observed_at": "2026-06-01T00:00:00Z",
            "valid_time": {"from": "2026-06-01T00:00:00Z", "until": null},
            "evidence_episode_ids": [unknown_episode.episode_id],
            "write_policy": {"id": "unregistered-future-policy", "version": "1"},
            "confidence": 1.0,
            "sensitivity": "internal",
            "retention_policy_id": "standard"
        }))
        .send()
        .await?;
    assert_write_policy_rejected(unknown_create).await?;

    let known = create_marker_fact(
        &client,
        target,
        MarkerFactFixture {
            bearer_token: &target.bearer_token,
            tenant_id: target.tenant_id,
            subject_id: target.subject_id,
            case_id: Uuid::parse_str("019be000-0000-7000-8000-000000000591")?,
            name: "unknown-policy-supersede",
            marker: "policy-registration-evidence",
            secret: "known-head",
            sensitivity: "internal",
            retention_policy_id: "standard",
        },
    )
    .await?;
    let known_revision = known
        .revision
        .as_ref()
        .context("known-policy fixture omitted its revision")?;
    let successor_episode_response = client
        .post(&episode_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "unknown-write-policy-supersede-episode")
        .json(&json!({
            "case_id": known.case_id,
            "kind": "retrieval-fixture",
            "observed_at": "2026-06-02T00:00:00Z",
            "provenance": {
                "source_type": "conformance",
                "source_uri": null,
                "external_id": "unknown-write-policy-supersede"
            },
            "sensitivity": "internal",
            "retention_policy_id": "standard",
            "payload": {"purpose": "unknown-write-policy-supersede"}
        }))
        .send()
        .await?;
    ensure!(successor_episode_response.status() == StatusCode::CREATED);
    let successor_episode: Episode = successor_episode_response.json().await?;
    let unknown_supersede = client
        .put(format!("{facts_url}/{}", known.fact_id))
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "unknown-write-policy-supersede-fact")
        .header(header::IF_MATCH, format!("\"{}\"", known.head_revision_id))
        .json(&json!({
            "supersedes_revision_id": known_revision.revision_id,
            "value": {"state": "should-not-supersede"},
            "observed_at": "2026-06-02T00:00:00Z",
            "valid_time": {"from": "2026-06-02T00:00:00Z", "until": null},
            "evidence_episode_ids": [successor_episode.episode_id],
            "write_policy": {"id": "unregistered-future-policy", "version": "1"},
            "confidence": 1.0,
            "sensitivity": "internal",
            "retention_policy_id": "standard"
        }))
        .send()
        .await?;
    assert_write_policy_rejected(unknown_supersede).await
}

async fn assert_write_policy_rejected(response: reqwest::Response) -> Result<()> {
    ensure!(
        response.status() == StatusCode::UNPROCESSABLE_ENTITY,
        "unknown write policy returned {}, expected 422",
        response.status()
    );
    let problem: Value = response.json().await?;
    ensure!(problem["code"] == "write_policy_rejected");
    ensure!(problem["status"] == 422);
    Ok(())
}

pub async fn creates_hybrid_fusion_fixture(target: &Target) -> Result<HybridFusionFixture> {
    let client = Client::new();
    let exact = create_hybrid_fact(
        &client,
        target,
        HybridFactFixture {
            name: "hybrid-exact",
            case_id: Uuid::parse_str("019be000-0000-7000-8000-000000000401")?,
            namespace: "case.allowed",
            key: "case.retrieval:fusiontoken",
            marker: "fusiontoken",
            vector_fixture: "vector_fixture_exact_4d",
            sensitivity: "internal",
        },
    )
    .await?;
    let alpha = create_hybrid_fact(
        &client,
        target,
        HybridFactFixture {
            name: "hybrid-alpha",
            case_id: Uuid::parse_str("019be000-0000-7000-8000-000000000402")?,
            namespace: "case.retrieval",
            key: "alpha",
            marker: "case.retrieval:fusiontoken case.retrieval:fusiontoken case.retrieval:fusiontoken",
            vector_fixture: "vector_fixture_alpha_4d",
            sensitivity: "internal",
        },
    )
    .await?;
    let beta = create_hybrid_fact(
        &client,
        target,
        HybridFactFixture {
            name: "hybrid-beta",
            case_id: Uuid::parse_str("019be000-0000-7000-8000-000000000403")?,
            namespace: "case.retrieval",
            key: "beta",
            marker: "case.retrieval:fusiontoken case.retrieval:fusiontoken",
            vector_fixture: "vector_fixture_beta_4d",
            sensitivity: "internal",
        },
    )
    .await?;
    let gamma = create_hybrid_fact(
        &client,
        target,
        HybridFactFixture {
            name: "hybrid-gamma",
            case_id: Uuid::parse_str("019be000-0000-7000-8000-000000000404")?,
            namespace: "case.retrieval",
            key: "gamma",
            marker: "case.retrieval:fusiontoken",
            vector_fixture: "vector_fixture_gamma_4d",
            sensitivity: "internal",
        },
    )
    .await?;
    let delta = create_hybrid_fact(
        &client,
        target,
        HybridFactFixture {
            name: "hybrid-delta",
            case_id: Uuid::parse_str("019be000-0000-7000-8000-000000000405")?,
            namespace: "case.vector",
            key: "delta",
            marker: "vector-only candidate",
            vector_fixture: "vector_fixture_delta_4d",
            sensitivity: "internal",
        },
    )
    .await?;
    let forbidden = create_hybrid_fact(
        &client,
        target,
        HybridFactFixture {
            name: "hybrid-forbidden-trap",
            case_id: Uuid::parse_str("019be000-0000-7000-8000-000000000406")?,
            namespace: "case.retrieval",
            key: "fusiontoken",
            marker: "case.retrieval:fusiontoken case.retrieval:fusiontoken case.retrieval:fusiontoken case.retrieval:fusiontoken",
            vector_fixture: "vector_fixture_forbidden_4d",
            sensitivity: "restricted",
        },
    )
    .await?;

    Ok(HybridFusionFixture {
        exact_revision_id: fact_revision_id(&exact)?,
        alpha_revision_id: fact_revision_id(&alpha)?,
        beta_revision_id: fact_revision_id(&beta)?,
        gamma_revision_id: fact_revision_id(&gamma)?,
        delta_revision_id: fact_revision_id(&delta)?,
        forbidden_revision_id: fact_revision_id(&forbidden)?,
    })
}

pub async fn creates_temporal_retrieval_fixture(
    target: &Target,
) -> Result<TemporalRetrievalFixture> {
    let client = Client::new();
    let exact = create_temporal_fact(
        &client,
        target,
        TemporalFactFixture {
            name: "temporal-exact",
            case_id: Uuid::parse_str("019be000-0000-7000-8000-000000000501")?,
            namespace: "case.allowed",
            key: "case.temporal:chronotoken",
            marker: "chronotoken",
            vector_fixture: "temporal_vector_fixture_exact_4d",
            observed_at: "2026-04-01T00:00:00Z",
            valid_from: "2026-04-01T00:00:00Z",
            write_policy_id: "temporal-important-active-case-evidence",
            confidence: 1.0,
        },
    )
    .await?;
    let beta = create_temporal_fact(
        &client,
        target,
        TemporalFactFixture {
            name: "temporal-beta-stale",
            case_id: Uuid::parse_str("019be000-0000-7000-8000-000000000503")?,
            namespace: "case.temporal",
            key: "beta",
            marker: "case.temporal:chronotoken case.temporal:chronotoken",
            vector_fixture: "temporal_vector_fixture_beta_4d",
            observed_at: "2026-04-01T00:00:00Z",
            valid_from: "2026-04-01T00:00:00Z",
            write_policy_id: "temporal-active-case-evidence",
            confidence: 1.0,
        },
    )
    .await?;
    let gamma = create_temporal_fact(
        &client,
        target,
        TemporalFactFixture {
            name: "temporal-gamma-recent",
            case_id: Uuid::parse_str("019be000-0000-7000-8000-000000000504")?,
            namespace: "case.temporal",
            key: "gamma",
            marker: "case.temporal:chronotoken",
            vector_fixture: "temporal_vector_fixture_gamma_4d",
            observed_at: "2026-05-31T00:00:00Z",
            valid_from: "2026-05-31T00:00:00Z",
            write_policy_id: "temporal-active-case-evidence",
            confidence: 1.0,
        },
    )
    .await?;
    let delta = create_temporal_fact(
        &client,
        target,
        TemporalFactFixture {
            name: "temporal-delta-future",
            case_id: Uuid::parse_str("019be000-0000-7000-8000-000000000505")?,
            namespace: "case.temporal",
            key: "delta",
            marker: "future vector-only candidate",
            vector_fixture: "temporal_vector_fixture_delta_4d",
            observed_at: "2099-01-01T00:00:00Z",
            valid_from: "2099-01-01T00:00:00Z",
            write_policy_id: "temporal-stable-evidence",
            confidence: 1.0,
        },
    )
    .await?;
    // Create alpha last so its first recorded-time coordinate includes every
    // independent fact while still excluding the later alpha successor.
    let alpha_root = create_temporal_fact(
        &client,
        target,
        TemporalFactFixture {
            name: "temporal-alpha-root",
            case_id: Uuid::parse_str("019be000-0000-7000-8000-000000000502")?,
            namespace: "case.temporal",
            key: "alpha",
            marker: "case.temporal:chronotoken case.temporal:chronotoken case.temporal:chronotoken",
            vector_fixture: "temporal_vector_fixture_alpha_4d",
            observed_at: "2026-04-01T00:00:00Z",
            valid_from: "2026-04-01T00:00:00Z",
            write_policy_id: "temporal-stable-evidence",
            confidence: 0.8,
        },
    )
    .await?;
    let alpha_successor = supersede_temporal_fact(
        &client,
        target,
        &alpha_root,
        TemporalSuccessorFixture {
            name: "temporal-alpha-successor",
            marker: "case.temporal:chronotoken case.temporal:chronotoken case.temporal:chronotoken",
            vector_fixture: "temporal_vector_fixture_alpha_4d",
            observed_at: "2026-03-01T00:00:00Z",
            valid_from: "2026-03-01T00:00:00Z",
            write_policy_id: "temporal-stable-evidence",
            confidence: 0.8,
            retention_policy_id: "standard",
        },
    )
    .await?;
    let alpha_root_revision = alpha_root
        .revision
        .as_ref()
        .context("temporal alpha root has no revision")?;
    let alpha_successor_revision = alpha_successor
        .revision
        .as_ref()
        .context("temporal alpha successor has no revision")?;

    Ok(TemporalRetrievalFixture {
        exact_revision_id: fact_revision_id(&exact)?,
        alpha_root_revision_id: alpha_root_revision.revision_id,
        alpha_successor_revision_id: alpha_successor_revision.revision_id,
        beta_revision_id: fact_revision_id(&beta)?,
        gamma_revision_id: fact_revision_id(&gamma)?,
        // A future-valid fact has no effective revision in the create response,
        // but its immutable head still identifies the inserted revision.
        delta_revision_id: delta.head_revision_id,
        alpha_root_recorded_at: alpha_root_revision.recorded_at.clone(),
        alpha_successor_recorded_at: alpha_successor_revision.recorded_at.clone(),
    })
}

pub async fn retrieves_with_the_fixed_temporal_policy(
    target: &Target,
    fixture: &TemporalRetrievalFixture,
) -> Result<TemporalReplayFixture> {
    let root_request = temporal_fixed_request_body(&fixture.alpha_root_recorded_at);
    let root_receipt = create_temporal_receipt(
        target,
        "temporal-policy-before-late-evidence",
        &root_request,
    )
    .await?;
    assert_temporal_receipt(&root_receipt, fixture, fixture.alpha_root_revision_id)?;

    let successor_request = temporal_fixed_request_body(&fixture.alpha_successor_recorded_at);
    let successor_receipt = create_temporal_receipt(
        target,
        "temporal-policy-after-late-evidence",
        &successor_request,
    )
    .await?;
    assert_temporal_receipt(
        &successor_receipt,
        fixture,
        fixture.alpha_successor_revision_id,
    )?;

    for key in ["case.temporal:chronotoken", "beta", "gamma"] {
        let before = root_receipt
            .items
            .iter()
            .find(|item| item.key == key)
            .with_context(|| format!("recorded-time root receipt omitted {key}"))?;
        let after = successor_receipt
            .items
            .iter()
            .find(|item| item.key == key)
            .with_context(|| format!("recorded-time successor receipt omitted {key}"))?;
        ensure!(
            before.scores == after.scores,
            "changing recorded time changed the valid-time score for {key}"
        );
    }

    let mut independent_retrieval_ids = vec![successor_receipt.retrieval_id];
    for repeat in 1..10 {
        let independent_receipt = create_temporal_receipt(
            target,
            &format!("temporal-policy-after-late-evidence-repeat-{repeat}"),
            &successor_request,
        )
        .await?;
        assert_temporal_receipt(
            &independent_receipt,
            fixture,
            fixture.alpha_successor_revision_id,
        )?;
        ensure!(
            !independent_retrieval_ids.contains(&independent_receipt.retrieval_id),
            "independent temporal requests reused one retrieval ID"
        );
        ensure!(
            successor_receipt.policy == independent_receipt.policy,
            "independent temporal requests changed the pinned policy"
        );
        ensure!(
            successor_receipt.items == independent_receipt.items,
            "independent temporal requests changed ordered IDs or score explanations"
        );
        independent_retrieval_ids.push(independent_receipt.retrieval_id);
    }

    let get_response = Client::new()
        .get(format!(
            "{}/{}",
            retrievals_url(target),
            successor_receipt.retrieval_id
        ))
        .bearer_auth(&target.principal_a_internal_bearer_token)
        .send()
        .await?;
    ensure!(get_response.status() == StatusCode::OK);
    let got: RetrievalReceipt = get_response.json().await?;
    ensure!(
        serde_json::to_value(&got)? == serde_json::to_value(&successor_receipt)?,
        "receipt GET changed the durable temporal representation"
    );

    let mut paginated_request = successor_request.clone();
    paginated_request["page_size"] = json!(2);
    let first_page = create_temporal_receipt(
        target,
        "temporal-policy-after-late-evidence-paginated",
        &paginated_request,
    )
    .await?;
    ensure!(first_page.items.len() == 2);
    let cursor = first_page
        .next_cursor
        .as_deref()
        .context("temporal first page omitted its durable cursor")?;
    let next_page_response = Client::new()
        .get(format!(
            "{}/{}?cursor={cursor}",
            retrievals_url(target),
            first_page.retrieval_id
        ))
        .bearer_auth(&target.principal_a_internal_bearer_token)
        .send()
        .await?;
    ensure!(next_page_response.status() == StatusCode::OK);
    let next_page: RetrievalReceipt = next_page_response.json().await?;
    ensure!(next_page.items.len() == 2);
    ensure!(next_page.next_cursor.is_none());
    let paginated_items = first_page
        .items
        .iter()
        .chain(&next_page.items)
        .cloned()
        .collect::<Vec<_>>();
    ensure!(
        paginated_items == successor_receipt.items,
        "temporal pagination changed order, scores, or item identity"
    );

    let empty = create_temporal_receipt(
        target,
        "temporal-policy-fixed-abstention",
        &temporal_request_body(
            json!({
                "kind": "as_of",
                "valid_at": "2026-06-30T00:00:00Z",
                "recorded_at": fixture.alpha_successor_recorded_at
            }),
            json!(["019be000-0000-7000-8000-000000000599"]),
        ),
    )
    .await?;
    ensure!(empty.status == "abstained");
    ensure!(empty.items.is_empty());
    ensure!(empty.policy == successor_receipt.policy);
    ensure!(empty.valid_at == "2026-06-30T00:00:00Z");
    ensure!(empty.recorded_at == fixture.alpha_successor_recorded_at);

    let current = create_temporal_receipt(
        target,
        "temporal-policy-current-effective",
        &temporal_request_body(json!({"kind": "current"}), temporal_fixture_case_ids()),
    )
    .await?;
    ensure!(current.status == "results");
    ensure!(current.policy == successor_receipt.policy);
    ensure!(
        current
            .items
            .iter()
            .any(|item| item.revision_id == fixture.alpha_successor_revision_id),
        "current temporal retrieval omitted the late alpha successor"
    );
    ensure!(
        current.items.iter().all(|item| {
            item.revision_id != fixture.alpha_root_revision_id
                && item.revision_id != fixture.delta_revision_id
        }),
        "current temporal retrieval returned an ineffective root or future-valid revision"
    );

    Ok(TemporalReplayFixture {
        first_retrieval_id: successor_receipt.retrieval_id,
        second_retrieval_id: independent_retrieval_ids[1],
        independent_retrieval_ids,
        paginated_retrieval_id: first_page.retrieval_id,
        request_body: successor_request,
        first_receipt: successor_receipt,
    })
}

pub async fn creates_temporal_receipt_through_nonbypass_runtime(
    target: &Target,
    fixture: &TemporalRetrievalFixture,
    reference: &TemporalReplayFixture,
    isolation: &RetrievalIsolationFixture,
) -> Result<TemporalRuntimeReplayFixture> {
    let client = Client::new();
    let retrievals = retrievals_url(target);
    let response = client
        .post(&retrievals)
        .bearer_auth(&target.principal_a_internal_bearer_token)
        .header("Idempotency-Key", "temporal-policy-nonbypass-runtime")
        .json(&reference.request_body)
        .send()
        .await?;
    ensure!(
        response.status() == StatusCode::CREATED,
        "non-bypass temporal retrieval returned {}",
        response.status()
    );
    let receipt: RetrievalReceipt = response.json().await?;
    assert_temporal_receipt(&receipt, fixture, fixture.alpha_successor_revision_id)?;
    ensure!(
        receipt.policy == reference.first_receipt.policy
            && receipt.items == reference.first_receipt.items,
        "non-bypass temporal execution changed the policy, order, or score explanation"
    );

    let receipt_value = serde_json::to_value(&receipt)?;
    let get_response = client
        .get(format!("{retrievals}/{}", receipt.retrieval_id))
        .bearer_auth(&target.principal_a_internal_bearer_token)
        .send()
        .await?;
    ensure!(get_response.status() == StatusCode::OK);
    ensure!(
        get_response.json::<Value>().await? == receipt_value,
        "non-bypass receipt GET changed the durable representation"
    );

    let same_scope_other_principal = client
        .get(format!("{retrievals}/{}", receipt.retrieval_id))
        .bearer_auth(&target.principal_d_same_scope_bearer_token)
        .send()
        .await?;
    ensure!(
        same_scope_other_principal.status() == StatusCode::NOT_FOUND,
        "a same-scope principal read another principal's non-bypass receipt"
    );

    let lexical_response = client
        .post(&retrievals)
        .bearer_auth(&target.principal_a_internal_bearer_token)
        .header("Idempotency-Key", "retrieval-isolation-nonbypass-runtime")
        .json(&json!({
            "query": "cobalt-otter-731",
            "perspective": {"kind": "current"},
            "page_size": 50
        }))
        .send()
        .await?;
    ensure!(lexical_response.status() == StatusCode::CREATED);
    let lexical_receipt: RetrievalReceipt = lexical_response.json().await?;
    ensure!(lexical_receipt.items.len() == 1);
    ensure!(lexical_receipt.items[0].revision_id == isolation.allowed_revision_id);
    ensure!(
        isolation
            .forbidden_revision_ids
            .iter()
            .all(|revision_id| lexical_receipt
                .items
                .iter()
                .all(|item| item.revision_id != *revision_id)),
        "non-bypass candidate generation admitted a forbidden revision"
    );
    let lexical_json = serde_json::to_string(&lexical_receipt)?;
    for hidden in [
        "restricted-hidden-value",
        "cross-subject-hidden-value",
        "cross-tenant-hidden-value",
    ] {
        ensure!(
            !lexical_json.contains(hidden),
            "non-bypass candidate response disclosed {hidden}"
        );
    }

    let mut paginated_request = reference.request_body.clone();
    paginated_request["page_size"] = json!(2);
    let first_page_response = client
        .post(&retrievals)
        .bearer_auth(&target.principal_a_internal_bearer_token)
        .header(
            "Idempotency-Key",
            "temporal-policy-nonbypass-runtime-paginated",
        )
        .json(&paginated_request)
        .send()
        .await?;
    ensure!(first_page_response.status() == StatusCode::CREATED);
    let first_page: RetrievalReceipt = first_page_response.json().await?;
    ensure!(first_page.items.len() == 2);
    let cursor = first_page
        .next_cursor
        .as_deref()
        .context("non-bypass temporal receipt omitted its cursor")?;
    let next_page_response = client
        .get(format!(
            "{retrievals}/{}?cursor={cursor}",
            first_page.retrieval_id
        ))
        .bearer_auth(&target.principal_a_internal_bearer_token)
        .send()
        .await?;
    ensure!(next_page_response.status() == StatusCode::OK);
    let next_page: RetrievalReceipt = next_page_response.json().await?;
    ensure!(next_page.items.len() == 2 && next_page.next_cursor.is_none());
    ensure!(
        first_page
            .items
            .iter()
            .chain(&next_page.items)
            .eq(receipt.items.iter()),
        "non-bypass temporal pagination changed durable order or scores"
    );

    Ok(TemporalRuntimeReplayFixture {
        retrieval_id: receipt.retrieval_id,
        request_body: reference.request_body.clone(),
        receipt: receipt_value,
    })
}

pub async fn replays_temporal_receipt_through_nonbypass_runtime(
    target: &Target,
    fixture: &TemporalRuntimeReplayFixture,
) -> Result<()> {
    let response = Client::new()
        .post(retrievals_url(target))
        .bearer_auth(&target.principal_a_internal_bearer_token)
        .header("Idempotency-Key", "temporal-policy-nonbypass-runtime")
        .json(&fixture.request_body)
        .send()
        .await?;
    ensure!(response.status() == StatusCode::CREATED);
    ensure!(
        response
            .headers()
            .get("Idempotency-Replayed")
            .is_some_and(|value| value == "true"),
        "non-bypass temporal replay was not identified"
    );
    ensure!(
        response.json::<Value>().await? == fixture.receipt,
        "non-bypass temporal replay changed the durable representation"
    );
    Ok(())
}

async fn create_temporal_receipt(
    target: &Target,
    idempotency_key: &str,
    request_body: &Value,
) -> Result<RetrievalReceipt> {
    let response = Client::new()
        .post(retrievals_url(target))
        .bearer_auth(&target.principal_a_internal_bearer_token)
        .header("Idempotency-Key", idempotency_key)
        .json(request_body)
        .send()
        .await?;
    if response.status() != StatusCode::CREATED {
        let status = response.status();
        let problem = response.text().await?;
        bail!("temporal retrieval returned {status}, expected 201: {problem}");
    }
    response.json().await.map_err(Into::into)
}

fn temporal_fixed_request_body(recorded_at: &str) -> Value {
    temporal_request_body(
        json!({
            "kind": "as_of",
            "valid_at": "2026-06-30T00:00:00Z",
            "recorded_at": recorded_at
        }),
        temporal_fixture_case_ids(),
    )
}

fn temporal_fixture_case_ids() -> Value {
    json!([
        "019be000-0000-7000-8000-000000000501",
        "019be000-0000-7000-8000-000000000502",
        "019be000-0000-7000-8000-000000000503",
        "019be000-0000-7000-8000-000000000504",
        "019be000-0000-7000-8000-000000000505"
    ])
}

fn temporal_request_body(perspective: Value, case_ids: Value) -> Value {
    json!({
        "query": "case.temporal:chronotoken",
        "perspective": perspective,
        "page_size": 10,
        "policy_id": "retrieval-hybrid-temporal-v1",
        "filters": {"case_ids": case_ids}
    })
}

fn assert_temporal_receipt(
    receipt: &RetrievalReceipt,
    fixture: &TemporalRetrievalFixture,
    expected_alpha_revision_id: Uuid,
) -> Result<()> {
    ensure!(receipt.status == "results");
    ensure!(receipt.policy.id == "retrieval-hybrid-temporal-v1");
    ensure!(receipt.policy.version == "1");
    ensure!(receipt.policy.digest.len() == 64);
    ensure!(receipt.valid_at == "2026-06-30T00:00:00Z");
    ensure!(receipt.items.len() == 4);
    let expected = [
        (
            "case.temporal:chronotoken",
            fixture.exact_revision_id,
            "0.047891458496",
            "0.125000000000",
            "1.000000000000",
            "1.250000000000",
            "-0.041905026184",
            "0.000000000000",
            "0.001496608078",
            "0.008196721311",
            "0.015679761701",
        ),
        (
            "alpha",
            expected_alpha_revision_id,
            "0.032266458496",
            "1.000000000000",
            "0.800000000000",
            "1.000000000000",
            "0.000000000000",
            "-0.006453291699",
            "0.000000000000",
            "0.000000000000",
            "0.025813166797",
        ),
        (
            "gamma",
            fixture.gamma_revision_id,
            "0.031754032258",
            "0.500000000000",
            "1.000000000000",
            "1.000000000000",
            "-0.015877016129",
            "0.000000000000",
            "0.000000000000",
            "0.000000000000",
            "0.015877016129",
        ),
        (
            "beta",
            fixture.beta_revision_id,
            "0.032522474881",
            "0.125000000000",
            "1.000000000000",
            "1.000000000000",
            "-0.028457165521",
            "0.000000000000",
            "0.000000000000",
            "0.000000000000",
            "0.004065309360",
        ),
    ];
    for (index, item) in receipt.items.iter().enumerate() {
        let (
            key,
            revision_id,
            fused,
            recency,
            confidence,
            importance,
            temporal,
            confidence_adjustment,
            importance_adjustment,
            bonus,
            final_score,
        ) = expected[index];
        ensure!(item.key == key, "temporal item {index} had the wrong key");
        ensure!(
            item.revision_id == revision_id,
            "{key} had the wrong revision"
        );
        assert_score(item, "fused_score", fused)?;
        assert_score(item, "recency_factor", recency)?;
        assert_score(item, "confidence_factor", confidence)?;
        assert_score(item, "importance_factor", importance)?;
        assert_score(item, "temporal_adjustment", temporal)?;
        assert_score(item, "confidence_adjustment", confidence_adjustment)?;
        assert_score(item, "importance_adjustment", importance_adjustment)?;
        assert_score(item, "exact_identity_bonus", bonus)?;
        assert_score(item, "final_score", final_score)?;
        assert_score(item, "final_rank", &(index + 1).to_string())?;
    }
    ensure!(
        receipt
            .items
            .iter()
            .all(|item| item.revision_id != fixture.delta_revision_id),
        "future-valid delta entered the historical candidate set"
    );
    let response_json = serde_json::to_string(receipt)?;
    let hidden_alpha_revision_id = if expected_alpha_revision_id == fixture.alpha_root_revision_id {
        fixture.alpha_successor_revision_id
    } else {
        fixture.alpha_root_revision_id
    };
    ensure!(
        !response_json.contains(&hidden_alpha_revision_id.to_string()),
        "the ineffective alpha revision leaked into the temporal receipt"
    );
    Ok(())
}

pub async fn temporal_retrieval_survives_projection_rebuild(
    target: &Target,
    fixture: &TemporalRetrievalFixture,
    replay: &TemporalReplayFixture,
) -> Result<Uuid> {
    let rebuilt_receipt = create_temporal_receipt(
        target,
        "temporal-policy-after-projection-rebuild",
        &replay.request_body,
    )
    .await?;
    assert_temporal_receipt(
        &rebuilt_receipt,
        fixture,
        fixture.alpha_successor_revision_id,
    )?;
    ensure!(
        rebuilt_receipt.policy == replay.first_receipt.policy,
        "projection rebuild changed the temporal policy"
    );
    ensure!(
        rebuilt_receipt.items == replay.first_receipt.items,
        "projection rebuild changed temporal order, values, or scores"
    );

    let replay_response = Client::new()
        .post(retrievals_url(target))
        .bearer_auth(&target.principal_a_internal_bearer_token)
        .header("Idempotency-Key", "temporal-policy-after-late-evidence")
        .json(&replay.request_body)
        .send()
        .await?;
    ensure!(replay_response.status() == StatusCode::CREATED);
    ensure!(
        replay_response
            .headers()
            .get("Idempotency-Replayed")
            .is_some_and(|value| value == "true"),
        "temporal receipt replay was not identified after projection rebuild"
    );
    let replayed: RetrievalReceipt = replay_response.json().await?;
    ensure!(
        serde_json::to_value(replayed)? == serde_json::to_value(&replay.first_receipt)?,
        "projection rebuild changed the first durable temporal receipt replay"
    );

    Ok(rebuilt_receipt.retrieval_id)
}

pub async fn temporal_receipt_survives_service_restart(
    target: &Target,
    replay: &TemporalReplayFixture,
) -> Result<()> {
    let get_response = Client::new()
        .get(format!(
            "{}/{}",
            retrievals_url(target),
            replay.first_retrieval_id
        ))
        .bearer_auth(&target.principal_a_internal_bearer_token)
        .send()
        .await?;
    ensure!(get_response.status() == StatusCode::OK);
    let got: RetrievalReceipt = get_response.json().await?;
    ensure!(
        got.retrieval_id == replay.first_receipt.retrieval_id
            && got.policy == replay.first_receipt.policy
            && got.items == replay.first_receipt.items
            && got.valid_at == replay.first_receipt.valid_at
            && got.recorded_at == replay.first_receipt.recorded_at,
        "process restart changed temporal receipt identity, policy, coordinates, or items"
    );

    let replay_response = Client::new()
        .post(retrievals_url(target))
        .bearer_auth(&target.principal_a_internal_bearer_token)
        .header("Idempotency-Key", "temporal-policy-after-late-evidence")
        .json(&replay.request_body)
        .send()
        .await?;
    ensure!(replay_response.status() == StatusCode::CREATED);
    ensure!(
        replay_response
            .headers()
            .get("Idempotency-Replayed")
            .is_some_and(|value| value == "true"),
        "service restart did not identify the durable temporal replay"
    );
    let replayed: RetrievalReceipt = replay_response.json().await?;
    ensure!(
        replayed.retrieval_id == replay.first_receipt.retrieval_id
            && replayed.policy == replay.first_receipt.policy
            && replayed.items == replay.first_receipt.items
            && replayed.valid_at == replay.first_receipt.valid_at
            && replayed.recorded_at == replay.first_receipt.recorded_at,
        "process restart changed the durable temporal replay"
    );
    Ok(())
}

pub async fn creates_temporal_lifecycle_fixture(
    target: &Target,
) -> Result<TemporalLifecycleFixture> {
    let client = Client::new();
    let deleted_case_id = Uuid::parse_str("019be000-0000-7000-8000-000000000506")?;
    let deleted_root = create_temporal_fact(
        &client,
        target,
        TemporalFactFixture {
            name: "temporal-deleted-root",
            case_id: deleted_case_id,
            namespace: "case.temporal.lifecycle",
            key: "deleted-successor",
            marker: "case.temporal:chronotoken temporal-deleted-root-private",
            vector_fixture: "temporal_vector_fixture_beta_4d",
            observed_at: "2026-04-01T00:00:00Z",
            valid_from: "2026-04-01T00:00:00Z",
            write_policy_id: "temporal-stable-evidence",
            confidence: 1.0,
        },
    )
    .await?;
    let deleted_successor = supersede_temporal_fact(
        &client,
        target,
        &deleted_root,
        TemporalSuccessorFixture {
            name: "temporal-deleted-successor",
            marker: "case.temporal:chronotoken temporal-deleted-successor-private",
            vector_fixture: "temporal_vector_fixture_beta_4d",
            observed_at: "2026-05-01T00:00:00Z",
            valid_from: "2026-05-01T00:00:00Z",
            write_policy_id: "temporal-stable-evidence",
            confidence: 1.0,
            retention_policy_id: "standard",
        },
    )
    .await?;

    let expired_case_id = Uuid::parse_str("019be000-0000-7000-8000-000000000507")?;
    let expired_root = create_temporal_fact(
        &client,
        target,
        TemporalFactFixture {
            name: "temporal-expired-root",
            case_id: expired_case_id,
            namespace: "case.temporal.lifecycle",
            key: "expired-successor",
            marker: "case.temporal:chronotoken temporal-expired-root-private",
            vector_fixture: "temporal_vector_fixture_gamma_4d",
            observed_at: "2026-04-01T00:00:00Z",
            valid_from: "2026-04-01T00:00:00Z",
            write_policy_id: "temporal-stable-evidence",
            confidence: 1.0,
        },
    )
    .await?;
    let expired_successor = supersede_temporal_fact(
        &client,
        target,
        &expired_root,
        TemporalSuccessorFixture {
            name: "temporal-expired-successor",
            marker: "case.temporal:chronotoken temporal-expired-successor-private",
            vector_fixture: "temporal_vector_fixture_gamma_4d",
            observed_at: "2026-05-01T00:00:00Z",
            valid_from: "2026-05-01T00:00:00Z",
            write_policy_id: "temporal-stable-evidence",
            confidence: 1.0,
            retention_policy_id: "retrieval-test-1s-v1",
        },
    )
    .await?;
    ensure!(
        deleted_successor
            .revision
            .as_ref()
            .is_some_and(|revision| { revision.revision_id == deleted_successor.head_revision_id }),
        "deleted lifecycle successor was not initially effective"
    );
    ensure!(
        expired_successor
            .revision
            .as_ref()
            .is_some_and(|revision| { revision.revision_id == expired_successor.head_revision_id }),
        "expiring lifecycle successor was not initially effective"
    );

    Ok(TemporalLifecycleFixture {
        deleted_case_id,
        deleted_root_revision_id: fact_revision_id(&deleted_root)?,
        deleted_successor_revision_id: deleted_successor.head_revision_id,
        expired_case_id,
        expired_root_revision_id: fact_revision_id(&expired_root)?,
        expired_successor_revision_id: expired_successor.head_revision_id,
    })
}

pub async fn temporal_policy_does_not_resurrect_ineligible_successors(
    target: &Target,
    fixture: &TemporalLifecycleFixture,
    replay: &TemporalLifecycleReplayFixture,
) -> Result<()> {
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    for receipt_fixture in &replay.receipts {
        let get_response = Client::new()
            .get(format!(
                "{}/{}",
                retrievals_url(target),
                receipt_fixture.retrieval_id
            ))
            .bearer_auth(&target.principal_a_internal_bearer_token)
            .send()
            .await?;
        ensure!(get_response.status() == StatusCode::OK);
        assert_temporal_lifecycle_receipt_hidden(get_response.json().await?, receipt_fixture)?;

        let replay_response = Client::new()
            .post(retrievals_url(target))
            .bearer_auth(&target.principal_a_internal_bearer_token)
            .header("Idempotency-Key", &receipt_fixture.idempotency_key)
            .json(&receipt_fixture.request_body)
            .send()
            .await?;
        ensure!(replay_response.status() == StatusCode::CREATED);
        ensure!(
            replay_response
                .headers()
                .get("Idempotency-Replayed")
                .is_some_and(|value| value == "true"),
            "ineligible temporal {} receipt was not replayed",
            receipt_fixture.name
        );
        assert_temporal_lifecycle_receipt_hidden(replay_response.json().await?, receipt_fixture)?;
    }

    for (name, case_id, root_revision_id, successor_revision_id, private_marker) in [
        (
            "deleted",
            fixture.deleted_case_id,
            fixture.deleted_root_revision_id,
            fixture.deleted_successor_revision_id,
            "temporal-deleted",
        ),
        (
            "expired",
            fixture.expired_case_id,
            fixture.expired_root_revision_id,
            fixture.expired_successor_revision_id,
            "temporal-expired",
        ),
    ] {
        let receipt = create_temporal_receipt(
            target,
            &format!("temporal-policy-{name}-successor-no-resurrection"),
            &temporal_request_body(json!({"kind": "current"}), json!([case_id])),
        )
        .await?;
        ensure!(receipt.status == "abstained");
        ensure!(receipt.items.is_empty());
        ensure!(receipt.policy.id == "retrieval-hybrid-temporal-v1");
        let response_json = serde_json::to_string(&receipt)?;
        ensure!(!response_json.contains(&root_revision_id.to_string()));
        ensure!(!response_json.contains(&successor_revision_id.to_string()));
        ensure!(!response_json.contains(private_marker));
    }
    Ok(())
}

pub async fn captures_temporal_lifecycle_receipts(
    target: &Target,
    fixture: &TemporalLifecycleFixture,
) -> Result<TemporalLifecycleReplayFixture> {
    let mut receipts = Vec::new();
    for (name, case_id, root_revision_id, successor_revision_id, private_marker) in [
        (
            "deleted",
            fixture.deleted_case_id,
            fixture.deleted_root_revision_id,
            fixture.deleted_successor_revision_id,
            "temporal-deleted",
        ),
        (
            "expired",
            fixture.expired_case_id,
            fixture.expired_root_revision_id,
            fixture.expired_successor_revision_id,
            "temporal-expired",
        ),
    ] {
        let idempotency_key = format!("temporal-policy-{name}-successor-before-ineligible");
        let request_body = temporal_request_body(json!({"kind": "current"}), json!([case_id]));
        let receipt = create_temporal_receipt(target, &idempotency_key, &request_body).await?;
        ensure!(receipt.status == "results");
        ensure!(receipt.items.len() == 1);
        ensure!(receipt.items[0].revision_id == successor_revision_id);
        ensure!(receipt.items[0].revision_id != root_revision_id);
        receipts.push(TemporalLifecycleReceiptFixture {
            name,
            retrieval_id: receipt.retrieval_id,
            idempotency_key,
            request_body,
            root_revision_id,
            successor_revision_id,
            private_marker,
        });
    }
    Ok(TemporalLifecycleReplayFixture { receipts })
}

fn assert_temporal_lifecycle_receipt_hidden(
    receipt: RetrievalReceipt,
    fixture: &TemporalLifecycleReceiptFixture,
) -> Result<()> {
    ensure!(receipt.status == "abstained");
    ensure!(receipt.items.is_empty());
    ensure!(receipt.policy.id == "retrieval-hybrid-temporal-v1");
    let response_json = serde_json::to_string(&receipt)?;
    ensure!(!response_json.contains(&fixture.root_revision_id.to_string()));
    ensure!(!response_json.contains(&fixture.successor_revision_id.to_string()));
    ensure!(!response_json.contains(fixture.private_marker));
    Ok(())
}

pub async fn creates_deterministic_hybrid_fusion_receipts(
    target: &Target,
    fixture: &HybridFusionFixture,
) -> Result<HybridReplayFixture> {
    let client = Client::new();
    let request_body = hybrid_request_body();
    let first_response = client
        .post(retrievals_url(target))
        .bearer_auth(&target.principal_a_internal_bearer_token)
        .header("Idempotency-Key", "hybrid-fusion-first")
        .json(&request_body)
        .send()
        .await?;
    ensure!(
        first_response.status() == StatusCode::CREATED,
        "hybrid retrieval returned {}, expected 201",
        first_response.status()
    );
    let first: RetrievalReceipt = first_response.json().await?;
    assert_hybrid_receipt(&first, fixture)?;

    let second_response = client
        .post(retrievals_url(target))
        .bearer_auth(&target.principal_a_internal_bearer_token)
        .header("Idempotency-Key", "hybrid-fusion-independent")
        .json(&request_body)
        .send()
        .await?;
    ensure!(second_response.status() == StatusCode::CREATED);
    let second: RetrievalReceipt = second_response.json().await?;
    assert_hybrid_receipt(&second, fixture)?;
    ensure!(
        first.retrieval_id != second.retrieval_id,
        "independent idempotency keys reused one retrieval ID"
    );
    ensure!(
        first.policy == second.policy,
        "independent receipts changed the pinned hybrid policy"
    );
    ensure!(
        first.items == second.items,
        "independent receipts changed deterministic fusion order or explanations"
    );

    Ok(HybridReplayFixture {
        request_body,
        receipt: serde_json::to_value(first)?,
    })
}

pub async fn replays_hybrid_receipt_before_provider_io(
    target: &Target,
    fixture: &HybridReplayFixture,
) -> Result<()> {
    let response = Client::new()
        .post(retrievals_url(target))
        .bearer_auth(&target.principal_a_internal_bearer_token)
        .header("Idempotency-Key", "hybrid-fusion-first")
        .json(&fixture.request_body)
        .send()
        .await?;
    ensure!(response.status() == StatusCode::CREATED);
    ensure!(
        response
            .headers()
            .get("Idempotency-Replayed")
            .is_some_and(|value| value == "true"),
        "completed hybrid request was not replayed"
    );
    let replayed: Value = response.json().await?;
    ensure!(
        replayed == fixture.receipt,
        "provider outage changed a completed durable receipt"
    );
    Ok(())
}

pub async fn hybrid_retrieval_fails_closed_without_leaking(
    target: &Target,
    idempotency_key: &str,
    forbidden_text: &str,
) -> Result<()> {
    let response = Client::new()
        .post(retrievals_url(target))
        .bearer_auth(&target.principal_a_internal_bearer_token)
        .header("Idempotency-Key", idempotency_key)
        .json(&hybrid_request_body())
        .send()
        .await?;
    assert_retryable_hybrid_failure(
        response,
        &[
            forbidden_text,
            "fusiontoken",
            "[1,0,0,0]",
            "[-1,0,0,0]",
            "vector_fixture_forbidden_4d",
        ],
    )
    .await
}

pub async fn hybrid_retrieval_recovers_after_projection_rebuild(
    target: &Target,
    fixture: &HybridFusionFixture,
    idempotency_key: &str,
) -> Result<()> {
    let response = Client::new()
        .post(retrievals_url(target))
        .bearer_auth(&target.principal_a_internal_bearer_token)
        .header("Idempotency-Key", idempotency_key)
        .json(&hybrid_request_body())
        .send()
        .await?;
    ensure!(response.status() == StatusCode::CREATED);
    ensure!(response.headers().get("Idempotency-Replayed").is_none());
    let receipt: RetrievalReceipt = response.json().await?;
    assert_hybrid_receipt(&receipt, fixture)
}

fn assert_hybrid_receipt(receipt: &RetrievalReceipt, fixture: &HybridFusionFixture) -> Result<()> {
    ensure!(receipt.status == "results");
    ensure!(receipt.policy.id == "retrieval-hybrid-v1");
    ensure!(receipt.policy.version == "1");
    ensure!(receipt.policy.digest.len() == 64);
    let query_lineage = receipt
        .query_embedding
        .as_ref()
        .context("hybrid receipt omitted query embedding lineage")?;
    ensure!(query_lineage.profile_id == "embedding-conformance-4d-v1");
    ensure!(query_lineage.profile_version == "1");
    ensure!(query_lineage.projection_profile_id == "fact-embedding-projection-v1");
    ensure!(query_lineage.projection_profile_version == "1");
    for digest in [
        &query_lineage.profile_digest,
        &query_lineage.projection_profile_digest,
        &query_lineage.input_sha256,
        &query_lineage.vector_sha256,
    ] {
        ensure!(
            digest.len() == 64,
            "query embedding lineage digest was not SHA-256"
        );
    }
    ensure!(receipt.items.len() == 5);
    let expected = [
        (
            "case.retrieval:fusiontoken",
            fixture.exact_revision_id,
            Some("1"),
            Some("3"),
            "5",
            "2.000000000000",
            "-1.000000000000",
            Some("0.016393442623"),
            Some("0.015873015873"),
            "0.015384615385",
            "0.047651073881",
        ),
        (
            "beta",
            fixture.beta_revision_id,
            None,
            Some("2"),
            "1",
            "0.199999988079",
            "0.800000011921",
            None,
            Some("0.016129032258"),
            "0.016393442623",
            "0.032522474881",
        ),
        (
            "alpha",
            fixture.alpha_revision_id,
            None,
            Some("1"),
            "4",
            "1.600000023842",
            "-0.600000023842",
            None,
            Some("0.016393442623"),
            "0.015625000000",
            "0.032018442623",
        ),
        (
            "gamma",
            fixture.gamma_revision_id,
            None,
            Some("4"),
            "2",
            "0.399999976158",
            "0.600000023842",
            None,
            Some("0.015625000000"),
            "0.016129032258",
            "0.031754032258",
        ),
        (
            "delta",
            fixture.delta_revision_id,
            None,
            None,
            "3",
            "1.000000000000",
            "0.000000000000",
            None,
            None,
            "0.015873015873",
            "0.015873015873",
        ),
    ];
    for (index, item) in receipt.items.iter().enumerate() {
        let (
            key,
            revision_id,
            exact_rank,
            lexical_rank,
            vector_rank,
            distance,
            similarity,
            exact_rrf,
            lexical_rrf,
            vector_rrf,
            fused,
        ) = expected[index];
        ensure!(item.key == key, "hybrid item {index} had the wrong key");
        ensure!(
            item.revision_id == revision_id,
            "hybrid item {key} had the wrong revision"
        );
        ensure!(item.revision_id != fixture.forbidden_revision_id);
        assert_optional_score(item, "exact_rank", exact_rank)?;
        assert_optional_score(item, "lexical_rank", lexical_rank)?;
        assert_score(item, "vector_rank", vector_rank)?;
        assert_score(item, "vector_distance", distance)?;
        assert_score(item, "vector_similarity", similarity)?;
        assert_optional_score(item, "exact_rrf", exact_rrf)?;
        assert_optional_score(item, "lexical_rrf", lexical_rrf)?;
        assert_score(item, "vector_rrf", vector_rrf)?;
        assert_score(item, "fused_score", fused)?;
        assert_score(item, "final_score", fused)?;
        assert_score(item, "final_rank", &(index + 1).to_string())?;
        let lineage = item
            .embedding
            .as_ref()
            .context("hybrid item omitted embedding lineage")?;
        ensure!(lineage.profile_id == "embedding-conformance-4d-v1");
        ensure!(lineage.profile_version == "1");
        for digest in [
            &lineage.profile_digest,
            &lineage.projection_sha256,
            &lineage.input_sha256,
            &lineage.vector_sha256,
        ] {
            ensure!(
                digest.len() == 64,
                "embedding lineage digest was not SHA-256"
            );
        }
    }
    let receipt_json = serde_json::to_string(receipt)?;
    ensure!(
        !receipt_json.contains(&fixture.forbidden_revision_id.to_string()),
        "forbidden trap revision reached the fused receipt"
    );
    for private_text in [
        "vector_fixture_forbidden_4d",
        "restricted-vector-trap",
        "[1,0,0,0]",
        "[-1,0,0,0]",
        "[-0.6,0.8,0,0]",
        "[0.8,0.6,0,0]",
    ] {
        ensure!(
            !receipt_json.contains(private_text),
            "hybrid receipt disclosed raw provider material: {private_text}"
        );
    }
    Ok(())
}

fn assert_score(item: &RetrievalItem, component: &str, expected: &str) -> Result<()> {
    let score = item
        .scores
        .iter()
        .find(|score| score.component == component)
        .with_context(|| format!("{} omitted {component}", item.key))?;
    ensure!(
        score.value == expected,
        "{} {component} was {}, expected {expected}",
        item.key,
        score.value
    );
    Ok(())
}

fn assert_optional_score(
    item: &RetrievalItem,
    component: &str,
    expected: Option<&str>,
) -> Result<()> {
    match expected {
        Some(expected) => assert_score(item, component, expected),
        None => {
            ensure!(
                item.scores.iter().all(|score| score.component != component),
                "{} unexpectedly exposed {component}",
                item.key
            );
            Ok(())
        }
    }
}

async fn assert_retryable_hybrid_failure(
    response: reqwest::Response,
    forbidden_texts: &[&str],
) -> Result<()> {
    ensure!(response.status() == StatusCode::SERVICE_UNAVAILABLE);
    ensure!(
        response
            .headers()
            .get(header::RETRY_AFTER)
            .is_some_and(|value| value == "1")
    );
    let problem: Value = response.json().await?;
    let problem_json = problem.to_string();
    for forbidden in forbidden_texts {
        ensure!(
            !problem_json.contains(forbidden),
            "hybrid failure disclosed {forbidden}"
        );
    }
    Ok(())
}

fn retrievals_url(target: &Target) -> String {
    format!(
        "{}/v1/tenants/{}/subjects/{}/retrievals",
        target.base_url.trim_end_matches('/'),
        target.tenant_id,
        target.subject_id
    )
}

fn hybrid_request_body() -> Value {
    json!({
        "query": "case.retrieval:fusiontoken",
        "perspective": {"kind": "current"},
        "page_size": 10,
        "policy_id": "retrieval-hybrid-v1",
        "filters": {
            "case_ids": [
                "019be000-0000-7000-8000-000000000401",
                "019be000-0000-7000-8000-000000000402",
                "019be000-0000-7000-8000-000000000403",
                "019be000-0000-7000-8000-000000000404",
                "019be000-0000-7000-8000-000000000405",
                "019be000-0000-7000-8000-000000000406"
            ]
        }
    })
}

pub struct RetrievalIsolationFixture {
    pub retrieval_id: Uuid,
    pub allowed_revision_id: Uuid,
    pub forbidden_revision_ids: Vec<Uuid>,
}

pub async fn retrieval_candidates_are_authorized_before_ranking(
    target: &Target,
) -> Result<RetrievalIsolationFixture> {
    let client = Client::new();
    let marker = "cobalt-otter-731";
    let internal = create_marker_fact(
        &client,
        target,
        MarkerFactFixture {
            bearer_token: &target.bearer_token,
            tenant_id: target.tenant_id,
            subject_id: target.subject_id,
            case_id: Uuid::parse_str("019be000-0000-7000-8000-000000000301")?,
            name: "retrieval-internal",
            marker,
            secret: "internal-visible-value",
            sensitivity: "internal",
            retention_policy_id: "standard",
        },
    )
    .await?;
    let restricted = create_marker_fact(
        &client,
        target,
        MarkerFactFixture {
            bearer_token: &target.bearer_token,
            tenant_id: target.tenant_id,
            subject_id: target.subject_id,
            case_id: Uuid::parse_str("019be000-0000-7000-8000-000000000302")?,
            name: "retrieval-restricted",
            marker,
            secret: "restricted-hidden-value",
            sensitivity: "restricted",
            retention_policy_id: "standard",
        },
    )
    .await?;
    let cross_subject = create_marker_fact(
        &client,
        target,
        MarkerFactFixture {
            bearer_token: &target.principal_c_bearer_token,
            tenant_id: target.tenant_id,
            subject_id: target.principal_c_subject_id,
            case_id: Uuid::parse_str("019be000-0000-7000-8000-000000000303")?,
            name: "retrieval-cross-subject",
            marker,
            secret: "cross-subject-hidden-value",
            sensitivity: "restricted",
            retention_policy_id: "standard",
        },
    )
    .await?;
    let cross_tenant = create_marker_fact(
        &client,
        target,
        MarkerFactFixture {
            bearer_token: &target.principal_b_bearer_token,
            tenant_id: target.principal_b_tenant_id,
            subject_id: target.principal_b_subject_id,
            case_id: Uuid::parse_str("019be000-0000-7000-8000-000000000304")?,
            name: "retrieval-cross-tenant",
            marker,
            secret: "cross-tenant-hidden-value",
            sensitivity: "restricted",
            retention_policy_id: "standard",
        },
    )
    .await?;
    let retrievals_url = format!(
        "{}/v1/tenants/{}/subjects/{}/retrievals",
        target.base_url.trim_end_matches('/'),
        target.tenant_id,
        target.subject_id
    );
    let body = json!({
        "query": marker,
        "perspective": {"kind": "current"},
        "page_size": 50
    });
    let internal_response = client
        .post(&retrievals_url)
        .bearer_auth(&target.principal_a_internal_bearer_token)
        .header("Idempotency-Key", "retrieval-isolation-internal")
        .json(&body)
        .send()
        .await?;
    ensure!(internal_response.status() == StatusCode::CREATED);
    let internal_receipt: RetrievalReceipt = internal_response.json().await?;
    ensure!(internal_receipt.items.len() == 1);
    ensure!(
        internal_receipt.items[0].revision_id
            == internal
                .revision
                .as_ref()
                .context("internal marker fact has no revision")?
                .revision_id
    );
    let internal_json = serde_json::to_string(&internal_receipt)?;
    for hidden in [
        "restricted-hidden-value",
        "cross-subject-hidden-value",
        "cross-tenant-hidden-value",
    ] {
        ensure!(
            !internal_json.contains(hidden),
            "retrieval disclosed {hidden}"
        );
    }

    let full_response = client
        .post(&retrievals_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "retrieval-isolation-full")
        .json(&body)
        .send()
        .await?;
    ensure!(full_response.status() == StatusCode::CREATED);
    let full_location = full_response
        .headers()
        .get(header::LOCATION)
        .context("full-scope retrieval omitted Location")?
        .to_str()?
        .to_owned();
    let full_receipt: RetrievalReceipt = full_response.json().await?;
    let full_ids = full_receipt
        .items
        .iter()
        .map(|item| item.revision_id)
        .collect::<Vec<_>>();
    ensure!(full_ids.len() == 2);
    ensure!(
        full_ids.contains(
            &internal
                .revision
                .as_ref()
                .context("internal marker fact has no revision")?
                .revision_id
        )
    );
    ensure!(
        full_ids.contains(
            &restricted
                .revision
                .as_ref()
                .context("restricted marker fact has no revision")?
                .revision_id
        )
    );

    let full_receipt_url =
        if full_location.starts_with("http://") || full_location.starts_with("https://") {
            full_location
        } else {
            format!("{}{}", target.base_url.trim_end_matches('/'), full_location)
        };
    let reauthorized_response = client
        .get(&full_receipt_url)
        .bearer_auth(&target.principal_a_internal_bearer_token)
        .send()
        .await?;
    ensure!(reauthorized_response.status() == StatusCode::OK);
    let reauthorized: RetrievalReceipt = reauthorized_response.json().await?;
    ensure!(reauthorized.items.len() == 1);
    ensure!(reauthorized.items[0].revision_id == internal_receipt.items[0].revision_id);
    ensure!(
        reauthorized.authorization.scope_digest != full_receipt.authorization.scope_digest,
        "receipt reauthorization retained the broader authorization scope digest"
    );
    ensure!(
        !serde_json::to_string(&reauthorized)?.contains("restricted-hidden-value"),
        "receipt reauthorization disclosed a revoked sensitivity"
    );

    let same_scope_other_principal = client
        .get(&full_receipt_url)
        .bearer_auth(&target.principal_d_same_scope_bearer_token)
        .send()
        .await?;
    ensure!(
        same_scope_other_principal.status() == StatusCode::NOT_FOUND,
        "same-scope principal rehydrated another principal's receipt"
    );
    ensure!(
        !same_scope_other_principal
            .text()
            .await?
            .contains("restricted-hidden-value"),
        "same-scope cloaked response disclosed another principal's content"
    );

    let hidden_response = client
        .get(full_receipt_url)
        .bearer_auth(&target.principal_b_bearer_token)
        .send()
        .await?;
    ensure!(hidden_response.status() == StatusCode::NOT_FOUND);
    let hidden_problem: Value = hidden_response.json().await?;
    let hidden_json = hidden_problem.to_string();
    for hidden in [
        "internal-visible-value",
        "restricted-hidden-value",
        "cross-subject-hidden-value",
        "cross-tenant-hidden-value",
    ] {
        ensure!(
            !hidden_json.contains(hidden),
            "cloaked 404 disclosed {hidden}"
        );
    }

    Ok(RetrievalIsolationFixture {
        retrieval_id: internal_receipt.retrieval_id,
        allowed_revision_id: internal_receipt.items[0].revision_id,
        forbidden_revision_ids: vec![
            restricted
                .revision
                .context("restricted marker fact has no revision")?
                .revision_id,
            cross_subject
                .revision
                .context("cross-subject marker fact has no revision")?
                .revision_id,
            cross_tenant
                .revision
                .context("cross-tenant marker fact has no revision")?
                .revision_id,
        ],
    })
}

pub async fn concurrent_retrievals_converge_on_one_receipt(target: &Target) -> Result<()> {
    let client = Client::builder().timeout(Duration::from_secs(10)).build()?;
    let retrievals_url = format!(
        "{}/v1/tenants/{}/subjects/{}/retrievals",
        target.base_url.trim_end_matches('/'),
        target.tenant_id,
        target.subject_id
    );
    let body = json!({
        "query": "cobalt-otter-731",
        "perspective": {"kind": "current"},
        "page_size": 50
    });
    let request_a = client
        .post(&retrievals_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "retrieval-concurrent-identical")
        .json(&body)
        .send();
    let request_b = client
        .post(&retrievals_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "retrieval-concurrent-identical")
        .json(&body)
        .send();
    let (response_a, response_b) = tokio::join!(request_a, request_b);
    let response_a = response_a?;
    let response_b = response_b?;
    ensure!(response_a.status() == StatusCode::CREATED);
    ensure!(response_b.status() == StatusCode::CREATED);
    let replay_count = [&response_a, &response_b]
        .into_iter()
        .filter(|response| {
            response
                .headers()
                .get("Idempotency-Replayed")
                .is_some_and(|value| value == "true")
        })
        .count();
    ensure!(
        replay_count == 1,
        "concurrent identical retrievals did not identify exactly one replay"
    );
    let receipt_a: RetrievalReceipt = response_a.json().await?;
    let receipt_b: RetrievalReceipt = response_b.json().await?;
    ensure!(
        serde_json::to_value(receipt_a)? == serde_json::to_value(receipt_b)?,
        "concurrent identical retrievals did not converge on one durable receipt"
    );
    Ok(())
}

pub async fn rejects_cross_subject_retrieval_idempotency_reuse(target: &Target) -> Result<()> {
    let client = Client::new();
    let body = json!({
        "query": "cobalt-otter-731",
        "perspective": {"kind": "current"},
        "page_size": 10
    });
    let first = client
        .post(format!(
            "{}/v1/tenants/{}/subjects/{}/retrievals",
            target.base_url.trim_end_matches('/'),
            target.tenant_id,
            target.subject_id
        ))
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "retrieval-cross-subject-reuse")
        .json(&body)
        .send()
        .await?;
    ensure!(first.status() == StatusCode::CREATED);

    for _ in 0..2 {
        let reused = client
            .post(format!(
                "{}/v1/tenants/{}/subjects/{}/retrievals",
                target.base_url.trim_end_matches('/'),
                target.tenant_id,
                target.principal_a_secondary_subject_id
            ))
            .bearer_auth(&target.bearer_token)
            .header("Idempotency-Key", "retrieval-cross-subject-reuse")
            .json(&body)
            .send()
            .await?;
        assert_problem(reused, StatusCode::CONFLICT, "idempotency-key-reused").await?;
    }
    Ok(())
}

pub async fn retrieval_fails_closed_when_projection_is_missing(target: &Target) -> Result<()> {
    retrieval_fails_closed_for_projection_state(target, "retrieval-projection-retry").await
}

pub async fn retrieval_fails_closed_when_projection_is_corrupt(
    target: &Target,
    idempotency_key: &str,
) -> Result<()> {
    retrieval_fails_closed_for_projection_state(target, idempotency_key).await
}

async fn retrieval_fails_closed_for_projection_state(
    target: &Target,
    idempotency_key: &str,
) -> Result<()> {
    let response = Client::new()
        .post(format!(
            "{}/v1/tenants/{}/subjects/{}/retrievals",
            target.base_url.trim_end_matches('/'),
            target.tenant_id,
            target.subject_id
        ))
        .bearer_auth(&target.principal_a_internal_bearer_token)
        .header("Idempotency-Key", idempotency_key)
        .json(&json!({
            "query": "cobalt-otter-731",
            "perspective": {"kind": "current"},
            "page_size": 10
        }))
        .send()
        .await?;
    ensure!(response.status() == StatusCode::SERVICE_UNAVAILABLE);
    ensure!(
        response
            .headers()
            .get(header::RETRY_AFTER)
            .is_some_and(|value| value == "1")
    );
    let problem: Value = response.json().await?;
    ensure!(
        !problem.to_string().contains("cobalt-otter-731"),
        "projection failure disclosed raw query text"
    );
    Ok(())
}

pub async fn retrieval_recovers_after_projection_rebuild(
    target: &Target,
    expected_revision_id: Uuid,
) -> Result<()> {
    retrieval_succeeds_after_projection_rebuild(
        target,
        expected_revision_id,
        "retrieval-projection-retry",
    )
    .await
}

pub async fn retrieval_succeeds_after_projection_rebuild(
    target: &Target,
    expected_revision_id: Uuid,
    idempotency_key: &str,
) -> Result<()> {
    let response = Client::new()
        .post(format!(
            "{}/v1/tenants/{}/subjects/{}/retrievals",
            target.base_url.trim_end_matches('/'),
            target.tenant_id,
            target.subject_id
        ))
        .bearer_auth(&target.principal_a_internal_bearer_token)
        .header("Idempotency-Key", idempotency_key)
        .json(&json!({
            "query": "cobalt-otter-731",
            "perspective": {"kind": "current"},
            "page_size": 10
        }))
        .send()
        .await?;
    ensure!(response.status() == StatusCode::CREATED);
    ensure!(response.headers().get("Idempotency-Replayed").is_none());
    let receipt: RetrievalReceipt = response.json().await?;
    ensure!(receipt.items.len() == 1);
    ensure!(receipt.items[0].revision_id == expected_revision_id);
    Ok(())
}

pub async fn retrieval_paginates_and_rejects_invalid_replays(target: &Target) -> Result<()> {
    let client = Client::new();
    let marker = "violet-lantern-842";
    let fixtures = [
        ("retrieval-page-a", "019be000-0000-7000-8000-000000000311"),
        ("retrieval-page-b", "019be000-0000-7000-8000-000000000312"),
        ("retrieval-page-c", "019be000-0000-7000-8000-000000000313"),
    ];
    let mut expected_revision_ids = Vec::new();
    for (name, case_id) in fixtures {
        let view = create_marker_fact(
            &client,
            target,
            MarkerFactFixture {
                bearer_token: &target.bearer_token,
                tenant_id: target.tenant_id,
                subject_id: target.subject_id,
                case_id: Uuid::parse_str(case_id)?,
                name,
                marker,
                secret: name,
                sensitivity: "internal",
                retention_policy_id: "standard",
            },
        )
        .await?;
        expected_revision_ids.push(
            view.revision
                .context("pagination marker fact has no revision")?
                .revision_id,
        );
    }
    let retrievals_url = format!(
        "{}/v1/tenants/{}/subjects/{}/retrievals",
        target.base_url.trim_end_matches('/'),
        target.tenant_id,
        target.subject_id
    );
    let request = json!({
        "query": marker,
        "perspective": {"kind": "current"},
        "page_size": 2
    });
    let first_response = client
        .post(&retrievals_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "retrieval-pagination")
        .json(&request)
        .send()
        .await?;
    ensure!(first_response.status() == StatusCode::CREATED);
    let location = first_response
        .headers()
        .get(header::LOCATION)
        .context("paginated retrieval omitted Location")?
        .to_str()?
        .to_owned();
    let first: RetrievalReceipt = first_response.json().await?;
    ensure!(first.items.len() == 2);
    let cursor = first
        .next_cursor
        .as_deref()
        .context("paginated retrieval omitted next_cursor")?;
    let receipt_url = if location.starts_with("http://") || location.starts_with("https://") {
        location
    } else {
        format!("{}{}", target.base_url.trim_end_matches('/'), location)
    };
    let second_response = client
        .get(&receipt_url)
        .query(&[("cursor", cursor)])
        .bearer_auth(&target.bearer_token)
        .send()
        .await?;
    ensure!(second_response.status() == StatusCode::OK);
    let second: RetrievalReceipt = second_response.json().await?;
    ensure!(second.items.len() == 1);
    ensure!(second.next_cursor.is_none());
    let actual_items = first.items.iter().chain(&second.items).collect::<Vec<_>>();
    for item in &actual_items {
        ensure_lexical_only_scores(item)?;
    }
    let actual_revision_ids = actual_items
        .iter()
        .map(|item| item.revision_id)
        .collect::<Vec<_>>();
    ensure!(actual_revision_ids.len() == expected_revision_ids.len());
    ensure!(
        actual_revision_ids.iter().all(|revision_id| {
            expected_revision_ids.contains(revision_id)
                && actual_revision_ids
                    .iter()
                    .filter(|candidate| *candidate == revision_id)
                    .count()
                    == 1
        }),
        "pagination duplicated, omitted, or substituted a revision"
    );
    let actual_scores = actual_items
        .iter()
        .map(|item| {
            item.scores
                .iter()
                .map(|score| (score.component.clone(), score.value.clone()))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let policy_digest = first.policy.digest.clone();

    let equivalent_response = client
        .post(&retrievals_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "retrieval-pagination-equivalent")
        .json(&request)
        .send()
        .await?;
    ensure!(equivalent_response.status() == StatusCode::CREATED);
    let equivalent_location = equivalent_response
        .headers()
        .get(header::LOCATION)
        .context("equivalent retrieval omitted Location")?
        .to_str()?
        .to_owned();
    let equivalent_first: RetrievalReceipt = equivalent_response.json().await?;
    let equivalent_cursor = equivalent_first
        .next_cursor
        .as_deref()
        .context("equivalent retrieval omitted next_cursor")?;
    let equivalent_receipt_url = if equivalent_location.starts_with("http://")
        || equivalent_location.starts_with("https://")
    {
        equivalent_location
    } else {
        format!(
            "{}{}",
            target.base_url.trim_end_matches('/'),
            equivalent_location
        )
    };
    let equivalent_second_response = client
        .get(equivalent_receipt_url)
        .query(&[("cursor", equivalent_cursor)])
        .bearer_auth(&target.bearer_token)
        .send()
        .await?;
    ensure!(equivalent_second_response.status() == StatusCode::OK);
    let equivalent_second: RetrievalReceipt = equivalent_second_response.json().await?;
    let equivalent_items = equivalent_first
        .items
        .iter()
        .chain(&equivalent_second.items)
        .collect::<Vec<_>>();
    let equivalent_revision_ids = equivalent_items
        .iter()
        .map(|item| item.revision_id)
        .collect::<Vec<_>>();
    let equivalent_scores = equivalent_items
        .iter()
        .map(|item| {
            item.scores
                .iter()
                .map(|score| (score.component.clone(), score.value.clone()))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    ensure!(
        equivalent_revision_ids == actual_revision_ids,
        "equivalent retrieval changed the ordered revision IDs"
    );
    ensure!(
        equivalent_scores == actual_scores,
        "equivalent retrieval changed ordered score components or values"
    );
    ensure!(
        equivalent_first.policy.digest == policy_digest
            && equivalent_second.policy.digest == policy_digest,
        "equivalent retrieval changed the policy digest"
    );

    let changed_replay = client
        .post(&retrievals_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "retrieval-pagination")
        .json(&json!({
            "query": "different query",
            "perspective": {"kind": "current"},
            "page_size": 2
        }))
        .send()
        .await?;
    ensure!(changed_replay.status() == StatusCode::CONFLICT);

    let invalid_cursor = client
        .get(&receipt_url)
        .query(&[("cursor", Uuid::now_v7().to_string())])
        .bearer_auth(&target.bearer_token)
        .send()
        .await?;
    ensure!(invalid_cursor.status() == StatusCode::NOT_FOUND);

    let exact_response = client
        .post(&retrievals_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "retrieval-exact-identity")
        .json(&json!({
            "query": "retrieval-page-b",
            "perspective": {"kind": "current"},
            "page_size": 10
        }))
        .send()
        .await?;
    ensure!(exact_response.status() == StatusCode::CREATED);
    let exact: RetrievalReceipt = exact_response.json().await?;
    ensure!(!exact.items.is_empty());
    ensure!(exact.items[0].key == "retrieval-page-b");
    ensure!(
        exact.items[0]
            .scores
            .iter()
            .any(|score| score.component == "exact_identity_rank")
    );

    let empty_response = client
        .post(&retrievals_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "retrieval-abstention")
        .json(&json!({
            "query": "marker-that-does-not-exist-999",
            "perspective": {"kind": "current"},
            "page_size": 10
        }))
        .send()
        .await?;
    ensure!(empty_response.status() == StatusCode::CREATED);
    let empty: RetrievalReceipt = empty_response.json().await?;
    ensure!(empty.status == "abstained");
    ensure!(empty.items.is_empty());

    let future_response = client
        .post(&retrievals_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "retrieval-future-perspective")
        .json(&json!({
            "query": marker,
            "perspective": {
                "kind": "as_of",
                "valid_at": "2026-07-29T00:00:00Z",
                "recorded_at": "2999-01-01T00:00:00Z"
            },
            "page_size": 10
        }))
        .send()
        .await?;
    ensure!(future_response.status() == StatusCode::UNPROCESSABLE_ENTITY);

    let oversized_response = client
        .post(&retrievals_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "retrieval-oversized")
        .json(&json!({
            "query": "x".repeat(4097),
            "perspective": {"kind": "current"},
            "page_size": 10
        }))
        .send()
        .await?;
    ensure!(oversized_response.status() == StatusCode::PAYLOAD_TOO_LARGE);

    let missing_key_response = client
        .post(&retrievals_url)
        .bearer_auth(&target.bearer_token)
        .json(&request)
        .send()
        .await?;
    ensure!(missing_key_response.status() == StatusCode::BAD_REQUEST);

    Ok(())
}

pub async fn retrieval_receipt_hides_expired_content(target: &Target) -> Result<()> {
    let client = Client::new();
    let marker = "amber-kestrel-953";
    let fact = create_marker_fact(
        &client,
        target,
        MarkerFactFixture {
            bearer_token: &target.bearer_token,
            tenant_id: target.tenant_id,
            subject_id: target.subject_id,
            case_id: Uuid::parse_str("019be000-0000-7000-8000-000000000321")?,
            name: "retrieval-expiring",
            marker,
            secret: "expired-hidden-value",
            sensitivity: "internal",
            retention_policy_id: "retrieval-test-1s-v1",
        },
    )
    .await?;
    let revision_id = fact
        .revision
        .context("expiring fact has no revision")?
        .revision_id;
    let retrievals_url = format!(
        "{}/v1/tenants/{}/subjects/{}/retrievals",
        target.base_url.trim_end_matches('/'),
        target.tenant_id,
        target.subject_id
    );
    let response = client
        .post(&retrievals_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "retrieval-expiring")
        .json(&json!({
            "query": marker,
            "perspective": {"kind": "current"},
            "page_size": 10
        }))
        .send()
        .await?;
    ensure!(response.status() == StatusCode::CREATED);
    let location = response
        .headers()
        .get(header::LOCATION)
        .context("expiring retrieval omitted Location")?
        .to_str()?
        .to_owned();
    let created: RetrievalReceipt = response.json().await?;
    ensure!(created.items.len() == 1);
    ensure!(created.items[0].revision_id == revision_id);
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let receipt_url = if location.starts_with("http://") || location.starts_with("https://") {
        location
    } else {
        format!("{}{}", target.base_url.trim_end_matches('/'), location)
    };
    let expired_response = client
        .get(receipt_url)
        .bearer_auth(&target.bearer_token)
        .send()
        .await?;
    ensure!(expired_response.status() == StatusCode::OK);
    let expired: RetrievalReceipt = expired_response.json().await?;
    ensure!(expired.status == "abstained");
    ensure!(expired.items.is_empty());
    ensure!(
        !serde_json::to_string(&expired)?.contains("expired-hidden-value"),
        "expired receipt rehydrated private content"
    );
    Ok(())
}

pub struct RetrievalLifecycleFixture {
    pub receipt_url: String,
    pub retrieval_id: Uuid,
    pub superseded_revision_id: Uuid,
    pub deleted_revision_id: Uuid,
}

pub async fn creates_retrieval_lifecycle_fixture(
    target: &Target,
) -> Result<RetrievalLifecycleFixture> {
    let client = Client::new();
    let marker = "silver-heron-417";
    let first = create_marker_fact(
        &client,
        target,
        MarkerFactFixture {
            bearer_token: &target.bearer_token,
            tenant_id: target.tenant_id,
            subject_id: target.subject_id,
            case_id: Uuid::parse_str("019be000-0000-7000-8000-000000000322")?,
            name: "retrieval-lifecycle",
            marker,
            secret: "superseded-hidden-value",
            sensitivity: "internal",
            retention_policy_id: "standard",
        },
    )
    .await?;
    let first_revision = first
        .revision
        .as_ref()
        .context("lifecycle fact has no first revision")?;
    let episode_response = client
        .post(format!(
            "{}/v1/tenants/{}/subjects/{}/episodes",
            target.base_url.trim_end_matches('/'),
            target.tenant_id,
            target.subject_id
        ))
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "retrieval-lifecycle-successor-episode")
        .json(&json!({
            "case_id": first.case_id,
            "kind": "retrieval-fixture",
            "observed_at": "2026-05-11T09:00:00Z",
            "provenance": {
                "source_type": "conformance",
                "source_uri": null,
                "external_id": "retrieval-lifecycle-successor"
            },
            "sensitivity": "internal",
            "retention_policy_id": "standard",
            "payload": {"marker": marker}
        }))
        .send()
        .await?;
    ensure!(episode_response.status() == StatusCode::CREATED);
    let episode: Episode = episode_response.json().await?;
    let successor_response = client
        .put(format!(
            "{}/v1/tenants/{}/subjects/{}/facts/{}",
            target.base_url.trim_end_matches('/'),
            target.tenant_id,
            target.subject_id,
            first.fact_id
        ))
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "retrieval-lifecycle-successor")
        .header(header::IF_MATCH, format!("\"{}\"", first.head_revision_id))
        .json(&json!({
            "supersedes_revision_id": first_revision.revision_id,
            "value": {"marker": marker, "secret": "deleted-successor-value"},
            "observed_at": "2026-05-11T09:00:00Z",
            "valid_time": {"from": "2026-05-11T00:00:00Z", "until": null},
            "evidence_episode_ids": [episode.episode_id],
            "write_policy": {"id": "direct-evidence", "version": "1"},
            "confidence": 1.0,
            "sensitivity": "internal",
            "retention_policy_id": "standard"
        }))
        .send()
        .await?;
    ensure!(successor_response.status() == StatusCode::OK);
    let successor: FactView = successor_response.json().await?;
    let successor_revision = successor
        .revision
        .as_ref()
        .context("lifecycle fact has no successor")?;
    let retrieval_response = client
        .post(format!(
            "{}/v1/tenants/{}/subjects/{}/retrievals",
            target.base_url.trim_end_matches('/'),
            target.tenant_id,
            target.subject_id
        ))
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "retrieval-lifecycle-receipt")
        .json(&json!({
            "query": marker,
            "perspective": {"kind": "current"},
            "page_size": 10
        }))
        .send()
        .await?;
    ensure!(retrieval_response.status() == StatusCode::CREATED);
    let location = retrieval_response
        .headers()
        .get(header::LOCATION)
        .context("lifecycle retrieval omitted Location")?
        .to_str()?
        .to_owned();
    let receipt: RetrievalReceipt = retrieval_response.json().await?;
    ensure!(receipt.items.len() == 1);
    ensure!(receipt.items[0].revision_id == successor_revision.revision_id);
    let receipt_url = if location.starts_with("http://") || location.starts_with("https://") {
        location
    } else {
        format!("{}{}", target.base_url.trim_end_matches('/'), location)
    };
    Ok(RetrievalLifecycleFixture {
        receipt_url,
        retrieval_id: receipt.retrieval_id,
        superseded_revision_id: first_revision.revision_id,
        deleted_revision_id: successor_revision.revision_id,
    })
}

pub async fn retrieval_receipt_does_not_resurrect_deleted_history(
    target: &Target,
    fixture: &RetrievalLifecycleFixture,
) -> Result<()> {
    let response = Client::new()
        .get(&fixture.receipt_url)
        .bearer_auth(&target.bearer_token)
        .send()
        .await?;
    ensure!(response.status() == StatusCode::OK);
    let receipt: RetrievalReceipt = response.json().await?;
    ensure!(receipt.status == "abstained");
    ensure!(receipt.items.is_empty());
    let response_json = serde_json::to_string(&receipt)?;
    ensure!(!response_json.contains(&fixture.deleted_revision_id.to_string()));
    ensure!(!response_json.contains(&fixture.superseded_revision_id.to_string()));
    ensure!(!response_json.contains("deleted-successor-value"));
    ensure!(!response_json.contains("superseded-hidden-value"));
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

pub async fn retrieves_the_effective_bitemporal_revision(target: &Target) -> Result<()> {
    let fixture = supersedes_the_fact_head(target).await?;
    let client = Client::new();
    let retrievals_url = format!(
        "{}/v1/tenants/{}/subjects/{}/retrievals",
        target.base_url.trim_end_matches('/'),
        target.tenant_id,
        target.subject_id
    );
    let historical_response = client
        .post(&retrievals_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "retrieval-shipping-address-as-of")
        .json(&json!({
            "query": "Wellington",
            "perspective": {
                "kind": "as_of",
                "valid_at": "2026-01-15T00:00:00Z",
                "recorded_at": fixture.second_recorded_at
            },
            "page_size": 10,
            "filters": {"namespaces": ["case.profile"]}
        }))
        .send()
        .await?;
    ensure!(historical_response.status() == StatusCode::CREATED);
    let historical: RetrievalReceipt = historical_response.json().await?;
    ensure!(historical.items.len() == 1);
    ensure!(historical.items[0].revision_id == fixture.first_revision_id);
    ensure!(historical.items[0].value["city"] == "Wellington");

    let current_response = client
        .post(&retrievals_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "retrieval-shipping-address-current")
        .json(&json!({
            "query": "Auckland",
            "perspective": {"kind": "current"},
            "page_size": 10,
            "filters": {"namespaces": ["case.profile"]}
        }))
        .send()
        .await?;
    ensure!(current_response.status() == StatusCode::CREATED);
    let current: RetrievalReceipt = current_response.json().await?;
    ensure!(current.items.len() == 1);
    ensure!(current.items[0].revision_id == fixture.second_revision_id);
    ensure!(current.items[0].value["city"] == "Auckland");

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

struct MarkerFactFixture<'a> {
    bearer_token: &'a str,
    tenant_id: Uuid,
    subject_id: Uuid,
    case_id: Uuid,
    name: &'a str,
    marker: &'a str,
    secret: &'a str,
    sensitivity: &'a str,
    retention_policy_id: &'a str,
}

struct HybridFactFixture<'a> {
    name: &'a str,
    case_id: Uuid,
    namespace: &'a str,
    key: &'a str,
    marker: &'a str,
    vector_fixture: &'a str,
    sensitivity: &'a str,
}

struct TemporalFactFixture<'a> {
    name: &'a str,
    case_id: Uuid,
    namespace: &'a str,
    key: &'a str,
    marker: &'a str,
    vector_fixture: &'a str,
    observed_at: &'a str,
    valid_from: &'a str,
    write_policy_id: &'a str,
    confidence: f64,
}

struct TemporalSuccessorFixture<'a> {
    name: &'a str,
    marker: &'a str,
    vector_fixture: &'a str,
    observed_at: &'a str,
    valid_from: &'a str,
    write_policy_id: &'a str,
    confidence: f64,
    retention_policy_id: &'a str,
}

async fn create_hybrid_fact(
    client: &Client,
    target: &Target,
    fixture: HybridFactFixture<'_>,
) -> Result<FactView> {
    let episode_response = client
        .post(format!(
            "{}/v1/tenants/{}/subjects/{}/episodes",
            target.base_url.trim_end_matches('/'),
            target.tenant_id,
            target.subject_id
        ))
        .bearer_auth(&target.bearer_token)
        .header(
            "Idempotency-Key",
            format!("{}-episode-create", fixture.name),
        )
        .json(&json!({
            "case_id": fixture.case_id,
            "kind": "retrieval-fixture",
            "observed_at": "2026-04-20T09:00:00Z",
            "provenance": {
                "source_type": "conformance",
                "source_uri": null,
                "external_id": format!("{}-episode", fixture.name)
            },
            "sensitivity": fixture.sensitivity,
            "retention_policy_id": "standard",
            "payload": {"marker": fixture.marker}
        }))
        .send()
        .await?;
    ensure!(episode_response.status() == StatusCode::CREATED);
    let episode: Episode = episode_response.json().await?;
    let fact_response = client
        .post(format!(
            "{}/v1/tenants/{}/subjects/{}/facts",
            target.base_url.trim_end_matches('/'),
            target.tenant_id,
            target.subject_id
        ))
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", format!("{}-fact-create", fixture.name))
        .json(&json!({
            "case_id": fixture.case_id,
            "namespace": fixture.namespace,
            "key": fixture.key,
            "value": {
                "marker": fixture.marker,
                "vector_fixture": fixture.vector_fixture,
                "secret": if fixture.sensitivity == "restricted" {
                    "restricted-vector-trap"
                } else {
                    "allowed-vector-fixture"
                }
            },
            "observed_at": "2026-04-20T09:00:00Z",
            "valid_time": {"from": "2026-04-20T00:00:00Z", "until": null},
            "evidence_episode_ids": [episode.episode_id],
            "write_policy": {"id": "direct-evidence", "version": "1"},
            "confidence": 1.0,
            "sensitivity": fixture.sensitivity,
            "retention_policy_id": "standard"
        }))
        .send()
        .await?;
    ensure!(fact_response.status() == StatusCode::CREATED);
    fact_response.json().await.map_err(Into::into)
}

async fn create_temporal_fact(
    client: &Client,
    target: &Target,
    fixture: TemporalFactFixture<'_>,
) -> Result<FactView> {
    let episode_response = client
        .post(format!(
            "{}/v1/tenants/{}/subjects/{}/episodes",
            target.base_url.trim_end_matches('/'),
            target.tenant_id,
            target.subject_id
        ))
        .bearer_auth(&target.bearer_token)
        .header(
            "Idempotency-Key",
            format!("{}-episode-create", fixture.name),
        )
        .json(&json!({
            "case_id": fixture.case_id,
            "kind": "temporal-retrieval-fixture",
            "observed_at": fixture.observed_at,
            "provenance": {
                "source_type": "conformance",
                "source_uri": null,
                "external_id": format!("{}-episode", fixture.name)
            },
            "sensitivity": "internal",
            "retention_policy_id": "standard",
            "payload": {"marker": fixture.marker}
        }))
        .send()
        .await?;
    ensure!(episode_response.status() == StatusCode::CREATED);
    let episode: Episode = episode_response.json().await?;
    let fact_response = client
        .post(format!(
            "{}/v1/tenants/{}/subjects/{}/facts",
            target.base_url.trim_end_matches('/'),
            target.tenant_id,
            target.subject_id
        ))
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", format!("{}-fact-create", fixture.name))
        .json(&json!({
            "case_id": fixture.case_id,
            "namespace": fixture.namespace,
            "key": fixture.key,
            "value": {
                "marker": fixture.marker,
                "vector_fixture": fixture.vector_fixture,
                "version": "root"
            },
            "observed_at": fixture.observed_at,
            "valid_time": {"from": fixture.valid_from, "until": null},
            "evidence_episode_ids": [episode.episode_id],
            "write_policy": {"id": fixture.write_policy_id, "version": "1"},
            "confidence": fixture.confidence,
            "sensitivity": "internal",
            "retention_policy_id": "standard"
        }))
        .send()
        .await?;
    ensure!(fact_response.status() == StatusCode::CREATED);
    fact_response.json().await.map_err(Into::into)
}

async fn supersede_temporal_fact(
    client: &Client,
    target: &Target,
    root: &FactView,
    fixture: TemporalSuccessorFixture<'_>,
) -> Result<FactView> {
    let root_revision = root
        .revision
        .as_ref()
        .context("temporal successor root has no revision")?;
    let episode_response = client
        .post(format!(
            "{}/v1/tenants/{}/subjects/{}/episodes",
            target.base_url.trim_end_matches('/'),
            target.tenant_id,
            target.subject_id
        ))
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", format!("{}-episode", fixture.name))
        .json(&json!({
            "case_id": root.case_id,
            "kind": "temporal-retrieval-fixture",
            "observed_at": fixture.observed_at,
            "provenance": {
                "source_type": "conformance",
                "source_uri": null,
                "external_id": format!("{}-episode", fixture.name)
            },
            "sensitivity": "internal",
            "retention_policy_id": fixture.retention_policy_id,
            "payload": {"marker": fixture.marker}
        }))
        .send()
        .await?;
    ensure!(episode_response.status() == StatusCode::CREATED);
    let episode: Episode = episode_response.json().await?;
    let response = client
        .put(format!(
            "{}/v1/tenants/{}/subjects/{}/facts/{}",
            target.base_url.trim_end_matches('/'),
            target.tenant_id,
            target.subject_id,
            root.fact_id
        ))
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", fixture.name)
        .header(header::IF_MATCH, format!("\"{}\"", root.head_revision_id))
        .json(&json!({
            "supersedes_revision_id": root_revision.revision_id,
            "value": {
                "marker": fixture.marker,
                "vector_fixture": fixture.vector_fixture,
                "version": "successor"
            },
            "observed_at": fixture.observed_at,
            "valid_time": {"from": fixture.valid_from, "until": null},
            "evidence_episode_ids": [episode.episode_id],
            "write_policy": {"id": fixture.write_policy_id, "version": "1"},
            "confidence": fixture.confidence,
            "sensitivity": "internal",
            "retention_policy_id": fixture.retention_policy_id
        }))
        .send()
        .await?;
    ensure!(response.status() == StatusCode::OK);
    response.json().await.map_err(Into::into)
}

fn fact_revision_id(view: &FactView) -> Result<Uuid> {
    view.revision
        .as_ref()
        .map(|revision| revision.revision_id)
        .context("hybrid fixture fact has no effective revision")
}

async fn create_marker_fact(
    client: &Client,
    target: &Target,
    fixture: MarkerFactFixture<'_>,
) -> Result<FactView> {
    let episode_url = format!(
        "{}/v1/tenants/{}/subjects/{}/episodes",
        target.base_url.trim_end_matches('/'),
        fixture.tenant_id,
        fixture.subject_id
    );
    let episode_response = client
        .post(episode_url)
        .bearer_auth(fixture.bearer_token)
        .header(
            "Idempotency-Key",
            format!("{}-episode-create", fixture.name),
        )
        .json(&json!({
            "case_id": fixture.case_id,
            "kind": "retrieval-fixture",
            "observed_at": "2026-04-10T09:00:00Z",
            "provenance": {
                "source_type": "conformance",
                "source_uri": null,
                "external_id": format!("{}-episode", fixture.name)
            },
            "sensitivity": fixture.sensitivity,
            "retention_policy_id": fixture.retention_policy_id,
            "payload": {"marker": fixture.marker}
        }))
        .send()
        .await?;
    ensure!(episode_response.status() == StatusCode::CREATED);
    let episode: Episode = episode_response.json().await?;
    let fact_response = client
        .post(format!(
            "{}/v1/tenants/{}/subjects/{}/facts",
            target.base_url.trim_end_matches('/'),
            fixture.tenant_id,
            fixture.subject_id
        ))
        .bearer_auth(fixture.bearer_token)
        .header("Idempotency-Key", format!("{}-fact-create", fixture.name))
        .json(&json!({
            "case_id": fixture.case_id,
            "namespace": "case.retrieval",
            "key": fixture.name,
            "value": {"marker": fixture.marker, "secret": fixture.secret},
            "observed_at": "2026-04-10T09:00:00Z",
            "valid_time": {"from": "2026-04-10T00:00:00Z", "until": null},
            "evidence_episode_ids": [episode.episode_id],
            "write_policy": {"id": "direct-evidence", "version": "1"},
            "confidence": 1.0,
            "sensitivity": fixture.sensitivity,
            "retention_policy_id": fixture.retention_policy_id
        }))
        .send()
        .await?;
    ensure!(fact_response.status() == StatusCode::CREATED);
    fact_response.json().await.map_err(Into::into)
}
