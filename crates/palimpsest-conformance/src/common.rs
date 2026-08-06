//! common — extracted from lib.rs by the ADR-0031 token-efficiency split (structure-only).

use anyhow::{Context, Result, ensure};
use reqwest::{StatusCode, header};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Provenance {
    pub(crate) source_type: String,
    source_uri: Option<String>,
    pub(crate) external_id: Option<String>,
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
pub(crate) struct Episode {
    pub(crate) tenant_id: Uuid,
    pub(crate) subject_id: Uuid,
    pub(crate) case_id: Uuid,
    pub(crate) episode_id: Uuid,
    kind: String,
    pub(crate) observed_at: String,
    pub(crate) recorded_at: String,
    writer_principal_id: String,
    pub(crate) provenance: Provenance,
    sensitivity: String,
    retention_policy_id: String,
    pub(crate) schema_version: u32,
    pub(crate) payload: Value,
    pub(crate) payload_sha256: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct ValidTime {
    from: String,
    until: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct WritePolicy {
    id: String,
    version: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct FactRevision {
    pub(crate) tenant_id: Uuid,
    pub(crate) subject_id: Uuid,
    pub(crate) case_id: Uuid,
    pub(crate) fact_id: Uuid,
    pub(crate) revision_id: Uuid,
    pub(crate) revision_number: u64,
    pub(crate) supersedes_revision_id: Option<Uuid>,
    pub(crate) namespace: String,
    pub(crate) key: String,
    value: Value,
    observed_at: String,
    pub(crate) recorded_at: String,
    valid_time: ValidTime,
    pub(crate) evidence_episode_ids: Vec<Uuid>,
    write_policy: WritePolicy,
    confidence: f64,
    sensitivity: String,
    retention_policy_id: String,
    pub(crate) writer_principal_id: String,
    schema_version: u32,
}

#[derive(Debug, Deserialize)]
pub(crate) struct FactView {
    pub(crate) tenant_id: Uuid,
    pub(crate) subject_id: Uuid,
    pub(crate) case_id: Uuid,
    pub(crate) fact_id: Uuid,
    pub(crate) namespace: String,
    pub(crate) key: String,
    pub(crate) head_revision_id: Uuid,
    pub(crate) evaluated_at: String,
    pub(crate) valid_at: String,
    pub(crate) recorded_at: String,
    pub(crate) revision: Option<FactRevision>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct RetrievalPolicy {
    pub(crate) id: String,
    pub(crate) version: String,
    pub(crate) digest: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct RetrievalAuthorization {
    pub(crate) decision: String,
    pub(crate) scope_digest: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct RetrievalScore {
    pub(crate) component: String,
    pub(crate) value: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct RetrievalEmbeddingLineage {
    pub(crate) profile_id: String,
    pub(crate) profile_version: String,
    pub(crate) profile_digest: String,
    pub(crate) projection_sha256: String,
    pub(crate) input_sha256: String,
    pub(crate) vector_sha256: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct RetrievalQueryEmbeddingLineage {
    pub(crate) profile_id: String,
    pub(crate) profile_version: String,
    pub(crate) profile_digest: String,
    pub(crate) projection_profile_id: String,
    pub(crate) projection_profile_version: String,
    pub(crate) projection_profile_digest: String,
    pub(crate) input_sha256: String,
    pub(crate) vector_sha256: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct RetrievalItem {
    pub(crate) memory_kind: String,
    fact_id: Uuid,
    pub(crate) revision_id: Uuid,
    pub(crate) namespace: String,
    pub(crate) key: String,
    pub(crate) value: Value,
    pub(crate) evidence_episode_ids: Vec<Uuid>,
    pub(crate) scores: Vec<RetrievalScore>,
    #[serde(default)]
    pub(crate) embedding: Option<RetrievalEmbeddingLineage>,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct RetrievalReceipt {
    pub(crate) tenant_id: Uuid,
    pub(crate) subject_id: Uuid,
    pub(crate) retrieval_id: Uuid,
    pub(crate) status: String,
    pub(crate) evaluated_at: String,
    pub(crate) valid_at: String,
    pub(crate) recorded_at: String,
    pub(crate) policy: RetrievalPolicy,
    pub(crate) authorization: RetrievalAuthorization,
    pub(crate) document_schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) query_embedding: Option<RetrievalQueryEmbeddingLineage>,
    pub(crate) items: Vec<RetrievalItem>,
    pub(crate) next_cursor: Option<String>,
}

pub(crate) fn ensure_lexical_only_scores(item: &RetrievalItem) -> Result<()> {
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
pub(crate) struct Checkpoint {
    pub(crate) tenant_id: Uuid,
    pub(crate) subject_id: Uuid,
    pub(crate) case_id: Uuid,
    pub(crate) agent_id: Uuid,
    pub(crate) thread_id: Uuid,
    pub(crate) checkpoint_id: Uuid,
    pub(crate) checkpoint_revision_id: Uuid,
    pub(crate) revision_number: u64,
    pub(crate) parent_revision_id: Option<Uuid>,
    pub(crate) recorded_at: String,
    pub(crate) state: Value,
    pub(crate) state_schema_version: u32,
    pub(crate) state_sha256: String,
    pub(crate) effects: Vec<CheckpointEffect>,
    pub(crate) provenance: Provenance,
    sensitivity: String,
    pub(crate) retention_policy_id: String,
    pub(crate) expires_at: String,
    pub(crate) writer_principal_id: String,
    pub(crate) schema_version: u32,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct CheckpointEffect {
    pub(crate) effect_id: Uuid,
    pub(crate) effect_key: String,
    pub(crate) kind: String,
    pub(crate) recovery_mode: String,
    pub(crate) status: String,
    prepared_at: String,
    pub(crate) completed_at: Option<String>,
    pub(crate) receipt: Option<EffectReceipt>,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct EffectReceipt {
    observed_at: String,
    pub(crate) external_reference: Option<String>,
    outcome_sha256: String,
}

pub(crate) async fn assert_problem(
    response: reqwest::Response,
    status: StatusCode,
    kind: &str,
) -> Result<()> {
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
