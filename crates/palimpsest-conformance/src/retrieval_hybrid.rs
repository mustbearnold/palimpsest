//! retrieval_hybrid — extracted from retrieval.rs by the ADR-0031 token-efficiency split (structure-only).

//! retrieval — extracted from lib.rs by the ADR-0031 token-efficiency split (structure-only).

use anyhow::{Context, Result, ensure};
use reqwest::{Client, StatusCode, header};
use serde_json::{Value, json};
use uuid::Uuid;

use super::common::{Episode, RetrievalReceipt, Target};
use super::facts::{
    HybridFactFixture, MarkerFactFixture, create_hybrid_fact, create_marker_fact, fact_revision_id,
};

use super::retrieval_asserts::{
    assert_hybrid_receipt, assert_retryable_hybrid_failure, assert_write_policy_rejected,
    hybrid_request_body, retrievals_url,
};
use super::retrieval_fixtures::{HybridFusionFixture, HybridReplayFixture};

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
