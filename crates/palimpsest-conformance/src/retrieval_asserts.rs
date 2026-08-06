//! retrieval_asserts — extracted from retrieval.rs by the ADR-0031 token-efficiency split (structure-only).

//! retrieval — extracted from lib.rs by the ADR-0031 token-efficiency split (structure-only).

use anyhow::{Context, Result, bail, ensure};
use reqwest::{Client, StatusCode, header};
use serde_json::{Value, json};
use uuid::Uuid;

use super::common::{RetrievalItem, RetrievalReceipt, Target};

use super::retrieval_fixtures::{
    HybridFusionFixture, TemporalLifecycleReceiptFixture, TemporalRetrievalFixture,
};

pub(crate) async fn assert_write_policy_rejected(response: reqwest::Response) -> Result<()> {
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

pub(crate) async fn create_temporal_receipt(
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

pub(crate) fn temporal_fixed_request_body(recorded_at: &str) -> Value {
    temporal_request_body(
        json!({
            "kind": "as_of",
            "valid_at": "2026-06-30T00:00:00Z",
            "recorded_at": recorded_at
        }),
        temporal_fixture_case_ids(),
    )
}

pub(crate) fn temporal_fixture_case_ids() -> Value {
    json!([
        "019be000-0000-7000-8000-000000000501",
        "019be000-0000-7000-8000-000000000502",
        "019be000-0000-7000-8000-000000000503",
        "019be000-0000-7000-8000-000000000504",
        "019be000-0000-7000-8000-000000000505"
    ])
}

pub(crate) fn temporal_request_body(perspective: Value, case_ids: Value) -> Value {
    json!({
        "query": "case.temporal:chronotoken",
        "perspective": perspective,
        "page_size": 10,
        "policy_id": "retrieval-hybrid-temporal-v1",
        "filters": {"case_ids": case_ids}
    })
}

pub(crate) fn assert_temporal_receipt(
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

pub(crate) fn assert_temporal_lifecycle_receipt_hidden(
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

pub(crate) fn assert_hybrid_receipt(
    receipt: &RetrievalReceipt,
    fixture: &HybridFusionFixture,
) -> Result<()> {
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

pub(crate) fn assert_score(item: &RetrievalItem, component: &str, expected: &str) -> Result<()> {
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

pub(crate) fn assert_optional_score(
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

pub(crate) async fn assert_retryable_hybrid_failure(
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

pub(crate) fn retrievals_url(target: &Target) -> String {
    format!(
        "{}/v1/tenants/{}/subjects/{}/retrievals",
        target.base_url.trim_end_matches('/'),
        target.tenant_id,
        target.subject_id
    )
}

pub(crate) fn hybrid_request_body() -> Value {
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
