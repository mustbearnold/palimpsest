//! retrieval — extracted from lib.rs by the ADR-0031 token-efficiency split (structure-only).

use anyhow::{Context, Result, ensure};
use reqwest::{Client, StatusCode, header};
use serde_json::{Value, json};
use std::time::Duration;
use uuid::Uuid;

use super::common::{
    Episode, FactView, RetrievalReceipt, Target, assert_problem, ensure_lexical_only_scores,
};
use super::facts::{MarkerFactFixture, create_marker_fact};
use super::retrieval_fixtures::{RetrievalIsolationFixture, RetrievalLifecycleFixture};

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

pub(crate) async fn retrieval_fails_closed_for_projection_state(
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

pub async fn retrieval_receipt_hides_expired_content(
    target: &Target,
    migration_pool: &sqlx::PgPool,
) -> Result<()> {
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
    // Rewind the marker fact retention expiry instead of waiting for it.
    crate::rewind_expiry_under_disabled_trigger(
        migration_pool,
        "memory.fact_revision_governance",
        "fact_revision_governance_restrict_mutation",
        "retention_expires_at",
        &format!("revision_id = '{revision_id}'"),
        "rewind the marker fact retention expiry",
    )
    .await?;
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
