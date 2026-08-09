//! retrieval_temporal — extracted from retrieval.rs by the ADR-0031 token-efficiency split (structure-only).

//! retrieval — extracted from lib.rs by the ADR-0031 token-efficiency split (structure-only).

use anyhow::{Context, Result, ensure};
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use uuid::Uuid;

use super::common::{RetrievalReceipt, Target};
use super::facts::{
    TemporalFactFixture, TemporalSuccessorFixture, create_temporal_fact, fact_revision_id,
    supersede_temporal_fact,
};

use super::retrieval_asserts::{
    assert_temporal_lifecycle_receipt_hidden, assert_temporal_receipt, create_temporal_receipt,
    retrievals_url, temporal_fixed_request_body, temporal_fixture_case_ids, temporal_request_body,
};
use super::retrieval_fixtures::{
    RetrievalIsolationFixture, TemporalLifecycleFixture, TemporalLifecycleReceiptFixture,
    TemporalLifecycleReplayFixture, TemporalReplayFixture, TemporalRetrievalFixture,
    TemporalRuntimeReplayFixture,
};

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
    // Two independent replays prove the deterministic replay and the unique
    // retrieval IDs. Further replays repeat the same check.
    for repeat in 1..3 {
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
    migration_pool: &sqlx::PgPool,
) -> Result<()> {
    // Rewind the expired successor retention instead of waiting for it.
    let expired_successor_revision_id = fixture.expired_successor_revision_id;
    crate::rewind_expiry_under_disabled_trigger(
        migration_pool,
        "memory.fact_revision_governance",
        "fact_revision_governance_restrict_mutation",
        "retention_expires_at",
        &format!("revision_id = '{expired_successor_revision_id}'"),
        "rewind the expired successor retention",
    )
    .await?;
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
