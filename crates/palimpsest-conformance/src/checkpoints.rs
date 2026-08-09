//! checkpoints — extracted from lib.rs by the ADR-0031 token-efficiency split (structure-only).

use anyhow::{Context, Result, ensure};
use reqwest::{Client, StatusCode, header};
use serde_json::{Value, json};
use uuid::Uuid;

use super::common::{Checkpoint, Target, assert_problem};

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

pub async fn expires_only_the_targeted_checkpoint(
    target: &Target,
    migration_pool: &sqlx::PgPool,
) -> Result<()> {
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

    // Rewind the short-lived checkpoint expiry instead of waiting for it.
    // Both the head revision and the checkpoint row store the expiry, and
    // both must read as expired for the deadline probes.
    let checkpoint_revision_id = short_lived.checkpoint_revision_id;
    let revision_rewind = format!(
        "UPDATE memory.checkpoint_revisions \
         SET expires_at = clock_timestamp() - interval '1 second' \
         WHERE revision_id = '{checkpoint_revision_id}'"
    );
    let rewound = crate::rewind_under_disabled_trigger(
        migration_pool,
        "memory.checkpoint_revisions",
        "checkpoint_revisions_reject_mutation",
        &revision_rewind,
    )
    .await
    .context("rewind the short-lived checkpoint revision expiry")?;
    ensure!(
        rewound >= 1,
        "the checkpoint revision rewind missed the short-lived revision"
    );
    let checkpoint_rewind = format!(
        "UPDATE memory.checkpoints \
         SET expires_at = clock_timestamp() - interval '1 second' \
         WHERE head_revision_id = '{checkpoint_revision_id}'"
    );
    let rewound = crate::rewind_under_disabled_trigger(
        migration_pool,
        "memory.checkpoints",
        "checkpoints_prepare_transition",
        &checkpoint_rewind,
    )
    .await
    .context("rewind the short-lived checkpoint expiry")?;
    ensure!(
        rewound >= 1,
        "the checkpoint rewind missed the short-lived checkpoint"
    );
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
