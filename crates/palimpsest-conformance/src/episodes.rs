//! episodes — extracted from lib.rs by the ADR-0031 token-efficiency split (structure-only).

use anyhow::{Context, Result, ensure};
use reqwest::{Client, StatusCode, header};
use serde_json::json;
use uuid::Uuid;

use super::common::{Episode, Target, assert_problem};

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
