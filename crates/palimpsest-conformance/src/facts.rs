//! facts — extracted from lib.rs by the ADR-0031 token-efficiency split (structure-only).

use anyhow::{Context, Result, ensure};
use reqwest::{Client, StatusCode, header};
use serde_json::{Value, json};
use uuid::Uuid;

use super::common::{Episode, FactView, RetrievalReceipt, Target};

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

pub(crate) struct PrivateFactFixture<'a> {
    bearer_token: &'a str,
    tenant_id: Uuid,
    subject_id: Uuid,
    case_id: Uuid,
    name: &'a str,
    secret: &'a str,
}

pub(crate) async fn create_private_fact(
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

pub(crate) struct MarkerFactFixture<'a> {
    pub(crate) bearer_token: &'a str,
    pub(crate) tenant_id: Uuid,
    pub(crate) subject_id: Uuid,
    pub(crate) case_id: Uuid,
    pub(crate) name: &'a str,
    pub(crate) marker: &'a str,
    pub(crate) secret: &'a str,
    pub(crate) sensitivity: &'a str,
    pub(crate) retention_policy_id: &'a str,
}

pub(crate) struct HybridFactFixture<'a> {
    pub(crate) name: &'a str,
    pub(crate) case_id: Uuid,
    pub(crate) namespace: &'a str,
    pub(crate) key: &'a str,
    pub(crate) marker: &'a str,
    pub(crate) vector_fixture: &'a str,
    pub(crate) sensitivity: &'a str,
}

pub(crate) struct TemporalFactFixture<'a> {
    pub(crate) name: &'a str,
    pub(crate) case_id: Uuid,
    pub(crate) namespace: &'a str,
    pub(crate) key: &'a str,
    pub(crate) marker: &'a str,
    pub(crate) vector_fixture: &'a str,
    pub(crate) observed_at: &'a str,
    pub(crate) valid_from: &'a str,
    pub(crate) write_policy_id: &'a str,
    pub(crate) confidence: f64,
}

pub(crate) struct TemporalSuccessorFixture<'a> {
    pub(crate) name: &'a str,
    pub(crate) marker: &'a str,
    pub(crate) vector_fixture: &'a str,
    pub(crate) observed_at: &'a str,
    pub(crate) valid_from: &'a str,
    pub(crate) write_policy_id: &'a str,
    pub(crate) confidence: f64,
    pub(crate) retention_policy_id: &'a str,
}

pub(crate) async fn create_hybrid_fact(
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

pub(crate) async fn create_temporal_fact(
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

pub(crate) async fn supersede_temporal_fact(
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

pub(crate) fn fact_revision_id(view: &FactView) -> Result<Uuid> {
    view.revision
        .as_ref()
        .map(|revision| revision.revision_id)
        .context("hybrid fixture fact has no effective revision")
}

pub(crate) async fn create_marker_fact(
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
