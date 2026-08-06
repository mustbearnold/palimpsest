//! corpus — extracted from conformance_postgres18.rs by the ADR-0031 token-efficiency split (structure-only).

use anyhow::{Context, Result, bail, ensure};
use reqwest::{Client, StatusCode};
use serde_json::json;
use std::collections::BTreeMap;

use palimpsest_conformance::Target;
use palimpsest_conformance::retrieval_evaluation::{LifecycleFixture, PreparedCorpus};
use palimpsest_domain::{SubjectId, TenantId};
use palimpsest_postgres::EmbeddingProjectionCoordinator;
use sqlx::PgPool;
use uuid::Uuid;

use super::deletion_ops::transition_revision_to_deleted;
use super::fixtures::{DeterministicEmbeddingProvider, EmbeddingFixtureMode};
use super::projection_helpers::set_retrieval_test_scope;

pub(crate) async fn apply_corpus_lifecycle(
    pool: &PgPool,
    target: &Target,
    prepared: &PreparedCorpus,
) -> Result<()> {
    for mutation in &prepared.lifecycle {
        ensure!(
            mutation.tenant_id == target.tenant_id && mutation.subject_id == target.subject_id,
            "corpus lifecycle mutation escaped the primary test scope"
        );
        match mutation.lifecycle {
            LifecycleFixture::Deleted => {
                let mut transaction = pool.begin().await?;
                set_retrieval_test_scope(&mut transaction, target).await?;
                transition_revision_to_deleted(&mut transaction, target, mutation.revision_id)
                    .await?;
                transaction.commit().await?;
            }
            LifecycleFixture::Expired => {}
            LifecycleFixture::Active => bail!("active corpus fact requested a lifecycle mutation"),
        }
    }
    Ok(())
}

pub(crate) async fn verify_corpus_error_surface_redaction(
    target: &Target,
    corpus: &palimpsest_conformance::retrieval_evaluation::Corpus,
    prepared: &PreparedCorpus,
    provider: &DeterministicEmbeddingProvider,
) -> Result<()> {
    let scenario = corpus
        .scenarios
        .iter()
        .find(|scenario| !scenario.forbidden_ids.is_empty())
        .context("corpus has no forbidden-ID error probe")?;
    provider.set_mode(EmbeddingFixtureMode::Unavailable);
    let response = Client::new()
        .post(format!(
            "{}/v1/tenants/{}/subjects/{}/retrievals",
            target.base_url.trim_end_matches('/'),
            target.tenant_id,
            target.subject_id
        ))
        .bearer_auth(&target.principal_a_internal_bearer_token)
        .header("Idempotency-Key", "corpus-forbidden-error-redaction")
        .json(&json!({
            "query": scenario.query,
            "perspective": {"kind": "current"},
            "page_size": 10,
            "policy_id": "retrieval-hybrid-temporal-v1",
            "filters": {"case_ids": [scenario.case_id]}
        }))
        .send()
        .await;
    provider.set_mode(EmbeddingFixtureMode::Valid);
    let response = response?;
    ensure!(response.status() == StatusCode::SERVICE_UNAVAILABLE);
    let raw = response.text().await?;
    for logical_id in corpus
        .scenarios
        .iter()
        .flat_map(|scenario| &scenario.forbidden_ids)
    {
        let revision_id = prepared
            .revisions
            .get(logical_id)
            .context("missing forbidden revision for error probe")?;
        ensure!(!raw.contains(logical_id));
        ensure!(!raw.contains(&revision_id.to_string()));
    }
    Ok(())
}

pub(crate) async fn verify_corpus_manifests_exclude_forbidden(
    pool: &PgPool,
    corpus: &palimpsest_conformance::retrieval_evaluation::Corpus,
    prepared: &PreparedCorpus,
) -> Result<()> {
    let forbidden = corpus
        .scenarios
        .iter()
        .flat_map(|scenario| scenario.forbidden_ids.iter())
        .map(|logical| {
            prepared
                .revisions
                .get(logical)
                .copied()
                .with_context(|| format!("missing forbidden corpus revision {logical}"))
        })
        .collect::<Result<Vec<_>>>()?;
    let leaked: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM memory.retrieval_manifest_items
        WHERE revision_id = ANY($1::uuid[])
        "#,
    )
    .bind(&forbidden)
    .fetch_one(pool)
    .await?;
    ensure!(
        leaked == 0,
        "forbidden corpus revisions entered durable manifests"
    );
    Ok(())
}

pub(crate) async fn rebuild_corpus_projections(
    pool: &PgPool,
    coordinator: &EmbeddingProjectionCoordinator,
    target: &Target,
    prepared: &PreparedCorpus,
) -> Result<()> {
    let mut scopes = BTreeMap::<(Uuid, Uuid), Vec<Uuid>>::new();
    for projection in &prepared.projections {
        scopes
            .entry((projection.tenant_id, projection.subject_id))
            .or_default()
            .push(projection.revision_id);
    }
    for ((tenant_id, subject_id), revision_ids) in scopes {
        let scoped_target = Target {
            tenant_id,
            subject_id,
            ..target.clone()
        };
        let mut transaction = pool.begin().await?;
        set_retrieval_test_scope(&mut transaction, &scoped_target).await?;
        sqlx::query(
            r#"
            DELETE FROM memory.fact_revision_embedding_projections
            WHERE revision_id = ANY($1::uuid[])
            "#,
        )
        .bind(&revision_ids)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"
            DELETE FROM memory.fact_revision_search_documents
            WHERE revision_id = ANY($1::uuid[])
            "#,
        )
        .bind(&revision_ids)
        .execute(&mut *transaction)
        .await?;
        let rebuilt_search = sqlx::query(
            r#"
            INSERT INTO memory.fact_revision_search_documents (
                tenant_id, subject_id, case_id, fact_id, revision_id,
                projection_schema_version, projection_schema_sha256,
                source_content_sha256, projection_sha256, search_vector
            )
            SELECT revision.tenant_id, revision.subject_id, revision.case_id,
                revision.fact_id, revision.revision_id,
                projection.projection_schema_version, projection.projection_sha256,
                revision.content_sha256,
                memory.fact_projection_sha256_v1(
                    fact.namespace, fact.fact_key, revision.value
                ),
                memory.fact_search_vector_v1(
                    fact.namespace, fact.fact_key, revision.value
                )
            FROM memory.fact_revisions AS revision
            JOIN memory.facts AS fact
              ON fact.tenant_id = revision.tenant_id
             AND fact.subject_id = revision.subject_id
             AND fact.case_id = revision.case_id
             AND fact.fact_id = revision.fact_id
            CROSS JOIN memory.search_projection_schemas AS projection
            WHERE revision.revision_id = ANY($1::uuid[])
              AND projection.projection_schema_version = 1
            "#,
        )
        .bind(&revision_ids)
        .execute(&mut *transaction)
        .await?;
        ensure!(
            rebuilt_search.rows_affected() == revision_ids.len() as u64,
            "corpus search-projection rebuild was incomplete"
        );
        transaction.commit().await?;
        let rebuilt = coordinator
            .rebuild_pending(TenantId(tenant_id), SubjectId(subject_id), 1_000)
            .await?;
        ensure!(rebuilt.failed == 0, "corpus projection rebuild failed");
    }
    Ok(())
}
