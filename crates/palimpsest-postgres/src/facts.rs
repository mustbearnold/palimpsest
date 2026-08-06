//! facts — extracted from lib.rs by the ADR-0031 token-efficiency split (structure-only).

use async_trait::async_trait;
use palimpsest_application::{
    FactMutationOutcome, FactRepository, IdempotencyRequest, RepositoryError,
};
use palimpsest_domain::{
    CaseId, EpisodeId, FactId, FactKey, FactNamespace, FactRevision, FactView, NewFact,
    PrincipalId, RetentionPolicyId, RevisionId, Sensitivity, SubjectId, TenantId, ValidTime,
    WritePolicy, WritePolicyId, WritePolicyVersion,
};
use sqlx::{Postgres, Row, Transaction, postgres::PgRow};
use time::OffsetDateTime;

use super::retrieval::set_scope;
use super::write_path::{
    GovernedWrite, IdempotencyCompletion, IdempotencyScope, complete_idempotency,
    record_governed_write, reserve_idempotency,
};
use super::{PostgresMemoryRepository, map_sqlx, text_value_from_row, unexpected};

#[async_trait]
impl FactRepository for PostgresMemoryRepository {
    async fn create(
        &self,
        fact: NewFact,
        idempotency: IdempotencyRequest,
    ) -> Result<FactMutationOutcome, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, fact.tenant_id, fact.subject_id).await?;

        let idempotency_scope = IdempotencyScope {
            tenant_id: fact.tenant_id,
            subject_id: fact.subject_id,
            principal_id: &fact.writer_principal_id.0,
            operation_id: "createFact",
        };
        if let Some(response_body) =
            reserve_idempotency(&mut transaction, idempotency_scope, &idempotency).await?
        {
            let view: FactView = serde_json::from_value(response_body).map_err(unexpected)?;
            transaction.commit().await.map_err(unexpected)?;
            return Ok(FactMutationOutcome {
                view,
                replayed: true,
            });
        }

        sqlx::query(
            r#"
            INSERT INTO memory.facts (
                tenant_id, subject_id, case_id, fact_id, namespace, fact_key, schema_version
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(fact.tenant_id.0)
        .bind(fact.subject_id.0)
        .bind(fact.case_id.0)
        .bind(fact.fact_id.0)
        .bind(fact.namespace.as_str())
        .bind(fact.key.as_str())
        .bind(i32::try_from(fact.schema_version).map_err(|error| {
            RepositoryError::Unexpected(format!("schema version is out of range: {error}"))
        })?)
        .execute(&mut *transaction)
        .await
        .map_err(map_sqlx)?;

        let revision_row = sqlx::query(
            r#"
            INSERT INTO memory.fact_revisions (
                tenant_id, subject_id, case_id, fact_id, revision_id, revision_no,
                supersedes_revision_id, observed_at, valid_during, value, confidence,
                writer_principal_id, write_policy_id, write_policy_version,
                sensitivity, retention_policy_id, schema_version, content_sha256
            )
            VALUES (
                $1, $2, $3, $4, $5, 1, NULL, $6,
                tstzrange($7, $8, '[)'), $9, $10, $11, $12, $13, $14, $15, $16, $17
            )
            RETURNING recorded_at
            "#,
        )
        .bind(fact.tenant_id.0)
        .bind(fact.subject_id.0)
        .bind(fact.case_id.0)
        .bind(fact.fact_id.0)
        .bind(fact.revision_id.0)
        .bind(fact.observed_at)
        .bind(fact.valid_time.from)
        .bind(fact.valid_time.until)
        .bind(&fact.value)
        .bind(fact.confidence)
        .bind(&fact.writer_principal_id.0)
        .bind(fact.write_policy.id.as_str())
        .bind(fact.write_policy.version.as_str())
        .bind(fact.sensitivity.as_str())
        .bind(fact.retention_policy_id.as_str())
        .bind(i32::try_from(fact.schema_version).map_err(|error| {
            RepositoryError::Unexpected(format!("schema version is out of range: {error}"))
        })?)
        .bind(&fact.value_sha256)
        .fetch_one(&mut *transaction)
        .await
        .map_err(map_sqlx)?;
        let recorded_at: OffsetDateTime =
            revision_row.try_get("recorded_at").map_err(unexpected)?;

        for episode_id in &fact.evidence_episode_ids {
            sqlx::query(
                r#"
                INSERT INTO memory.fact_revision_evidence (
                    tenant_id, subject_id, case_id, fact_id, revision_id,
                    episode_id, evidence_role
                )
                VALUES ($1, $2, $3, $4, $5, $6, 'supporting')
                "#,
            )
            .bind(fact.tenant_id.0)
            .bind(fact.subject_id.0)
            .bind(fact.case_id.0)
            .bind(fact.fact_id.0)
            .bind(fact.revision_id.0)
            .bind(episode_id.0)
            .execute(&mut *transaction)
            .await
            .map_err(map_sqlx)?;
        }

        record_governed_write(
            &mut transaction,
            GovernedWrite {
                tenant_id: fact.tenant_id,
                subject_id: fact.subject_id,
                case_id: fact.case_id,
                principal_id: &fact.writer_principal_id.0,
                operation_id: "createFact",
                request_fingerprint: &idempotency.fingerprint,
                resource_episode_id: None,
                resource_fact_id: Some(fact.fact_id),
                resource_revision_id: Some(fact.revision_id),
                resource_checkpoint: None,
                event_type: "memory.fact.created.v1",
            },
        )
        .await?;

        let view = select_fact_view(
            &mut transaction,
            fact.tenant_id,
            fact.subject_id,
            fact.fact_id,
            recorded_at,
            recorded_at,
            recorded_at,
        )
        .await?
        .ok_or_else(|| {
            RepositoryError::Unexpected("created fact could not be reconstructed".to_owned())
        })?;
        let response_body = serde_json::to_value(&view).map_err(unexpected)?;
        let response_etag = format!("\"{}\"", view.head_revision_id.0);
        let response_location = format!(
            "/v1/tenants/{}/subjects/{}/facts/{}",
            view.tenant_id.0, view.subject_id.0, view.fact_id.0
        );
        complete_idempotency(
            &mut transaction,
            IdempotencyCompletion {
                scope: idempotency_scope,
                key: &idempotency.key,
                resource_episode_id: None,
                resource_fact_id: Some(fact.fact_id),
                resource_checkpoint: None,
                status: 201,
                body: response_body,
                etag: &response_etag,
                location: &response_location,
            },
        )
        .await?;

        transaction.commit().await.map_err(unexpected)?;
        Ok(FactMutationOutcome {
            view,
            replayed: false,
        })
    }

    async fn get_current(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        fact_id: FactId,
    ) -> Result<FactView, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, tenant_id, subject_id).await?;
        let evaluation_row = sqlx::query("SELECT clock_timestamp() AS evaluated_at")
            .fetch_one(&mut *transaction)
            .await
            .map_err(unexpected)?;
        let evaluated_at: OffsetDateTime =
            evaluation_row.try_get("evaluated_at").map_err(unexpected)?;
        let view = select_fact_view(
            &mut transaction,
            tenant_id,
            subject_id,
            fact_id,
            evaluated_at,
            evaluated_at,
            evaluated_at,
        )
        .await?;
        transaction.commit().await.map_err(unexpected)?;
        view.ok_or(RepositoryError::NotFound)
    }

    async fn supersede(
        &self,
        revision: palimpsest_domain::NewFactRevision,
        idempotency: IdempotencyRequest,
    ) -> Result<FactMutationOutcome, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, revision.tenant_id, revision.subject_id).await?;

        let idempotency_scope = IdempotencyScope {
            tenant_id: revision.tenant_id,
            subject_id: revision.subject_id,
            principal_id: &revision.writer_principal_id.0,
            operation_id: "supersedeFact",
        };
        if let Some(response_body) =
            reserve_idempotency(&mut transaction, idempotency_scope, &idempotency).await?
        {
            let view: FactView = serde_json::from_value(response_body).map_err(unexpected)?;
            transaction.commit().await.map_err(unexpected)?;
            return Ok(FactMutationOutcome {
                view,
                replayed: true,
            });
        }

        let lock_key = format!(
            "{}:{}:{}",
            revision.tenant_id.0, revision.subject_id.0, revision.fact_id.0
        );
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(lock_key)
            .execute(&mut *transaction)
            .await
            .map_err(unexpected)?;
        let head = sqlx::query(
            r#"
            SELECT facts.case_id, head.revision_id, head.revision_no
            FROM memory.facts AS facts
            JOIN LATERAL (
                SELECT revision_id, revision_no
                FROM memory.fact_revisions
                WHERE tenant_id = facts.tenant_id
                  AND subject_id = facts.subject_id
                  AND case_id = facts.case_id
                  AND fact_id = facts.fact_id
                ORDER BY revision_no DESC
                LIMIT 1
            ) AS head ON true
            WHERE facts.tenant_id = $1
              AND facts.subject_id = $2
              AND facts.fact_id = $3
            "#,
        )
        .bind(revision.tenant_id.0)
        .bind(revision.subject_id.0)
        .bind(revision.fact_id.0)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(unexpected)?
        .ok_or(RepositoryError::NotFound)?;
        let case_id = CaseId(head.try_get("case_id").map_err(unexpected)?);
        let head_revision_id = RevisionId(head.try_get("revision_id").map_err(unexpected)?);
        let head_revision_number: i64 = head.try_get("revision_no").map_err(unexpected)?;
        if revision.expected_head_revision_id != head_revision_id {
            return Err(RepositoryError::PreconditionFailed);
        }
        if revision.supersedes_revision_id != head_revision_id {
            return Err(RepositoryError::SupersessionConflict);
        }
        let next_revision_number = head_revision_number.checked_add(1).ok_or_else(|| {
            RepositoryError::Unexpected("fact revision number overflowed".to_owned())
        })?;

        let revision_row = sqlx::query(
            r#"
            INSERT INTO memory.fact_revisions (
                tenant_id, subject_id, case_id, fact_id, revision_id, revision_no,
                supersedes_revision_id, observed_at, valid_during, value, confidence,
                writer_principal_id, write_policy_id, write_policy_version,
                sensitivity, retention_policy_id, schema_version, content_sha256
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8,
                tstzrange($9, $10, '[)'), $11, $12, $13, $14, $15, $16, $17, $18, $19
            )
            RETURNING recorded_at
            "#,
        )
        .bind(revision.tenant_id.0)
        .bind(revision.subject_id.0)
        .bind(case_id.0)
        .bind(revision.fact_id.0)
        .bind(revision.revision_id.0)
        .bind(next_revision_number)
        .bind(revision.supersedes_revision_id.0)
        .bind(revision.observed_at)
        .bind(revision.valid_time.from)
        .bind(revision.valid_time.until)
        .bind(&revision.value)
        .bind(revision.confidence)
        .bind(&revision.writer_principal_id.0)
        .bind(revision.write_policy.id.as_str())
        .bind(revision.write_policy.version.as_str())
        .bind(revision.sensitivity.as_str())
        .bind(revision.retention_policy_id.as_str())
        .bind(i32::try_from(revision.schema_version).map_err(unexpected)?)
        .bind(&revision.value_sha256)
        .fetch_one(&mut *transaction)
        .await
        .map_err(map_sqlx)?;
        let recorded_at: OffsetDateTime =
            revision_row.try_get("recorded_at").map_err(unexpected)?;

        for episode_id in &revision.evidence_episode_ids {
            sqlx::query(
                r#"
                INSERT INTO memory.fact_revision_evidence (
                    tenant_id, subject_id, case_id, fact_id, revision_id,
                    episode_id, evidence_role
                )
                VALUES ($1, $2, $3, $4, $5, $6, 'supporting')
                "#,
            )
            .bind(revision.tenant_id.0)
            .bind(revision.subject_id.0)
            .bind(case_id.0)
            .bind(revision.fact_id.0)
            .bind(revision.revision_id.0)
            .bind(episode_id.0)
            .execute(&mut *transaction)
            .await
            .map_err(map_sqlx)?;
        }

        record_governed_write(
            &mut transaction,
            GovernedWrite {
                tenant_id: revision.tenant_id,
                subject_id: revision.subject_id,
                case_id,
                principal_id: &revision.writer_principal_id.0,
                operation_id: "supersedeFact",
                request_fingerprint: &idempotency.fingerprint,
                resource_episode_id: None,
                resource_fact_id: Some(revision.fact_id),
                resource_revision_id: Some(revision.revision_id),
                resource_checkpoint: None,
                event_type: "memory.fact.superseded.v1",
            },
        )
        .await?;

        let view = select_fact_view(
            &mut transaction,
            revision.tenant_id,
            revision.subject_id,
            revision.fact_id,
            recorded_at,
            recorded_at,
            recorded_at,
        )
        .await?
        .ok_or_else(|| {
            RepositoryError::Unexpected("superseded fact could not be reconstructed".to_owned())
        })?;
        let response_body = serde_json::to_value(&view).map_err(unexpected)?;
        let response_etag = format!("\"{}\"", view.head_revision_id.0);
        let response_location = format!(
            "/v1/tenants/{}/subjects/{}/facts/{}",
            view.tenant_id.0, view.subject_id.0, view.fact_id.0
        );
        complete_idempotency(
            &mut transaction,
            IdempotencyCompletion {
                scope: idempotency_scope,
                key: &idempotency.key,
                resource_episode_id: None,
                resource_fact_id: Some(revision.fact_id),
                resource_checkpoint: None,
                status: 200,
                body: response_body,
                etag: &response_etag,
                location: &response_location,
            },
        )
        .await?;

        transaction.commit().await.map_err(unexpected)?;
        Ok(FactMutationOutcome {
            view,
            replayed: false,
        })
    }

    async fn get_as_of(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        fact_id: FactId,
        valid_at: OffsetDateTime,
        recorded_at: OffsetDateTime,
    ) -> Result<FactView, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, tenant_id, subject_id).await?;
        let evaluation_row = sqlx::query("SELECT clock_timestamp() AS evaluated_at")
            .fetch_one(&mut *transaction)
            .await
            .map_err(unexpected)?;
        let evaluated_at: OffsetDateTime =
            evaluation_row.try_get("evaluated_at").map_err(unexpected)?;
        if recorded_at > evaluated_at {
            return Err(RepositoryError::FutureRecordedTime);
        }
        let view = select_fact_view(
            &mut transaction,
            tenant_id,
            subject_id,
            fact_id,
            valid_at,
            recorded_at,
            evaluated_at,
        )
        .await?;
        transaction.commit().await.map_err(unexpected)?;
        view.ok_or(RepositoryError::NotFound)
    }
}

pub(crate) async fn select_fact_view(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: TenantId,
    subject_id: SubjectId,
    fact_id: FactId,
    valid_at: OffsetDateTime,
    recorded_at: OffsetDateTime,
    evaluated_at: OffsetDateTime,
) -> Result<Option<FactView>, RepositoryError> {
    let metadata = sqlx::query(
        r#"
        SELECT case_id, namespace, fact_key
        FROM memory.facts
        WHERE tenant_id = $1 AND subject_id = $2 AND fact_id = $3
        "#,
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .bind(fact_id.0)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(unexpected)?;
    let Some(metadata) = metadata else {
        return Ok(None);
    };
    let case_id = CaseId(metadata.try_get("case_id").map_err(unexpected)?);
    let namespace = text_value_from_row::<FactNamespace>(&metadata, "namespace")?;
    let key = text_value_from_row::<FactKey>(&metadata, "fact_key")?;

    let head = sqlx::query(
        r#"
        SELECT revision_id
        FROM memory.fact_revisions
        WHERE tenant_id = $1 AND subject_id = $2 AND fact_id = $3
          AND recorded_at <= $4
        ORDER BY revision_no DESC
        LIMIT 1
        "#,
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .bind(fact_id.0)
    .bind(recorded_at)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(unexpected)?;
    let Some(head) = head else {
        return Ok(None);
    };
    let head_revision_id = RevisionId(head.try_get("revision_id").map_err(unexpected)?);

    let revision = sqlx::query(
        r#"
        SELECT fr.case_id, fr.revision_id, fr.revision_no,
            fr.supersedes_revision_id, fr.observed_at, fr.recorded_at,
            lower(fr.valid_during) AS valid_from,
            CASE WHEN upper_inf(fr.valid_during) THEN NULL
                 ELSE upper(fr.valid_during) END AS valid_until,
            fr.value, fr.confidence::double precision AS confidence,
            fr.writer_principal_id, fr.write_policy_id, fr.write_policy_version,
            fr.sensitivity, fr.retention_policy_id, fr.schema_version,
            ARRAY(
                SELECT evidence.episode_id
                FROM memory.fact_revision_evidence AS evidence
                WHERE evidence.tenant_id = fr.tenant_id
                  AND evidence.subject_id = fr.subject_id
                  AND evidence.case_id = fr.case_id
                  AND evidence.fact_id = fr.fact_id
                  AND evidence.revision_id = fr.revision_id
                ORDER BY evidence.episode_id
            ) AS evidence_episode_ids
        FROM memory.fact_revisions AS fr
        WHERE fr.tenant_id = $1 AND fr.subject_id = $2 AND fr.fact_id = $3
          AND fr.valid_during @> $4::timestamptz
          AND fr.recorded_at <= $5
        ORDER BY fr.revision_no DESC
        LIMIT 1
        "#,
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .bind(fact_id.0)
    .bind(valid_at)
    .bind(recorded_at)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(unexpected)?;
    let revision = revision
        .as_ref()
        .map(|row| fact_revision_from_row(row, tenant_id, subject_id, fact_id, &namespace, &key))
        .transpose()?;

    Ok(Some(FactView {
        tenant_id,
        subject_id,
        case_id,
        fact_id,
        namespace,
        key,
        head_revision_id,
        evaluated_at,
        valid_at,
        recorded_at,
        revision,
    }))
}

pub(crate) fn fact_revision_from_row(
    row: &PgRow,
    tenant_id: TenantId,
    subject_id: SubjectId,
    fact_id: FactId,
    namespace: &FactNamespace,
    key: &FactKey,
) -> Result<FactRevision, RepositoryError> {
    let revision_number: i64 = row.try_get("revision_no").map_err(unexpected)?;
    let schema_version: i32 = row.try_get("schema_version").map_err(unexpected)?;
    let supersedes_revision_id: Option<uuid::Uuid> =
        row.try_get("supersedes_revision_id").map_err(unexpected)?;
    let evidence_episode_ids: Vec<uuid::Uuid> =
        row.try_get("evidence_episode_ids").map_err(unexpected)?;
    Ok(FactRevision {
        tenant_id,
        subject_id,
        case_id: CaseId(row.try_get("case_id").map_err(unexpected)?),
        fact_id,
        revision_id: RevisionId(row.try_get("revision_id").map_err(unexpected)?),
        revision_number: u64::try_from(revision_number).map_err(unexpected)?,
        supersedes_revision_id: supersedes_revision_id.map(RevisionId),
        namespace: namespace.clone(),
        key: key.clone(),
        value: row.try_get("value").map_err(unexpected)?,
        observed_at: row.try_get("observed_at").map_err(unexpected)?,
        recorded_at: row.try_get("recorded_at").map_err(unexpected)?,
        valid_time: ValidTime {
            from: row.try_get("valid_from").map_err(unexpected)?,
            until: row.try_get("valid_until").map_err(unexpected)?,
        },
        evidence_episode_ids: evidence_episode_ids.into_iter().map(EpisodeId).collect(),
        write_policy: WritePolicy {
            id: text_value_from_row::<WritePolicyId>(row, "write_policy_id")?,
            version: text_value_from_row::<WritePolicyVersion>(row, "write_policy_version")?,
        },
        confidence: row.try_get("confidence").map_err(unexpected)?,
        sensitivity: text_value_from_row::<Sensitivity>(row, "sensitivity")?,
        retention_policy_id: text_value_from_row::<RetentionPolicyId>(row, "retention_policy_id")?,
        writer_principal_id: PrincipalId(row.try_get("writer_principal_id").map_err(unexpected)?),
        schema_version: u32::try_from(schema_version).map_err(unexpected)?,
    })
}
