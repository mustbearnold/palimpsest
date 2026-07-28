use async_trait::async_trait;
use palimpsest_application::{
    AppendOutcome, EpisodeRepository, FactMutationOutcome, FactRepository, IdempotencyRequest,
    RepositoryError,
};
use palimpsest_domain::{
    CaseId, Episode, EpisodeId, FactId, FactRevision, FactView, NewEpisode, NewFact, PrincipalId,
    Provenance, RevisionId, SubjectId, TenantId, ValidTime, WritePolicy,
};
use sqlx::{PgPool, Postgres, Row, Transaction, postgres::PgRow};
use time::OffsetDateTime;

#[derive(Clone)]
pub struct PostgresEpisodeRepository {
    pool: PgPool,
}

#[async_trait]
impl FactRepository for PostgresEpisodeRepository {
    async fn create(
        &self,
        fact: NewFact,
        idempotency: IdempotencyRequest,
    ) -> Result<FactMutationOutcome, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, fact.tenant_id, fact.subject_id).await?;

        let reserved = sqlx::query(
            r#"
            INSERT INTO memory.idempotency_receipts (
                tenant_id, subject_id, principal_id, operation_id,
                idempotency_key, request_fingerprint, state
            )
            VALUES ($1, $2, $3, 'createFact', $4, $5, 'in_progress')
            ON CONFLICT (tenant_id, principal_id, operation_id, idempotency_key)
                DO NOTHING
            RETURNING true AS reserved
            "#,
        )
        .bind(fact.tenant_id.0)
        .bind(fact.subject_id.0)
        .bind(&fact.writer_principal_id.0)
        .bind(&idempotency.key)
        .bind(&idempotency.fingerprint)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(unexpected)?
        .is_some();

        if !reserved {
            let receipt = sqlx::query(
                r#"
                SELECT request_fingerprint, state, resource_fact_id, response_body
                FROM memory.idempotency_receipts
                WHERE tenant_id = $1
                  AND principal_id = $2
                  AND operation_id = 'createFact'
                  AND idempotency_key = $3
                FOR UPDATE
                "#,
            )
            .bind(fact.tenant_id.0)
            .bind(&fact.writer_principal_id.0)
            .bind(&idempotency.key)
            .fetch_one(&mut *transaction)
            .await
            .map_err(unexpected)?;
            let stored_fingerprint: String =
                receipt.try_get("request_fingerprint").map_err(unexpected)?;
            if stored_fingerprint != idempotency.fingerprint {
                return Err(RepositoryError::IdempotencyKeyReused);
            }
            let state: String = receipt.try_get("state").map_err(unexpected)?;
            if state != "completed" {
                return Err(RepositoryError::IdempotencyInProgress);
            }
            let resource_fact_id: Option<uuid::Uuid> =
                receipt.try_get("resource_fact_id").map_err(unexpected)?;
            resource_fact_id.ok_or_else(|| {
                RepositoryError::Unexpected("completed idempotency receipt has no fact".to_owned())
            })?;
            let response_body: serde_json::Value =
                receipt.try_get("response_body").map_err(unexpected)?;
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
        .bind(&fact.namespace)
        .bind(&fact.key)
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
        .bind(&fact.write_policy.id)
        .bind(&fact.write_policy.version)
        .bind(&fact.sensitivity)
        .bind(&fact.retention_policy_id)
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
        sqlx::query(
            r#"
            UPDATE memory.idempotency_receipts
            SET state = 'completed', resource_fact_id = $1,
                response_status = 201, response_body = $2, response_etag = $3,
                response_location = $4, completed_at = clock_timestamp()
            WHERE tenant_id = $5
              AND principal_id = $6
              AND operation_id = 'createFact'
              AND idempotency_key = $7
              AND state = 'in_progress'
            "#,
        )
        .bind(fact.fact_id.0)
        .bind(response_body)
        .bind(response_etag)
        .bind(response_location)
        .bind(fact.tenant_id.0)
        .bind(&fact.writer_principal_id.0)
        .bind(&idempotency.key)
        .execute(&mut *transaction)
        .await
        .map_err(unexpected)?;

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

        let reserved = sqlx::query(
            r#"
            INSERT INTO memory.idempotency_receipts (
                tenant_id, subject_id, principal_id, operation_id,
                idempotency_key, request_fingerprint, state
            )
            VALUES ($1, $2, $3, 'supersedeFact', $4, $5, 'in_progress')
            ON CONFLICT (tenant_id, principal_id, operation_id, idempotency_key)
                DO NOTHING
            RETURNING true AS reserved
            "#,
        )
        .bind(revision.tenant_id.0)
        .bind(revision.subject_id.0)
        .bind(&revision.writer_principal_id.0)
        .bind(&idempotency.key)
        .bind(&idempotency.fingerprint)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(unexpected)?
        .is_some();

        if !reserved {
            let receipt = sqlx::query(
                r#"
                SELECT request_fingerprint, state, resource_fact_id, response_body
                FROM memory.idempotency_receipts
                WHERE tenant_id = $1
                  AND principal_id = $2
                  AND operation_id = 'supersedeFact'
                  AND idempotency_key = $3
                FOR UPDATE
                "#,
            )
            .bind(revision.tenant_id.0)
            .bind(&revision.writer_principal_id.0)
            .bind(&idempotency.key)
            .fetch_one(&mut *transaction)
            .await
            .map_err(unexpected)?;
            let stored_fingerprint: String =
                receipt.try_get("request_fingerprint").map_err(unexpected)?;
            if stored_fingerprint != idempotency.fingerprint {
                return Err(RepositoryError::IdempotencyKeyReused);
            }
            let state: String = receipt.try_get("state").map_err(unexpected)?;
            if state != "completed" {
                return Err(RepositoryError::IdempotencyInProgress);
            }
            let resource_fact_id: Option<uuid::Uuid> =
                receipt.try_get("resource_fact_id").map_err(unexpected)?;
            resource_fact_id.ok_or_else(|| {
                RepositoryError::Unexpected("completed idempotency receipt has no fact".to_owned())
            })?;
            let response_body: serde_json::Value =
                receipt.try_get("response_body").map_err(unexpected)?;
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
        .bind(&revision.write_policy.id)
        .bind(&revision.write_policy.version)
        .bind(&revision.sensitivity)
        .bind(&revision.retention_policy_id)
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
        sqlx::query(
            r#"
            UPDATE memory.idempotency_receipts
            SET state = 'completed', resource_fact_id = $1,
                response_status = 200, response_body = $2, response_etag = $3,
                response_location = $4, completed_at = clock_timestamp()
            WHERE tenant_id = $5
              AND principal_id = $6
              AND operation_id = 'supersedeFact'
              AND idempotency_key = $7
              AND state = 'in_progress'
            "#,
        )
        .bind(revision.fact_id.0)
        .bind(response_body)
        .bind(response_etag)
        .bind(response_location)
        .bind(revision.tenant_id.0)
        .bind(&revision.writer_principal_id.0)
        .bind(&idempotency.key)
        .execute(&mut *transaction)
        .await
        .map_err(unexpected)?;

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

impl PostgresEpisodeRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

pub async fn migrate(pool: &PgPool) -> Result<(), sqlx::migrate::MigrateError> {
    sqlx::migrate!("../../migrations").run(pool).await
}

#[async_trait]
impl EpisodeRepository for PostgresEpisodeRepository {
    async fn append(
        &self,
        episode: NewEpisode,
        idempotency: IdempotencyRequest,
    ) -> Result<AppendOutcome, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, episode.tenant_id, episode.subject_id).await?;

        let reserved = sqlx::query(
            r#"
            INSERT INTO memory.idempotency_receipts (
                tenant_id, subject_id, principal_id, operation_id,
                idempotency_key, request_fingerprint, state
            )
            VALUES ($1, $2, $3, 'appendEpisode', $4, $5, 'in_progress')
            ON CONFLICT (tenant_id, principal_id, operation_id, idempotency_key)
                DO NOTHING
            RETURNING true AS reserved
            "#,
        )
        .bind(episode.tenant_id.0)
        .bind(episode.subject_id.0)
        .bind(&episode.writer_principal_id.0)
        .bind(&idempotency.key)
        .bind(&idempotency.fingerprint)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(unexpected)?
        .is_some();

        if !reserved {
            let receipt = sqlx::query(
                r#"
                SELECT request_fingerprint, state, resource_episode_id, response_body
                FROM memory.idempotency_receipts
                WHERE tenant_id = $1
                  AND principal_id = $2
                  AND operation_id = 'appendEpisode'
                  AND idempotency_key = $3
                FOR UPDATE
                "#,
            )
            .bind(episode.tenant_id.0)
            .bind(&episode.writer_principal_id.0)
            .bind(&idempotency.key)
            .fetch_one(&mut *transaction)
            .await
            .map_err(unexpected)?;
            let stored_fingerprint: String =
                receipt.try_get("request_fingerprint").map_err(unexpected)?;
            if stored_fingerprint != idempotency.fingerprint {
                return Err(RepositoryError::IdempotencyKeyReused);
            }
            let state: String = receipt.try_get("state").map_err(unexpected)?;
            if state != "completed" {
                return Err(RepositoryError::IdempotencyInProgress);
            }
            let episode_id: Option<uuid::Uuid> =
                receipt.try_get("resource_episode_id").map_err(unexpected)?;
            episode_id.ok_or_else(|| {
                RepositoryError::Unexpected(
                    "completed idempotency receipt has no episode".to_owned(),
                )
            })?;
            let response_body: serde_json::Value =
                receipt.try_get("response_body").map_err(unexpected)?;
            let stored: Episode = serde_json::from_value(response_body).map_err(unexpected)?;
            transaction.commit().await.map_err(unexpected)?;
            return Ok(AppendOutcome {
                episode: stored,
                replayed: true,
            });
        }

        let row = sqlx::query(
            r#"
            INSERT INTO memory.episodes (
                tenant_id, subject_id, case_id, episode_id, kind, observed_at,
                writer_principal_id, source_type, source_uri, external_id,
                sensitivity, retention_policy_id, schema_version, payload, payload_sha256
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            RETURNING tenant_id, subject_id, case_id, episode_id, kind, observed_at,
                recorded_at, writer_principal_id, source_type, source_uri, external_id,
                sensitivity, retention_policy_id, schema_version, payload, payload_sha256
            "#,
        )
        .bind(episode.tenant_id.0)
        .bind(episode.subject_id.0)
        .bind(episode.case_id.0)
        .bind(episode.episode_id.0)
        .bind(&episode.kind)
        .bind(episode.observed_at)
        .bind(&episode.writer_principal_id.0)
        .bind(&episode.provenance.source_type)
        .bind(&episode.provenance.source_uri)
        .bind(&episode.provenance.external_id)
        .bind(&episode.sensitivity)
        .bind(&episode.retention_policy_id)
        .bind(i32::try_from(episode.schema_version).map_err(|error| {
            RepositoryError::Unexpected(format!("schema version is out of range: {error}"))
        })?)
        .bind(&episode.payload)
        .bind(&episode.payload_sha256)
        .fetch_one(&mut *transaction)
        .await
        .map_err(map_sqlx)?;

        let stored_episode = episode_from_row(&row)?;
        let response_body = serde_json::to_value(&stored_episode).map_err(unexpected)?;
        let response_etag = format!("\"{}\"", stored_episode.payload_sha256);
        let response_location = format!(
            "/v1/tenants/{}/subjects/{}/episodes/{}",
            stored_episode.tenant_id.0, stored_episode.subject_id.0, stored_episode.episode_id.0
        );
        sqlx::query(
            r#"
            UPDATE memory.idempotency_receipts
            SET state = 'completed', resource_episode_id = $1,
                response_status = 201, response_body = $2, response_etag = $3,
                response_location = $4, completed_at = clock_timestamp()
            WHERE tenant_id = $5
              AND principal_id = $6
              AND operation_id = 'appendEpisode'
              AND idempotency_key = $7
              AND state = 'in_progress'
            "#,
        )
        .bind(episode.episode_id.0)
        .bind(response_body)
        .bind(response_etag)
        .bind(response_location)
        .bind(episode.tenant_id.0)
        .bind(&episode.writer_principal_id.0)
        .bind(&idempotency.key)
        .execute(&mut *transaction)
        .await
        .map_err(unexpected)?;

        transaction.commit().await.map_err(unexpected)?;
        Ok(AppendOutcome {
            episode: stored_episode,
            replayed: false,
        })
    }

    async fn get(
        &self,
        tenant_id: TenantId,
        subject_id: SubjectId,
        episode_id: EpisodeId,
    ) -> Result<Episode, RepositoryError> {
        let mut transaction = self.pool.begin().await.map_err(unexpected)?;
        set_scope(&mut transaction, tenant_id, subject_id).await?;

        let row = select_episode(&mut transaction, tenant_id, subject_id, episode_id).await?;

        transaction.commit().await.map_err(unexpected)?;
        row.ok_or(RepositoryError::NotFound)
    }
}

async fn select_fact_view(
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
    let namespace: String = metadata.try_get("namespace").map_err(unexpected)?;
    let key: String = metadata.try_get("fact_key").map_err(unexpected)?;

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

fn fact_revision_from_row(
    row: &PgRow,
    tenant_id: TenantId,
    subject_id: SubjectId,
    fact_id: FactId,
    namespace: &str,
    key: &str,
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
        namespace: namespace.to_owned(),
        key: key.to_owned(),
        value: row.try_get("value").map_err(unexpected)?,
        observed_at: row.try_get("observed_at").map_err(unexpected)?,
        recorded_at: row.try_get("recorded_at").map_err(unexpected)?,
        valid_time: ValidTime {
            from: row.try_get("valid_from").map_err(unexpected)?,
            until: row.try_get("valid_until").map_err(unexpected)?,
        },
        evidence_episode_ids: evidence_episode_ids.into_iter().map(EpisodeId).collect(),
        write_policy: WritePolicy {
            id: row.try_get("write_policy_id").map_err(unexpected)?,
            version: row.try_get("write_policy_version").map_err(unexpected)?,
        },
        confidence: row.try_get("confidence").map_err(unexpected)?,
        sensitivity: row.try_get("sensitivity").map_err(unexpected)?,
        retention_policy_id: row.try_get("retention_policy_id").map_err(unexpected)?,
        writer_principal_id: PrincipalId(row.try_get("writer_principal_id").map_err(unexpected)?),
        schema_version: u32::try_from(schema_version).map_err(unexpected)?,
    })
}

async fn select_episode(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: TenantId,
    subject_id: SubjectId,
    episode_id: EpisodeId,
) -> Result<Option<Episode>, RepositoryError> {
    let row = sqlx::query(
        r#"
        SELECT tenant_id, subject_id, case_id, episode_id, kind, observed_at,
            recorded_at, writer_principal_id, source_type, source_uri, external_id,
            sensitivity, retention_policy_id, schema_version, payload, payload_sha256
        FROM memory.episodes
        WHERE tenant_id = $1 AND subject_id = $2 AND episode_id = $3
        "#,
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .bind(episode_id.0)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(unexpected)?;
    row.as_ref().map(episode_from_row).transpose()
}

async fn set_scope(
    transaction: &mut Transaction<'_, Postgres>,
    tenant_id: TenantId,
    subject_id: SubjectId,
) -> Result<(), RepositoryError> {
    sqlx::query("SELECT set_config('palimpsest.tenant_id', $1, true)")
        .bind(tenant_id.0.to_string())
        .execute(&mut **transaction)
        .await
        .map_err(unexpected)?;
    sqlx::query("SELECT set_config('palimpsest.subject_id', $1, true)")
        .bind(subject_id.0.to_string())
        .execute(&mut **transaction)
        .await
        .map_err(unexpected)?;
    Ok(())
}

fn episode_from_row(row: &PgRow) -> Result<Episode, RepositoryError> {
    let schema_version: i32 = row.try_get("schema_version").map_err(unexpected)?;
    Ok(Episode {
        tenant_id: TenantId(row.try_get("tenant_id").map_err(unexpected)?),
        subject_id: SubjectId(row.try_get("subject_id").map_err(unexpected)?),
        case_id: CaseId(row.try_get("case_id").map_err(unexpected)?),
        episode_id: EpisodeId(row.try_get("episode_id").map_err(unexpected)?),
        kind: row.try_get("kind").map_err(unexpected)?,
        observed_at: row.try_get("observed_at").map_err(unexpected)?,
        recorded_at: row.try_get("recorded_at").map_err(unexpected)?,
        writer_principal_id: PrincipalId(row.try_get("writer_principal_id").map_err(unexpected)?),
        provenance: Provenance {
            source_type: row.try_get("source_type").map_err(unexpected)?,
            source_uri: row.try_get("source_uri").map_err(unexpected)?,
            external_id: row.try_get("external_id").map_err(unexpected)?,
        },
        sensitivity: row.try_get("sensitivity").map_err(unexpected)?,
        retention_policy_id: row.try_get("retention_policy_id").map_err(unexpected)?,
        schema_version: u32::try_from(schema_version).map_err(|error| {
            RepositoryError::Unexpected(format!("stored schema version is invalid: {error}"))
        })?,
        payload: row.try_get("payload").map_err(unexpected)?,
        payload_sha256: row.try_get("payload_sha256").map_err(unexpected)?,
    })
}

fn map_sqlx(error: sqlx::Error) -> RepositoryError {
    if error
        .as_database_error()
        .and_then(|database_error| database_error.code())
        .is_some_and(|code| code == "23505")
    {
        RepositoryError::Conflict
    } else {
        unexpected(error)
    }
}

fn unexpected(error: impl std::fmt::Display) -> RepositoryError {
    RepositoryError::Unexpected(error.to_string())
}
