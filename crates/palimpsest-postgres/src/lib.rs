use std::collections::{BTreeMap, BTreeSet};

use palimpsest_application::RepositoryError;
use sha2::{Digest, Sha256};
use sqlx::{
    Decode, Either, PgPool, Postgres, Row, Type,
    postgres::{PgAdvisoryLock, PgRow},
};

mod checkpoints;
mod episodes;
mod export;
mod facts;
mod hybrid_receipt;
mod lifecycle;
mod projection;
mod receipt_write;
mod retrieval;
mod write_path;

pub use projection::{EmbeddingProjectionCoordinator, ProjectionRebuildReport};
#[derive(Clone)]
pub struct PostgresMemoryRepository {
    pool: PgPool,
}

#[derive(Clone)]
pub struct PostgresSubjectLifecycleRepository {
    content: PostgresMemoryRepository,
    controller: PostgresMemoryRepository,
}

impl PostgresSubjectLifecycleRepository {
    pub fn new(content_pool: PgPool, controller_pool: PgPool) -> Self {
        Self {
            content: PostgresMemoryRepository::new(content_pool),
            controller: PostgresMemoryRepository::new(controller_pool),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RestoreFenceReplayReport {
    pub scopes_found: u64,
    pub scopes_purged: u64,
    pub residual_rows: u64,
    pub ledger_sha256: String,
}

pub const MIGRATION_LOCK_NAME: &str = "palimpsest:migrations:v1";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationPlanEntry {
    pub version: i64,
    pub description: String,
    pub transactional: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationStatus {
    pub database: String,
    pub expected_version: i64,
    pub migration_table_exists: bool,
    pub applied_versions: Vec<i64>,
    pub failed_versions: Vec<i64>,
    pub unknown_versions: Vec<i64>,
    pub checksum_mismatches: Vec<i64>,
    pub pending: Vec<MigrationPlanEntry>,
    pub lock_available: bool,
}

fn embedding_vector_sha256(values: &[f32]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"palimpsest.embedding.float32-be.v1\0");
    for value in values {
        digest.update(value.to_bits().to_be_bytes());
    }
    hex::encode(digest.finalize())
}

fn required_column<T>(row: &PgRow, column: &str) -> Result<T, RepositoryError>
where
    for<'row> T: Decode<'row, Postgres> + Type<Postgres>,
{
    row.try_get::<Option<T>, _>(column)
        .map_err(unexpected)?
        .ok_or_else(|| RepositoryError::Unexpected("retrieval policy is incomplete".to_owned()))
}

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

pub async fn migrate(pool: &PgPool) -> Result<(), sqlx::migrate::MigrateError> {
    let connection = pool
        .acquire()
        .await
        .map_err(sqlx::migrate::MigrateError::Execute)?;
    let lock = PgAdvisoryLock::new(MIGRATION_LOCK_NAME);
    let mut guard = lock
        .acquire(connection)
        .await
        .map_err(sqlx::migrate::MigrateError::Execute)?;
    let migration_result = MIGRATOR.run(&mut *guard).await;
    let release_result = guard
        .release_now()
        .await
        .map_err(sqlx::migrate::MigrateError::Execute);
    match (migration_result, release_result) {
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(_)) => Ok(()),
    }
}

pub async fn migration_status(pool: &PgPool) -> Result<MigrationStatus, sqlx::Error> {
    let database: String = sqlx::query_scalar("SELECT current_database()::text")
        .fetch_one(pool)
        .await?;
    let migration_table_exists: bool =
        sqlx::query_scalar("SELECT to_regclass('_sqlx_migrations') IS NOT NULL")
            .fetch_one(pool)
            .await?;
    let known_migrations: BTreeMap<_, _> = MIGRATOR
        .iter()
        .map(|migration| (migration.version, migration))
        .collect();
    let mut recorded_versions = BTreeSet::new();
    let mut applied_versions = Vec::new();
    let mut failed_versions = Vec::new();
    let mut unknown_versions = Vec::new();
    let mut checksum_mismatches = Vec::new();

    if migration_table_exists {
        let rows = sqlx::query(
            "SELECT version, success, checksum
             FROM _sqlx_migrations
             ORDER BY version",
        )
        .fetch_all(pool)
        .await?;
        for row in rows {
            let version: i64 = row.try_get("version")?;
            let success: bool = row.try_get("success")?;
            let checksum: Vec<u8> = row.try_get("checksum")?;
            recorded_versions.insert(version);
            if success {
                applied_versions.push(version);
            } else {
                failed_versions.push(version);
            }
            match known_migrations.get(&version) {
                Some(migration) if migration.checksum.as_ref() != checksum.as_slice() => {
                    checksum_mismatches.push(version);
                }
                None => unknown_versions.push(version),
                Some(_) => {}
            }
        }
    }

    let pending = known_migrations
        .values()
        .filter(|migration| migration.migration_type.is_up_migration())
        .filter(|migration| !recorded_versions.contains(&migration.version))
        .map(|migration| MigrationPlanEntry {
            version: migration.version,
            description: migration.description.to_string(),
            transactional: !migration.no_tx,
        })
        .collect();
    let lock_available = migration_lock_available(pool).await?;

    Ok(MigrationStatus {
        database,
        expected_version: latest_migration_version(),
        migration_table_exists,
        applied_versions,
        failed_versions,
        unknown_versions,
        checksum_mismatches,
        pending,
        lock_available,
    })
}

async fn migration_lock_available(pool: &PgPool) -> Result<bool, sqlx::Error> {
    let connection = pool.acquire().await?;
    let lock = PgAdvisoryLock::new(MIGRATION_LOCK_NAME);
    match lock.try_acquire(connection).await? {
        Either::Left(guard) => {
            guard.release_now().await?;
            Ok(true)
        }
        Either::Right(_) => Ok(false),
    }
}

pub fn latest_migration_version() -> i64 {
    MIGRATOR
        .iter()
        .map(|migration| migration.version)
        .max()
        .unwrap_or(0)
}

fn text_value_from_row<T>(row: &PgRow, column: &'static str) -> Result<T, RepositoryError>
where
    T: TryFrom<String>,
    T::Error: std::fmt::Display,
{
    let raw: String = row.try_get(column).map_err(unexpected)?;
    T::try_from(raw).map_err(unexpected)
}

fn map_sqlx(error: sqlx::Error) -> RepositoryError {
    if error
        .as_database_error()
        .and_then(|database_error| database_error.constraint())
        == Some("fact_retrieval_metadata_policy_known")
    {
        return RepositoryError::WritePolicyRejected;
    }
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

#[cfg(test)]
mod tests {
    use super::projection::run_with_content_lease_deadline;
    use super::*;
    use time::OffsetDateTime;

    #[tokio::test]
    async fn projection_work_is_cancelled_at_the_content_lease_deadline() {
        let result = run_with_content_lease_deadline::<()>(
            OffsetDateTime::now_utc() + time::Duration::milliseconds(10),
            std::future::pending(),
        )
        .await;
        assert!(
            matches!(result, Err(RepositoryError::Unexpected(message)) if message == "projection content lease expired")
        );
    }

    #[test]
    fn latest_migration_version_matches_the_checked_in_schema() {
        assert_eq!(latest_migration_version(), 20);
    }
}
