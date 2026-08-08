use std::{env, fs, io::Write, sync::Arc};

use anyhow::{Context, Result, bail};
use palimpsest_application::{
    backup::{
        BackupIndexEntry, S3BackupObjectStore, S3BackupStoreError, base_object_key, wal_object_key,
    },
    export::sha256_hex,
    verify_restore_fence_ledger,
    RestoreFenceEntry, RestoreFenceLedger,
};
use palimpsest_domain::{
    OperationGrant, PrincipalId, PrincipalScope, Sensitivity, SubjectId, TenantId,
};
use palimpsest_http::StaticAuthenticator;
use palimpsest_postgres::PostgresMemoryRepository;
use serde_json::{Value, json};
use sqlx::{PgPool, Row};
use time::{Duration, OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::net::TcpListener;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    match env::args().nth(1).as_deref() {
        Some("doctor") => return run_doctor().await,
        Some("migrate") => return run_migrate().await,
        Some("restore") => return run_restore().await,
        Some("backup") => return run_backup().await,
        Some("--help" | "-h") => {
            write_stdout(
                "Usage: palimpsest-server [doctor|migrate|restore|backup]\n  doctor  check PostgreSQL, pgvector, schema, and runtime-role prerequisites\n  migrate status|plan|apply  inspect or apply checked-in SQLx migrations\n  restore verify|apply|export-ledger  verify a fence ledger, replay it against the restore database, or export the live fence ledger\n  backup push-base|archive-wal|fetch-base|fetch-wal|expire  manage base backups, WAL segments, and backup expiry",
            )?;
            return Ok(());
        }
        Some(command) => bail!("unknown command {command}"),
        None => {}
    }
    if restore_mode_enabled()? {
        return run_restore_mode().await;
    }
    let database_url = required("PALIMPSEST_DATABASE_URL")?;
    let bearer_token = required("PALIMPSEST_BEARER_TOKEN")?;
    let principal_id = required("PALIMPSEST_PRINCIPAL_ID")?;
    let tenant_id = parse_uuid("PALIMPSEST_TENANT_ID")?;
    let subject_id = parse_uuid("PALIMPSEST_SUBJECT_ID")?;
    let allowed_sensitivities = env::var("PALIMPSEST_ALLOWED_SENSITIVITIES")
        .unwrap_or_default()
        .split(',')
        .filter(|value| !value.trim().is_empty())
        .map(|value| Sensitivity::try_from(value.trim().to_owned()))
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("PALIMPSEST_ALLOWED_SENSITIVITIES contains an invalid label")?;
    let operation_grants =
        parse_operation_grants(&env::var("PALIMPSEST_OPERATION_GRANTS").unwrap_or_default())
            .context("PALIMPSEST_OPERATION_GRANTS contains an unknown grant")?;
    let bind = env::var("PALIMPSEST_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_owned());

    let pool = PgPool::connect(&database_url)
        .await
        .context("connect to PALIMPSEST_DATABASE_URL")?;
    let lifecycle_controller_pool = if operation_grants.contains(&OperationGrant::SubjectDelete) {
        let controller_database_url = required("PALIMPSEST_LIFECYCLE_CONTROLLER_DATABASE_URL")?;
        PgPool::connect(&controller_database_url)
            .await
            .context("connect to PALIMPSEST_LIFECYCLE_CONTROLLER_DATABASE_URL")?
    } else {
        pool.clone()
    };

    let authenticator = Arc::new(StaticAuthenticator::new([(
        bearer_token,
        PrincipalScope {
            principal_id: PrincipalId(principal_id),
            tenant_id: TenantId(tenant_id),
            subject_ids: vec![SubjectId(subject_id)],
            allowed_sensitivities,
            operation_grants,
        },
    )]));
    let listener = TcpListener::bind(&bind)
        .await
        .with_context(|| format!("bind HTTP listener to {bind}"))?;
    record_projection_lease_policy(&pool).await?;
    axum::serve(
        listener,
        palimpsest_server::app(pool, lifecycle_controller_pool, authenticator),
    )
    .await
    .context("serve HTTP API")?;
    Ok(())
}

/// Records the deployed embedding-projection lease policy into the
/// content-free `/metrics` gauges (spec 010 R3: metrics stay database-free;
/// the policy is read once at startup and is CHECK-constrained).
async fn record_projection_lease_policy(pool: &PgPool) -> Result<()> {
    let policy = sqlx::query_as::<_, (i32, i32)>(
        "SELECT lease_seconds, renewal_interval_seconds
         FROM memory.embedding_projection_lease_policies
         WHERE policy_id = 'embedding-projection-v1'",
    )
    .fetch_optional(pool)
    .await
    .context("read embedding-projection lease policy for metrics")?;
    if let Some((lease_seconds, renewal_interval_seconds)) = policy {
        palimpsest_http::record_projection_lease_policy(
            u64::try_from(lease_seconds).unwrap_or_default(),
            u64::try_from(renewal_interval_seconds).unwrap_or_default(),
        );
    }
    Ok(())
}

async fn run_restore() -> Result<()> {
    let operation = env::args().nth(2);
    if matches!(operation.as_deref(), Some("--help" | "-h") | None) {
        write_stdout(
            "Usage: palimpsest-server restore <verify|apply|export-ledger>\n  restore verify  validate the independent fence ledger without database access\n  restore apply   replay the verified fence ledger against PALIMPSEST_RESTORE_DATABASE_URL\n  restore export-ledger  export the live fence ledger from PALIMPSEST_RESTORE_EXPORT_DATABASE_URL",
        )?;
        if operation.is_none() {
            bail!("restore operation is required");
        }
        return Ok(());
    }
    match operation.as_deref() {
        Some("verify") => run_restore_verify().await,
        Some("apply") => run_restore_mode().await,
        Some("export-ledger") => run_restore_export_ledger().await,
        Some(operation) => bail!("unknown restore operation {operation}"),
        None => unreachable!("restore operation was checked above"),
    }
}

async fn run_restore_verify() -> Result<()> {
    let ledger_path = match env::var("PALIMPSEST_RESTORE_FENCE_LEDGER_PATH") {
        Ok(value) if !value.trim().is_empty() => value,
        _ => {
            write_restore_failure("ledger-path-missing")?;
            bail!("restore verification failed");
        }
    };
    let expected_sha256 = match env::var("PALIMPSEST_RESTORE_FENCE_LEDGER_SHA256") {
        Ok(value) if !value.trim().is_empty() => value,
        _ => {
            write_restore_failure("ledger-digest-missing")?;
            bail!("restore verification failed");
        }
    };
    let bytes = match fs::read(&ledger_path) {
        Ok(bytes) => bytes,
        Err(_) => {
            write_restore_failure("ledger-read-failed")?;
            bail!("restore verification failed");
        }
    };
    let ledger = match verify_restore_fence_ledger(
        Some(&bytes),
        &expected_sha256,
        OffsetDateTime::now_utc(),
    ) {
        Ok(ledger) => ledger,
        Err(_) => {
            write_restore_failure("ledger-verification-failed")?;
            bail!("restore verification failed");
        }
    };
    write_stdout(&serde_json::to_string_pretty(&json!({
        "operation": "verify",
        "status": "verified",
        "profile": ledger.profile,
        "schema_version": ledger.schema_version,
        "generated_at": ledger.generated_at,
        "entry_count": ledger.entries.len(),
        "ledger_sha256": ledger.ledger_sha256
    }))?)
}

fn write_restore_failure(code: &str) -> Result<()> {
    write_stdout(&serde_json::to_string_pretty(&json!({
        "operation": "verify",
        "status": "blocked",
        "error": {"code": code}
    }))?)
}

async fn run_migrate() -> Result<()> {
    let operation = env::args().nth(2);
    if matches!(operation.as_deref(), Some("--help" | "-h") | None) {
        write_stdout(
            "Usage: palimpsest-server migrate <status|plan|apply>\n  migrate status  report applied, pending, failed, and incompatible migrations\n  migrate plan    show pending migrations and transaction mode\n  migrate apply   acquire the migration lock and apply pending migrations",
        )?;
        if operation.is_none() {
            bail!("migrate operation is required");
        }
        return Ok(());
    }
    let operation = operation.expect("migrate operation was checked above");
    if !matches!(operation.as_str(), "status" | "plan" | "apply") {
        bail!("unknown migrate operation {operation}");
    }

    let database_url = env::var("PALIMPSEST_MIGRATION_DATABASE_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            env::var("PALIMPSEST_DATABASE_URL")
                .ok()
                .filter(|value| !value.trim().is_empty())
        });
    let database_url = match database_url {
        Some(database_url) => database_url,
        None => {
            write_migration_failure(&operation, "database-url-missing")?;
            bail!("migration command failed");
        }
    };
    let pool = match PgPool::connect(&database_url).await {
        Ok(pool) => pool,
        Err(_) => {
            write_migration_failure(&operation, "connection-failed")?;
            bail!("migration command failed");
        }
    };

    if operation == "apply" && palimpsest_postgres::migrate(&pool).await.is_err() {
        pool.close().await;
        write_migration_failure(&operation, "migration-failed")?;
        bail!("migration command failed");
    }
    let status = match palimpsest_postgres::migration_status(&pool).await {
        Ok(status) => status,
        Err(_) => {
            pool.close().await;
            write_migration_failure(&operation, "status-query-failed")?;
            bail!("migration command failed");
        }
    };
    pool.close().await;
    let report = migration_report(&operation, &status);
    let report_json = serde_json::to_string_pretty(&report)?;
    write_stdout(&report_json)?;
    if report["status"] == "blocked" {
        bail!("migration command failed");
    }
    Ok(())
}

fn migration_report(operation: &str, status: &palimpsest_postgres::MigrationStatus) -> Value {
    let blocked = !status.failed_versions.is_empty()
        || !status.unknown_versions.is_empty()
        || !status.checksum_mismatches.is_empty();
    let current = status.migration_table_exists && status.pending.is_empty() && !blocked;
    json!({
        "operation": operation,
        "status": if current { "current" } else if blocked { "blocked" } else { "pending" },
        "database": status.database,
        "migration_table_exists": status.migration_table_exists,
        "expected_version": status.expected_version,
        "applied_versions": status.applied_versions,
        "failed_versions": status.failed_versions,
        "unknown_versions": status.unknown_versions,
        "checksum_mismatches": status.checksum_mismatches,
        "pending": status.pending.iter().map(|migration| json!({
            "version": migration.version,
            "description": migration.description,
            "transactional": migration.transactional
        })).collect::<Vec<_>>(),
        "lock": {
            "name": palimpsest_postgres::MIGRATION_LOCK_NAME,
            "available": status.lock_available
        }
    })
}

fn write_migration_failure(operation: &str, code: &str) -> Result<()> {
    write_stdout(&serde_json::to_string_pretty(&json!({
        "operation": operation,
        "status": "blocked",
        "error": {"code": code}
    }))?)
}

async fn run_doctor() -> Result<()> {
    let database_url = match env::var("PALIMPSEST_DATABASE_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
    {
        Some(value) => value,
        None => {
            write_stdout(
                serde_json::to_string_pretty(&json!({
                    "status": "not_ready",
                    "checks": {
                        "database": {"status": "error", "code": "database-url-missing"}
                    }
                }))?
                .as_str(),
            )?;
            bail!("doctor reported not_ready");
        }
    };
    let pool = match PgPool::connect(&database_url).await {
        Ok(pool) => pool,
        Err(_) => {
            write_stdout(
                serde_json::to_string_pretty(&json!({
                    "status": "not_ready",
                    "checks": {
                        "database": {"status": "error", "code": "connection-failed"}
                    }
                }))?
                .as_str(),
            )?;
            bail!("doctor reported not_ready");
        }
    };
    let report = match doctor_report(&pool).await {
        Ok(report) => report,
        Err(_) => {
            pool.close().await;
            write_stdout(
                serde_json::to_string_pretty(&json!({
                    "status": "not_ready",
                    "checks": {
                        "database": {"status": "error", "code": "query-failed"}
                    }
                }))?
                .as_str(),
            )?;
            bail!("doctor reported not_ready");
        }
    };
    pool.close().await;
    let report_json = serde_json::to_string_pretty(&report)?;
    write_stdout(&report_json)?;
    if report["status"] != "ready" {
        bail!("doctor reported not_ready");
    }
    Ok(())
}

fn write_stdout(value: &str) -> Result<()> {
    let mut stdout = std::io::stdout().lock();
    stdout.write_all(value.as_bytes())?;
    stdout.write_all(b"\n")?;
    stdout.flush()?;
    Ok(())
}

async fn doctor_report(pool: &PgPool) -> Result<Value> {
    let database = sqlx::query(
        "SELECT current_database() AS database,
                current_setting('server_version_num')::bigint AS server_version_num,
                current_user AS role,
                roles.rolcanlogin AS can_login,
                roles.rolsuper AS superuser,
                roles.rolbypassrls AS bypass_rls
         FROM pg_roles AS roles
         WHERE roles.rolname = current_user",
    )
    .fetch_one(pool)
    .await?;
    let database_name: String = database.try_get("database")?;
    let server_version_num: i64 = database.try_get("server_version_num")?;
    let role: String = database.try_get("role")?;
    let can_login: bool = database.try_get("can_login")?;
    let superuser: bool = database.try_get("superuser")?;
    let bypass_rls: bool = database.try_get("bypass_rls")?;

    let vector_version: Option<String> =
        sqlx::query_scalar("SELECT extversion::text FROM pg_extension WHERE extname = 'vector'")
            .fetch_optional(pool)
            .await?;
    let migration_table_exists: bool =
        sqlx::query_scalar("SELECT to_regclass('_sqlx_migrations') IS NOT NULL")
            .fetch_one(pool)
            .await?;
    let (first_version, latest_version, successful_count, failed_count) = if migration_table_exists
    {
        let migrations = sqlx::query(
            "SELECT min(version)::bigint AS first_version,
                        max(version)::bigint AS latest_version,
                        count(*) FILTER (WHERE success)::bigint AS successful_count,
                        count(*) FILTER (WHERE NOT success)::bigint AS failed_count
                 FROM _sqlx_migrations",
        )
        .fetch_one(pool)
        .await?;
        (
            migrations.try_get::<Option<i64>, _>("first_version")?,
            migrations.try_get::<Option<i64>, _>("latest_version")?,
            migrations.try_get::<i64, _>("successful_count")?,
            migrations.try_get::<i64, _>("failed_count")?,
        )
    } else {
        (None, None, 0, 0)
    };
    let schema_objects_ready: bool = sqlx::query_scalar(
        "SELECT to_regclass('memory.subject_lifecycles') IS NOT NULL
             AND to_regclass('memory.deletion_operations') IS NOT NULL
             AND to_regclass('memory.export_operations') IS NOT NULL",
    )
    .fetch_one(pool)
    .await?;

    let expected_version = palimpsest_postgres::latest_migration_version();
    let database_ready = server_version_num >= 180_000;
    let vector_ready = vector_version.as_deref() == Some("0.8.5");
    let migrations_ready = migration_table_exists
        && first_version == Some(1)
        && latest_version == Some(expected_version)
        && successful_count == expected_version
        && failed_count == 0;
    let role_ready = can_login && !superuser && !bypass_rls;
    let ready =
        database_ready && vector_ready && migrations_ready && schema_objects_ready && role_ready;

    Ok(json!({
        "status": if ready { "ready" } else { "not_ready" },
        "checks": {
            "database": {
                "status": if database_ready { "pass" } else { "fail" },
                "database": database_name,
                "server_version_num": server_version_num
            },
            "pgvector": {
                "status": if vector_ready { "pass" } else { "fail" },
                "version": vector_version
            },
            "migrations": {
                "status": if migrations_ready { "pass" } else { "fail" },
                "expected_version": expected_version,
                "first_version": first_version,
                "latest_version": latest_version,
                "successful_count": successful_count,
                "failed_count": failed_count
            },
            "schema_objects": {
                "status": if schema_objects_ready { "pass" } else { "fail" }
            },
            "runtime_role": {
                "status": if role_ready { "pass" } else { "fail" },
                "role": role,
                "can_login": can_login,
                "superuser": superuser,
                "bypass_rls": bypass_rls
            }
        }
    }))
}

fn restore_mode_enabled() -> Result<bool> {
    restore_mode_enabled_with_value(&env::var("PALIMPSEST_RESTORE_MODE").unwrap_or_default())
}

fn restore_mode_enabled_with_value(mode: &str) -> Result<bool> {
    if mode.is_empty() || mode == "0" {
        return Ok(false);
    }
    if mode != "1" {
        bail!("PALIMPSEST_RESTORE_MODE must be 0 or 1");
    }
    Ok(true)
}

async fn run_restore_mode() -> Result<()> {
    let ledger_path = required("PALIMPSEST_RESTORE_FENCE_LEDGER_PATH")?;
    let expected_sha256 = required("PALIMPSEST_RESTORE_FENCE_LEDGER_SHA256")?;
    let database_url = required("PALIMPSEST_RESTORE_DATABASE_URL")?;
    let bytes = fs::read(ledger_path).context("read restore fence ledger")?;
    verify_restore_mode_inputs(
        "1",
        Some(&bytes),
        Some(&expected_sha256),
        OffsetDateTime::now_utc(),
    )?;
    let pool = PgPool::connect(&database_url)
        .await
        .context("connect to PALIMPSEST_RESTORE_DATABASE_URL")?;
    let repository = PostgresMemoryRepository::new(pool.clone());
    let report = repository
        .replay_restore_fence_ledger(&bytes, &expected_sha256)
        .await
        .map_err(|_| anyhow::anyhow!("restore fence replay failed"))?;
    if report.residual_rows != 0 || report.ledger_sha256 != expected_sha256 {
        bail!("restore fence replay returned an invalid report");
    }
    pool.close().await;
    Ok(())
}

fn verify_restore_mode_inputs(
    mode: &str,
    bytes: Option<&[u8]>,
    expected_sha256: Option<&str>,
    now: OffsetDateTime,
) -> Result<()> {
    if mode.is_empty() || mode == "0" {
        return Ok(());
    }
    if mode != "1" {
        bail!("PALIMPSEST_RESTORE_MODE must be 0 or 1");
    }
    let expected_sha256 = expected_sha256
        .filter(|value| !value.is_empty())
        .context("restore fence ledger digest is required")?;
    verify_restore_fence_ledger(bytes, expected_sha256, now)
        .map(|_| ())
        .map_err(|_| anyhow::anyhow!("restore fence ledger verification failed"))
}

fn parse_operation_grants(value: &str) -> Result<Vec<OperationGrant>> {
    let mut canonical_history_export = false;
    let mut subject_delete = false;
    for name in value
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
    {
        match name {
            "canonical_history_export" => canonical_history_export = true,
            "subject_delete" => subject_delete = true,
            _ => bail!("unknown operation grant {name}"),
        }
    }
    let mut grants = Vec::with_capacity(2);
    if canonical_history_export {
        grants.push(OperationGrant::CanonicalHistoryExport);
    }
    if subject_delete {
        grants.push(OperationGrant::SubjectDelete);
    }
    Ok(grants)
}

fn required(name: &str) -> Result<String> {
    env::var(name).with_context(|| format!("{name} must be set"))
}

fn parse_uuid(name: &str) -> Result<Uuid> {
    required(name)?
        .parse()
        .with_context(|| format!("{name} must be a UUID"))
}

async fn run_backup() -> Result<()> {
    let operation = env::args().nth(2);
    if matches!(operation.as_deref(), Some("--help" | "-h") | None) {
        write_stdout(
            "Usage: palimpsest-server backup <push-base|archive-wal|fetch-base|fetch-wal|expire>\n  backup push-base <backup_id> <base_path> <retention_policy_id> <wal_from> <wal_to>  upload a base archive and record its index entry\n  backup archive-wal <wal_name> <wal_path>  upload one WAL segment\n  backup fetch-base <backup_id> <out_path> [max_age_seconds]  fetch, verify, and refuse stale base archives\n  backup fetch-wal <wal_name> <out_path>  fetch one WAL segment\n  backup expire <retention_policy_id> <retention_seconds>  remove expired backups and their WAL ranges",
        )?;
        if operation.is_none() {
            bail!("backup operation is required");
        }
        return Ok(());
    }
    let mut arguments = env::args().skip(3);
    match operation.as_deref() {
        Some("push-base") => run_backup_push_base(&mut arguments).await,
        Some("archive-wal") => run_backup_archive_wal(&mut arguments).await,
        Some("fetch-base") => run_backup_fetch_base(&mut arguments).await,
        Some("fetch-wal") => run_backup_fetch_wal(&mut arguments).await,
        Some("expire") => run_backup_expire(&mut arguments).await,
        Some(operation) => bail!("unknown backup operation {operation}"),
        None => unreachable!("backup operation was checked above"),
    }
}

fn backup_store() -> Result<S3BackupObjectStore> {
    S3BackupObjectStore::from_environment()
        .map_err(|_| anyhow::anyhow!("backup object store configuration is invalid"))?
        .ok_or_else(|| anyhow::anyhow!("PALIMPSEST_BACKUP_S3_* configuration is missing"))
}

fn write_backup_failure(operation: &str, code: &str) -> Result<()> {
    write_stdout(&serde_json::to_string_pretty(&json!({
        "operation": operation,
        "status": "blocked",
        "error": {"code": code}
    }))?)
}

async fn run_backup_push_base(arguments: &mut impl Iterator<Item = String>) -> Result<()> {
    let (backup_id, base_path, retention_policy_id, wal_from, wal_to) = match (
        arguments.next(),
        arguments.next(),
        arguments.next(),
        arguments.next(),
        arguments.next(),
    ) {
        (
            Some(backup_id),
            Some(base_path),
            Some(retention_policy_id),
            Some(wal_from),
            Some(wal_to),
        ) => (backup_id, base_path, retention_policy_id, wal_from, wal_to),
        _ => bail!(
            "backup push-base requires <backup_id> <base_path> <retention_policy_id> <wal_from> <wal_to>"
        ),
    };
    let store = backup_store()?;
    let bytes = fs::read(&base_path).context("read base archive")?;
    let base_sha256 = sha256_hex(&bytes);
    let base_size_bytes = u64::try_from(bytes.len()).context("base archive is too large")?;
    let base_object = base_object_key(&backup_id);
    let uploaded_at = OffsetDateTime::now_utc();
    store
        .put_object(&base_object, &bytes)
        .await
        .map_err(|_| anyhow::anyhow!("base archive upload failed"))?;
    let mut index = store
        .read_index()
        .await
        .map_err(|_| anyhow::anyhow!("backup index read failed"))?;
    index.insert(BackupIndexEntry {
        backup_id: backup_id.clone(),
        retention_policy_id,
        created_at: uploaded_at
            .format(&Rfc3339)
            .context("format backup timestamp")?,
        base_object: base_object.clone(),
        base_sha256: base_sha256.clone(),
        base_size_bytes,
        wal_from,
        wal_to,
    });
    store
        .write_index(&index)
        .await
        .map_err(|_| anyhow::anyhow!("backup index write failed"))?;
    write_stdout(&serde_json::to_string_pretty(&json!({
        "operation": "push-base",
        "status": "pushed",
        "backup_id": backup_id,
        "base_object": base_object,
        "base_sha256": base_sha256,
        "base_size_bytes": base_size_bytes
    }))?)
}

async fn run_backup_archive_wal(arguments: &mut impl Iterator<Item = String>) -> Result<()> {
    let (wal_name, wal_path) = match (arguments.next(), arguments.next()) {
        (Some(wal_name), Some(wal_path)) => (wal_name, wal_path),
        _ => bail!("backup archive-wal requires <wal_name> <wal_path>"),
    };
    let store = backup_store()?;
    let bytes = fs::read(&wal_path).context("read WAL segment")?;
    let object = wal_object_key(&wal_name);
    store
        .put_object(&object, &bytes)
        .await
        .map_err(|_| anyhow::anyhow!("WAL segment upload failed"))?;
    write_stdout(&serde_json::to_string_pretty(&json!({
        "operation": "archive-wal",
        "status": "archived",
        "wal_name": wal_name,
        "object": object,
        "size_bytes": bytes.len()
    }))?)
}

async fn run_backup_fetch_base(arguments: &mut impl Iterator<Item = String>) -> Result<()> {
    let (backup_id, out_path, max_age_seconds) = match (
        arguments.next(),
        arguments.next(),
        arguments.next(),
    ) {
        (Some(backup_id), Some(out_path), max_age_seconds) => (backup_id, out_path, max_age_seconds),
        _ => bail!("backup fetch-base requires <backup_id> <out_path> [max_age_seconds]"),
    };
    let max_age_seconds = match max_age_seconds.as_deref() {
        None => None,
        Some(value) => Some(
            value
                .parse::<i64>()
                .context("max_age_seconds must be an integer")?,
        ),
    };
    let store = backup_store()?;
    let index = store
        .read_index()
        .await
        .map_err(|_| anyhow::anyhow!("backup index read failed"))?;
    let entry = match index.entries.iter().find(|entry| entry.backup_id == backup_id) {
        Some(entry) => entry,
        None => {
            write_backup_failure("fetch-base", "base-not-indexed")?;
            bail!("backup fetch-base failed");
        }
    };
    if let Some(max_age_seconds) = max_age_seconds {
        let created_at = OffsetDateTime::parse(&entry.created_at, &Rfc3339)
            .context("parse backup timestamp")?;
        if created_at + Duration::seconds(max_age_seconds) < OffsetDateTime::now_utc() {
            write_backup_failure("fetch-base", "backup-stale")?;
            bail!("backup fetch-base failed");
        }
    }
    let bytes = match store.get_object(&entry.base_object).await {
        Ok(bytes) => bytes,
        Err(S3BackupStoreError::NotFound) => {
            write_backup_failure("fetch-base", "base-missing")?;
            bail!("backup fetch-base failed");
        }
        Err(_) => bail!("backup fetch-base failed"),
    };
    if sha256_hex(&bytes) != entry.base_sha256 {
        write_backup_failure("fetch-base", "base-corrupt")?;
        bail!("backup fetch-base failed");
    }
    fs::write(&out_path, &bytes).context("write base archive")?;
    write_stdout(&serde_json::to_string_pretty(&json!({
        "operation": "fetch-base",
        "status": "verified",
        "backup_id": backup_id,
        "base_sha256": entry.base_sha256,
        "base_size_bytes": entry.base_size_bytes,
        "wal_from": entry.wal_from,
        "wal_to": entry.wal_to
    }))?)
}

async fn run_backup_fetch_wal(arguments: &mut impl Iterator<Item = String>) -> Result<()> {
    let (wal_name, out_path) = match (arguments.next(), arguments.next()) {
        (Some(wal_name), Some(out_path)) => (wal_name, out_path),
        _ => bail!("backup fetch-wal requires <wal_name> <out_path>"),
    };
    let store = backup_store()?;
    let bytes = match store.get_object(&wal_object_key(&wal_name)).await {
        Ok(bytes) => bytes,
        Err(S3BackupStoreError::NotFound) => {
            write_backup_failure("fetch-wal", "wal-missing")?;
            bail!("backup fetch-wal failed");
        }
        Err(_) => bail!("backup fetch-wal failed"),
    };
    fs::write(&out_path, &bytes).context("write WAL segment")?;
    write_stdout(&serde_json::to_string_pretty(&json!({
        "operation": "fetch-wal",
        "status": "fetched",
        "wal_name": wal_name,
        "size_bytes": bytes.len()
    }))?)
}

async fn run_backup_expire(arguments: &mut impl Iterator<Item = String>) -> Result<()> {
    let (retention_policy_id, retention_seconds) = match (arguments.next(), arguments.next()) {
        (Some(retention_policy_id), Some(retention_seconds)) => {
            let retention_seconds = retention_seconds
                .parse::<i64>()
                .context("retention_seconds must be an integer")?;
            if retention_seconds < 0 {
                bail!("retention_seconds must not be negative");
            }
            (retention_policy_id, retention_seconds)
        }
        _ => bail!("backup expire requires <retention_policy_id> <retention_seconds>"),
    };
    let store = backup_store()?;
    let mut index = store
        .read_index()
        .await
        .map_err(|_| anyhow::anyhow!("backup index read failed"))?;
    let now = OffsetDateTime::now_utc();
    let mut expired = Vec::new();
    let mut kept = Vec::new();
    for entry in index.entries {
        let created_at = OffsetDateTime::parse(&entry.created_at, &Rfc3339)
            .map_err(|_| anyhow::anyhow!("backup index contains an invalid timestamp"))?;
        if entry.retention_policy_id == retention_policy_id
            && created_at + Duration::seconds(retention_seconds) < now
        {
            expired.push(entry);
        } else {
            kept.push(entry);
        }
    }
    let earliest_kept_wal_from = kept
        .iter()
        .map(|entry| entry.wal_from.clone())
        .min();
    for entry in &expired {
        store
            .delete_object(&entry.base_object)
            .await
            .map_err(|_| anyhow::anyhow!("base archive removal failed"))?;
        let wal_upper = match &earliest_kept_wal_from {
            Some(first_kept) => min_wal_name(&entry.wal_to, &previous_wal_name(first_kept)),
            None => entry.wal_to.clone(),
        };
        for wal_name in wal_name_range(&entry.wal_from, &wal_upper) {
            store
                .delete_object(&wal_object_key(&wal_name))
                .await
                .map_err(|_| anyhow::anyhow!("WAL segment removal failed"))?;
        }
    }
    index.entries = kept;
    store
        .write_index(&index)
        .await
        .map_err(|_| anyhow::anyhow!("backup index write failed"))?;
    write_stdout(&serde_json::to_string_pretty(&json!({
        "operation": "expire",
        "status": "expired",
        "retention_policy_id": retention_policy_id,
        "retention_seconds": retention_seconds,
        "removed": expired.iter().map(|entry| entry.backup_id.clone()).collect::<Vec<_>>(),
        "remaining": index.entries.len()
    }))?)
}

fn wal_segment_number(name: &str) -> Option<u64> {
    let suffix = name.get(name.len().saturating_sub(16)..)?;
    u64::from_str_radix(suffix, 16).ok()
}

fn wal_timeline(name: &str) -> &str {
    name.get(..8.min(name.len())).unwrap_or(name)
}

fn previous_wal_name(name: &str) -> String {
    format!(
        "{}{:016x}",
        wal_timeline(name),
        wal_segment_number(name).unwrap_or(0).saturating_sub(1)
    )
}

fn min_wal_name(left: &str, right: &str) -> String {
    if wal_segment_number(left).unwrap_or(0) <= wal_segment_number(right).unwrap_or(0) {
        left.to_owned()
    } else {
        right.to_owned()
    }
}

fn wal_name_range(from: &str, to: &str) -> Vec<String> {
    let timeline = wal_timeline(from);
    let start = wal_segment_number(from).unwrap_or(0);
    let end = wal_segment_number(to).unwrap_or(0);
    (start..=end)
        .map(|number| format!("{timeline}{number:016x}"))
        .collect()
}

async fn run_restore_export_ledger() -> Result<()> {
    let database_url = env::var("PALIMPSEST_RESTORE_EXPORT_DATABASE_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .context("PALIMPSEST_RESTORE_EXPORT_DATABASE_URL must be set")?;
    let ledger_path = required("PALIMPSEST_RESTORE_FENCE_LEDGER_PATH")?;
    let sha256_path = required("PALIMPSEST_RESTORE_FENCE_LEDGER_SHA256")?;
    let expiry_hours = match env::var("PALIMPSEST_RESTORE_FENCE_EXPIRY_HOURS") {
        Ok(value) if !value.trim().is_empty() => value
            .parse::<i64>()
            .context("PALIMPSEST_RESTORE_FENCE_EXPIRY_HOURS must be an integer")?,
        _ => 24,
    };
    let pool = PgPool::connect(&database_url)
        .await
        .context("connect to PALIMPSEST_RESTORE_EXPORT_DATABASE_URL")?;
    let rows = sqlx::query(
        "SELECT tenant_id, subject_id, state_version, updated_at
         FROM memory.subject_lifecycles
         WHERE lifecycle_state <> 'active'
         ORDER BY tenant_id, subject_id",
    )
    .fetch_all(&pool)
    .await
    .context("read live fence state")?;
    let mut entries = Vec::with_capacity(rows.len());
    for row in rows {
        let tenant_id: Uuid = row.try_get("tenant_id")?;
        let subject_id: Uuid = row.try_get("subject_id")?;
        let state_version: i64 = row.try_get("state_version")?;
        let updated_at: OffsetDateTime = row.try_get("updated_at")?;
        let scope_digest: String = sqlx::query_scalar("SELECT memory.deletion_scope_digest($1, $2)")
            .bind(tenant_id)
            .bind(subject_id)
            .fetch_one(&pool)
            .await
            .context("compute scope digest")?;
        entries.push(
            RestoreFenceEntry::new(
                scope_digest,
                u64::try_from(state_version).context("state version must not be negative")?,
                updated_at,
                updated_at + Duration::hours(expiry_hours),
            )
            .context("build fence entry")?,
        );
    }
    let ledger =
        RestoreFenceLedger::build(OffsetDateTime::now_utc(), entries).context("build fence ledger")?;
    let bytes = ledger.to_bytes().context("encode fence ledger")?;
    fs::write(&ledger_path, &bytes).context("write fence ledger")?;
    fs::write(&sha256_path, ledger.ledger_sha256.as_bytes()).context("write fence ledger digest")?;
    pool.close().await;
    write_stdout(&serde_json::to_string_pretty(&json!({
        "operation": "export-ledger",
        "status": "exported",
        "entry_count": ledger.entries.len(),
        "ledger_sha256": ledger.ledger_sha256,
        "ledger_path": ledger_path,
        "sha256_path": sha256_path
    }))?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use palimpsest_application::{RestoreFenceEntry, RestoreFenceLedger};

    fn at(seconds: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(seconds).expect("test timestamp should be valid")
    }

    #[test]
    fn operation_grants_accept_only_the_closed_trusted_vocabulary() {
        assert_eq!(
            parse_operation_grants("canonical_history_export,subject_delete")
                .expect("known operation grants should parse"),
            vec![
                OperationGrant::CanonicalHistoryExport,
                OperationGrant::SubjectDelete,
            ]
        );
        assert!(parse_operation_grants("").is_ok_and(|grants| grants.is_empty()));
        assert!(parse_operation_grants("controller_override").is_err());
    }

    #[test]
    fn restore_mode_is_disabled_by_default_and_fails_closed_when_enabled() {
        assert!(!restore_mode_enabled_with_value("").expect("empty mode should parse"));
        assert!(!restore_mode_enabled_with_value("0").expect("zero mode should parse"));
        assert!(restore_mode_enabled_with_value("1").expect("one mode should parse"));
        assert!(restore_mode_enabled_with_value("2").is_err());
        assert!(verify_restore_mode_inputs("0", None, None, at(3_000)).is_ok());
        assert!(verify_restore_mode_inputs("1", None, None, at(3_000)).is_err());
        assert!(verify_restore_mode_inputs("2", None, None, at(3_000)).is_err());
    }

    #[test]
    fn restore_mode_accepts_only_a_verified_current_ledger() {
        let ledger = RestoreFenceLedger::build(
            at(2_000),
            vec![
                RestoreFenceEntry::new(format!("v1:{:064x}", 1), 1, at(1_000), at(10_000))
                    .expect("test entry should be valid"),
            ],
        )
        .expect("test ledger should be valid");
        let bytes = ledger.to_bytes().expect("test ledger should encode");

        assert!(
            verify_restore_mode_inputs("1", Some(&bytes), Some(&ledger.ledger_sha256), at(3_000),)
                .is_ok()
        );
        assert!(
            verify_restore_mode_inputs("1", Some(&bytes), Some(&"0".repeat(64)), at(3_000),)
                .is_err()
        );
    }
}
