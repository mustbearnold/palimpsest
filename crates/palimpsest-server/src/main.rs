use std::{env, fs, io::Write, sync::Arc};

use anyhow::{Context, Result, bail};
use palimpsest_application::verify_restore_fence_ledger;
use palimpsest_domain::{
    OperationGrant, PrincipalId, PrincipalScope, Sensitivity, SubjectId, TenantId,
};
use palimpsest_http::StaticAuthenticator;
use palimpsest_postgres::PostgresMemoryRepository;
use serde_json::{Value, json};
use sqlx::{PgPool, Row};
use time::OffsetDateTime;
use tokio::net::TcpListener;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    match env::args().nth(1).as_deref() {
        Some("doctor") => return run_doctor().await,
        Some("--help" | "-h") => {
            write_stdout(
                "Usage: palimpsest-server [doctor]\n  doctor  check PostgreSQL, pgvector, schema, and runtime-role prerequisites",
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
    palimpsest_postgres::migrate(&pool)
        .await
        .context("apply database migrations")?;
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
    axum::serve(
        listener,
        palimpsest_server::app(pool, lifecycle_controller_pool, authenticator),
    )
    .await
    .context("serve HTTP API")?;
    Ok(())
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
