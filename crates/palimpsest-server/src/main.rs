use std::{env, fs, sync::Arc};

use anyhow::{Context, Result, bail};
use palimpsest_application::verify_restore_fence_ledger;
use palimpsest_domain::{
    OperationGrant, PrincipalId, PrincipalScope, Sensitivity, SubjectId, TenantId,
};
use palimpsest_http::StaticAuthenticator;
use palimpsest_postgres::PostgresMemoryRepository;
use sqlx::PgPool;
use time::OffsetDateTime;
use tokio::net::TcpListener;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
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
