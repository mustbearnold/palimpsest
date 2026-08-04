use std::{env, process::Command, str::FromStr};

use anyhow::{Context, Result, ensure};
use serde_json::Value;
use sqlx::{AssertSqlSafe, ConnectOptions, PgPool, postgres::PgConnectOptions};
use uuid::Uuid;

#[tokio::test]
async fn doctor_reports_a_ready_supported_runtime_database() -> Result<()> {
    let provisioned = if nonempty_env("PALIMPSEST_DOCTOR_DATABASE_URL").is_none()
        && nonempty_env("PALIMPSEST_MIGRATION_DATABASE_URL").is_some()
    {
        Some(ProvisionedDoctorDatabase::create().await?)
    } else {
        None
    };
    let database_url = nonempty_env("PALIMPSEST_DOCTOR_DATABASE_URL")
        .or_else(|| provisioned.as_ref().map(|database| database.runtime_url.clone()))
        .or_else(|| nonempty_env("PALIMPSEST_TEST_DATABASE_URL"))
        .context(
            "PALIMPSEST_DOCTOR_DATABASE_URL or PALIMPSEST_TEST_DATABASE_URL must identify the migrated test database",
        )?;
    let result = run_ready_doctor(&database_url);
    let cleanup_result = match provisioned {
        Some(database) => database.cleanup().await,
        None => Ok(()),
    };
    result?;
    cleanup_result
}

fn run_ready_doctor(database_url: &str) -> Result<()> {
    let output = Command::new(env!("CARGO_BIN_EXE_palimpsest-server"))
        .arg("doctor")
        .env("PALIMPSEST_DATABASE_URL", database_url)
        .output()
        .context("run palimpsest doctor")?;

    ensure!(
        output.status.success(),
        "doctor failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout).context("parse doctor JSON")?;
    ensure!(
        report["status"] == "ready",
        "doctor report was not ready: {report}"
    );
    ensure!(
        report["checks"]["database"]["server_version_num"]
            .as_i64()
            .is_some_and(|version| version >= 180_000),
        "doctor did not verify PostgreSQL 18: {report}"
    );
    ensure!(
        report["checks"]["pgvector"]["version"] == "0.8.5",
        "doctor did not verify pgvector 0.8.5: {report}"
    );
    ensure!(
        report["checks"]["migrations"]["latest_version"] == 20,
        "doctor did not verify the latest migration: {report}"
    );
    ensure!(
        report["checks"]["runtime_role"]["superuser"] == false
            && report["checks"]["runtime_role"]["bypass_rls"] == false,
        "doctor did not verify a restricted runtime role: {report}"
    );
    ensure!(
        !String::from_utf8_lossy(&output.stdout).contains(database_url),
        "doctor leaked the database URL"
    );
    Ok(())
}

fn nonempty_env(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.trim().is_empty())
}

struct ProvisionedDoctorDatabase {
    database_name: String,
    migration_admin_pool: PgPool,
    runtime_url: String,
}

impl ProvisionedDoctorDatabase {
    async fn create() -> Result<Self> {
        let runtime_base_url = nonempty_env("PALIMPSEST_DOCTOR_RUNTIME_DATABASE_URL")
            .or_else(|| nonempty_env("PALIMPSEST_TEST_DATABASE_URL"))
            .unwrap_or_else(|| {
                "postgresql://mustbearn@localhost/postgres?host=/var/run/postgresql".to_owned()
            });
        let migration_database_url = nonempty_env("PALIMPSEST_MIGRATION_DATABASE_URL")
            .unwrap_or_else(|| runtime_base_url.clone());
        let runtime_base_pool = PgPool::connect(&runtime_base_url)
            .await
            .context("connect to the runtime test database")?;
        let runtime_role: String = sqlx::query_scalar("SELECT current_user::text")
            .fetch_one(&runtime_base_pool)
            .await
            .context("identify the runtime test role")?;
        runtime_base_pool.close().await;

        let migration_admin_pool = PgPool::connect(&migration_database_url)
            .await
            .context("connect to the migration authority")?;
        let database_name = format!("palimpsest_doctor_{}", Uuid::now_v7().simple());
        let setup = async {
            let quoted_runtime_role: String = sqlx::query_scalar("SELECT quote_ident($1::text)")
                .bind(&runtime_role)
                .fetch_one(&migration_admin_pool)
                .await?;
            sqlx::query(AssertSqlSafe(format!(
                "CREATE DATABASE \"{database_name}\" OWNER {quoted_runtime_role}"
            )))
            .execute(&migration_admin_pool)
            .await?;

            let runtime_options =
                PgConnectOptions::from_str(&runtime_base_url)?.database(&database_name);
            let runtime_url = runtime_options.to_url_lossy().to_string();
            let migration_options =
                PgConnectOptions::from_str(&migration_database_url)?.database(&database_name);
            let extension_pool = PgPool::connect_with(migration_options).await?;
            sqlx::query("CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public")
                .execute(&extension_pool)
                .await?;
            extension_pool.close().await;

            let runtime_pool = PgPool::connect_with(runtime_options).await?;
            palimpsest_postgres::migrate(&runtime_pool).await?;
            runtime_pool.close().await;
            Ok::<_, anyhow::Error>(runtime_url)
        }
        .await;

        match setup {
            Ok(runtime_url) => Ok(Self {
                database_name,
                migration_admin_pool,
                runtime_url,
            }),
            Err(error) => {
                let _ = sqlx::query(AssertSqlSafe(format!(
                    "DROP DATABASE IF EXISTS \"{database_name}\" WITH (FORCE)"
                )))
                .execute(&migration_admin_pool)
                .await;
                migration_admin_pool.close().await;
                Err(error)
            }
        }
    }

    async fn cleanup(self) -> Result<()> {
        let drop_result = sqlx::query(AssertSqlSafe(format!(
            "DROP DATABASE \"{}\" WITH (FORCE)",
            self.database_name
        )))
        .execute(&self.migration_admin_pool)
        .await;
        self.migration_admin_pool.close().await;
        drop_result
            .map(|_| ())
            .context("drop the doctor test database")
    }
}

#[test]
fn doctor_reports_connection_failure_without_echoing_credentials() -> Result<()> {
    let database_url = "postgresql://doctor:super-secret@[";
    let output = Command::new(env!("CARGO_BIN_EXE_palimpsest-server"))
        .arg("doctor")
        .env("PALIMPSEST_DATABASE_URL", database_url)
        .output()
        .context("run palimpsest doctor against an unavailable database")?;

    ensure!(
        !output.status.success(),
        "doctor must fail when the database is unavailable"
    );
    let report: Value = serde_json::from_slice(&output.stdout).context("parse doctor JSON")?;
    ensure!(
        report["status"] == "not_ready",
        "unexpected doctor report: {report}"
    );
    ensure!(
        report["checks"]["database"]["code"] == "connection-failed",
        "doctor did not classify the connection failure: {report}"
    );
    ensure!(
        !String::from_utf8_lossy(&output.stdout).contains(database_url)
            && !String::from_utf8_lossy(&output.stderr).contains("super-secret"),
        "doctor echoed credentials"
    );
    Ok(())
}

#[test]
fn doctor_reports_missing_database_configuration_as_not_ready() -> Result<()> {
    let output = Command::new(env!("CARGO_BIN_EXE_palimpsest-server"))
        .arg("doctor")
        .env_remove("PALIMPSEST_DATABASE_URL")
        .output()
        .context("run palimpsest doctor without database configuration")?;

    ensure!(
        !output.status.success(),
        "doctor must fail without a database URL"
    );
    let report: Value = serde_json::from_slice(&output.stdout).context("parse doctor JSON")?;
    ensure!(
        report["status"] == "not_ready",
        "unexpected doctor report: {report}"
    );
    ensure!(
        report["checks"]["database"]["code"] == "database-url-missing",
        "doctor did not classify missing configuration: {report}"
    );
    Ok(())
}
