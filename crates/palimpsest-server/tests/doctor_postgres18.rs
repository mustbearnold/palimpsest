use std::process::Command;

use anyhow::{Context, Result, ensure};
use serde_json::Value;

#[test]
fn doctor_reports_a_ready_supported_runtime_database() -> Result<()> {
    let database_url = std::env::var("PALIMPSEST_DOCTOR_DATABASE_URL")
        .or_else(|_| std::env::var("PALIMPSEST_TEST_DATABASE_URL"))
        .context(
            "PALIMPSEST_DOCTOR_DATABASE_URL or PALIMPSEST_TEST_DATABASE_URL must identify the migrated test database",
        )?;
    let output = Command::new(env!("CARGO_BIN_EXE_palimpsest-server"))
        .arg("doctor")
        .env("PALIMPSEST_DATABASE_URL", &database_url)
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
        report["checks"]["migrations"]["latest_version"] == 16,
        "doctor did not verify the latest migration: {report}"
    );
    ensure!(
        report["checks"]["runtime_role"]["superuser"] == false
            && report["checks"]["runtime_role"]["bypass_rls"] == false,
        "doctor did not verify a restricted runtime role: {report}"
    );
    ensure!(
        !String::from_utf8_lossy(&output.stdout).contains(&database_url),
        "doctor leaked the database URL"
    );
    Ok(())
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
