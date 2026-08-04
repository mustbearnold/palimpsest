use std::process::Command;

use anyhow::{Context, Result, ensure};
use serde_json::Value;

fn database_url() -> String {
    std::env::var("PALIMPSEST_TEST_DATABASE_URL")
        .or_else(|_| std::env::var("PALIMPSEST_DATABASE_URL"))
        .unwrap_or_else(|_| {
            "postgresql://mustbearn@localhost/postgres?host=/var/run/postgresql".to_owned()
        })
}

fn migration_database_url(database_url: &str) -> String {
    std::env::var("PALIMPSEST_MIGRATION_DATABASE_URL").unwrap_or_else(|_| database_url.to_owned())
}

#[test]
fn migrate_status_and_plan_are_machine_readable_without_memory_content() -> Result<()> {
    let database_url = database_url();
    let migration_database_url = migration_database_url(&database_url);
    for operation in ["status", "plan"] {
        let output = Command::new(env!("CARGO_BIN_EXE_palimpsest-server"))
            .args(["migrate", operation])
            .env("PALIMPSEST_DATABASE_URL", &database_url)
            .env("PALIMPSEST_MIGRATION_DATABASE_URL", &migration_database_url)
            .output()
            .with_context(|| format!("run migrate {operation}"))?;

        ensure!(
            output.status.success(),
            "migrate {operation} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let report: Value = serde_json::from_slice(&output.stdout)
            .with_context(|| format!("parse migrate {operation} JSON"))?;
        ensure!(
            report["operation"] == operation,
            "unexpected report: {report}"
        );
        ensure!(
            report["status"] == "current" || report["status"] == "pending",
            "unexpected migration status: {report}"
        );
        ensure!(
            report["expected_version"] == 20
                && report["applied_versions"].is_array()
                && report["pending"].is_array()
                && report["failed_versions"].is_array()
                && report["checksum_mismatches"].is_array(),
            "migration report omitted required fields: {report}"
        );
        ensure!(
            report["lock"]["name"] == "palimpsest:migrations:v1"
                && report["lock"]["available"].is_boolean(),
            "migration report omitted lock state: {report}"
        );
        ensure!(
            !String::from_utf8_lossy(&output.stdout).contains(&database_url),
            "migration command leaked the database URL"
        );
        ensure!(
            !String::from_utf8_lossy(&output.stdout).contains(&migration_database_url),
            "migration command leaked the migration database URL"
        );
    }
    Ok(())
}

#[test]
fn migrate_help_is_content_free_usage() -> Result<()> {
    let output = Command::new(env!("CARGO_BIN_EXE_palimpsest-server"))
        .args(["migrate", "--help"])
        .output()
        .context("run migrate help")?;

    ensure!(output.status.success(), "migrate help failed");
    let help = String::from_utf8_lossy(&output.stdout);
    ensure!(
        help.contains("migrate status")
            && help.contains("migrate plan")
            && help.contains("migrate apply"),
        "migrate help omitted an operation: {help}"
    );
    Ok(())
}
