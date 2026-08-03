use std::{fs, process::Command};

use anyhow::{Context, Result, ensure};
use palimpsest_application::{RestoreFenceEntry, RestoreFenceLedger};
use serde_json::Value;
use time::OffsetDateTime;
use uuid::Uuid;

fn write_test_ledger() -> Result<(std::path::PathBuf, RestoreFenceLedger)> {
    let now = OffsetDateTime::now_utc();
    let ledger = RestoreFenceLedger::build(
        now,
        vec![RestoreFenceEntry::new(
            format!("v1:{:064x}", 7),
            3,
            now - time::Duration::seconds(1),
            now + time::Duration::hours(1),
        )?],
    )?;
    let path = std::env::temp_dir().join(format!("palimpsest-restore-{}.json", Uuid::now_v7()));
    fs::write(&path, ledger.to_bytes()?)?;
    Ok((path, ledger))
}

#[test]
fn restore_verify_reports_only_content_free_ledger_metadata() -> Result<()> {
    let (path, ledger) = write_test_ledger()?;
    let output = Command::new(env!("CARGO_BIN_EXE_palimpsest-server"))
        .args(["restore", "verify"])
        .env("PALIMPSEST_RESTORE_FENCE_LEDGER_PATH", &path)
        .env(
            "PALIMPSEST_RESTORE_FENCE_LEDGER_SHA256",
            &ledger.ledger_sha256,
        )
        .output()
        .context("run restore verify")?;
    let _ = fs::remove_file(&path);

    ensure!(
        output.status.success(),
        "restore verify failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout).context("parse restore report")?;
    ensure!(report["operation"] == "verify");
    ensure!(report["status"] == "verified");
    ensure!(report["profile"] == "palimpsest-deletion-fence-ledger-v1");
    ensure!(report["schema_version"] == 1 && report["entry_count"] == 1);
    ensure!(report["ledger_sha256"] == ledger.ledger_sha256);
    ensure!(!String::from_utf8_lossy(&output.stdout).contains(&format!("v1:{:064x}", 7)));
    ensure!(!String::from_utf8_lossy(&output.stdout).contains(path.to_string_lossy().as_ref()));
    Ok(())
}

#[test]
fn restore_verify_fails_closed_without_echoing_ledger_content() -> Result<()> {
    let (path, ledger) = write_test_ledger()?;
    let output = Command::new(env!("CARGO_BIN_EXE_palimpsest-server"))
        .args(["restore", "verify"])
        .env("PALIMPSEST_RESTORE_FENCE_LEDGER_PATH", &path)
        .env("PALIMPSEST_RESTORE_FENCE_LEDGER_SHA256", "0".repeat(64))
        .output()
        .context("run invalid restore verify")?;
    let _ = fs::remove_file(&path);

    ensure!(!output.status.success());
    let report: Value = serde_json::from_slice(&output.stdout).context("parse failure report")?;
    ensure!(report["operation"] == "verify");
    ensure!(report["status"] == "blocked");
    ensure!(report["error"]["code"] == "ledger-verification-failed");
    ensure!(!String::from_utf8_lossy(&output.stdout).contains(&ledger.ledger_sha256));
    Ok(())
}
