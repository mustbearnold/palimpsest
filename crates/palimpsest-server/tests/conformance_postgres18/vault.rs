//! vault — spec 017 P1 conformance: markdown vault projection and push-only git sync.
//!
//! AC1 vault pages rebuild byte for byte from the canonical layer.
//! AC2 the vault export kind renders derived pages; spec 004 packages do not change.
//! AC3 direct sync-back is rejected; no renderer output enters canonical memory.

use anyhow::{Context, Result, ensure};
use std::{env, fs, path::Path, sync::Arc};

use palimpsest_application::{
    CANONICAL_HISTORY_EXPORT_PROFILE, ExportOperationState, FileExportPackageStore, MemoryService,
    WIKI_VAULT_EXPORT_PROFILE,
};
use palimpsest_domain::{
    AppendEpisode, CaseId, CreateFact, EpisodeId, EpisodeKind, FactId, FactKey, FactNamespace,
    OperationGrant, PrincipalId, PrincipalScope, Provenance, RetentionPolicyId, Sensitivity,
    SourceType, SubjectId, SupersedeFact, TenantId, ValidTime, WritePolicy, WritePolicyId,
    WritePolicyVersion,
};
use palimpsest_http::StaticAuthenticator;
use palimpsest_postgres::PostgresMemoryRepository;
use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

use super::fixtures::StaticExportWorkerAuthorizer;

const SCRIPT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../scripts/wiki-vault-sync.sh"
);

struct VaultHarness {
    service: MemoryService,
    store_dir: std::path::PathBuf,
    principal: PrincipalScope,
    tenant_id: TenantId,
    subject_id: SubjectId,
}

async fn harness(
    pool: &PgPool,
    migration_pool: &PgPool,
    tenant_id: TenantId,
    subject_id: SubjectId,
    principal_name: &str,
    token: &str,
) -> Result<VaultHarness> {
    sqlx::query(
        "INSERT INTO memory.subject_lifecycles
            (tenant_id, subject_id, lifecycle_state, state_version)
         VALUES ($1, $2, 'active', 0)",
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .execute(migration_pool)
    .await
    .context("seed active vault lifecycle")?;

    let principal = PrincipalScope {
        principal_id: PrincipalId(principal_name.to_owned()),
        tenant_id,
        subject_ids: vec![subject_id],
        allowed_sensitivities: vec![Sensitivity::try_from("internal".to_owned())?],
        operation_grants: vec![OperationGrant::CanonicalHistoryExport],
    };
    let authenticator = Arc::new(StaticAuthenticator::new([(
        token.to_owned(),
        principal.clone(),
    )]));
    let repository = Arc::new(PostgresMemoryRepository::new(pool.clone()));
    let store_dir = env::temp_dir().join(format!("palimpsest-vault-store-{}", Uuid::now_v7()));
    fs::create_dir_all(&store_dir).context("create vault package store directory")?;
    let service = MemoryService::new(
        repository.clone(),
        repository.clone(),
        repository.clone(),
        repository.clone(),
        repository.clone(),
    )
    .with_export_components(
        repository.clone(),
        Arc::new(FileExportPackageStore::new(store_dir.clone())),
    )
    .with_export_worker_authorizer(Arc::new(StaticExportWorkerAuthorizer { authenticator }));
    Ok(VaultHarness {
        service,
        store_dir,
        principal,
        tenant_id,
        subject_id,
    })
}

async fn seed_episode_and_fact(
    harness: &VaultHarness,
    prefix: &str,
) -> Result<(EpisodeId, FactId)> {
    let lease = harness
        .service
        .acquire_subject_content_lease(&harness.principal, harness.tenant_id, harness.subject_id)
        .await
        .context("acquire content lease for vault corpus")?;
    let episode = harness
        .service
        .append_episode(
            &lease,
            &harness.principal,
            format!("{prefix}-episode"),
            AppendEpisode {
                tenant_id: harness.tenant_id,
                subject_id: harness.subject_id,
                case_id: CaseId(Uuid::from_u128(0x701)),
                kind: EpisodeKind::try_from("message".to_owned())?,
                observed_at: OffsetDateTime::parse(
                    "2026-07-31T09:00:00Z",
                    &time::format_description::well_known::Rfc3339,
                )?,
                provenance: Provenance {
                    source_type: SourceType::try_from("vault-conformance".to_owned())?,
                    source_uri: Some(format!("urn:vault:{prefix}")),
                    external_id: Some(format!("vault-{prefix}-episode")),
                },
                sensitivity: Sensitivity::try_from("internal".to_owned())?,
                retention_policy_id: RetentionPolicyId::try_from("standard".to_owned())?,
                payload: serde_json::json!({"marker": format!("vault-episode-{prefix}")}),
            },
        )
        .await
        .context("append vault corpus episode")?
        .episode
        .episode_id;
    let created = harness
        .service
        .create_fact(
            &lease,
            &harness.principal,
            format!("{prefix}-fact"),
            CreateFact {
                tenant_id: harness.tenant_id,
                subject_id: harness.subject_id,
                case_id: CaseId(Uuid::from_u128(0x701)),
                namespace: FactNamespace::try_from("scratch".to_owned())?,
                key: FactKey::try_from("temperature".to_owned())?,
                value: serde_json::json!({"value_celsius": 21.5}),
                observed_at: OffsetDateTime::parse(
                    "2026-07-31T09:01:00Z",
                    &time::format_description::well_known::Rfc3339,
                )?,
                valid_time: ValidTime {
                    from: OffsetDateTime::parse(
                        "2026-07-31T09:01:00Z",
                        &time::format_description::well_known::Rfc3339,
                    )?,
                    until: None,
                },
                evidence_episode_ids: vec![episode],
                write_policy: WritePolicy {
                    id: WritePolicyId::try_from("direct-evidence".to_owned())?,
                    version: WritePolicyVersion::try_from("1".to_owned())?,
                },
                confidence: 0.9,
                sensitivity: Sensitivity::try_from("internal".to_owned())?,
                retention_policy_id: RetentionPolicyId::try_from("standard".to_owned())?,
            },
        )
        .await
        .context("create vault corpus fact")?;
    harness
        .service
        .supersede_fact(
            &lease,
            &harness.principal,
            format!("{prefix}-fact-v2"),
            created.view.head_revision_id,
            SupersedeFact {
                tenant_id: harness.tenant_id,
                subject_id: harness.subject_id,
                fact_id: created.view.fact_id,
                supersedes_revision_id: created.view.head_revision_id,
                value: serde_json::json!({"value_celsius": 22.0}),
                observed_at: OffsetDateTime::parse(
                    "2026-07-31T10:00:00Z",
                    &time::format_description::well_known::Rfc3339,
                )?,
                valid_time: ValidTime {
                    from: OffsetDateTime::parse(
                        "2026-07-31T10:00:00Z",
                        &time::format_description::well_known::Rfc3339,
                    )?,
                    until: None,
                },
                evidence_episode_ids: vec![episode],
                write_policy: WritePolicy {
                    id: WritePolicyId::try_from("direct-evidence".to_owned())?,
                    version: WritePolicyVersion::try_from("1".to_owned())?,
                },
                confidence: 0.95,
                sensitivity: Sensitivity::try_from("internal".to_owned())?,
                retention_policy_id: RetentionPolicyId::try_from("standard".to_owned())?,
            },
        )
        .await
        .context("supersede vault corpus fact")?;
    Ok((episode, created.view.fact_id))
}

async fn materialize_vault_export(
    harness: &VaultHarness,
    key: &str,
) -> Result<(String, u64, usize)> {
    let created = harness
        .service
        .create_export(
            &harness.principal,
            harness.tenant_id,
            harness.subject_id,
            key.to_owned(),
            WIKI_VAULT_EXPORT_PROFILE,
        )
        .await
        .context("create wiki vault export")?;
    ensure!(harness.service.run_export_worker_once().await?);
    let operation = harness
        .service
        .get_export(
            &harness.principal,
            harness.tenant_id,
            harness.subject_id,
            created.operation.export_id,
        )
        .await
        .context("read wiki vault export operation")?;
    ensure!(operation.state == ExportOperationState::Ready);
    ensure!(operation.profile == WIKI_VAULT_EXPORT_PROFILE);
    Ok((
        operation
            .content_sha256
            .context("vault export lacks content digest")?,
        operation.size_bytes.context("vault export lacks size")?,
        operation
            .record_count
            .context("vault export lacks record count")?
            .try_into()
            .context("vault export record count overflow")?,
    ))
}

/// AC1 — the same canonical state rebuilds the same vault pages byte for byte.
pub(crate) async fn vault_pages_rebuild_byte_for_byte(
    pool: &PgPool,
    migration_pool: &PgPool,
) -> Result<()> {
    let tenant_id = TenantId(Uuid::parse_str("019be000-0000-7000-8000-000000000070")?);
    let subject_id = SubjectId(Uuid::parse_str("019be000-0000-7000-8000-000000000071")?);
    let harness = harness(
        pool,
        migration_pool,
        tenant_id,
        subject_id,
        "vault-rebuild-principal",
        "vault-rebuild-token",
    )
    .await?;
    seed_episode_and_fact(&harness, "vault-rebuild").await?;
    let first = materialize_vault_export(&harness, "vault-rebuild-export-a").await?;
    let second = materialize_vault_export(&harness, "vault-rebuild-export-b").await?;
    ensure!(
        first.0 != second.0,
        "distinct exports must differ in at least the processing context"
    );
    let zips = fs::read_dir(&harness.store_dir)?
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "zip"))
        .collect::<Vec<_>>();
    ensure!(
        zips.len() == 2,
        "expected two published vault ZIPs, found {}",
        zips.len()
    );
    let extract_a = env::temp_dir().join(format!("palimpsest-vault-extract-a-{}", Uuid::now_v7()));
    let extract_b = env::temp_dir().join(format!("palimpsest-vault-extract-b-{}", Uuid::now_v7()));
    fs::create_dir_all(&extract_a)?;
    fs::create_dir_all(&extract_b)?;
    for (zip, target) in [(&zips[0], &extract_a), (&zips[1], &extract_b)] {
        let status = std::process::Command::new("unzip")
            .args(["-q", "-o"])
            .arg(zip)
            .arg("-d")
            .arg(target)
            .status()?;
        ensure!(status.success(), "failed to extract vault export ZIP");
    }
    // Pages must rebuild byte for byte; the operation-level digests differ only
    // because the manifest and processing context embed the export id.
    let status = std::process::Command::new("diff")
        .arg("-r")
        .arg(extract_a.join("pages"))
        .arg(extract_b.join("pages"))
        .status()?;
    ensure!(
        status.success(),
        "vault pages did not rebuild byte for byte"
    );
    Ok(())
}

/// AC2 — the vault kind renders derived pages; spec 004 packages do not change.
pub(crate) async fn vault_export_kind_leaves_canonical_packages_unchanged(
    pool: &PgPool,
    migration_pool: &PgPool,
) -> Result<()> {
    let tenant_id = TenantId(Uuid::parse_str("019be000-0000-7000-8000-000000000072")?);
    let subject_id = SubjectId(Uuid::parse_str("019be000-0000-7000-8000-000000000073")?);
    let harness = harness(
        pool,
        migration_pool,
        tenant_id,
        subject_id,
        "vault-boundary-principal",
        "vault-boundary-token",
    )
    .await?;
    seed_episode_and_fact(&harness, "vault-boundary").await?;

    let before = harness
        .service
        .create_export(
            &harness.principal,
            harness.tenant_id,
            harness.subject_id,
            "vault-boundary-canonical-before".to_owned(),
            CANONICAL_HISTORY_EXPORT_PROFILE,
        )
        .await
        .context("create canonical export before vault")?;
    ensure!(harness.service.run_export_worker_once().await?);
    let before = harness
        .service
        .get_export(
            &harness.principal,
            harness.tenant_id,
            harness.subject_id,
            before.operation.export_id,
        )
        .await
        .context("read canonical export before vault")?;
    ensure!(before.profile == CANONICAL_HISTORY_EXPORT_PROFILE);

    let (_, _, _) = materialize_vault_export(&harness, "vault-boundary-export").await?;

    let after = harness
        .service
        .create_export(
            &harness.principal,
            harness.tenant_id,
            harness.subject_id,
            "vault-boundary-canonical-after".to_owned(),
            CANONICAL_HISTORY_EXPORT_PROFILE,
        )
        .await
        .context("create canonical export after vault")?;
    ensure!(harness.service.run_export_worker_once().await?);
    let after = harness
        .service
        .get_export(
            &harness.principal,
            harness.tenant_id,
            harness.subject_id,
            after.operation.export_id,
        )
        .await
        .context("read canonical export after vault")?;
    ensure!(
        before.record_count == after.record_count,
        "the vault export kind changed spec 004 record counts"
    );
    // The two canonical exports freeze the same canonical records. Operation
    // metadata (processing-context, snapshot) differs per export by design;
    // the record files must be byte-identical.
    let before_zip = harness
        .store_dir
        .join(format!("{}.zip", before.export_id.0));
    let after_zip = harness.store_dir.join(format!("{}.zip", after.export_id.0));
    let before_records = zip_record_entries(&before_zip)?;
    let after_records = zip_record_entries(&after_zip)?;
    ensure!(
        before_records == after_records,
        "the vault export kind changed spec 004 record content"
    );

    // The vault package holds derived markdown pages; the canonical package
    // holds NDJSON records only. The boundary must not blur.
    let vault_zip = fs::read_dir(&harness.store_dir)?
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(|entry| entry.path())
        .find(|path| {
            let name = path
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();
            path.extension().is_some_and(|ext| ext == "zip")
                && name != format!("{}.zip", before.export_id.0)
                && name != format!("{}.zip", after.export_id.0)
        })
        .context("no vault ZIP published")?;
    let canonical_zip = harness
        .store_dir
        .join(format!("{}.zip", before.export_id.0));
    let vault_names = zip_entry_names(&vault_zip)?;
    let canonical_names = zip_entry_names(&canonical_zip)?;
    ensure!(
        vault_names
            .iter()
            .any(|name| name.starts_with("pages/facts/") && name.ends_with(".md")),
        "vault package lacks rendered fact pages: {vault_names:?}"
    );
    ensure!(
        vault_names
            .iter()
            .any(|name| name.starts_with("pages/episodes/") && name.ends_with(".md")),
        "vault package lacks rendered episode pages: {vault_names:?}"
    );
    ensure!(
        !canonical_names.iter().any(|name| name.ends_with(".md")),
        "canonical package leaked derived markdown pages: {canonical_names:?}"
    );
    ensure!(
        vault_names.iter().any(|name| name == "manifest.json"),
        "vault package lacks a manifest"
    );
    Ok(())
}

/// Read the `records/` NDJSON entries of an export ZIP as (name, bytes) pairs.
fn zip_record_entries(zip_path: &Path) -> Result<Vec<(String, Vec<u8>)>> {
    let mut entries = Vec::new();
    for name in zip_entry_names(zip_path)? {
        if name.starts_with("records/") {
            let output = std::process::Command::new("unzip")
                .args(["-p", zip_path.to_str().context("zip path")?, &name])
                .output()
                .context("unzip -p failed")?;
            ensure!(output.status.success(), "unzip -p failed for {name}");
            entries.push((name, output.stdout));
        }
    }
    entries.sort();
    Ok(entries)
}

/// AC3 — direct sync-back is rejected; the sync is push-only and the renderer
/// writes nothing back into canonical memory.
pub(crate) async fn vault_sync_rejects_direct_sync_back(
    pool: &PgPool,
    migration_pool: &PgPool,
) -> Result<()> {
    let tenant_id = TenantId(Uuid::parse_str("019be000-0000-7000-8000-000000000074")?);
    let subject_id = SubjectId(Uuid::parse_str("019be000-0000-7000-8000-000000000075")?);
    let harness = harness(
        pool,
        migration_pool,
        tenant_id,
        subject_id,
        "vault-sync-principal",
        "vault-sync-token",
    )
    .await?;
    seed_episode_and_fact(&harness, "vault-sync").await?;
    let (_, _, _) = materialize_vault_export(&harness, "vault-sync-export").await?;
    let vault_zip = fs::read_dir(&harness.store_dir)?
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(|entry| entry.path())
        .find(|path| path.extension().is_some_and(|ext| ext == "zip"))
        .context("no vault ZIP published for sync")?;

    // The script is the mechanism of one-way sync: it pushes and it never
    // pulls, merges, rebases, fetches, or clones.
    let script = fs::read_to_string(Path::new(SCRIPT)).context("read wiki vault sync script")?;
    for forbidden in ["pull", "merge", "rebase", "fetch", "clone"] {
        ensure!(
            !script.split_whitespace().any(|word| word == forbidden),
            "sync script contains an inbound git verb: {forbidden}"
        );
    }
    ensure!(script.contains("git push"), "sync script does not push");

    let fact_count_before: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM memory.fact_revisions WHERE tenant_id = $1")
            .bind(tenant_id.0)
            .fetch_one(pool)
            .await
            .context("count fact revisions before sync")?;

    // A foreign file in the vault is discarded: the rebuild treats the package
    // as the only source of truth for the directory.
    let vault_dir = env::temp_dir().join(format!("palimpsest-vault-dir-{}", Uuid::now_v7()));
    fs::create_dir_all(vault_dir.join("pages/facts"))?;
    fs::write(
        vault_dir.join("pages/facts/foreign.md"),
        b"not a renderer output",
    )?;
    let sync = |dir: &std::path::Path| -> Result<()> {
        let status = std::process::Command::new("bash")
            .arg(Path::new(SCRIPT))
            .arg(dir)
            .arg(&vault_zip)
            .status()?;
        ensure!(status.success(), "vault sync failed");
        Ok(())
    };
    sync(&vault_dir)?;
    ensure!(
        !vault_dir.join("pages/facts/foreign.md").exists(),
        "a direct sync-back file survived the rebuild"
    );
    let status = std::process::Command::new("git")
        .args(["-C"])
        .arg(&vault_dir)
        .args(["status", "--porcelain"])
        .status()?;
    ensure!(
        status.success(),
        "vault directory is not a git working tree"
    );
    let log = std::process::Command::new("git")
        .args(["-C"])
        .arg(&vault_dir)
        .args(["log", "--oneline"])
        .output()?;
    let log = String::from_utf8(log.stdout)?;
    ensure!(
        log.contains("sync palimpsest-wiki-vault-v1"),
        "sync commit carries the deterministic message: {log}"
    );

    // A second sync over the same package is a no-op: the tree is unchanged.
    sync(&vault_dir)?;
    let log_again = std::process::Command::new("git")
        .args(["-C"])
        .arg(&vault_dir)
        .args(["log", "--oneline"])
        .output()?;
    let log_again = String::from_utf8(log_again.stdout)?;
    ensure!(log_again == log, "an unchanged vault produced a new commit");

    // The renderer wrote nothing back into canonical memory.
    let fact_count_after: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM memory.fact_revisions WHERE tenant_id = $1")
            .bind(tenant_id.0)
            .fetch_one(pool)
            .await
            .context("count fact revisions after sync")?;
    ensure!(
        fact_count_after == fact_count_before,
        "the vault sync mutated canonical memory"
    );
    Ok(())
}

fn zip_entry_names(zip_path: &Path) -> Result<Vec<String>> {
    let output = std::process::Command::new("unzip")
        .args(["-Z1"])
        .arg(zip_path)
        .output()?;
    ensure!(output.status.success(), "failed to list ZIP entries");
    Ok(String::from_utf8(output.stdout)?
        .lines()
        .map(str::to_owned)
        .collect())
}
