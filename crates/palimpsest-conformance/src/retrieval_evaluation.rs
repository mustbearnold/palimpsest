use super::{Episode, FactView, RetrievalItem, RetrievalReceipt, Target};
use anyhow::{Context, Result, bail, ensure};
use reqwest::{Client, StatusCode, header};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fs, path::PathBuf};
use uuid::Uuid;

pub const CORPUS_VERSION: &str = "retrieval-corpus-v1";
pub const EXPECTED_SCENARIO_COUNT: usize = 128;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Corpus {
    pub version: String,
    pub seed: String,
    pub scenarios: Vec<Scenario>,
}

#[derive(Clone, Debug, Deserialize)]
struct CorpusManifest {
    version: String,
    corpus_sha256: String,
    seed: String,
    scenario_count: usize,
    calibration_count: usize,
    gate_count: usize,
    baselines: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Scenario {
    pub id: String,
    pub category: String,
    pub split: String,
    pub query: String,
    pub expected_disposition: String,
    pub relevant_ids: Vec<String>,
    pub forbidden_ids: Vec<String>,
    pub case_id: String,
    pub perspective: String,
    pub facts: Vec<FactFixture>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FactFixture {
    pub id: String,
    pub scope: String,
    pub namespace: String,
    pub key: String,
    pub text: String,
    pub vector: String,
    pub sensitivity: String,
    pub retention_policy_id: String,
    pub observed_at: String,
    pub valid_from: String,
    pub write_policy_id: String,
    pub confidence: f64,
    pub supersedes: Option<String>,
    pub lifecycle: String,
}

#[derive(Clone, Debug)]
pub struct PreparedCorpus {
    pub revisions: BTreeMap<String, Uuid>,
    pub recorded_cutoffs: BTreeMap<String, ScenarioCutoffs>,
    pub lifecycle: Vec<LifecycleMutation>,
    pub projections: Vec<PreparedProjection>,
}

#[derive(Clone, Debug)]
pub struct ScenarioCutoffs {
    pub before_successor: Option<String>,
    pub after_setup: String,
}

#[derive(Clone, Debug)]
pub struct LifecycleMutation {
    pub logical_id: String,
    pub tenant_id: Uuid,
    pub subject_id: Uuid,
    pub case_id: Uuid,
    pub fact_id: Uuid,
    pub revision_id: Uuid,
    pub lifecycle: String,
}

#[derive(Clone, Debug)]
pub struct PreparedProjection {
    pub tenant_id: Uuid,
    pub subject_id: Uuid,
    pub revision_id: Uuid,
}

#[derive(Clone, Debug)]
struct PreparedFact {
    fact_id: Uuid,
    head_revision_id: Uuid,
    revision_id: Uuid,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ScenarioPrediction {
    pub scenario_id: String,
    pub disposition: String,
    pub ranked_ids: Vec<String>,
    pub scores: BTreeMap<String, BTreeMap<String, String>>,
    pub policy_id: String,
    pub policy_version: String,
    pub policy_digest: String,
    pub embedder_profile_digest: Option<String>,
    pub projection_profile_digest: Option<String>,
    pub provenance_complete: bool,
    pub forbidden_leaks: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct BaselinePredictions {
    pub baseline: String,
    pub scenarios: Vec<ScenarioPrediction>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Metrics {
    pub exact_name_hit_at_1: String,
    pub temporal_selection: String,
    pub abstention_correctness: String,
    pub recall_at_10_overall: String,
    pub recall_at_10_temporal_update: String,
    pub ndcg_at_10: String,
    pub mrr_at_10: String,
    pub provenance_coverage: String,
    pub forbidden_leaks: usize,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct EvaluationArtifact {
    pub schema_version: String,
    pub corpus_version: String,
    pub corpus_sha256: String,
    pub baselines: Vec<BaselinePredictions>,
    pub baseline_metrics: BTreeMap<String, Metrics>,
    pub full_policy_metrics: Metrics,
    pub repeated_runs: usize,
    pub repetitions_identical: bool,
    pub rebuild_identical: bool,
}

pub fn load_frozen_corpus() -> Result<Corpus> {
    let path = artifact_root().join("corpus.json");
    let bytes = fs::read(&path).with_context(|| format!("read {}", path.display()))?;
    let corpus: Corpus =
        serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))?;
    validate_corpus(&corpus)?;
    let manifest_path = artifact_root().join("manifest.json");
    let manifest: CorpusManifest = serde_json::from_slice(
        &fs::read(&manifest_path).with_context(|| format!("read {}", manifest_path.display()))?,
    )?;
    ensure!(manifest.version == "retrieval-corpus-manifest-v1");
    ensure!(manifest.corpus_sha256 == sha256_bytes(&bytes));
    ensure!(manifest.seed == corpus.seed);
    ensure!(manifest.scenario_count == EXPECTED_SCENARIO_COUNT);
    ensure!(manifest.calibration_count == 32 && manifest.gate_count == 96);
    ensure!(
        manifest.baselines
            == [
                "exact-fts-only",
                "exact-vector-only",
                "hybrid-without-temporal",
                "full-policy"
            ]
    );
    Ok(corpus)
}

pub fn artifact_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../evaluations/retrieval-corpus-v1")
}

fn validate_corpus(corpus: &Corpus) -> Result<()> {
    ensure!(corpus.version == CORPUS_VERSION);
    ensure!(corpus.seed.len() == 64);
    ensure!(corpus.scenarios.len() == EXPECTED_SCENARIO_COUNT);

    let mut category_counts = BTreeMap::new();
    let mut split_counts = BTreeMap::new();
    let mut ids = std::collections::BTreeSet::new();
    for scenario in &corpus.scenarios {
        ensure!(
            ids.insert(&scenario.id),
            "duplicate scenario {}",
            scenario.id
        );
        *category_counts
            .entry(scenario.category.as_str())
            .or_insert(0usize) += 1;
        *split_counts
            .entry(scenario.split.as_str())
            .or_insert(0usize) += 1;
        ensure!(matches!(scenario.split.as_str(), "calibration" | "gate"));
        ensure!(matches!(
            scenario.expected_disposition.as_str(),
            "results" | "abstained"
        ));
        ensure!(!scenario.query.is_empty());
        ensure!(Uuid::parse_str(&scenario.case_id).is_ok());
        ensure!(matches!(
            scenario.perspective.as_str(),
            "fixed" | "before-successor" | "after-successor"
        ));
        ensure!(
            scenario
                .relevant_ids
                .iter()
                .all(|id| !scenario.forbidden_ids.contains(id))
        );
        let fact_ids = scenario
            .facts
            .iter()
            .map(|fact| fact.id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        ensure!(fact_ids.len() == scenario.facts.len());
        ensure!(
            scenario
                .relevant_ids
                .iter()
                .chain(&scenario.forbidden_ids)
                .all(|id| fact_ids.contains(id.as_str()))
        );
        for fact in &scenario.facts {
            ensure!(matches!(
                fact.scope.as_str(),
                "primary" | "secondary-subject" | "secondary-tenant"
            ));
            ensure!(matches!(
                fact.vector.as_str(),
                "relevant" | "near" | "distractor" | "trap"
            ));
            ensure!(matches!(
                fact.lifecycle.as_str(),
                "active" | "deleted" | "expired"
            ));
            ensure!(!fact.retention_policy_id.is_empty());
            ensure!((0.0..=1.0).contains(&fact.confidence));
            if let Some(root) = &fact.supersedes {
                ensure!(fact_ids.contains(root.as_str()));
            }
        }
    }
    ensure!(
        category_counts
            == BTreeMap::from([
                ("abstention-conflict-ready", 16),
                ("exact-name", 24),
                ("isolation-lifecycle", 32),
                ("stable-versus-decaying", 16),
                ("stale-distractor", 16),
                ("temporal-contradiction", 24),
            ])
    );
    ensure!(split_counts.get("calibration") == Some(&32));
    ensure!(split_counts.get("gate") == Some(&96));
    Ok(())
}

pub async fn prepare_frozen_corpus(target: &Target, corpus: &Corpus) -> Result<PreparedCorpus> {
    let client = Client::new();
    let mut revisions = BTreeMap::new();
    let mut facts = BTreeMap::<String, PreparedFact>::new();
    let mut recorded_cutoffs = BTreeMap::new();
    let mut lifecycle = Vec::new();
    let mut projections = Vec::new();
    for scenario in &corpus.scenarios {
        let mut before_successor = None;
        let mut after_setup = None;
        for fixture in &scenario.facts {
            let scoped_target = scoped_target(target, &fixture.scope);
            let view = create_corpus_fact(
                &client,
                &scoped_target,
                scenario,
                fixture,
                fixture.supersedes.as_ref().and_then(|id| facts.get(id)),
            )
            .await?;
            let revision = view
                .revision
                .as_ref()
                .context("corpus fact response omitted its effective revision")?;
            if fixture.supersedes.is_none()
                && scenario.perspective == "before-successor"
                && before_successor.is_none()
            {
                before_successor = Some(revision.recorded_at.clone());
            }
            after_setup = Some(revision.recorded_at.clone());
            revisions.insert(fixture.id.clone(), revision.revision_id);
            facts.insert(
                fixture.id.clone(),
                PreparedFact {
                    fact_id: view.fact_id,
                    head_revision_id: view.head_revision_id,
                    revision_id: revision.revision_id,
                },
            );
            projections.push(PreparedProjection {
                tenant_id: scoped_target.tenant_id,
                subject_id: scoped_target.subject_id,
                revision_id: revision.revision_id,
            });
            if fixture.lifecycle != "active" {
                lifecycle.push(LifecycleMutation {
                    logical_id: fixture.id.clone(),
                    tenant_id: scoped_target.tenant_id,
                    subject_id: scoped_target.subject_id,
                    case_id: view.case_id,
                    fact_id: view.fact_id,
                    revision_id: revision.revision_id,
                    lifecycle: fixture.lifecycle.clone(),
                });
            }
        }
        recorded_cutoffs.insert(
            scenario.id.clone(),
            ScenarioCutoffs {
                before_successor,
                after_setup: after_setup.context("corpus scenario has no setup timestamp")?,
            },
        );
    }
    Ok(PreparedCorpus {
        revisions,
        recorded_cutoffs,
        lifecycle,
        projections,
    })
}

pub async fn evaluate_frozen_corpus(
    target: &Target,
    corpus: &Corpus,
    prepared: &PreparedCorpus,
    repetitions: usize,
) -> Result<EvaluationArtifact> {
    ensure!(repetitions >= 1);
    let reverse_ids = prepared
        .revisions
        .iter()
        .map(|(logical, revision)| (*revision, logical.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut lexical = Vec::with_capacity(corpus.scenarios.len());
    let mut vector = Vec::with_capacity(corpus.scenarios.len());
    let mut hybrid = Vec::with_capacity(corpus.scenarios.len());
    let mut full = Vec::with_capacity(corpus.scenarios.len());
    let mut repetitions_identical = true;
    for scenario in &corpus.scenarios {
        lexical.push(
            request_prediction(
                target,
                scenario,
                prepared,
                &reverse_ids,
                "retrieval-lexical-v1",
                0,
            )
            .await?,
        );
        let hybrid_prediction = request_prediction(
            target,
            scenario,
            prepared,
            &reverse_ids,
            "retrieval-hybrid-v1",
            0,
        )
        .await?;
        vector.push(vector_only_prediction(&hybrid_prediction));
        hybrid.push(hybrid_prediction);
        let first = request_prediction(
            target,
            scenario,
            prepared,
            &reverse_ids,
            "retrieval-hybrid-temporal-v1",
            0,
        )
        .await?;
        for repetition in 1..repetitions {
            let repeated = request_prediction(
                target,
                scenario,
                prepared,
                &reverse_ids,
                "retrieval-hybrid-temporal-v1",
                repetition,
            )
            .await?;
            repetitions_identical &= repeated == first;
        }
        full.push(first);
    }
    let baselines = vec![
        BaselinePredictions {
            baseline: "exact-fts-only".to_owned(),
            scenarios: lexical,
        },
        BaselinePredictions {
            baseline: "exact-vector-only".to_owned(),
            scenarios: vector,
        },
        BaselinePredictions {
            baseline: "hybrid-without-temporal".to_owned(),
            scenarios: hybrid,
        },
        BaselinePredictions {
            baseline: "full-policy".to_owned(),
            scenarios: full,
        },
    ];
    let baseline_metrics = baselines
        .iter()
        .map(|baseline| {
            Ok((
                baseline.baseline.clone(),
                calculate_metrics(corpus, &baseline.scenarios)?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let metrics = baseline_metrics
        .get("full-policy")
        .context("full-policy metrics are missing")?
        .clone();
    Ok(EvaluationArtifact {
        schema_version: "retrieval-evaluation-artifact-v1".to_owned(),
        corpus_version: corpus.version.clone(),
        corpus_sha256: sha256_file(&artifact_root().join("corpus.json"))?,
        baselines,
        baseline_metrics,
        full_policy_metrics: metrics,
        repeated_runs: repetitions,
        repetitions_identical,
        rebuild_identical: false,
    })
}

pub async fn evaluate_full_policy_once(
    target: &Target,
    corpus: &Corpus,
    prepared: &PreparedCorpus,
    repetition: usize,
) -> Result<BaselinePredictions> {
    let reverse_ids = prepared
        .revisions
        .iter()
        .map(|(name, id)| (*id, name.clone()))
        .collect();
    let mut scenarios = Vec::with_capacity(corpus.scenarios.len());
    for scenario in &corpus.scenarios {
        scenarios.push(
            request_prediction(
                target,
                scenario,
                prepared,
                &reverse_ids,
                "retrieval-hybrid-temporal-v1",
                repetition,
            )
            .await?,
        );
    }
    Ok(BaselinePredictions {
        baseline: "full-policy".to_owned(),
        scenarios,
    })
}

pub fn write_or_verify_artifact(artifact: &EvaluationArtifact) -> Result<()> {
    let path = artifact_root().join("predictions.json");
    if std::env::var_os("PALIMPSEST_UPDATE_RETRIEVAL_CORPUS").is_some() {
        fs::write(&path, serde_json::to_vec_pretty(artifact)?)
            .with_context(|| format!("write {}", path.display()))?;
        return Ok(());
    }
    let expected: EvaluationArtifact = serde_json::from_slice(
        &fs::read(&path).with_context(|| format!("read {}", path.display()))?,
    )?;
    ensure!(
        &expected == artifact,
        "frozen retrieval predictions changed"
    );
    Ok(())
}

pub fn verify_frozen_artifact() -> Result<EvaluationArtifact> {
    let path = artifact_root().join("predictions.json");
    let artifact: EvaluationArtifact = serde_json::from_slice(
        &fs::read(&path).with_context(|| format!("read {}", path.display()))?,
    )?;
    ensure!(artifact.schema_version == "retrieval-evaluation-artifact-v1");
    ensure!(artifact.corpus_version == CORPUS_VERSION);
    ensure!(artifact.corpus_sha256 == sha256_file(&artifact_root().join("corpus.json"))?);
    ensure!(artifact.baselines.len() == 4);
    ensure!(artifact.baseline_metrics.len() == 4);
    for baseline in &artifact.baselines {
        ensure!(baseline.scenarios.len() == EXPECTED_SCENARIO_COUNT);
    }
    enforce_issue_22_gates(&artifact)?;
    Ok(artifact)
}

async fn create_corpus_fact(
    client: &Client,
    target: &Target,
    scenario: &Scenario,
    fixture: &FactFixture,
    root: Option<&PreparedFact>,
) -> Result<FactView> {
    let episode_url = format!(
        "{}/v1/tenants/{}/subjects/{}/episodes",
        target.base_url.trim_end_matches('/'),
        target.tenant_id,
        target.subject_id
    );
    let episode_response = client
        .post(&episode_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", format!("corpus-{}-episode", fixture.id))
        .json(&json!({
            "case_id": scenario.case_id,
            "kind": "retrieval-corpus-fixture",
            "observed_at": fixture.observed_at,
            "provenance": {"source_type": "conformance", "source_uri": null, "external_id": fixture.id},
            "sensitivity": fixture.sensitivity,
            "retention_policy_id": fixture.retention_policy_id,
            "payload": {"logical_id": fixture.id}
        }))
        .send().await?;
    ensure!(
        episode_response.status() == StatusCode::CREATED,
        "{} episode returned {}",
        fixture.id,
        episode_response.status()
    );
    let episode: Episode = episode_response.json().await?;
    let body = json!({
        "value": {"text": fixture.text, "corpus_vector": fixture.vector, "logical_id": fixture.id},
        "observed_at": fixture.observed_at,
        "valid_time": {"from": fixture.valid_from, "until": null},
        "evidence_episode_ids": [episode.episode_id],
        "write_policy": {"id": fixture.write_policy_id, "version": "1"},
        "confidence": fixture.confidence,
        "sensitivity": fixture.sensitivity,
        "retention_policy_id": fixture.retention_policy_id
    });
    let response = if let Some(root) = root {
        let mut successor = body;
        successor["supersedes_revision_id"] = json!(root.revision_id);
        client
            .put(format!(
                "{}/v1/tenants/{}/subjects/{}/facts/{}",
                target.base_url.trim_end_matches('/'),
                target.tenant_id,
                target.subject_id,
                root.fact_id
            ))
            .bearer_auth(&target.bearer_token)
            .header("Idempotency-Key", format!("corpus-{}-fact", fixture.id))
            .header(header::IF_MATCH, format!("\"{}\"", root.head_revision_id))
            .json(&successor)
            .send()
            .await?
    } else {
        let mut create = body;
        create["case_id"] = json!(scenario.case_id);
        create["namespace"] = json!(fixture.namespace);
        create["key"] = json!(fixture.key);
        client
            .post(format!(
                "{}/v1/tenants/{}/subjects/{}/facts",
                target.base_url.trim_end_matches('/'),
                target.tenant_id,
                target.subject_id
            ))
            .bearer_auth(&target.bearer_token)
            .header("Idempotency-Key", format!("corpus-{}-fact", fixture.id))
            .json(&create)
            .send()
            .await?
    };
    ensure!(
        matches!(response.status(), StatusCode::CREATED | StatusCode::OK),
        "{} fact returned {}",
        fixture.id,
        response.status()
    );
    response.json().await.map_err(Into::into)
}

fn scoped_target(target: &Target, scope: &str) -> Target {
    match scope {
        "primary" => target.clone(),
        "secondary-subject" => Target {
            subject_id: target.principal_a_secondary_subject_id,
            ..target.clone()
        },
        "secondary-tenant" => Target {
            bearer_token: target.principal_b_bearer_token.clone(),
            tenant_id: target.principal_b_tenant_id,
            subject_id: target.principal_b_subject_id,
            ..target.clone()
        },
        _ => unreachable!("validated corpus scope"),
    }
}

async fn request_prediction(
    target: &Target,
    scenario: &Scenario,
    prepared: &PreparedCorpus,
    reverse_ids: &BTreeMap<Uuid, String>,
    policy_id: &str,
    repetition: usize,
) -> Result<ScenarioPrediction> {
    let cutoffs = prepared
        .recorded_cutoffs
        .get(&scenario.id)
        .context("missing scenario cutoffs")?;
    let recorded_at = if scenario.perspective == "before-successor" {
        cutoffs
            .before_successor
            .as_ref()
            .context("missing before-successor cutoff")?
    } else {
        &cutoffs.after_setup
    };
    let response = Client::new()
        .post(format!(
            "{}/v1/tenants/{}/subjects/{}/retrievals",
            target.base_url.trim_end_matches('/'), target.tenant_id, target.subject_id
        ))
        .bearer_auth(&target.principal_a_internal_bearer_token)
        .header("Idempotency-Key", format!("corpus-{}-{}-{repetition}", scenario.id, policy_id))
        .json(&json!({
            "query": scenario.query,
            "perspective": {"kind": "as_of", "valid_at": "2026-06-30T00:00:00Z", "recorded_at": recorded_at},
            "page_size": 10,
            "policy_id": policy_id,
            "filters": {"case_ids": [scenario.case_id]}
        }))
        .send().await?;
    ensure!(
        response.status() == StatusCode::CREATED,
        "{} under {} returned {}",
        scenario.id,
        policy_id,
        response.status()
    );
    let raw = response.bytes().await?;
    let forbidden_leaks = scenario
        .forbidden_ids
        .iter()
        .filter_map(|logical| {
            let revision = prepared.revisions.get(logical)?;
            std::str::from_utf8(&raw)
                .ok()?
                .contains(&revision.to_string())
                .then(|| logical.clone())
        })
        .collect::<Vec<_>>();
    let receipt: RetrievalReceipt = serde_json::from_slice(&raw)?;
    prediction_from_receipt(scenario, receipt, reverse_ids, forbidden_leaks)
}

fn prediction_from_receipt(
    scenario: &Scenario,
    receipt: RetrievalReceipt,
    reverse_ids: &BTreeMap<Uuid, String>,
    forbidden_leaks: Vec<String>,
) -> Result<ScenarioPrediction> {
    let mut ranked_ids = Vec::new();
    let mut scores = BTreeMap::new();
    let mut provenance_complete = true;
    let embedder_profile_digest = receipt
        .query_embedding
        .as_ref()
        .map(|embedding| embedding.profile_digest.clone());
    let projection_profile_digest = receipt
        .query_embedding
        .as_ref()
        .map(|embedding| embedding.projection_profile_digest.clone());
    for item in &receipt.items {
        let logical = reverse_ids
            .get(&item.revision_id)
            .with_context(|| {
                format!(
                    "{} returned unknown revision {}",
                    scenario.id, item.revision_id
                )
            })?
            .clone();
        ranked_ids.push(logical.clone());
        scores.insert(logical, score_map(item));
        provenance_complete &= !item.evidence_episode_ids.is_empty();
    }
    Ok(ScenarioPrediction {
        scenario_id: scenario.id.clone(),
        disposition: receipt.status,
        ranked_ids,
        scores,
        policy_id: receipt.policy.id,
        policy_version: receipt.policy.version,
        policy_digest: receipt.policy.digest,
        embedder_profile_digest,
        projection_profile_digest,
        provenance_complete,
        forbidden_leaks,
    })
}

fn score_map(item: &RetrievalItem) -> BTreeMap<String, String> {
    item.scores
        .iter()
        .map(|score| (score.component.clone(), score.value.clone()))
        .collect()
}

fn vector_only_prediction(hybrid: &ScenarioPrediction) -> ScenarioPrediction {
    let mut prediction = hybrid.clone();
    prediction.policy_id = "derived-exact-vector-only-v1".to_owned();
    prediction.policy_digest =
        sha256_bytes(b"derived from retrieval-hybrid-v1 vector_rank; fixture evaluation only");
    prediction.embedder_profile_digest = hybrid.embedder_profile_digest.clone();
    prediction.projection_profile_digest = hybrid.projection_profile_digest.clone();
    prediction.ranked_ids.sort_by_key(|id| {
        prediction
            .scores
            .get(id)
            .and_then(|scores| scores.get("vector_rank"))
            .and_then(|rank| rank.parse::<u64>().ok())
            .unwrap_or(u64::MAX)
    });
    prediction
}

fn calculate_metrics(corpus: &Corpus, predictions: &[ScenarioPrediction]) -> Result<Metrics> {
    ensure!(corpus.scenarios.len() == predictions.len());
    let by_id = predictions
        .iter()
        .map(|p| (p.scenario_id.as_str(), p))
        .collect::<BTreeMap<_, _>>();
    let mut exact_hits = Vec::new();
    let mut temporal_hits = Vec::new();
    let mut abstention_hits = Vec::new();
    let mut recalls = Vec::new();
    let mut temporal_recalls = Vec::new();
    let mut ndcgs = Vec::new();
    let mut reciprocal_ranks = Vec::new();
    let mut provenance = Vec::new();
    let mut forbidden_leaks = 0;
    for scenario in &corpus.scenarios {
        let prediction = by_id
            .get(scenario.id.as_str())
            .context("missing scenario prediction")?;
        forbidden_leaks += prediction.forbidden_leaks.len();
        provenance.push(prediction.provenance_complete);
        let first_relevant_rank = prediction
            .ranked_ids
            .iter()
            .position(|id| scenario.relevant_ids.contains(id))
            .map(|i| i + 1);
        if scenario.category == "exact-name" {
            exact_hits.push(first_relevant_rank == Some(1));
        }
        if scenario.category == "temporal-contradiction" {
            temporal_hits.push(first_relevant_rank == Some(1));
        }
        if scenario.expected_disposition == "abstained" {
            abstention_hits
                .push(prediction.disposition == "abstained" && prediction.ranked_ids.is_empty());
        }
        if !scenario.relevant_ids.is_empty() {
            let hits = prediction
                .ranked_ids
                .iter()
                .take(10)
                .filter(|id| scenario.relevant_ids.contains(id))
                .count();
            let recall = hits as f64 / scenario.relevant_ids.len() as f64;
            recalls.push(recall);
            if matches!(
                scenario.category.as_str(),
                "temporal-contradiction" | "stale-distractor" | "stable-versus-decaying"
            ) {
                temporal_recalls.push(recall);
            }
            let rr = first_relevant_rank
                .filter(|rank| *rank <= 10)
                .map(|rank| 1.0 / rank as f64)
                .unwrap_or(0.0);
            reciprocal_ranks.push(rr);
            ndcgs.push(
                first_relevant_rank
                    .filter(|rank| *rank <= 10)
                    .map(|rank| 1.0 / (rank as f64 + 1.0).log2())
                    .unwrap_or(0.0),
            );
        }
    }
    Ok(Metrics {
        exact_name_hit_at_1: ratio(&exact_hits),
        temporal_selection: ratio(&temporal_hits),
        abstention_correctness: ratio(&abstention_hits),
        recall_at_10_overall: mean(&recalls),
        recall_at_10_temporal_update: mean(&temporal_recalls),
        ndcg_at_10: mean(&ndcgs),
        mrr_at_10: mean(&reciprocal_ranks),
        provenance_coverage: ratio(&provenance),
        forbidden_leaks,
    })
}

fn ratio(values: &[bool]) -> String {
    if values.is_empty() {
        return "0.000000".to_owned();
    }
    format!(
        "{:.6}",
        values.iter().filter(|value| **value).count() as f64 / values.len() as f64
    )
}

fn mean(values: &[f64]) -> String {
    if values.is_empty() {
        return "0.000000".to_owned();
    }
    format!("{:.6}", values.iter().sum::<f64>() / values.len() as f64)
}

fn sha256_file(path: &PathBuf) -> Result<String> {
    Ok(sha256_bytes(
        &fs::read(path).with_context(|| format!("read {}", path.display()))?,
    ))
}

fn sha256_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub fn enforce_issue_22_gates(artifact: &EvaluationArtifact) -> Result<()> {
    let metric = &artifact.full_policy_metrics;
    ensure!(metric.forbidden_leaks == 0);
    ensure!(metric.exact_name_hit_at_1 == "1.000000");
    ensure!(metric.temporal_selection == "1.000000");
    ensure!(metric.abstention_correctness == "1.000000");
    ensure!(metric.provenance_coverage == "1.000000");
    ensure!(metric.recall_at_10_overall.parse::<f64>()? >= 0.90);
    ensure!(metric.recall_at_10_temporal_update.parse::<f64>()? >= 0.85);
    ensure!(artifact.repeated_runs >= 10 && artifact.repetitions_identical);
    ensure!(artifact.rebuild_identical);
    if artifact.baselines.len() != 4 {
        bail!("all four baselines are required");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frozen_corpus_has_the_issue_22_shape() {
        let corpus = load_frozen_corpus().expect("frozen corpus should be valid");
        assert_eq!(corpus.scenarios.len(), EXPECTED_SCENARIO_COUNT);
    }

    #[test]
    fn frozen_predictions_satisfy_the_issue_22_gates() {
        let artifact = verify_frozen_artifact().expect("frozen predictions should be valid");
        assert_eq!(artifact.baselines.len(), 4);
    }
}
