//! property_idempotency — model-based randomized property tests for the
//! checkpoint idempotency-replay state machine (ADR-0031 improvement R6).
//!
//! The scenario conformance suite covers replay and cross-subject reuse with
//! fixed fixtures (`palimpsest-conformance/src/checkpoints.rs`). This module
//! checks the same state machine against a small reference model over random
//! sequences of colliding (Idempotency-Key, body) pairs:
//!
//! - first use of a key  → 201, no `idempotency-replayed` header;
//! - same key + same body → 201, `idempotency-replayed: true`, same ETag,
//!   byte-identical committed representation;
//! - same key + different body → 409, and the committed receipt stays
//!   replayable with the original body (a rejected mismatch must not poison
//!   the receipt);
//! - per key, exactly one completed idempotency receipt and exactly one new
//!   checkpoint revision (replays and mismatches never double-commit).
//!
//! The loop is driven manually (`Strategy::new_tree` + `current()`) instead
//! of `TestRunner::run` so the HTTP calls can stay in async context; a
//! failing case reports the case index and the op that broke the model.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use reqwest::{Client, StatusCode, header};
use serde_json::{Value, json};
use sqlx::{AssertSqlSafe, PgPool, Row, postgres::PgConnectOptions};
use tokio::net::TcpListener;
use uuid::Uuid;

use palimpsest_domain::{PrincipalId, PrincipalScope, Sensitivity, SubjectId, TenantId};
use palimpsest_http::StaticAuthenticator;

/// Randomized cases; each case is one random write sequence on one thread.
const CASES: u32 = 60;
/// Each case performs 1..=MAX_OPS checkpoint writes (collisions are the point).
const MAX_OPS: usize = 8;
/// Small key alphabet: repeated keys exercise replay and mismatch paths.
const KEYS: [&str; 6] = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta"];

const BEARER_TOKEN: &str = "property-test-token";
const TENANT_ID: &str = "019be000-0000-7000-8000-000000000010";
const SUBJECT_ID: &str = "019be000-0000-7000-8000-000000000020";
const AGENT_ID: &str = "019be000-0000-7000-8000-000000000601";
const THREAD_ID: &str = "019be000-0000-7000-8000-000000000602";
const THREAD_B_ID: &str = "019be000-0000-7000-8000-000000000603";

fn thread_strategy() -> impl Strategy<Value = &'static str> {
    prop::sample::select(&["a", "b"][..])
}

fn key_strategy() -> impl Strategy<Value = &'static str> {
    prop::sample::select(&KEYS[..])
}

/// Three distinct checkpoint bodies; `value` picks the body so "same key,
/// same body" and "same key, different body" are both reachable.
fn body_strategy() -> impl Strategy<Value = Value> {
    prop::sample::select(&[0u32, 1, 2][..]).prop_map(|value| {
        json!({
            "case_id": format!("019be000-0000-7000-8000-{value:012}"),
            "parent_revision_id": null,
            "state": { "step": "awaiting-provider", "work_item": format!("property-{value}") },
            "state_schema_version": 1,
            "effect_transitions": [],
            "provenance": {
                "source_type": "agent.runtime",
                "source_uri": null,
                "external_id": format!("property-run-{value}"),
            },
            "sensitivity": "internal",
            "retention_policy_id": "checkpoint-active-30d-v1",
        })
    })
}

fn checkpoint_url(base_url: &str) -> String {
    format!(
        "{base_url}/v1/tenants/{TENANT_ID}/subjects/{SUBJECT_ID}/agents/{AGENT_ID}/threads/{THREAD_ID}/checkpoint"
    )
}

fn checkpoint_url_for(base_url: &str, thread_id: &str) -> String {
    format!(
        "{base_url}/v1/tenants/{TENANT_ID}/subjects/{SUBJECT_ID}/agents/{AGENT_ID}/threads/{thread_id}/checkpoint"
    )
}

async fn put_checkpoint(
    client: &Client,
    url: &str,
    key: &str,
    body: &Value,
) -> Result<reqwest::Response> {
    Ok(client
        .put(url)
        .bearer_auth(BEARER_TOKEN)
        .header("Idempotency-Key", key)
        .header(header::IF_NONE_MATCH, "*")
        .json(body)
        .send()
        .await?)
}

async fn count_rows(pool: &PgPool, table: &str) -> Result<i64> {
    let sql = match table {
        "receipts" => {
            "SELECT count(*) FROM memory.idempotency_receipts \
             WHERE tenant_id = $1 AND principal_id = $2 AND operation_id = 'saveCheckpoint'"
        }
        "revisions" => {
            "SELECT count(*) FROM memory.checkpoint_revisions \
             WHERE tenant_id = $1 AND subject_id = $2"
        }
        other => anyhow::bail!("unknown count target {other}"),
    };
    let tenant_id = Uuid::parse_str(TENANT_ID)?;
    let subject_id = Uuid::parse_str(SUBJECT_ID)?;
    let mut query = sqlx::query(sql).bind(tenant_id);
    if table == "receipts" {
        query = query.bind("principal-a");
    } else {
        query = query.bind(subject_id);
    }
    Ok(query.fetch_one(pool).await?.try_get(0)?)
}

#[derive(Debug, Clone)]
struct Committed {
    etag: String,
    /// The committed response representation (CheckpointView), which a
    /// replay must reproduce byte-for-byte.
    representation: Value,
}

#[tokio::test]
async fn idempotency_replay_semantics_are_exactly_once_under_random_sequences() -> Result<()> {
    let database_url = std::env::var("PALIMPSEST_TEST_DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://mustbearnold@localhost/postgres?host=/var/run/postgresql".to_owned()
    });
    let admin_pool = PgPool::connect(&database_url)
        .await
        .with_context(|| format!("connect to PostgreSQL through {database_url}"))?;
    let database_name = format!("palimpsest_prop_{}", Uuid::now_v7().simple());
    sqlx::query(AssertSqlSafe(format!(
        "CREATE DATABASE \"{database_name}\""
    )))
    .execute(&admin_pool)
    .await?;
    let options = PgConnectOptions::from_str(&database_url)?.database(&database_name);
    let pool = PgPool::connect_with(options).await?;
    let migration_database_url =
        std::env::var("PALIMPSEST_MIGRATION_DATABASE_URL").unwrap_or_else(|_| database_url.clone());
    let migration_options =
        PgConnectOptions::from_str(&migration_database_url)?.database(&database_name);
    let migration_pool = PgPool::connect_with(migration_options).await?;
    palimpsest_postgres::migrate(&pool).await?;

    let tenant_id = Uuid::parse_str(TENANT_ID)?;
    let subject_id = Uuid::parse_str(SUBJECT_ID)?;
    let authenticator: Arc<dyn palimpsest_http::Authenticator> =
        Arc::new(StaticAuthenticator::new([(
            BEARER_TOKEN.to_owned(),
            PrincipalScope {
                principal_id: PrincipalId("principal-a".to_owned()),
                tenant_id: TenantId(tenant_id),
                subject_ids: vec![SubjectId(subject_id)],
                allowed_sensitivities: vec![Sensitivity::try_from("internal".to_owned())?],
                operation_grants: vec![],
            },
        )]));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server_pool = pool.clone();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            palimpsest_server::app(server_pool.clone(), server_pool, authenticator),
        )
        .await
    });
    let url = checkpoint_url(&format!("http://{address}"));
    let url_a = url.clone();
    let url_b = checkpoint_url_for(&format!("http://{address}"), THREAD_B_ID);
    let url_for = |thread: &str| {
        if thread == "a" {
            url_a.clone()
        } else {
            url_b.clone()
        }
    };
    let client = Client::builder().timeout(Duration::from_secs(30)).build()?;

    let mut runner = proptest::test_runner::TestRunner::new(proptest::test_runner::Config {
        cases: CASES,
        ..proptest::test_runner::Config::default()
    });
    // Each op is (thread, key, body). The reference model below mirrors the
    // live checkpoint contract discovered from the router and repository:
    //   - a fresh key on a thread without a live head   -> 201 (create)
    //   - same key + same thread + same body            -> 201 replay
    //   - same key, different thread or body            -> 409
    //   - fresh key while the thread head is live       -> 412 (head fence)
    // Receipts are scoped (tenant, principal, operation, key) across threads,
    // so keys_used must persist across cases, like the committed heads do.
    let strategy = prop::collection::vec(
        (thread_strategy(), key_strategy(), body_strategy()),
        1..=MAX_OPS,
    );
    // key -> (thread it committed on, committed body)
    let mut keys_used: HashMap<&'static str, (&'static str, Value)> = HashMap::new();
    // thread -> committed head
    let mut heads: HashMap<&'static str, Committed> = HashMap::new();
    let mut total_ops = 0usize;

    for case_index in 0..CASES {
        let tree = strategy
            .new_tree(&mut runner)
            .map_err(|reason| anyhow::anyhow!("proptest case generation failed: {reason}"))?;
        let ops = tree.current();
        let receipts_before = count_rows(&migration_pool, "receipts").await?;
        let revisions_before = count_rows(&migration_pool, "revisions").await?;
        let mut creates_in_case = 0usize;

        for (thread, key, body) in &ops {
            total_ops += 1;
            let url = match *thread {
                "a" => &url_a,
                "b" => &url_b,
                other => anyhow::bail!("unknown thread label {other}"),
            };
            match keys_used.get(*key) {
                // Fresh key: either the thread accepts a new head (201) or
                // the live head fences it (412).
                None => {
                    if heads.contains_key(*thread) {
                        let response = put_checkpoint(&client, url, key, body).await?;
                        ensure!(
                            response.status() == StatusCode::PRECONDITION_FAILED,
                            "case {case_index}: fresh key {key:?} on thread {thread:?} with a \
                             live head returned {}, expected 412",
                            response.status()
                        );
                        ensure!(
                            !keys_used.contains_key(key),
                            "case {case_index}: a 412 rejected key {key:?} was consumed"
                        );
                        continue;
                    }
                    let response = put_checkpoint(&client, url, key, body).await?;
                    ensure!(
                        response.status() == StatusCode::CREATED,
                        "case {case_index}: first use of key {key:?} on thread {thread:?} \
                         returned {}, expected 201",
                        response.status()
                    );
                    ensure!(
                        response.headers().get("idempotency-replayed").is_none(),
                        "case {case_index}: first use of key {key:?} was marked as a replay"
                    );
                    let etag = response
                        .headers()
                        .get(header::ETAG)
                        .context("first checkpoint create omitted ETag")?
                        .to_str()?
                        .to_owned();
                    let created: Value = response.json().await?;
                    ensure!(
                        created["checkpoint_id"].is_string(),
                        "case {case_index}: created checkpoint lacks checkpoint_id"
                    );
                    ensure!(
                        created["thread_id"].is_string(),
                        "case {case_index}: created checkpoint lacks thread_id"
                    );
                    heads.insert(
                        thread,
                        Committed {
                            etag,
                            representation: created.clone(),
                        },
                    );
                    keys_used.insert(key, (thread, body.clone()));
                    creates_in_case += 1;
                }
                // Known key: replay only when thread and body match; any
                // other combination is a hard 409 that must not poison the
                // committed receipt.
                Some((thread0, body0)) => {
                    if thread0 == thread && body0 == body {
                        let committed = heads.get(*thread).context(
                            "model invariant broken: replayed key without a committed head",
                        )?;
                        let response = put_checkpoint(&client, url, key, body).await?;
                        ensure!(
                            response.status() == StatusCode::CREATED,
                            "case {case_index}: replay of key {key:?} on thread {thread:?} \
                             returned {}, expected 201",
                            response.status()
                        );
                        ensure!(
                            response
                                .headers()
                                .get("idempotency-replayed")
                                .is_some_and(|value| value == "true"),
                            "case {case_index}: replay of key {key:?} lacked \
                             idempotency-replayed: true"
                        );
                        ensure!(
                            response.headers().get(header::ETAG)
                                == Some(&header::HeaderValue::from_str(&committed.etag)?),
                            "case {case_index}: replay of key {key:?} changed the ETag"
                        );
                        let replayed: Value = response.json().await?;
                        ensure!(
                            replayed == committed.representation,
                            "case {case_index}: replay of key {key:?} returned a different \
                             representation"
                        );
                    } else {
                        let response = put_checkpoint(&client, url, key, body).await?;
                        ensure!(
                            response.status() == StatusCode::CONFLICT,
                            "case {case_index}: key {key:?} with thread {thread:?}/different \
                             body returned {}, expected 409",
                            response.status()
                        );
                        // The rejected reuse must not poison the receipt: the
                        // original thread+body still replays to the committed
                        // ETag.
                        let committed = heads.get(*thread0).context(
                            "model invariant broken: reused key without a committed head",
                        )?;
                        let (_, committed_body) = keys_used
                            .get(key)
                            .context("model invariant broken: reused key not tracked globally")?;
                        let replay =
                            put_checkpoint(&client, &url_for(thread0), key, committed_body).await?;
                        ensure!(
                            replay.status() == StatusCode::CREATED,
                            "case {case_index}: after a 409, key {key:?} no longer replays on \
                             thread {thread0:?}"
                        );
                        ensure!(
                            replay.headers().get(header::ETAG)
                                == Some(&header::HeaderValue::from_str(&committed.etag)?),
                            "case {case_index}: after a 409, key {key:?} changed ETag on thread \
                             {thread0:?}"
                        );
                    }
                }
            }
        }

        let receipts_after = count_rows(&migration_pool, "receipts").await?;
        let revisions_after = count_rows(&migration_pool, "revisions").await?;
        ensure!(
            receipts_after - receipts_before == creates_in_case as i64,
            "case {case_index}: {creates_in_case} creates produced {} idempotency receipts, \
             expected exactly one per committed key",
            receipts_after - receipts_before
        );
        ensure!(
            revisions_after - revisions_before == creates_in_case as i64,
            "case {case_index}: {creates_in_case} creates produced {} checkpoint revisions, \
             expected exactly one per committed key",
            revisions_after - revisions_before
        );
    }

    server.abort();
    ensure!(
        total_ops > 0,
        "property test ran no operations; the strategy produced empty cases"
    );
    eprintln!(
        "property_idempotency: {CASES} cases, {total_ops} ops against {} distinct keys across \
         the run",
        keys_used.len()
    );
    Ok(())
}
