//! retrieval_lexical — extracted from retrieval.rs by the ADR-0031 token-efficiency split (structure-only).

//! retrieval — extracted from lib.rs by the ADR-0031 token-efficiency split (structure-only).

use anyhow::{Context, Result, ensure};
use reqwest::{Client, StatusCode, header};
use serde_json::json;

use super::common::{RetrievalReceipt, Target, ensure_lexical_only_scores};

pub async fn creates_and_replays_a_lexical_retrieval_receipt(target: &Target) -> Result<()> {
    let client = Client::new();
    let retrievals_url = format!(
        "{}/v1/tenants/{}/subjects/{}/retrievals",
        target.base_url.trim_end_matches('/'),
        target.tenant_id,
        target.subject_id
    );
    let body = json!({
        "query": "Wellington",
        "perspective": {"kind": "current"},
        "page_size": 10,
        "filters": {"namespaces": ["case.profile"]}
    });

    let create_response = client
        .post(&retrievals_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "retrieval-shipping-address")
        .json(&body)
        .send()
        .await
        .context("create retrieval receipt request failed")?;
    ensure!(
        create_response.status() == StatusCode::CREATED,
        "create retrieval receipt returned {}, expected 201",
        create_response.status()
    );
    ensure!(
        create_response
            .headers()
            .get(header::CACHE_CONTROL)
            .is_some_and(|value| value == "private, no-store"),
        "retrieval receipt can be cached"
    );
    let location = create_response
        .headers()
        .get(header::LOCATION)
        .context("retrieval receipt omitted Location")?
        .to_str()?
        .to_owned();
    let created: RetrievalReceipt = create_response
        .json()
        .await
        .context("create retrieval response was not a receipt")?;

    ensure!(created.tenant_id == target.tenant_id);
    ensure!(created.subject_id == target.subject_id);
    ensure!(created.retrieval_id.get_version_num() == 7);
    ensure!(created.status == "results");
    ensure!(created.valid_at == created.evaluated_at);
    ensure!(created.recorded_at == created.evaluated_at);
    ensure!(created.policy.id == "retrieval-lexical-v1");
    ensure!(created.policy.version == "1");
    ensure!(created.policy.digest.len() == 64);
    ensure!(created.authorization.decision == "authorized");
    ensure!(created.authorization.scope_digest.len() == 64);
    ensure!(created.document_schema_version == 1);
    ensure!(created.next_cursor.is_none());
    ensure!(created.items.len() == 1);
    let item = &created.items[0];
    ensure!(item.memory_kind == "fact_revision");
    ensure!(item.namespace == "case.profile");
    ensure!(item.key == "shipping_address");
    ensure!(item.value == json!({"city": "Wellington", "country": "NZ"}));
    ensure!(!item.evidence_episode_ids.is_empty());
    ensure!(
        item.scores.iter().any(|score| {
            score.component == "lexical_rank" && score.value.parse::<u32>() == Ok(1)
        })
    );
    ensure!(item.scores.iter().all(|score| !score.value.is_empty()));
    ensure_lexical_only_scores(item)?;

    let replay_response = client
        .post(&retrievals_url)
        .bearer_auth(&target.bearer_token)
        .header("Idempotency-Key", "retrieval-shipping-address")
        .json(&body)
        .send()
        .await?;
    ensure!(replay_response.status() == StatusCode::CREATED);
    ensure!(
        replay_response
            .headers()
            .get("Idempotency-Replayed")
            .is_some_and(|value| value == "true"),
        "retrieval replay was not identified"
    );
    let replayed: RetrievalReceipt = replay_response.json().await?;
    ensure!(
        serde_json::to_value(&replayed)? == serde_json::to_value(&created)?,
        "retrieval replay changed the durable receipt"
    );

    let receipt_url = if location.starts_with("http://") || location.starts_with("https://") {
        location
    } else {
        format!("{}{}", target.base_url.trim_end_matches('/'), location)
    };
    let get_response = client
        .get(receipt_url)
        .bearer_auth(&target.bearer_token)
        .send()
        .await?;
    ensure!(get_response.status() == StatusCode::OK);
    ensure!(
        get_response
            .headers()
            .get(header::CACHE_CONTROL)
            .is_some_and(|value| value == "private, no-store")
    );
    let fetched: RetrievalReceipt = get_response.json().await?;
    ensure!(
        serde_json::to_value(fetched)? == serde_json::to_value(created)?,
        "retrieval GET changed the durable receipt"
    );

    Ok(())
}
