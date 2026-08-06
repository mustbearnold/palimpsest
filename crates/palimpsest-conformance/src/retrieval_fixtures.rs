//! retrieval_fixtures — extracted from retrieval.rs by the ADR-0031 token-efficiency split (structure-only).

//! retrieval — extracted from lib.rs by the ADR-0031 token-efficiency split (structure-only).

use serde_json::Value;
use uuid::Uuid;

use super::common::RetrievalReceipt;

#[derive(Clone, Debug)]
pub struct HybridFusionFixture {
    pub exact_revision_id: Uuid,
    pub alpha_revision_id: Uuid,
    pub beta_revision_id: Uuid,
    pub gamma_revision_id: Uuid,
    pub delta_revision_id: Uuid,
    pub forbidden_revision_id: Uuid,
}

#[derive(Clone, Debug)]
pub struct HybridReplayFixture {
    pub request_body: Value,
    pub receipt: Value,
}

#[derive(Clone, Debug)]
pub struct TemporalRetrievalFixture {
    pub exact_revision_id: Uuid,
    pub alpha_root_revision_id: Uuid,
    pub alpha_successor_revision_id: Uuid,
    pub beta_revision_id: Uuid,
    pub gamma_revision_id: Uuid,
    pub delta_revision_id: Uuid,
    pub alpha_root_recorded_at: String,
    pub alpha_successor_recorded_at: String,
}

#[derive(Debug)]
pub struct TemporalReplayFixture {
    pub first_retrieval_id: Uuid,
    pub second_retrieval_id: Uuid,
    pub independent_retrieval_ids: Vec<Uuid>,
    pub paginated_retrieval_id: Uuid,
    pub(crate) request_body: Value,
    pub(crate) first_receipt: RetrievalReceipt,
}

#[derive(Clone, Debug)]
pub struct TemporalRuntimeReplayFixture {
    pub retrieval_id: Uuid,
    pub(crate) request_body: Value,
    pub(crate) receipt: Value,
}

#[derive(Clone, Debug)]
pub struct TemporalLifecycleFixture {
    pub deleted_case_id: Uuid,
    pub deleted_root_revision_id: Uuid,
    pub deleted_successor_revision_id: Uuid,
    pub expired_case_id: Uuid,
    pub expired_root_revision_id: Uuid,
    pub expired_successor_revision_id: Uuid,
}

#[derive(Debug)]
pub struct TemporalLifecycleReplayFixture {
    pub(crate) receipts: Vec<TemporalLifecycleReceiptFixture>,
}

#[derive(Debug)]
pub(crate) struct TemporalLifecycleReceiptFixture {
    pub(crate) name: &'static str,
    pub(crate) retrieval_id: Uuid,
    pub(crate) idempotency_key: String,
    pub(crate) request_body: Value,
    pub(crate) root_revision_id: Uuid,
    pub(crate) successor_revision_id: Uuid,
    pub(crate) private_marker: &'static str,
}

pub struct RetrievalIsolationFixture {
    pub retrieval_id: Uuid,
    pub allowed_revision_id: Uuid,
    pub forbidden_revision_ids: Vec<Uuid>,
}

pub struct RetrievalLifecycleFixture {
    pub receipt_url: String,
    pub retrieval_id: Uuid,
    pub superseded_revision_id: Uuid,
    pub deleted_revision_id: Uuid,
}
