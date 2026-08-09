//! Shared scenario harness for the conformance suite.
//!
//! Every scenario boots the same wiring: an active subject lifecycle, a
//! principal with scenario grants, a static authenticator, and a memory
//! service over one postgres repository. This module owns that shape once.
//! Scenarios keep their own stores and authorizers because those are the
//! scenario's point.

use std::sync::Arc;

use anyhow::Result;
use palimpsest_application::MemoryService;
use palimpsest_domain::{
    OperationGrant, PrincipalId, PrincipalScope, Sensitivity, SubjectId, TenantId,
};
use palimpsest_http::StaticAuthenticator;
use palimpsest_postgres::PostgresMemoryRepository;
use sqlx::PgPool;
use uuid::Uuid;

/// Constructs the memory service over one repository (five adapter slots).
pub(crate) fn service(repository: &Arc<PostgresMemoryRepository>) -> MemoryService {
    MemoryService::new(
        repository.clone(),
        repository.clone(),
        repository.clone(),
        repository.clone(),
        repository.clone(),
    )
}

/// Builds a scenario principal.
pub(crate) fn principal(
    principal_name: &str,
    tenant_id: TenantId,
    subject_id: SubjectId,
    grants: &[OperationGrant],
    sensitivities: &[Sensitivity],
) -> PrincipalScope {
    PrincipalScope {
        principal_id: PrincipalId(principal_name.to_owned()),
        tenant_id,
        subject_ids: vec![subject_id],
        allowed_sensitivities: sensitivities.to_vec(),
        operation_grants: grants.to_vec(),
    }
}

/// Builds a static authenticator that maps one token to one principal.
pub(crate) fn static_authenticator(
    principal: PrincipalScope,
    token: &str,
) -> Arc<StaticAuthenticator> {
    Arc::new(StaticAuthenticator::new([(token.to_owned(), principal)]))
}

/// Seeds an active subject lifecycle row through the migration pool.
pub(crate) async fn seed_active_lifecycle(
    migration_pool: &PgPool,
    tenant_id: TenantId,
    subject_id: SubjectId,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO memory.subject_lifecycles
            (tenant_id, subject_id, lifecycle_state, state_version)
         VALUES ($1, $2, 'active', 0)",
    )
    .bind(tenant_id.0)
    .bind(subject_id.0)
    .execute(migration_pool)
    .await
    .context("seed active subject lifecycle")
    .map(|_| ())
}

/// Allocates fixture ids from a scenario-owned base.
///
/// Ids are stable across runs (the base is fixed) and unique within a run
/// (the counter only moves forward). Scenarios must pick disjoint bases.
pub(crate) struct FixtureIds {
    next: u128,
}

impl FixtureIds {
    pub(crate) fn new(base: u128) -> Self {
        Self { next: base }
    }

    /// Returns the next fixture id.
    pub(crate) fn next(&mut self) -> Uuid {
        let id = self.next;
        self.next += 1;
        Uuid::from_u128(id)
    }
}
