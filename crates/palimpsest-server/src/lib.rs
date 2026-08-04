use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    Router,
    http::{StatusCode, header},
    response::IntoResponse,
    routing::get,
};
use palimpsest_application::{
    EmbeddingProvider, ExportPackageStore, ExportWorkerAuthorizer, FileExportPackageStore,
    MemoryService, S3ExportPackageStore, ServiceError, UnavailableEmbeddingProvider,
};
use palimpsest_domain::{PrincipalId, PrincipalScope, SubjectId, TenantId};
use palimpsest_http::{Authenticator, ContentLeaseCleanupCounters};
use palimpsest_postgres::{PostgresMemoryRepository, PostgresSubjectLifecycleRepository};
use sqlx::PgPool;

pub fn app(
    runtime_pool: PgPool,
    lifecycle_controller_pool: PgPool,
    authenticator: Arc<dyn Authenticator>,
) -> Router {
    app_with_embedding_provider(
        runtime_pool,
        lifecycle_controller_pool,
        authenticator,
        Arc::new(UnavailableEmbeddingProvider),
    )
}

pub fn app_without_workers(
    runtime_pool: PgPool,
    lifecycle_controller_pool: PgPool,
    authenticator: Arc<dyn Authenticator>,
) -> Router {
    app_without_workers_with_embedding_provider(
        runtime_pool,
        lifecycle_controller_pool,
        authenticator,
        Arc::new(UnavailableEmbeddingProvider),
    )
}

pub fn app_with_embedding_provider(
    runtime_pool: PgPool,
    lifecycle_controller_pool: PgPool,
    authenticator: Arc<dyn Authenticator>,
    embedding_provider: Arc<dyn EmbeddingProvider>,
) -> Router {
    app_with_embedding_provider_and_workers(
        runtime_pool,
        lifecycle_controller_pool,
        authenticator,
        embedding_provider,
        true,
    )
}

pub fn app_without_workers_with_embedding_provider(
    runtime_pool: PgPool,
    lifecycle_controller_pool: PgPool,
    authenticator: Arc<dyn Authenticator>,
    embedding_provider: Arc<dyn EmbeddingProvider>,
) -> Router {
    app_with_embedding_provider_and_workers(
        runtime_pool,
        lifecycle_controller_pool,
        authenticator,
        embedding_provider,
        false,
    )
}

fn app_with_embedding_provider_and_workers(
    runtime_pool: PgPool,
    lifecycle_controller_pool: PgPool,
    authenticator: Arc<dyn Authenticator>,
    embedding_provider: Arc<dyn EmbeddingProvider>,
    start_workers: bool,
) -> Router {
    let probes = probe_router(runtime_pool.clone());
    let export_authorizer = Arc::new(HttpExportWorkerAuthorizer {
        authenticator: authenticator.clone(),
    });
    let service = memory_service_with_embedding_provider(
        runtime_pool,
        lifecycle_controller_pool,
        embedding_provider,
    )
    .with_export_worker_authorizer(export_authorizer);
    if start_workers {
        spawn_deletion_worker(service.clone());
        spawn_export_worker(service.clone());
    }
    probes.merge(palimpsest_http::router(service, authenticator))
}

pub fn probe_router(runtime_pool: PgPool) -> Router {
    Router::new()
        .route("/healthz", get(health_status))
        .route("/metrics", get(metrics_status))
        .route(
            "/readyz",
            get(move || {
                let pool = runtime_pool.clone();
                async move { readiness_status(pool).await }
            }),
        )
}

async fn health_status() -> impl IntoResponse {
    (StatusCode::OK, [(header::CACHE_CONTROL, "no-store")])
}

async fn metrics_status() -> impl IntoResponse {
    (
        StatusCode::OK,
        [
            (header::CACHE_CONTROL, "no-store"),
            (
                header::CONTENT_TYPE,
                "text/plain; version=0.0.4; charset=utf-8",
            ),
        ],
        metrics_body(palimpsest_http::content_lease_cleanup_counters()),
    )
}

fn metrics_body(counters: ContentLeaseCleanupCounters) -> String {
    format!(
        "# HELP palimpsest_build_info Palimpsest build identity.\n\
# TYPE palimpsest_build_info gauge\n\
palimpsest_build_info{{version=\"{}\"}} 1\n\
# HELP palimpsest_schema_version Latest schema migration version in this binary.\n\
# TYPE palimpsest_schema_version gauge\n\
palimpsest_schema_version {}\n\
# HELP palimpsest_content_lease_release_retries_total Content lease release retries.\n\
# TYPE palimpsest_content_lease_release_retries_total counter\n\
palimpsest_content_lease_release_retries_total {}\n\
# HELP palimpsest_content_lease_release_runtime_unavailable_total Content lease releases deferred because runtime cleanup was unavailable.\n\
# TYPE palimpsest_content_lease_release_runtime_unavailable_total counter\n\
palimpsest_content_lease_release_runtime_unavailable_total {}\n\
# HELP palimpsest_content_lease_release_outstanding Content lease releases queued for cleanup.\n\
# TYPE palimpsest_content_lease_release_outstanding gauge\n\
palimpsest_content_lease_release_outstanding {}\n\
# HELP palimpsest_content_lease_release_deferred_to_expiry_total Content lease releases deferred to lease expiry.\n\
# TYPE palimpsest_content_lease_release_deferred_to_expiry_total counter\n\
palimpsest_content_lease_release_deferred_to_expiry_total {}\n",
        env!("CARGO_PKG_VERSION"),
        palimpsest_postgres::latest_migration_version(),
        counters.release_retries,
        counters.runtime_unavailable,
        counters.outstanding,
        counters.deferred_to_expiry,
    )
}

async fn readiness_status(pool: PgPool) -> impl IntoResponse {
    let current_schema_version = palimpsest_postgres::latest_migration_version();
    let schema_ready = sqlx::query_scalar::<_, bool>(
        "SELECT to_regclass('memory.subject_lifecycles') IS NOT NULL
            AND to_regclass('memory.deletion_operations') IS NOT NULL
            AND to_regclass('memory.export_operations') IS NOT NULL
            AND EXISTS (
                SELECT 1
                FROM _sqlx_migrations
                WHERE version = $1 AND success
            )
            AND NOT EXISTS (
                SELECT 1
                FROM _sqlx_migrations
                WHERE NOT success
            )
            AND (
                SELECT min(version)
                FROM _sqlx_migrations
                WHERE success
            ) = 1
            AND (
                SELECT max(version)
                FROM _sqlx_migrations
                WHERE success
            ) = $1
            AND (
                SELECT count(*)
                FROM _sqlx_migrations
                WHERE success
            ) = $1",
    )
    .bind(current_schema_version)
    .fetch_one(&pool)
    .await
    .unwrap_or(false);
    let status = if schema_ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, [(header::CACHE_CONTROL, "no-store")])
}

struct HttpExportWorkerAuthorizer {
    authenticator: Arc<dyn Authenticator>,
}

impl ExportWorkerAuthorizer for HttpExportWorkerAuthorizer {
    fn authorize_export(
        &self,
        principal_id: &PrincipalId,
        tenant_id: TenantId,
        subject_id: SubjectId,
        authorization_scope_sha256: &str,
    ) -> Result<PrincipalScope, ServiceError> {
        self.authenticator
            .authorize_export_worker(
                principal_id,
                tenant_id,
                subject_id,
                authorization_scope_sha256,
            )
            .ok_or(ServiceError::NotFound)
    }
}

pub fn memory_service(runtime_pool: PgPool, lifecycle_controller_pool: PgPool) -> MemoryService {
    memory_service_with_embedding_provider(
        runtime_pool,
        lifecycle_controller_pool,
        Arc::new(UnavailableEmbeddingProvider),
    )
}

pub fn memory_service_with_embedding_provider(
    runtime_pool: PgPool,
    lifecycle_controller_pool: PgPool,
    embedding_provider: Arc<dyn EmbeddingProvider>,
) -> MemoryService {
    let runtime_repository = Arc::new(PostgresMemoryRepository::new(runtime_pool.clone()));
    let lifecycle_repository = Arc::new(PostgresSubjectLifecycleRepository::new(
        runtime_pool.clone(),
        lifecycle_controller_pool,
    ));
    let exports = runtime_repository.clone();
    let export_store = export_store();
    MemoryService::new(
        lifecycle_repository,
        runtime_repository.clone(),
        runtime_repository.clone(),
        runtime_repository.clone(),
        runtime_repository,
    )
    .with_embedding_provider(embedding_provider)
    .with_export_components(exports, export_store)
}

fn export_root() -> PathBuf {
    std::env::var_os("PALIMPSEST_EXPORT_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("var/palimpsest/exports"))
}

fn export_store() -> Arc<dyn ExportPackageStore> {
    match S3ExportPackageStore::from_environment() {
        Ok(Some(store)) => Arc::new(store),
        Ok(None) => Arc::new(FileExportPackageStore::new(export_root())),
        Err(error) => panic!("invalid Palimpsest S3 export configuration: {error}"),
    }
}

fn spawn_deletion_worker(service: MemoryService) {
    tokio::spawn(async move {
        loop {
            match service.run_deletion_worker_once().await {
                Ok(summary) if summary.processed == 0 => {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
                Ok(_) => {}
                Err(_error) => {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    });
}

fn spawn_export_worker(service: MemoryService) {
    tokio::spawn(async move {
        loop {
            match service.run_export_worker_once().await {
                Ok(true) => {}
                Ok(false) => tokio::time::sleep(Duration::from_millis(250)).await,
                Err(_error) => tokio::time::sleep(Duration::from_secs(1)).await,
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_surface_is_fixed_and_content_free() {
        let body = metrics_body(palimpsest_http::ContentLeaseCleanupCounters {
            release_retries: 2,
            runtime_unavailable: 3,
            outstanding: 4,
            deferred_to_expiry: 5,
        });

        assert!(body.contains("# TYPE palimpsest_build_info gauge\n"));
        assert!(body.contains("palimpsest_schema_version 20\n"));
        assert!(body.contains("palimpsest_content_lease_release_retries_total 2\n"));
        assert!(body.contains("palimpsest_content_lease_release_runtime_unavailable_total 3\n"));
        assert!(body.contains("palimpsest_content_lease_release_outstanding 4\n"));
        assert!(body.contains("palimpsest_content_lease_release_deferred_to_expiry_total 5\n"));
        assert!(!body.contains("tenant"));
        assert!(!body.contains("subject"));
        assert!(!body.contains("memory"));
        assert!(!body.contains("password"));
    }

    #[tokio::test]
    async fn metrics_endpoint_is_cache_free_and_does_not_need_database_access() {
        let response = metrics_status().await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "text/plain; version=0.0.4; charset=utf-8"
        );
    }
}
