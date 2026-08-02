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
    EmbeddingProvider, ExportWorkerAuthorizer, FileExportPackageStore, MemoryService, ServiceError,
    UnavailableEmbeddingProvider,
};
use palimpsest_domain::{PrincipalId, PrincipalScope, SubjectId, TenantId};
use palimpsest_http::Authenticator;
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

pub fn app_with_embedding_provider(
    runtime_pool: PgPool,
    lifecycle_controller_pool: PgPool,
    authenticator: Arc<dyn Authenticator>,
    embedding_provider: Arc<dyn EmbeddingProvider>,
) -> Router {
    let readiness_pool = runtime_pool.clone();
    let export_authorizer = Arc::new(HttpExportWorkerAuthorizer {
        authenticator: authenticator.clone(),
    });
    let service = memory_service_with_embedding_provider(
        runtime_pool,
        lifecycle_controller_pool,
        embedding_provider,
    )
    .with_export_worker_authorizer(export_authorizer);
    spawn_deletion_worker(service.clone());
    spawn_export_worker(service.clone());
    Router::new()
        .route("/healthz", get(health_status))
        .route(
            "/readyz",
            get(move || {
                let pool = readiness_pool.clone();
                async move { readiness_status(pool).await }
            }),
        )
        .merge(palimpsest_http::router(service, authenticator))
}

async fn health_status() -> impl IntoResponse {
    (StatusCode::OK, [(header::CACHE_CONTROL, "no-store")])
}

async fn readiness_status(pool: PgPool) -> impl IntoResponse {
    let schema_ready = sqlx::query_scalar::<_, bool>(
        "SELECT to_regclass('memory.subject_lifecycles') IS NOT NULL
            AND to_regclass('memory.deletion_operations') IS NOT NULL
            AND to_regclass('memory.export_operations') IS NOT NULL",
    )
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
    let export_store = Arc::new(FileExportPackageStore::new(export_root()));
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
