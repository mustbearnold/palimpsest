use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    Router,
    extract::Request,
    http::{StatusCode, header},
    middleware,
    response::{IntoResponse, Response},
    routing::get,
};
use palimpsest_application::{
    EmbeddingProvider, ExportPackageStore, ExportWorkerAuthorizer, FileExportPackageStore,
    MemoryService, S3ExportPackageStore, ServiceError, UnavailableEmbeddingProvider,
};
use palimpsest_domain::{PrincipalId, PrincipalScope, SubjectId, TenantId};
use palimpsest_http::{
    Authenticator, ContentLeaseCleanupCounters, ServerMetricsSnapshot, record_request_latency,
};
use palimpsest_postgres::{PostgresMemoryRepository, PostgresSubjectLifecycleRepository};
use sqlx::PgPool;

/// Times every request through the merged router into the content-free
/// latency histogram (spec 010 R3: `/metrics` stays database-free).
async fn record_request_latency_middleware(request: Request, next: middleware::Next) -> Response {
    let start = Instant::now();
    let response = next.run(request).await;
    record_request_latency(start.elapsed());
    response
}

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
    probes
        .merge(palimpsest_http::router(service, authenticator))
        .layer(middleware::from_fn(record_request_latency_middleware))
}

pub fn probe_router(runtime_pool: PgPool) -> Router {
    Router::new()
        .route("/healthz", get(health_status))
        .route(
            "/metrics",
            get({
                let pool = runtime_pool.clone();
                move || async move { metrics_status(Some(pool)).await }
            }),
        )
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

async fn metrics_status(pool: Option<PgPool>) -> impl IntoResponse {
    (
        StatusCode::OK,
        [
            (header::CACHE_CONTROL, "no-store"),
            (
                header::CONTENT_TYPE,
                "text/plain; version=0.0.5; charset=utf-8",
            ),
        ],
        metrics_body(
            palimpsest_http::content_lease_cleanup_counters(),
            palimpsest_http::server_metrics_snapshot(),
            pool.as_ref(),
        ),
    )
}

fn metrics_body(
    counters: ContentLeaseCleanupCounters,
    snapshot: ServerMetricsSnapshot,
    pool: Option<&PgPool>,
) -> String {
    let (pool_size, pool_idle) = match pool {
        Some(pool) => (pool.size(), pool.num_idle()),
        None => (0, 0),
    };
    let mut latency = String::new();
    for (index, bound) in palimpsest_http::REQUEST_LATENCY_BUCKET_MS
        .iter()
        .enumerate()
    {
        latency.push_str(&format!(
            "palimpsest_http_request_duration_milliseconds_bucket{{le=\"{bound}\"}} {}\n",
            snapshot.latency_bucket_totals[index]
        ));
    }
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
palimpsest_content_lease_release_deferred_to_expiry_total {}\n\
# HELP palimpsest_http_request_duration_milliseconds_bucket Request latency histogram (cumulative le buckets).\n\
# TYPE palimpsest_http_request_duration_milliseconds_bucket counter\n\
{latency}\
# HELP palimpsest_http_request_duration_sum_microseconds Sum of request durations.\n\
# TYPE palimpsest_http_request_duration_sum_microseconds counter\n\
palimpsest_http_request_duration_sum_microseconds {}\n\
# HELP palimpsest_embedding_projection_lease_policy_seconds Deployed embedding-projection lease policy (recorded at startup).\n\
# TYPE palimpsest_embedding_projection_lease_policy_seconds gauge\n\
palimpsest_embedding_projection_lease_policy_seconds{{interval=\"lease\"}} {}\n\
palimpsest_embedding_projection_lease_policy_seconds{{interval=\"renewal\"}} {}\n\
# HELP palimpsest_pgpool_size PostgreSQL pool size.\n\
# TYPE palimpsest_pgpool_size gauge\n\
palimpsest_pgpool_size {}\n\
# HELP palimpsest_pgpool_idle PostgreSQL pool idle connections.\n\
# TYPE palimpsest_pgpool_idle gauge\n\
palimpsest_pgpool_idle {}\n",
        env!("CARGO_PKG_VERSION"),
        palimpsest_postgres::latest_migration_version(),
        counters.release_retries,
        counters.runtime_unavailable,
        counters.outstanding,
        counters.deferred_to_expiry,
        snapshot.latency_sum_micros,
        snapshot.projection_lease_seconds,
        snapshot.projection_renewal_interval_seconds,
        pool_size,
        pool_idle,
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
        let body = metrics_body(
            palimpsest_http::ContentLeaseCleanupCounters {
                release_retries: 2,
                runtime_unavailable: 3,
                outstanding: 4,
                deferred_to_expiry: 5,
            },
            palimpsest_http::ServerMetricsSnapshot {
                latency_bucket_totals: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                latency_sum_micros: 55_000,
                projection_lease_seconds: 60,
                projection_renewal_interval_seconds: 20,
            },
            None,
        );

        assert!(body.contains("# TYPE palimpsest_build_info gauge\n"));
        assert!(body.contains("palimpsest_schema_version 21\n"));
        assert!(body.contains("palimpsest_content_lease_release_retries_total 2\n"));
        assert!(body.contains("palimpsest_content_lease_release_runtime_unavailable_total 3\n"));
        assert!(body.contains("palimpsest_content_lease_release_outstanding 4\n"));
        assert!(body.contains("palimpsest_content_lease_release_deferred_to_expiry_total 5\n"));
        assert!(
            body.contains("palimpsest_http_request_duration_milliseconds_bucket{le=\"10\"} 1\n")
        );
        assert!(
            body.contains(
                "palimpsest_http_request_duration_milliseconds_bucket{le=\"10000\"} 10\n"
            )
        );
        assert!(body.contains("palimpsest_http_request_duration_sum_microseconds 55000\n"));
        assert!(body.contains(
            "palimpsest_embedding_projection_lease_policy_seconds{interval=\"lease\"} 60\n"
        ));
        assert!(body.contains(
            "palimpsest_embedding_projection_lease_policy_seconds{interval=\"renewal\"} 20\n"
        ));
        assert!(body.contains("palimpsest_pgpool_size 0\n"));
        assert!(body.contains("palimpsest_pgpool_idle 0\n"));
        assert!(!body.contains("tenant"));
        assert!(!body.contains("subject"));
        assert!(!body.contains("memory"));
        assert!(!body.contains("password"));
    }

    #[tokio::test]
    async fn metrics_endpoint_is_cache_free_and_does_not_need_database_access() {
        let response = metrics_status(None).await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "text/plain; version=0.0.5; charset=utf-8"
        );
    }
}
