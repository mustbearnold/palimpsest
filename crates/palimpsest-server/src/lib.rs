use std::sync::Arc;

use axum::Router;
use palimpsest_application::{EmbeddingProvider, MemoryService, UnavailableEmbeddingProvider};
use palimpsest_http::Authenticator;
use palimpsest_postgres::PostgresMemoryRepository;
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
    palimpsest_http::router(
        memory_service_with_embedding_provider(
            runtime_pool,
            lifecycle_controller_pool,
            embedding_provider,
        ),
        authenticator,
    )
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
    let runtime_repository = Arc::new(PostgresMemoryRepository::new(runtime_pool));
    let lifecycle_controller = Arc::new(PostgresMemoryRepository::new(lifecycle_controller_pool));
    MemoryService::new(
        runtime_repository.clone(),
        lifecycle_controller,
        runtime_repository.clone(),
        runtime_repository.clone(),
        runtime_repository.clone(),
        runtime_repository,
    )
    .with_embedding_provider(embedding_provider)
}
