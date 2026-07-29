use std::sync::Arc;

use axum::Router;
use palimpsest_application::{EmbeddingProvider, MemoryService, UnavailableEmbeddingProvider};
use palimpsest_http::Authenticator;
use palimpsest_postgres::PostgresMemoryRepository;
use sqlx::PgPool;

pub fn app(pool: PgPool, authenticator: Arc<dyn Authenticator>) -> Router {
    app_with_embedding_provider(pool, authenticator, Arc::new(UnavailableEmbeddingProvider))
}

pub fn app_with_embedding_provider(
    pool: PgPool,
    authenticator: Arc<dyn Authenticator>,
    embedding_provider: Arc<dyn EmbeddingProvider>,
) -> Router {
    let repository = Arc::new(PostgresMemoryRepository::new(pool));
    palimpsest_http::router(
        MemoryService::new(
            repository.clone(),
            repository.clone(),
            repository.clone(),
            repository,
        )
        .with_embedding_provider(embedding_provider),
        authenticator,
    )
}
