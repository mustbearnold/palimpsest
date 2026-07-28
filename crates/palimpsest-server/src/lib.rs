use std::sync::Arc;

use axum::Router;
use palimpsest_application::MemoryService;
use palimpsest_http::Authenticator;
use palimpsest_postgres::PostgresEpisodeRepository;
use sqlx::PgPool;

pub fn app(pool: PgPool, authenticator: Arc<dyn Authenticator>) -> Router {
    let repository = Arc::new(PostgresEpisodeRepository::new(pool));
    palimpsest_http::router(
        MemoryService::new(repository.clone(), repository),
        authenticator,
    )
}
