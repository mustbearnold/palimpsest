use std::{env, sync::Arc};

use anyhow::{Context, Result};
use palimpsest_domain::{PrincipalId, PrincipalScope, SubjectId, TenantId};
use palimpsest_http::StaticAuthenticator;
use sqlx::PgPool;
use tokio::net::TcpListener;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    let database_url = required("PALIMPSEST_DATABASE_URL")?;
    let bearer_token = required("PALIMPSEST_BEARER_TOKEN")?;
    let principal_id = required("PALIMPSEST_PRINCIPAL_ID")?;
    let tenant_id = parse_uuid("PALIMPSEST_TENANT_ID")?;
    let subject_id = parse_uuid("PALIMPSEST_SUBJECT_ID")?;
    let bind = env::var("PALIMPSEST_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_owned());

    let pool = PgPool::connect(&database_url)
        .await
        .context("connect to PALIMPSEST_DATABASE_URL")?;
    palimpsest_postgres::migrate(&pool)
        .await
        .context("apply database migrations")?;

    let authenticator = Arc::new(StaticAuthenticator::new([(
        bearer_token,
        PrincipalScope {
            principal_id: PrincipalId(principal_id),
            tenant_id: TenantId(tenant_id),
            subject_ids: vec![SubjectId(subject_id)],
        },
    )]));
    let listener = TcpListener::bind(&bind)
        .await
        .with_context(|| format!("bind HTTP listener to {bind}"))?;
    axum::serve(listener, palimpsest_server::app(pool, authenticator))
        .await
        .context("serve HTTP API")?;
    Ok(())
}

fn required(name: &str) -> Result<String> {
    env::var(name).with_context(|| format!("{name} must be set"))
}

fn parse_uuid(name: &str) -> Result<Uuid> {
    required(name)?
        .parse()
        .with_context(|| format!("{name} must be a UUID"))
}
