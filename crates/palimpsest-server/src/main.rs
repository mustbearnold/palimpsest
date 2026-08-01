use std::{env, sync::Arc};

use anyhow::{Context, Result, bail};
use palimpsest_domain::{
    OperationGrant, PrincipalId, PrincipalScope, Sensitivity, SubjectId, TenantId,
};
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
    let allowed_sensitivities = env::var("PALIMPSEST_ALLOWED_SENSITIVITIES")
        .unwrap_or_default()
        .split(',')
        .filter(|value| !value.trim().is_empty())
        .map(|value| Sensitivity::try_from(value.trim().to_owned()))
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("PALIMPSEST_ALLOWED_SENSITIVITIES contains an invalid label")?;
    let operation_grants =
        parse_operation_grants(&env::var("PALIMPSEST_OPERATION_GRANTS").unwrap_or_default())
            .context("PALIMPSEST_OPERATION_GRANTS contains an unknown grant")?;
    let bind = env::var("PALIMPSEST_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_owned());

    let pool = PgPool::connect(&database_url)
        .await
        .context("connect to PALIMPSEST_DATABASE_URL")?;
    palimpsest_postgres::migrate(&pool)
        .await
        .context("apply database migrations")?;
    let lifecycle_controller_pool = if operation_grants.contains(&OperationGrant::SubjectDelete) {
        let controller_database_url = required("PALIMPSEST_LIFECYCLE_CONTROLLER_DATABASE_URL")?;
        PgPool::connect(&controller_database_url)
            .await
            .context("connect to PALIMPSEST_LIFECYCLE_CONTROLLER_DATABASE_URL")?
    } else {
        pool.clone()
    };

    let authenticator = Arc::new(StaticAuthenticator::new([(
        bearer_token,
        PrincipalScope {
            principal_id: PrincipalId(principal_id),
            tenant_id: TenantId(tenant_id),
            subject_ids: vec![SubjectId(subject_id)],
            allowed_sensitivities,
            operation_grants,
        },
    )]));
    let listener = TcpListener::bind(&bind)
        .await
        .with_context(|| format!("bind HTTP listener to {bind}"))?;
    axum::serve(
        listener,
        palimpsest_server::app(pool, lifecycle_controller_pool, authenticator),
    )
    .await
    .context("serve HTTP API")?;
    Ok(())
}

fn parse_operation_grants(value: &str) -> Result<Vec<OperationGrant>> {
    let mut canonical_history_export = false;
    let mut subject_delete = false;
    for name in value
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
    {
        match name {
            "canonical_history_export" => canonical_history_export = true,
            "subject_delete" => subject_delete = true,
            _ => bail!("unknown operation grant {name}"),
        }
    }
    let mut grants = Vec::with_capacity(2);
    if canonical_history_export {
        grants.push(OperationGrant::CanonicalHistoryExport);
    }
    if subject_delete {
        grants.push(OperationGrant::SubjectDelete);
    }
    Ok(grants)
}

fn required(name: &str) -> Result<String> {
    env::var(name).with_context(|| format!("{name} must be set"))
}

fn parse_uuid(name: &str) -> Result<Uuid> {
    required(name)?
        .parse()
        .with_context(|| format!("{name} must be a UUID"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_grants_accept_only_the_closed_trusted_vocabulary() {
        assert_eq!(
            parse_operation_grants("canonical_history_export,subject_delete")
                .expect("known operation grants should parse"),
            vec![
                OperationGrant::CanonicalHistoryExport,
                OperationGrant::SubjectDelete,
            ]
        );
        assert!(parse_operation_grants("").is_ok_and(|grants| grants.is_empty()));
        assert!(parse_operation_grants("controller_override").is_err());
    }
}
