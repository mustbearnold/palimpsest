use anyhow::{Context, Result, ensure};
use sqlx::postgres::PgPoolOptions;
use tokio::net::TcpListener;

#[tokio::test]
async fn metrics_probe_is_public_content_free_and_database_independent() -> Result<()> {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgresql://metrics-runtime:metrics-runtime@127.0.0.1:1/palimpsest")?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        axum::serve(listener, palimpsest_server::probe_router(pool))
            .await
            .context("serve metrics probe")
    });

    let response = reqwest::get(format!("http://{address}/metrics"))
        .await
        .context("request metrics probe")?;
    ensure!(response.status().is_success());
    ensure!(
        response
            .headers()
            .get(reqwest::header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok())
            == Some("no-store")
    );
    ensure!(
        response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            == Some("text/plain; version=0.0.7; charset=utf-8")
    );
    let body = response.text().await?;
    ensure!(body.contains("palimpsest_build_info"));
    ensure!(body.contains("palimpsest_schema_version 27"));
    ensure!(!body.contains("metrics-runtime"));
    ensure!(!body.contains("password"));

    server.abort();
    Ok(())
}
