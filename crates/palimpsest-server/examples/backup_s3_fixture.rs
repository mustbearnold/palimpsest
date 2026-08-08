//! S3-compatible object fixture for the spec 016 backup conformance suite.
//!
//! The fixture speaks the object-shaped subset of S3 that the backup object
//! store uses: path-style PUT, GET, and DELETE. It is a test double only.
//! It never verifies signatures. It keeps all objects in memory.
//!
//! Usage: cargo run --example backup_s3_fixture -- [port]
//! The port defaults to 19000. The fixture prints "fixture-ready" once it
//! listens. The conformance runner waits for that line.

use std::{collections::HashMap, net::SocketAddr, sync::{Arc, Mutex}};

use axum::{
    body::to_bytes,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::any,
    Router,
};

#[derive(Clone, Default)]
struct FixtureState {
    objects: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

async fn handler(
    State(state): State<FixtureState>,
    request: axum::extract::Request,
) -> Response {
    let method = request.method().clone();
    let path = request.uri().path();
    if method.as_str() == "POST" && path == "/__wipe" {
        state.objects.lock().unwrap().clear();
        return StatusCode::NO_CONTENT.into_response();
    }
    let key = path.trim_start_matches('/').to_owned();
    match method.as_str() {
        "PUT" => {
            let bytes = match to_bytes(request.into_body(), 256 * 1024 * 1024).await {
                Ok(bytes) => bytes.to_vec(),
                Err(_) => return StatusCode::BAD_REQUEST.into_response(),
            };
            state.objects.lock().unwrap().insert(key, bytes);
            StatusCode::OK.into_response()
        }
        "GET" => match state.objects.lock().unwrap().get(&key) {
            Some(bytes) => {
                let length = bytes.len().to_string();
                (StatusCode::OK, [(axum::http::header::CONTENT_LENGTH, length)], bytes.clone())
                    .into_response()
            }
            None => StatusCode::NOT_FOUND.into_response(),
        },
        "DELETE" => {
            state.objects.lock().unwrap().remove(&key);
            StatusCode::NO_CONTENT.into_response()
        }
        _ => StatusCode::METHOD_NOT_ALLOWED.into_response(),
    }
}

#[tokio::main]
async fn main() {
    let port = std::env::args()
        .nth(1)
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(19_000);
    let address = SocketAddr::from(([127, 0, 0, 1], port));
    let app = Router::new()
        .route("/{*key}", any(handler))
        .with_state(FixtureState::default());
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .expect("fixture must bind its port");
    println!("fixture-ready");
    axum::serve(listener, app).await.expect("fixture must serve");
}
