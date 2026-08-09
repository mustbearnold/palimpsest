//! `palimpsest-embedded` — library-first embedded mode (spec 014, issue #40).
//!
//! Spec 014 embedded mode runs the same governed operations as the HTTP
//! service without opening a network listener by default (spec 014 A3).
//! [`connect`] and [`open`] assemble a [`MemoryService`] over PostgreSQL 18
//! (the canonical substrate, spec 002 R1); no listener is opened.
//! [`EmbeddedMemory::serve_loopback`] is the explicit opt-in that binds a
//! loopback-only listener and serves the canonical `palimpsest-http` router.
//!
//! The embedded crate never spawns background workers and carries no probe
//! router: callers drive consolidation, deletion, and export through
//! [`MemoryService`] library calls (`run_*_worker_once`), which keeps the
//! embedded surface free of framework-specific machinery (spec 014 A4).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use palimpsest_application::{
    EmbeddingProvider, ExportPackageStore, ExportWorkerAuthorizer, FixtureDeterministicInterpreter,
    InterpreterRegistry, MemoryService, ServiceError, UnavailableEmbeddingProvider,
};
use palimpsest_domain::{PrincipalId, PrincipalScope, SubjectId, TenantId};
use palimpsest_http::Authenticator;
use palimpsest_postgres::{PostgresMemoryRepository, PostgresSubjectLifecycleRepository};
use palimpsest_stores::FileExportPackageStore;
use sqlx::PgPool;
use tokio::net::TcpListener;

/// A running embedded Palimpsest instance.
///
/// No network listener is opened by construction (spec 014 A3): the handle
/// carries only the assembled [`MemoryService`] and its authenticator.
/// [`EmbeddedMemory::loopback_addr`] is `None` until
/// [`EmbeddedMemory::serve_loopback`] is called explicitly.
pub struct EmbeddedMemory {
    service: MemoryService,
    authenticator: Arc<dyn Authenticator>,
    loopback_addr: Option<SocketAddr>,
}

/// The loopback-only server started by [`EmbeddedMemory::serve_loopback`].
pub struct LoopbackServer {
    /// Bound loopback address (the listener is bound to `127.0.0.1`).
    pub address: SocketAddr,
    handle: tokio::task::JoinHandle<()>,
}

impl LoopbackServer {
    /// Stops the server and awaits its shutdown.
    pub async fn stop(self) {
        self.handle.abort();
        let _ = self.handle.await;
    }
}

/// Assembles the embedded service without applying migrations.
///
/// The caller owns the connection pools and migration lifecycle. Use
/// [`open`] when the embedded instance should apply the canonical migrations.
pub fn connect(
    runtime_pool: PgPool,
    lifecycle_controller_pool: PgPool,
    authenticator: Arc<dyn Authenticator>,
) -> EmbeddedMemory {
    connect_with_embedding_provider(
        runtime_pool,
        lifecycle_controller_pool,
        authenticator,
        Arc::new(UnavailableEmbeddingProvider),
    )
}

/// Assembles the embedded service with a caller-provided embedding provider.
pub fn connect_with_embedding_provider(
    runtime_pool: PgPool,
    lifecycle_controller_pool: PgPool,
    authenticator: Arc<dyn Authenticator>,
    embedding_provider: Arc<dyn EmbeddingProvider>,
) -> EmbeddedMemory {
    let runtime_repository = Arc::new(PostgresMemoryRepository::new(runtime_pool.clone()));
    let lifecycle_repository = Arc::new(PostgresSubjectLifecycleRepository::new(
        runtime_pool.clone(),
        lifecycle_controller_pool,
    ));
    let exports = runtime_repository.clone();
    let export_store = export_store();
    let service = MemoryService::new(
        lifecycle_repository,
        runtime_repository.clone(),
        runtime_repository.clone(),
        runtime_repository.clone(),
        runtime_repository.clone(),
    )
    .with_embedding_provider(embedding_provider)
    .with_export_components(exports, export_store)
    .with_export_worker_authorizer(Arc::new(EmbeddedExportWorkerAuthorizer {
        authenticator: authenticator.clone(),
    }))
    .with_consolidation_components(runtime_repository.clone(), Arc::new(interpreter_registry()))
    .with_surface_components(runtime_repository);
    EmbeddedMemory {
        service,
        authenticator,
        loopback_addr: None,
    }
}

/// Applies the canonical migrations (spec 014 A6 substrate) and opens an
/// embedded instance. This is the single run entry for embedded mode.
pub async fn open(
    runtime_pool: PgPool,
    lifecycle_controller_pool: PgPool,
    authenticator: Arc<dyn Authenticator>,
) -> anyhow::Result<EmbeddedMemory> {
    palimpsest_postgres::migrate(&runtime_pool).await?;
    Ok(connect(
        runtime_pool,
        lifecycle_controller_pool,
        authenticator,
    ))
}

/// The canonical governed router served by [`EmbeddedMemory::serve_loopback`].
///
/// This is `palimpsest-http`'s router verbatim — the embedded surface and
/// the HTTP service surface are the same contract (spec 014 A4). There is
/// no probe router and no latency middleware in embedded mode.
pub fn router(service: MemoryService, authenticator: Arc<dyn Authenticator>) -> Router {
    palimpsest_http::router(service, authenticator)
}

impl EmbeddedMemory {
    /// Library-first access to the governed operations (spec 014 D2).
    pub fn service(&self) -> &MemoryService {
        &self.service
    }

    /// The bound loopback listener address, or `None` while no listener is
    /// open (spec 014 A3: no listener is opened by default).
    pub fn loopback_addr(&self) -> Option<SocketAddr> {
        self.loopback_addr
    }

    /// Explicit opt-in (spec 014 A3): binds a loopback-only listener and
    /// serves the canonical HTTP router until the returned
    /// [`LoopbackServer`] is stopped.
    pub async fn serve_loopback(self) -> anyhow::Result<LoopbackServer> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let router = router(self.service, self.authenticator);
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        Ok(LoopbackServer { address, handle })
    }
}

/// Re-establishes export worker authorization from the trusted
/// [`Authenticator`] (spec 014 A2: worker authorization fails closed unless
/// the authenticator grants it).
struct EmbeddedExportWorkerAuthorizer {
    authenticator: Arc<dyn Authenticator>,
}

impl ExportWorkerAuthorizer for EmbeddedExportWorkerAuthorizer {
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

fn export_root() -> PathBuf {
    std::env::var_os("PALIMPSEST_EXPORT_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("var/palimpsest/exports"))
}

fn export_store() -> Arc<dyn ExportPackageStore> {
    Arc::new(FileExportPackageStore::new(export_root()))
}

fn interpreter_registry() -> InterpreterRegistry {
    let mut registry = InterpreterRegistry::default();
    registry.register(Box::new(FixtureDeterministicInterpreter));
    registry
}
