//! Spec 015 acceptance scenarios (A1–A7).
//!
//! Each `verify_cache_*` scenario maps to a named acceptance criterion in
//! `specs/015-hot-cache/spec.md`. The scenarios are pure in-process: the hot
//! cache is a library component, so no HTTP target is needed.

use palimpsest_cache::{MemoryHotCache, ValkeyHotCache};
use palimpsest_domain::{HotCache, HotCacheKind, NoopHotCache, TenantId, VersionedHotCache};
use uuid::Uuid;

fn tenant(n: u128) -> TenantId {
    TenantId::from(Uuid::from_u128(n))
}

/// Cache that can be switched to "down". A down cache behaves as a total miss.
struct FlakyHotCache {
    inner: MemoryHotCache,
    down: std::sync::atomic::AtomicBool,
}

impl FlakyHotCache {
    fn new() -> Self {
        Self {
            inner: MemoryHotCache::new(),
            down: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn set_down(&self) {
        self.down.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl HotCache for FlakyHotCache {
    async fn get(&self, tenant: TenantId, kind: HotCacheKind, scope: &str) -> Option<Vec<u8>> {
        if self.down.load(std::sync::atomic::Ordering::SeqCst) {
            return None;
        }
        self.inner.get(tenant, kind, scope).await
    }

    async fn put(
        &self,
        tenant: TenantId,
        kind: HotCacheKind,
        scope: &str,
        value: &[u8],
        ttl_seconds: u64,
    ) {
        if !self.down.load(std::sync::atomic::Ordering::SeqCst) {
            self.inner
                .put(tenant, kind, scope, value, ttl_seconds)
                .await;
        }
    }

    async fn delete(&self, tenant: TenantId, kind: HotCacheKind, scope: &str) {
        if !self.down.load(std::sync::atomic::Ordering::SeqCst) {
            self.inner.delete(tenant, kind, scope).await;
        }
    }
}

/// A1 — verify_cache_optional_off: the default cache opens no connection and
/// is always a miss, so every path falls back to the canonical store.
#[tokio::test]
async fn verify_cache_optional_off() {
    let cache = NoopHotCache;
    let t = tenant(1);
    cache
        .put(t, HotCacheKind::Receipt, "ep-1", b"receipt", 300)
        .await;
    cache
        .put(t, HotCacheKind::Lock, "proj-1", b"lock", 60)
        .await;
    assert!(cache.get(t, HotCacheKind::Receipt, "ep-1").await.is_none());
    assert!(cache.get(t, HotCacheKind::Lock, "proj-1").await.is_none());
}

/// A2 — verify_cache_loss_safe: eviction, restart, or total wipe must leave
/// retrieval correct (a miss falls back to the canonical path).
#[tokio::test]
async fn verify_cache_loss_safe() {
    let cache = MemoryHotCache::new();
    let t = tenant(2);
    cache
        .put(t, HotCacheKind::Checkpoint, "rev-1", b"marker", 600)
        .await;
    assert_eq!(
        cache.get(t, HotCacheKind::Checkpoint, "rev-1").await,
        Some(b"marker".to_vec())
    );

    // Simulate a total cache loss (eviction, restart, or wipe).
    cache.wipe();

    assert!(
        cache
            .get(t, HotCacheKind::Checkpoint, "rev-1")
            .await
            .is_none()
    );
}

/// A3 — verify_cache_rebuildable: the cache is provably rebuildable from
/// canonical records; after a wipe, a rebuild restores correct reads.
#[tokio::test]
async fn verify_cache_rebuildable() {
    let cache = MemoryHotCache::new();
    let t = tenant(3);
    cache
        .put(t, HotCacheKind::Receipt, "ep-2", b"receipt-b", 300)
        .await;
    cache.wipe();
    assert!(cache.get(t, HotCacheKind::Receipt, "ep-2").await.is_none());

    // Rebuild from the canonical record.
    cache
        .put(t, HotCacheKind::Receipt, "ep-2", b"receipt-b", 300)
        .await;
    assert_eq!(
        cache.get(t, HotCacheKind::Receipt, "ep-2").await,
        Some(b"receipt-b".to_vec())
    );
}

/// A4 — verify_cache_tenant_isolation: keys are tenant-scoped; a shared cache
/// cannot leak data between tenants.
#[tokio::test]
async fn verify_cache_tenant_isolation() {
    let cache = MemoryHotCache::new();
    let tenant_a = tenant(4);
    let tenant_b = tenant(5);

    cache
        .put(
            tenant_a,
            HotCacheKind::Receipt,
            "shared-scope",
            b"a-only",
            300,
        )
        .await;

    assert_eq!(
        cache
            .get(tenant_a, HotCacheKind::Receipt, "shared-scope")
            .await,
        Some(b"a-only".to_vec())
    );
    assert!(
        cache
            .get(tenant_b, HotCacheKind::Receipt, "shared-scope")
            .await
            .is_none()
    );
}

/// A5 — verify_cache_content_free: the cache stores only the versioned
/// envelope (version + caller payload); it never synthesizes or transforms
/// content, and raw private memory is never a routine cache field.
#[tokio::test]
async fn verify_cache_content_free() {
    let cache = MemoryHotCache::new();
    let t = tenant(6);
    let payload = b"receipt-marker";

    let versioned = VersionedHotCache::new(cache);
    versioned
        .put(t, HotCacheKind::Receipt, "ep-3", 7, payload, 300)
        .await;

    // The stored envelope is exactly version || payload — nothing more.
    let raw = versioned
        .inner()
        .get(t, HotCacheKind::Receipt, "ep-3")
        .await
        .expect("entry");
    assert_eq!(raw.len(), 8 + payload.len());
    assert_eq!(u64::from_le_bytes(raw[..8].try_into().unwrap()), 7);
    assert_eq!(&raw[8..], payload);

    // Reads return the payload unchanged.
    assert_eq!(
        versioned.get(t, HotCacheKind::Receipt, "ep-3", 7).await,
        Some(payload.to_vec())
    );
}

/// A6 — verify_cache_failure_injection: a cache that is unavailable during
/// reads behaves as a miss; canonical state is never touched by cache writes.
#[tokio::test]
async fn verify_cache_failure_injection() {
    let cache = FlakyHotCache::new();
    let t = tenant(7);

    cache
        .put(t, HotCacheKind::Lock, "proj-2", b"lease", 60)
        .await;
    assert_eq!(
        cache.get(t, HotCacheKind::Lock, "proj-2").await,
        Some(b"lease".to_vec())
    );

    cache.set_down();

    // Down cache = miss = the caller falls back to the canonical path.
    assert!(cache.get(t, HotCacheKind::Lock, "proj-2").await.is_none());

    // Writes while down are dropped silently; canonical state is unaffected.
    cache
        .put(t, HotCacheKind::Lock, "proj-2", b"new-lease", 60)
        .await;
    cache.set_down();
    cache.set_down(); // stays down
    assert!(cache.get(t, HotCacheKind::Lock, "proj-2").await.is_none());
}

/// A7 — verify_cache_invalidation: entries written under an older coverage
/// marker fail validation and are lazily refreshed from the canonical path.
#[tokio::test]
async fn verify_cache_invalidation() {
    let cache = MemoryHotCache::new();
    let t = tenant(8);
    let versioned = VersionedHotCache::new(cache);

    versioned
        .put(t, HotCacheKind::Checkpoint, "rev-2", 1, b"payload-v1", 600)
        .await;
    assert_eq!(
        versioned.get(t, HotCacheKind::Checkpoint, "rev-2", 1).await,
        Some(b"payload-v1".to_vec())
    );

    // The coverage marker is bumped; the old entry fails validation.
    assert!(
        versioned
            .get(t, HotCacheKind::Checkpoint, "rev-2", 2)
            .await
            .is_none()
    );

    // Lazy refresh: a new write under the current marker restores correctness.
    versioned
        .put(t, HotCacheKind::Checkpoint, "rev-2", 2, b"payload-v2", 600)
        .await;
    assert_eq!(
        versioned.get(t, HotCacheKind::Checkpoint, "rev-2", 2).await,
        Some(b"payload-v2".to_vec())
    );
}

/// Live Valkey integration (spec 015 design): runs only when VALKEY_URL is
/// set; otherwise the scenario is skipped. This is the only scenario that
/// requires a real server.
#[tokio::test]
async fn verify_cache_valkey_live_round_trip() {
    let url = match std::env::var("VALKEY_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("skipping live valkey scenario: VALKEY_URL is not set");
            return;
        }
    };

    let cache = ValkeyHotCache::connect(&url)
        .await
        .expect("connect to valkey");
    let t = tenant(9);
    let scope = format!("live-{}", Uuid::from_u128(9));

    cache
        .put(t, HotCacheKind::Receipt, &scope, b"live-receipt", 60)
        .await;
    assert_eq!(
        cache.get(t, HotCacheKind::Receipt, &scope).await,
        Some(b"live-receipt".to_vec())
    );

    cache.delete(t, HotCacheKind::Receipt, &scope).await;
    assert!(cache.get(t, HotCacheKind::Receipt, &scope).await.is_none());
}
