//! Optional hot cache implementations (spec 015).
//!
//! `ValkeyHotCache` talks the Redis protocol to a Valkey/Redis server.
//! `MemoryHotCache` is a deterministic in-process double used by tests and
//! conformance scenarios. Both implement the `HotCache` contract from
//! `palimpsest-domain`.
//!
//! The cache is never a source of truth. Every implementation must be safe to
//! lose: a miss falls back to the canonical path.

use palimpsest_domain::{HotCache, HotCacheKind, TenantId};
use std::collections::HashMap;
use std::sync::Mutex;

/// Key schema (spec 015): `palimpsest:{tenant}:{kind}:{scope}`.
fn cache_key(tenant: TenantId, kind: HotCacheKind, scope: &str) -> String {
    let kind_str = match kind {
        HotCacheKind::Checkpoint => "checkpoint",
        HotCacheKind::Lock => "lock",
        HotCacheKind::Receipt => "receipt",
    };
    format!("palimpsest:{}:{}:{}", tenant.0, kind_str, scope)
}

/// Valkey/Redis-backed cache over the Redis protocol.
#[derive(Clone)]
pub struct ValkeyHotCache {
    manager: redis::aio::ConnectionManager,
}

impl ValkeyHotCache {
    /// Connect to a Valkey/Redis server.
    pub async fn connect(url: &str) -> redis::RedisResult<Self> {
        let client = redis::Client::open(url)?;
        let manager = redis::aio::ConnectionManager::new(client).await?;
        Ok(Self { manager })
    }

    async fn cmd(&self) -> redis::aio::ConnectionManager {
        self.manager.clone()
    }
}

#[async_trait::async_trait]
impl HotCache for ValkeyHotCache {
    async fn get(&self, tenant: TenantId, kind: HotCacheKind, scope: &str) -> Option<Vec<u8>> {
        let mut cmd = self.cmd().await;
        redis::cmd("GET")
            .arg(cache_key(tenant, kind, scope))
            .query_async::<Option<Vec<u8>>>(&mut cmd)
            .await
            .unwrap_or(None)
    }

    async fn put(
        &self,
        tenant: TenantId,
        kind: HotCacheKind,
        scope: &str,
        value: &[u8],
        ttl_seconds: u64,
    ) {
        let mut cmd = self.cmd().await;
        let _: Result<(), _> = redis::cmd("SETEX")
            .arg(cache_key(tenant, kind, scope))
            .arg(ttl_seconds)
            .arg(value)
            .query_async(&mut cmd)
            .await;
    }

    async fn delete(&self, tenant: TenantId, kind: HotCacheKind, scope: &str) {
        let mut cmd = self.cmd().await;
        let _: Result<(), _> = redis::cmd("DEL")
            .arg(cache_key(tenant, kind, scope))
            .query_async(&mut cmd)
            .await;
    }
}

/// Deterministic in-process cache. Safe to lose: state lives only in memory.
/// Used by tests and conformance scenarios; never a production cache.
#[derive(Debug, Default)]
pub struct MemoryHotCache {
    entries: Mutex<HashMap<String, (Vec<u8>, u64)>>,
}

impl MemoryHotCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Wipe all entries. Simulates a total cache loss.
    pub fn wipe(&self) {
        self.entries.lock().expect("cache lock").clear();
    }

    /// Number of live entries.
    pub fn len(&self) -> usize {
        self.entries.lock().expect("cache lock").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait::async_trait]
impl HotCache for MemoryHotCache {
    async fn get(&self, tenant: TenantId, kind: HotCacheKind, scope: &str) -> Option<Vec<u8>> {
        let entries = self.entries.lock().expect("cache lock");
        entries
            .get(&cache_key(tenant, kind, scope))
            .map(|(value, _)| value.clone())
    }

    async fn put(
        &self,
        tenant: TenantId,
        kind: HotCacheKind,
        scope: &str,
        value: &[u8],
        ttl_seconds: u64,
    ) {
        let mut entries = self.entries.lock().expect("cache lock");
        entries.insert(
            cache_key(tenant, kind, scope),
            (value.to_vec(), ttl_seconds),
        );
    }

    async fn delete(&self, tenant: TenantId, kind: HotCacheKind, scope: &str) {
        let mut entries = self.entries.lock().expect("cache lock");
        entries.remove(&cache_key(tenant, kind, scope));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use palimpsest_domain::NoopHotCache;
    use uuid::Uuid;

    #[tokio::test]
    async fn memory_cache_round_trips_and_deletes() {
        let cache = MemoryHotCache::new();
        let tenant = TenantId::from(Uuid::from_u128(1));
        assert!(
            cache
                .get(tenant, HotCacheKind::Receipt, "ep-1")
                .await
                .is_none()
        );

        cache
            .put(tenant, HotCacheKind::Receipt, "ep-1", b"receipt-bytes", 300)
            .await;
        assert_eq!(
            cache.get(tenant, HotCacheKind::Receipt, "ep-1").await,
            Some(b"receipt-bytes".to_vec())
        );

        cache.delete(tenant, HotCacheKind::Receipt, "ep-1").await;
        assert!(
            cache
                .get(tenant, HotCacheKind::Receipt, "ep-1")
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn memory_cache_isolates_tenants() {
        let cache = MemoryHotCache::new();
        let tenant_a = TenantId::from(Uuid::from_u128(1));
        let tenant_b = TenantId::from(Uuid::from_u128(2));

        cache
            .put(tenant_a, HotCacheKind::Lock, "proj-1", b"a", 60)
            .await;
        assert!(
            cache
                .get(tenant_b, HotCacheKind::Lock, "proj-1")
                .await
                .is_none()
        );
        assert_eq!(
            cache.get(tenant_a, HotCacheKind::Lock, "proj-1").await,
            Some(b"a".to_vec())
        );
    }

    #[tokio::test]
    async fn memory_cache_wipe_is_a_total_miss() {
        let cache = MemoryHotCache::new();
        let tenant = TenantId::from(Uuid::from_u128(1));
        cache
            .put(tenant, HotCacheKind::Checkpoint, "rev-1", b"v", 60)
            .await;
        assert_eq!(cache.len(), 1);

        cache.wipe();

        assert_eq!(cache.len(), 0);
        assert!(
            cache
                .get(tenant, HotCacheKind::Checkpoint, "rev-1")
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn kinds_do_not_collide_in_the_key_schema() {
        let cache = MemoryHotCache::new();
        let tenant = TenantId::from(Uuid::from_u128(1));
        cache
            .put(tenant, HotCacheKind::Lock, "x", b"lock", 60)
            .await;
        cache
            .put(tenant, HotCacheKind::Receipt, "x", b"receipt", 60)
            .await;
        assert_eq!(
            cache.get(tenant, HotCacheKind::Lock, "x").await,
            Some(b"lock".to_vec())
        );
        assert_eq!(
            cache.get(tenant, HotCacheKind::Receipt, "x").await,
            Some(b"receipt".to_vec())
        );
    }

    #[tokio::test]
    async fn noop_is_a_miss_even_after_writes() {
        let cache = NoopHotCache;
        let tenant = TenantId::from(Uuid::from_u128(3));
        cache.put(tenant, HotCacheKind::Lock, "x", b"v", 60).await;
        assert!(cache.get(tenant, HotCacheKind::Lock, "x").await.is_none());
    }
}
