// Copyright (c) 2026 The NORA Authors
// SPDX-License-Identifier: MIT

//! In-memory repository index rebuilt by one background worker.
//!
//! Design:
//! - Request handlers only read the last published snapshot
//! - Invalidation increments a generation, so a write racing a rebuild cannot be lost
//! - One worker rebuilds active registries sequentially, bounding storage scan concurrency
//! - Protocol callers may wait for the generation visible when their request began

use crate::registry_type::RegistryType;
use crate::storage::{FileMeta, Storage};
use crate::ui::components::format_timestamp;
use crate::validation::ends_with_ci;
use parking_lot::RwLock;
use rand::Rng as _;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex as AsyncMutex, Notify};
use tokio::time::Instant;
use tracing::info;
use utoipa::ToSchema;

const INDEX_RETRY_BASE_SECS: u64 = 30;
const INDEX_RETRY_MAX_SECS: u64 = 300;

fn index_retry_ceiling_secs(attempt: u32) -> u64 {
    INDEX_RETRY_BASE_SECS
        .saturating_mul(1_u64 << attempt.min(4))
        .min(INDEX_RETRY_MAX_SECS)
}

fn index_retry_delay(attempt: u32) -> Duration {
    let ceiling = index_retry_ceiling_secs(attempt);
    let floor = (ceiling / 2).max(1);
    Duration::from_secs(rand::thread_rng().gen_range(floor..=ceiling))
}

/// Repository info for UI display
#[derive(Debug, Clone, Serialize, ToSchema, Default)]
pub struct RepoInfo {
    pub name: String,
    pub versions: usize,
    /// Retained as a numeric compatibility field. A zero value is meaningful
    /// only when `size_available` is true.
    pub size: u64,
    /// Whether `size` was derived from an authoritative index snapshot.
    #[serde(default)]
    pub size_available: bool,
    pub updated: String,
    /// True for root-level files in raw storage (not directories)
    #[serde(default)]
    pub is_file: bool,
}

/// Cached object metadata used by repository-aware browser views. Keeping it
/// in the same generation as the aggregate repository rows prevents UI
/// requests from issuing their own storage LIST/HEAD fan-out.
#[derive(Debug, Clone)]
pub struct IndexedObject {
    pub key: String,
    pub meta: FileMeta,
}

/// Minimal hosted npm search document derived from the same validated full
/// generation as the repository row. Request handlers filter and render this
/// projection in memory; split version/tag objects are never a search oracle.
#[derive(Debug, Clone)]
pub(crate) struct NpmSearchDocument {
    pub(crate) repository: String,
    pub(crate) package: String,
    pub(crate) version: String,
    pub(crate) fields: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ListedIdentity {
    size: u64,
    modified: u64,
}

impl From<&FileMeta> for ListedIdentity {
    fn from(meta: &FileMeta) -> Self {
        Self {
            size: meta.size,
            modified: meta.modified,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct NpmPackageId {
    repository: String,
    package: String,
}

#[derive(Debug, Clone)]
struct NpmHostedLkg {
    pointer_sha256: String,
    dependencies: BTreeMap<String, ListedIdentity>,
    versions: usize,
    modified: u64,
    search: Option<NpmSearchDocument>,
}

#[derive(Debug, Default)]
struct PublishedIndex {
    repos: Arc<Vec<RepoInfo>>,
    objects: Arc<Vec<IndexedObject>>,
    npm_hosted: Arc<HashMap<NpmPackageId, NpmHostedLkg>>,
    npm_search: Arc<Vec<NpmSearchDocument>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum IndexStatus {
    Warming,
    Ready,
    Degraded,
}

impl IndexStatus {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Ready,
            2 => Self::Degraded,
            _ => Self::Warming,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RebuildOutcome {
    Clean,
    Published { generation: u64, degraded: bool },
    Superseded,
    Failed { generation: u64 },
}

impl RebuildOutcome {
    #[cfg(test)]
    fn succeeded(self) -> bool {
        !matches!(self, Self::Failed { .. })
    }
}

#[derive(Debug, Clone, Copy)]
struct IndexRetryState {
    generation: u64,
    attempt: u32,
    deadline: Instant,
}

fn schedule_index_retry(
    retries: &mut HashMap<RegistryType, IndexRetryState>,
    registry: RegistryType,
    generation: u64,
) {
    let attempt = retries
        .get(&registry)
        .filter(|retry| retry.generation == generation)
        .map(|retry| retry.attempt.saturating_add(1))
        .unwrap_or(0);
    let delay = index_retry_delay(attempt);
    retries.insert(
        registry,
        IndexRetryState {
            generation,
            attempt,
            deadline: Instant::now() + delay,
        },
    );
    tracing::warn!(
        registry = registry.as_str(),
        generation,
        attempt = attempt.saturating_add(1),
        retry_after_secs = delay.as_secs(),
        "index rebuild scheduled for bounded retry"
    );
}

struct BuiltIndex {
    repos: Vec<RepoInfo>,
    objects: Vec<IndexedObject>,
    degraded: bool,
    npm_hosted: HashMap<NpmPackageId, NpmHostedLkg>,
}

impl BuiltIndex {
    #[cfg(test)]
    fn repos(repos: Vec<RepoInfo>) -> Self {
        Self {
            repos,
            objects: Vec::new(),
            degraded: false,
            npm_hosted: HashMap::new(),
        }
    }

    fn with_objects(repos: Vec<RepoInfo>, keys: Vec<(String, FileMeta)>) -> Self {
        Self::with_objects_and_status(repos, keys, false)
    }

    fn with_objects_and_status(
        repos: Vec<RepoInfo>,
        keys: Vec<(String, FileMeta)>,
        degraded: bool,
    ) -> Self {
        let mut objects: Vec<_> = keys
            .into_iter()
            .map(|(key, meta)| IndexedObject { key, meta })
            .collect();
        objects.sort_by(|left, right| left.key.cmp(&right.key));
        Self {
            repos,
            objects,
            degraded,
            npm_hosted: HashMap::new(),
        }
    }

    fn with_npm_hosted(mut self, npm_hosted: HashMap<NpmPackageId, NpmHostedLkg>) -> Self {
        self.npm_hosted = npm_hosted;
        self
    }

    fn into_published(self) -> PublishedIndex {
        let mut npm_search: Vec<_> = self
            .npm_hosted
            .values()
            .filter_map(|package| package.search.clone())
            .collect();
        npm_search.sort_by(|left, right| {
            (&left.repository, &left.package).cmp(&(&right.repository, &right.package))
        });
        PublishedIndex {
            repos: Arc::new(self.repos),
            objects: Arc::new(self.objects),
            npm_hosted: Arc::new(self.npm_hosted),
            npm_search: Arc::new(npm_search),
        }
    }
}

/// Index for a single registry type
pub struct RegistryIndex {
    published: RwLock<Arc<PublishedIndex>>,
    requested_generation: AtomicU64,
    published_generation: AtomicU64,
    failed_generation: AtomicU64,
    status: AtomicU8,
    rebuild_lock: AsyncMutex<()>,
    changed: Notify,
}

impl RegistryIndex {
    pub fn new() -> Self {
        Self {
            published: RwLock::new(Arc::new(PublishedIndex::default())),
            requested_generation: AtomicU64::new(1),
            published_generation: AtomicU64::new(0),
            failed_generation: AtomicU64::new(0),
            status: AtomicU8::new(0),
            rebuild_lock: AsyncMutex::new(()),
            changed: Notify::new(),
        }
    }

    /// Mark index as needing rebuild
    pub fn invalidate(&self) -> u64 {
        self.status.store(0, Ordering::Release);
        self.requested_generation.fetch_add(1, Ordering::AcqRel) + 1
    }

    fn is_dirty(&self) -> bool {
        self.published_generation.load(Ordering::Acquire)
            < self.requested_generation.load(Ordering::Acquire)
    }

    fn get_cached(&self) -> Arc<Vec<RepoInfo>> {
        Arc::clone(&self.published.read().repos)
    }

    fn get_objects(&self) -> Arc<Vec<IndexedObject>> {
        Arc::clone(&self.published.read().objects)
    }

    fn get_published(&self) -> Arc<PublishedIndex> {
        Arc::clone(&self.published.read())
    }

    fn get_npm_search(&self) -> Arc<Vec<NpmSearchDocument>> {
        Arc::clone(&self.published.read().npm_search)
    }

    fn set(&self, built: BuiltIndex, generation: u64) {
        let degraded = built.degraded;
        *self.published.write() = Arc::new(built.into_published());
        self.published_generation
            .store(generation, Ordering::Release);
        let status = if generation < self.requested_generation.load(Ordering::Acquire) {
            0
        } else if degraded {
            2
        } else {
            1
        };
        self.status.store(status, Ordering::Release);
        self.changed.notify_waiters();
    }

    fn fail(&self, generation: u64) {
        self.failed_generation.store(generation, Ordering::Release);
        self.status.store(2, Ordering::Release);
        self.changed.notify_waiters();
    }

    pub fn status(&self) -> IndexStatus {
        IndexStatus::from_u8(self.status.load(Ordering::Acquire))
    }

    pub fn count(&self) -> usize {
        // A directory that holds only generated metadata/sidecars — e.g. Maven's
        // artifact-level `maven-metadata.xml`, which lands in the parent dir of
        // the version dirs — materialises as a RepoInfo with zero versions. It
        // is not a repository, so it must not inflate the per-registry count
        // that `/api/ui/stats` and `nora_artifacts_total` report (it would show
        // maven:2 for a single pushed jar).
        self.published
            .read()
            .repos
            .iter()
            .filter(|r| r.versions > 0)
            .count()
    }

    /// Sum logical artifact bytes only when the published rows declare that
    /// value available. `None` avoids exporting a misleading zero series.
    pub fn total_size(&self) -> Option<u64> {
        let published = self.published.read();
        let data = &published.repos;
        if data.is_empty() {
            return (self.status() == IndexStatus::Ready).then_some(0);
        }
        data.iter()
            .all(|row| row.size_available)
            .then(|| data.iter().map(|row| row.size).sum())
    }
}

impl Default for RegistryIndex {
    fn default() -> Self {
        Self::new()
    }
}

/// Main repository index for all registries
pub struct RepoIndex {
    indexes: HashMap<RegistryType, RegistryIndex>,
    active: RwLock<HashSet<RegistryType>>,
    notify: Arc<Notify>,
    background_started: AtomicBool,
    /// Epoch-seconds of the last accepted admin reindex (0 = never). Used to
    /// debounce operator-triggered reindex so a tight `reindex + read` loop
    /// cannot amplify into repeated full-storage scans (see `try_accept_reindex`).
    last_reindex: AtomicU64,
}

impl RepoIndex {
    pub fn new() -> Self {
        let mut indexes = HashMap::new();
        for rt in RegistryType::all() {
            indexes.insert(*rt, RegistryIndex::new());
        }
        Self {
            indexes,
            active: RwLock::new(HashSet::new()),
            notify: Arc::new(Notify::new()),
            background_started: AtomicBool::new(false),
            last_reindex: AtomicU64::new(0),
        }
    }

    fn activate(&self, registry: RegistryType) {
        if self.active.write().insert(registry) {
            self.notify.notify_one();
        }
    }

    /// Start the single bounded index worker. The worker performs one storage
    /// scan at a time and exits with the supplied application cancellation
    /// token. Calling this more than once is harmless.
    pub fn start_background(
        self: &Arc<Self>,
        storage: Storage,
        registries: impl IntoIterator<Item = RegistryType>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Option<tokio::task::JoinHandle<()>> {
        let runtime = tokio::runtime::Handle::try_current().ok()?;
        for registry in registries {
            self.activate(registry);
        }
        if self
            .background_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            self.notify.notify_one();
            return None;
        }

        let weak = Arc::downgrade(self);
        Some(runtime.spawn(async move {
            let mut retries = HashMap::<RegistryType, IndexRetryState>::new();
            loop {
                let Some(repo_index) = weak.upgrade() else {
                    return;
                };
                let notified = Arc::clone(&repo_index.notify).notified_owned();
                let active = repo_index.active.read().clone();
                for registry in RegistryType::all() {
                    if !active.contains(registry) {
                        continue;
                    }
                    if cancel.is_cancelled() {
                        return;
                    }
                    let Some(index) = repo_index.indexes.get(registry) else {
                        continue;
                    };
                    let requested_generation = index.requested_generation.load(Ordering::Acquire);
                    if retries
                        .get(registry)
                        .is_some_and(|retry| requested_generation > retry.generation)
                    {
                        // A real mutation/admin invalidation is newer than the
                        // failed generation and must not wait behind its retry.
                        retries.remove(registry);
                    }
                    if let Some(retry) = retries.get_mut(registry) {
                        if retry.deadline > Instant::now() {
                            continue;
                        }
                        if !index.is_dirty() && index.status() == IndexStatus::Degraded {
                            // A partial snapshot was published successfully;
                            // create the next generation only when its bounded
                            // retry deadline arrives.
                            retry.generation = index.invalidate();
                        }
                    }
                    if !index.is_dirty() {
                        if index.status() != IndexStatus::Degraded {
                            retries.remove(registry);
                        }
                        continue;
                    }

                    match repo_index.rebuild_one(*registry, &storage).await {
                        RebuildOutcome::Clean => {
                            retries.remove(registry);
                        }
                        RebuildOutcome::Published {
                            generation,
                            degraded: false,
                        } => {
                            if retries.remove(registry).is_some() {
                                tracing::debug!(
                                    registry = registry.as_str(),
                                    generation,
                                    "index retry state cleared after successful rebuild"
                                );
                            }
                        }
                        RebuildOutcome::Published {
                            generation,
                            degraded: true,
                        }
                        | RebuildOutcome::Failed { generation } => {
                            schedule_index_retry(&mut retries, *registry, generation);
                        }
                        RebuildOutcome::Superseded => {
                            // An invalidate raced the scan. Coalesce immediately
                            // onto the newer requested generation rather than
                            // applying failure backoff to stale work.
                            retries.remove(registry);
                        }
                    }
                }

                let pending_after_pass = active.iter().any(|registry| {
                    repo_index
                        .indexes
                        .get(registry)
                        .is_some_and(RegistryIndex::is_dirty)
                        && !retries.contains_key(registry)
                });
                let next_retry = retries
                    .iter()
                    .filter(|(registry, _)| active.contains(registry))
                    .map(|(_, retry)| retry.deadline)
                    .min();
                drop(repo_index);
                if pending_after_pass {
                    tokio::select! {
                        _ = cancel.cancelled() => return,
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {},
                    }
                } else if let Some(deadline) = next_retry {
                    tokio::select! {
                        _ = cancel.cancelled() => return,
                        _ = notified => {},
                        _ = tokio::time::sleep_until(deadline) => {},
                    }
                } else {
                    tokio::select! {
                        _ = cancel.cancelled() => return,
                        _ = notified => {},
                    }
                }
            }
        }))
    }

    async fn rebuild_one(&self, registry: RegistryType, storage: &Storage) -> RebuildOutcome {
        let Some(index) = self.indexes.get(&registry) else {
            return RebuildOutcome::Failed { generation: 0 };
        };
        if !index.is_dirty() {
            return RebuildOutcome::Clean;
        }
        let _guard = index.rebuild_lock.lock().await;
        if !index.is_dirty() {
            return RebuildOutcome::Clean;
        }
        let generation = index.requested_generation.load(Ordering::Acquire);
        let previous = index.get_published();
        match build_index(registry, storage, &previous).await {
            Some(built) => {
                let degraded = built.degraded;
                info!(
                    registry = registry.as_str(),
                    count = built.repos.len(),
                    generation,
                    degraded,
                    "Index rebuilt"
                );
                index.set(built, generation);
                if index.is_dirty() {
                    RebuildOutcome::Superseded
                } else {
                    RebuildOutcome::Published {
                        generation,
                        degraded,
                    }
                }
            }
            None => {
                index.fail(generation);
                tracing::warn!(
                    registry = registry.as_str(),
                    generation,
                    "index rebuild failed; retaining last published snapshot"
                );
                if index.requested_generation.load(Ordering::Acquire) > generation {
                    RebuildOutcome::Superseded
                } else {
                    RebuildOutcome::Failed { generation }
                }
            }
        }
    }

    /// Invalidate a specific registry index
    pub fn invalidate(&self, registry: &str) {
        if let Some(rt) = RegistryType::from_str_opt(registry) {
            if let Some(idx) = self.indexes.get(&rt) {
                idx.invalidate();
                self.activate(rt);
                self.notify.notify_one();
            }
        }
    }

    /// Invalidate every registry index so each rebuilds from storage on next read.
    /// Backs the admin reindex endpoint for the "rescan all paths" case.
    pub fn invalidate_all(&self) {
        for (registry, idx) in &self.indexes {
            idx.invalidate();
            self.activate(*registry);
        }
        self.notify.notify_one();
    }

    /// Debounce gate for operator-triggered reindex. Returns `Ok(())` and records
    /// `now_epoch` if at least `min_interval` seconds have passed since the last
    /// accepted reindex; otherwise returns `Err(retry_after_secs)` without
    /// recording. This is the one DoS control that holds even when HTTP rate
    /// limiting is disabled in config.
    pub fn try_accept_reindex(&self, now_epoch: u64, min_interval: u64) -> Result<(), u64> {
        let last = self.last_reindex.load(Ordering::Acquire);
        if last != 0 {
            let elapsed = now_epoch.saturating_sub(last);
            if elapsed < min_interval {
                return Err(min_interval - elapsed);
            }
        }
        self.last_reindex.store(now_epoch, Ordering::Release);
        Ok(())
    }

    /// Return the last published snapshot immediately. No storage operation is
    /// ever initiated by this request-path method.
    pub async fn get(&self, registry: &str, _storage: &Storage) -> Arc<Vec<RepoInfo>> {
        let reg_type = match RegistryType::from_str_opt(registry) {
            Some(rt) => rt,
            None => return Arc::new(Vec::new()),
        };
        let index = match self.indexes.get(&reg_type) {
            Some(idx) => idx,
            None => return Arc::new(Vec::new()),
        };

        self.activate(reg_type);
        self.notify.notify_one();
        index.get_cached()
    }

    /// Rebuild a dirty index and surface storage uncertainty instead of
    /// serving stale data. Protocol handlers use this when an empty/partial
    /// answer would be semantically different from a UI's stale snapshot.
    pub async fn get_strict(
        &self,
        registry: &str,
        _storage: &Storage,
    ) -> Result<Arc<Vec<RepoInfo>>, ()> {
        let reg_type = RegistryType::from_str_opt(registry).ok_or(())?;
        let index = self.indexes.get(&reg_type).ok_or(())?;
        self.activate(reg_type);
        self.notify.notify_one();
        let required_generation = index.requested_generation.load(Ordering::Acquire);
        loop {
            let changed = index.changed.notified();
            if index.published_generation.load(Ordering::Acquire) >= required_generation {
                return Ok(index.get_cached());
            }
            if index.failed_generation.load(Ordering::Acquire) >= required_generation {
                return Err(());
            }
            changed.await;
        }
    }

    /// Return the hosted npm search projection from the exact published index
    /// generation required when the request began. The snapshot is entirely
    /// in memory and shares the atomic publication boundary with RepoInfo.
    pub(crate) async fn npm_search_strict(&self) -> Result<Arc<Vec<NpmSearchDocument>>, ()> {
        let registry = RegistryType::Npm;
        let index = self.indexes.get(&registry).ok_or(())?;
        self.activate(registry);
        self.notify.notify_one();
        let required_generation = index.requested_generation.load(Ordering::Acquire);
        loop {
            let changed = index.changed.notified();
            if index.published_generation.load(Ordering::Acquire) >= required_generation {
                return Ok(index.get_npm_search());
            }
            if index.failed_generation.load(Ordering::Acquire) >= required_generation {
                return Err(());
            }
            changed.await;
        }
    }

    pub fn status(&self, registry: &str) -> Option<IndexStatus> {
        let registry = RegistryType::from_str_opt(registry)?;
        self.indexes.get(&registry).map(RegistryIndex::status)
    }

    pub fn maven_objects(&self) -> Arc<Vec<IndexedObject>> {
        self.objects("maven")
    }

    /// Return the last published object snapshot for a registry. Browser
    /// request paths filter this in memory instead of issuing storage LISTs.
    pub fn objects(&self, registry: &str) -> Arc<Vec<IndexedObject>> {
        RegistryType::from_str_opt(registry)
            .and_then(|registry| self.indexes.get(&registry))
            .map(RegistryIndex::get_objects)
            .unwrap_or_else(|| Arc::new(Vec::new()))
    }

    #[cfg(test)]
    pub(crate) async fn rebuild_for_test(&self, registry: RegistryType, storage: &Storage) -> bool {
        self.activate(registry);
        self.rebuild_one(registry, storage).await.succeeded()
    }

    #[cfg(test)]
    pub(crate) fn publish_objects_for_test(
        &self,
        registry: RegistryType,
        objects: Vec<(String, FileMeta)>,
    ) {
        let index = self.indexes.get(&registry).expect("known registry");
        let generation = index.requested_generation.load(Ordering::Acquire);
        index.set(BuiltIndex::with_objects(Vec::new(), objects), generation);
    }

    /// Get counts for stats (no rebuild, just current state)
    pub fn counts(&self) -> HashMap<RegistryType, usize> {
        self.indexes
            .iter()
            .map(|(rt, idx)| (*rt, idx.count()))
            .collect()
    }

    /// Get total artifact bytes per registry from the cached index (no rebuild).
    pub fn sizes(&self) -> HashMap<RegistryType, u64> {
        self.indexes
            .iter()
            .filter(|(rt, _)| **rt != RegistryType::Npm)
            .filter_map(|(rt, idx)| idx.total_size().map(|size| (*rt, size)))
            .collect()
    }
}

async fn build_index(
    reg_type: RegistryType,
    storage: &Storage,
    previous: &PublishedIndex,
) -> Option<BuiltIndex> {
    if reg_type == RegistryType::Maven {
        return build_maven_index_with_objects(storage).await;
    }
    match reg_type {
        RegistryType::Docker => build_docker_index(storage).await,
        RegistryType::Maven => unreachable!("handled above"),
        RegistryType::Npm => build_npm_index(storage, &previous.npm_hosted).await,
        RegistryType::Cargo => build_cargo_index(storage).await,
        RegistryType::PyPI => build_pypi_index(storage).await,
        RegistryType::Go => build_go_index(storage).await,
        RegistryType::Raw => build_raw_index(storage).await,
        RegistryType::Nuget => {
            let (prefix, suffix) = crate::registry::nuget::INDEX_PATTERN;
            build_generic_index(storage, prefix, suffix).await
        }
        RegistryType::Gems => build_gems_index(storage).await,
        RegistryType::Terraform => {
            let (prefix, suffix) = crate::registry::terraform::INDEX_PATTERN;
            build_generic_index(storage, prefix, suffix).await
        }
        RegistryType::Ansible => {
            let (prefix, suffix) = crate::registry::ansible::INDEX_PATTERN;
            build_generic_index(storage, prefix, suffix).await
        }
        RegistryType::PubDart => {
            let (prefix, suffix) = crate::registry::pub_dart::INDEX_PATTERN;
            build_generic_index(storage, prefix, suffix).await
        }
        RegistryType::Conan => build_conan_index(storage).await,
        RegistryType::Rpm => {
            let (prefix, suffix) = crate::registry::rpm::INDEX_PATTERN;
            build_generic_index(storage, prefix, suffix).await
        }
        RegistryType::Deb => {
            let (prefix, suffix) = crate::registry::deb::INDEX_PATTERN;
            build_generic_index(storage, prefix, suffix).await
        }
    }
}

impl Default for RepoIndex {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Index builders
// ============================================================================

/// List storage keys under `prefix` for an index rebuild. Returns `None` (not an
/// empty Vec) when the listing itself fails, so the caller can keep the existing
/// index dirty and retry rather than caching a falsely-empty result as fresh.
async fn list_keys(storage: &Storage, prefix: &str) -> Option<Vec<(String, FileMeta)>> {
    match storage.list_with_meta(prefix).await {
        Ok(keys) => Some(keys),
        Err(_) => {
            tracing::warn!(
                prefix,
                backend = storage.backend_name(),
                error_class = "index_storage_list_failed",
                "index rebuild: storage list failed"
            );
            None
        }
    }
}

async fn build_docker_index(storage: &Storage) -> Option<BuiltIndex> {
    let keys = list_keys(storage, "docker/").await?;
    let mut repos: HashMap<String, (usize, u64, u64)> = HashMap::new();

    for (key, meta) in &keys {
        if ends_with_ci(key, ".meta.json") {
            continue;
        }

        if let Some(rest) = key.strip_prefix("docker/") {
            // Support both single-segment and namespaced images:
            // docker/alpine/manifests/latest.json → name="alpine"
            // docker/library/alpine/blobs/sha256:... → name="library/alpine"
            let parts: Vec<_> = rest.split('/').collect();
            // Repo name = everything before the "manifests"/"blobs" segment.
            let Some(boundary) = parts.iter().position(|&p| p == "manifests" || p == "blobs")
            else {
                continue;
            };
            if boundary < 1 {
                continue;
            }
            let raw_name = parts[..boundary].join("/");
            let name = crate::registry::docker::strip_docker_namespace(&raw_name).to_string();
            let entry = repos.entry(name).or_insert((0, 0, 0));

            // Size = ACTUAL on-disk bytes of every file in the repo (blobs +
            // manifests), each counted once. The old code summed the manifest's
            // declared config+layer sizes — a "virtual" size that multi-counts
            // layers shared across tags and ignores real storage, so a 7.2G
            // image tree could be reported as something else entirely (#588).
            entry.1 += meta.size;
            if meta.modified > entry.2 {
                entry.2 = meta.modified;
            }

            // Count = number of distinct tags. Each push writes BOTH a
            // tag manifest (`manifests/<tag>.json`) and a content-addressed
            // `manifests/sha256:<digest>.json`; counting both double-counted
            // every image (#588). Count only the tag form.
            if parts[boundary] == "manifests" && ends_with_ci(key, ".json") {
                if let Some(reference) = parts.get(boundary + 1) {
                    let reference = reference.trim_end_matches(".json");
                    if !reference.starts_with("sha256:") {
                        entry.0 += 1;
                    }
                }
            }
        }
    }

    Some(BuiltIndex::with_objects(to_sorted_vec(repos), keys))
}

#[cfg(test)]
async fn build_maven_index(storage: &Storage) -> Option<Vec<RepoInfo>> {
    Some(build_maven_index_with_objects(storage).await?.repos)
}

async fn build_maven_index_with_objects(storage: &Storage) -> Option<BuiltIndex> {
    let keys = list_keys(storage, "maven/").await?;
    let mut repos: HashMap<String, (usize, u64, u64)> = HashMap::new();

    for (key, meta) in &keys {
        if let Some(rest) = key.strip_prefix("maven/") {
            let parts: Vec<_> = rest.split('/').collect();
            if parts.len() >= 2 {
                let path = parts[..parts.len() - 1].join("/");
                let entry = repos.entry(path).or_insert((0, 0, 0));
                // A Maven artifact ships with a swarm of sidecars — `.sha1`,
                // `.md5`, `.sha256`, `.sha512` and `maven-metadata.xml` — none
                // of which are separate artifacts. Count only primary files so
                // the dashboard doesn't report 5× the real artifact count
                // (#588). Sidecar bytes still count toward size (= on-disk du).
                let is_metadata = key.ends_with("maven-metadata.xml");
                if !crate::gc::is_checksum_sidecar(key) && !is_metadata {
                    entry.0 += 1;
                }

                entry.1 += meta.size;
                if meta.modified > entry.2 {
                    entry.2 = meta.modified;
                }
            }
        }
    }

    Some(BuiltIndex::with_objects(to_sorted_vec(repos), keys))
}

#[derive(Debug)]
struct NpmHostedReadFailure {
    error_class: &'static str,
    pointer_sha256: Option<String>,
    lkg_eligible: bool,
}

impl NpmHostedReadFailure {
    fn hard(error_class: &'static str) -> Self {
        Self {
            error_class,
            pointer_sha256: None,
            lkg_eligible: false,
        }
    }

    fn after_pointer(error_class: &'static str, pointer_sha256: &str, lkg_eligible: bool) -> Self {
        Self {
            error_class,
            pointer_sha256: Some(pointer_sha256.to_string()),
            lkg_eligible,
        }
    }
}

#[derive(Default)]
struct NpmAuthorityObservation {
    current_key: Option<String>,
    retired_key: Option<String>,
    transitional: bool,
}

fn valid_index_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn npm_search_projection(
    repository: &str,
    package: &str,
    packument: &serde_json::Value,
) -> Option<NpmSearchDocument> {
    let versions = packument.get("versions")?.as_object()?;
    let tagged_latest = packument
        .get("dist-tags")
        .and_then(serde_json::Value::as_object)
        .and_then(|tags| tags.get("latest"))
        .and_then(serde_json::Value::as_str)
        .filter(|version| versions.contains_key(*version))
        .map(str::to_string);
    let version = tagged_latest.or_else(|| {
        versions
            .keys()
            .filter_map(|version| {
                semver::Version::parse(version.trim_start_matches('v'))
                    .ok()
                    .map(|parsed| (parsed, version.clone()))
            })
            .max_by(|left, right| left.0.cmp(&right.0))
            .map(|(_, version)| version)
    })?;
    let manifest = versions.get(&version)?;
    let mut fields = serde_json::Map::new();
    for field in ["description", "keywords", "publisher", "maintainers"] {
        if let Some(value) = packument.get(field).or_else(|| manifest.get(field)) {
            fields.insert(field.to_string(), value.clone());
        }
    }
    Some(NpmSearchDocument {
        repository: repository.to_string(),
        package: package.to_string(),
        version,
        fields,
    })
}

fn npm_lkg_dependencies_match(previous: &NpmHostedLkg, by_key: &HashMap<&str, &FileMeta>) -> bool {
    previous.dependencies.iter().all(|(key, identity)| {
        by_key
            .get(key.as_str())
            .is_some_and(|meta| ListedIdentity::from(*meta) == *identity)
    })
}

async fn read_active_hosted_npm_package(
    storage: &Storage,
    by_key: &HashMap<&str, &FileMeta>,
    repository: &str,
    package: &str,
    current_key: &str,
) -> Result<NpmHostedLkg, NpmHostedReadFailure> {
    let current_meta = by_key
        .get(current_key)
        .copied()
        .ok_or_else(|| NpmHostedReadFailure::hard("current_missing_from_list"))?;
    let pointer_bytes = storage
        .get(current_key)
        .await
        .map_err(|_| NpmHostedReadFailure::hard("current_unavailable"))?;
    let pointer: crate::npm_layout::HostedPackumentPointer = serde_json::from_slice(&pointer_bytes)
        .map_err(|_| NpmHostedReadFailure::hard("current_invalid"))?;
    if !valid_index_sha256(&pointer.generation)
        || !valid_index_sha256(&pointer.full_sha256)
        || !valid_index_sha256(&pointer.install_v1_sha256)
    {
        return Err(NpmHostedReadFailure::hard("current_invalid"));
    }
    let pointer_sha256 = crate::npm_layout::hosted_manifest_digest(&pointer_bytes);
    let full_key =
        crate::npm_layout::hosted_packument_full_key(repository, package, &pointer.generation);
    let install_key = crate::npm_layout::hosted_packument_install_v1_key(
        repository,
        package,
        &pointer.generation,
    );
    let full_meta = by_key.get(full_key.as_str()).copied().ok_or_else(|| {
        NpmHostedReadFailure::after_pointer("full_missing_from_list", &pointer_sha256, true)
    })?;
    let install_meta = by_key.get(install_key.as_str()).copied().ok_or_else(|| {
        NpmHostedReadFailure::after_pointer("install_v1_missing_from_list", &pointer_sha256, true)
    })?;
    let full = storage.get(&full_key).await.map_err(|_| {
        NpmHostedReadFailure::after_pointer("full_unavailable", &pointer_sha256, true)
    })?;
    let packument = crate::registry::validate_hosted_packument_generation(&full, package, &pointer)
        .ok_or_else(|| {
            NpmHostedReadFailure::after_pointer("generation_invalid", &pointer_sha256, false)
        })?;
    let versions = packument
        .get("versions")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| {
            NpmHostedReadFailure::after_pointer("versions_invalid", &pointer_sha256, false)
        })?;
    let mut modified = current_meta
        .modified
        .max(full_meta.modified)
        .max(install_meta.modified);
    let mut dependencies = BTreeMap::from([
        (current_key.to_string(), ListedIdentity::from(current_meta)),
        (full_key.clone(), ListedIdentity::from(full_meta)),
        (install_key.clone(), ListedIdentity::from(install_meta)),
    ]);
    for (version, manifest) in versions {
        if manifest.get("version").and_then(serde_json::Value::as_str) != Some(version.as_str()) {
            return Err(NpmHostedReadFailure::after_pointer(
                "version_manifest_mismatch",
                &pointer_sha256,
                false,
            ));
        }
        let manifest = serde_json::to_vec(manifest).map_err(|_| {
            NpmHostedReadFailure::after_pointer("version_manifest_invalid", &pointer_sha256, false)
        })?;
        let blob_key =
            crate::npm_layout::hosted_blob_key_from_manifest(repository, package, &manifest)
                .ok_or_else(|| {
                    NpmHostedReadFailure::after_pointer(
                        "blob_reference_invalid",
                        &pointer_sha256,
                        false,
                    )
                })?;
        let blob_meta = by_key.get(blob_key.as_str()).copied().ok_or_else(|| {
            NpmHostedReadFailure::after_pointer("blob_missing_from_list", &pointer_sha256, true)
        })?;
        modified = modified.max(blob_meta.modified);
        dependencies
            .entry(blob_key)
            .or_insert_with(|| ListedIdentity::from(blob_meta));
    }
    Ok(NpmHostedLkg {
        pointer_sha256,
        dependencies,
        versions: versions.len(),
        modified,
        search: npm_search_projection(repository, package, &packument),
    })
}

async fn build_npm_index(
    storage: &Storage,
    previous: &HashMap<NpmPackageId, NpmHostedLkg>,
) -> Option<BuiltIndex> {
    let keys = list_keys(storage, "npm/").await?;
    let by_key: HashMap<&str, &crate::storage::FileMeta> = keys
        .iter()
        .map(|(key, meta)| (key.as_str(), meta))
        .collect();
    let mut packages: HashMap<String, (usize, u64)> = HashMap::new();
    let mut authority = BTreeMap::<NpmPackageId, NpmAuthorityObservation>::new();

    // Proxy cache has no hosted authority document, so its concrete tarballs
    // remain path-derived. Hosted visibility comes only from current.json and
    // its active immutable generation; split versions can outlive retirement
    // and are therefore never an index commit point.
    for (key, meta) in &keys {
        let Some(parsed) = crate::npm_layout::parse_npm_object_key(key) else {
            continue;
        };
        let crate::npm_layout::NpmObjectPath {
            repository,
            package,
            kind,
        } = parsed;
        let name = format!("repositories/{repository}/{package}");
        let package_id = NpmPackageId {
            repository,
            package,
        };
        match kind {
            crate::npm_layout::NpmObjectKind::HostedPackumentCurrent => {
                authority.entry(package_id).or_default().current_key = Some(key.clone());
            }
            crate::npm_layout::NpmObjectKind::HostedPackumentRetired => {
                authority.entry(package_id).or_default().retired_key = Some(key.clone());
            }
            crate::npm_layout::NpmObjectKind::ProxyTarball(_) => {
                let entry = packages.entry(name).or_insert((0, 0));
                entry.0 += 1;
                entry.1 = entry.1.max(meta.modified);
            }
            crate::npm_layout::NpmObjectKind::HostedPackage
            | crate::npm_layout::NpmObjectKind::HostedMaintenanceActive
            | crate::npm_layout::NpmObjectKind::HostedImportPending
            | crate::npm_layout::NpmObjectKind::HostedImportEvidence { .. }
            | crate::npm_layout::NpmObjectKind::HostedImportReceipt(_)
            | crate::npm_layout::NpmObjectKind::HostedPublishPending(_)
            | crate::npm_layout::NpmObjectKind::HostedPublishPendingIndex
            | crate::npm_layout::NpmObjectKind::HostedPublishComplete(_)
            | crate::npm_layout::NpmObjectKind::HostedDistTag(_)
            | crate::npm_layout::NpmObjectKind::HostedDeprecation(_) => {
                authority.entry(package_id).or_default().transitional = true;
            }
            _ => {}
        }
    }

    let mut degraded = false;
    let mut npm_hosted = HashMap::<NpmPackageId, NpmHostedLkg>::new();
    for (package_id, observed) in authority {
        let name = format!(
            "repositories/{}/{}",
            package_id.repository, package_id.package
        );
        if let Some(current_key) = observed.current_key {
            match read_active_hosted_npm_package(
                storage,
                &by_key,
                &package_id.repository,
                &package_id.package,
                &current_key,
            )
            .await
            {
                Ok(hosted) => {
                    let entry = packages.entry(name).or_insert((0, 0));
                    entry.0 += hosted.versions;
                    entry.1 = entry.1.max(hosted.modified);
                    npm_hosted.insert(package_id, hosted);
                }
                Err(failure) => {
                    degraded = true;
                    let retained = failure.lkg_eligible
                        && previous.get(&package_id).is_some_and(|previous| {
                            failure.pointer_sha256.as_deref()
                                == Some(previous.pointer_sha256.as_str())
                                && npm_lkg_dependencies_match(previous, &by_key)
                        });
                    tracing::warn!(
                        repository = package_id.repository,
                        package = package_id.package,
                        error_class = failure.error_class,
                        retained_lkg = retained,
                        "npm index: hosted authority unavailable"
                    );
                    if retained {
                        let previous = previous
                            .get(&package_id)
                            .expect("retained LKG was checked")
                            .clone();
                        let entry = packages.entry(name).or_insert((0, 0));
                        entry.0 += previous.versions;
                        entry.1 = entry.1.max(previous.modified);
                        npm_hosted.insert(package_id, previous);
                    }
                }
            }
        } else if let Some(retired_key) = observed.retired_key {
            match storage.get(&retired_key).await {
                Ok(bytes) if bytes.as_ref() == crate::npm_layout::HOSTED_PACKUMENT_RETIRED_V1 => {}
                Ok(_) => {
                    degraded = true;
                    tracing::warn!(
                        repository = package_id.repository,
                        package = package_id.package,
                        error_class = "retired_invalid",
                        "npm index: retired authority is invalid"
                    );
                }
                Err(_) => {
                    degraded = true;
                    tracing::warn!(
                        repository = package_id.repository,
                        package = package_id.package,
                        error_class = "retired_unavailable",
                        "npm index: retired authority is unavailable"
                    );
                }
            }
        } else if observed.transitional {
            degraded = true;
            tracing::warn!(
                repository = package_id.repository,
                package = package_id.package,
                error_class = "current_absent_with_live_state",
                "npm index: live or transitional hosted authority has no current generation"
            );
        }
    }

    let mut rows: Vec<RepoInfo> = packages
        .into_iter()
        .map(|(name, (versions, modified))| RepoInfo {
            name,
            versions,
            size: 0,
            size_available: false,
            updated: if modified > 0 {
                format_timestamp(modified)
            } else {
                "N/A".to_string()
            },
            is_file: false,
        })
        .collect();
    rows.sort_by(|left, right| left.name.cmp(&right.name));
    Some(BuiltIndex::with_objects_and_status(rows, keys, degraded).with_npm_hosted(npm_hosted))
}

async fn build_cargo_index(storage: &Storage) -> Option<BuiltIndex> {
    let keys = list_keys(storage, "cargo/").await?;
    let mut crates: HashMap<String, (usize, u64, u64)> = HashMap::new();

    for (key, meta) in &keys {
        if ends_with_ci(key, ".crate") {
            if let Some(rest) = key.strip_prefix("cargo/") {
                let parts: Vec<_> = rest.split('/').collect();
                if !parts.is_empty() {
                    let name = parts[0].to_string();
                    let entry = crates.entry(name).or_insert((0, 0, 0));
                    entry.0 += 1;

                    entry.1 += meta.size;
                    if meta.modified > entry.2 {
                        entry.2 = meta.modified;
                    }
                }
            }
        }
    }

    Some(BuiltIndex::with_objects(to_sorted_vec(crates), keys))
}

async fn build_pypi_index(storage: &Storage) -> Option<BuiltIndex> {
    let keys = list_keys(storage, "pypi/").await?;
    let mut packages: HashMap<String, (usize, u64, u64)> = HashMap::new();

    for (key, meta) in &keys {
        if let Some(rest) = key.strip_prefix("pypi/") {
            let parts: Vec<_> = rest.split('/').collect();
            if parts.len() >= 2 {
                let name = parts[0].to_string();
                let entry = packages.entry(name).or_insert((0, 0, 0));
                // Count only real distribution files — a checksum sidecar
                // (`<file>.sha256`) is not a separate artifact (#588). Its bytes
                // still count toward size so the total matches on-disk du.
                if !crate::gc::is_checksum_sidecar(key) {
                    entry.0 += 1;
                }

                entry.1 += meta.size;
                if meta.modified > entry.2 {
                    entry.2 = meta.modified;
                }
            }
        }
    }

    Some(BuiltIndex::with_objects(to_sorted_vec(packages), keys))
}

async fn build_go_index(storage: &Storage) -> Option<BuiltIndex> {
    let keys = list_keys(storage, "go/").await?;
    let mut modules: HashMap<String, (usize, u64, u64)> = HashMap::new();

    for (key, meta) in &keys {
        if let Some(rest) = key.strip_prefix("go/") {
            // Pattern: go/{module}/@v/{version}.zip
            // Count .zip files as versions (authoritative artifacts)
            if rest.contains("/@v/") && ends_with_ci(key, ".zip") {
                // Extract module path: everything before /@v/
                if let Some(pos) = rest.rfind("/@v/") {
                    let module = &rest[..pos];
                    let entry = modules.entry(module.to_string()).or_insert((0, 0, 0));
                    entry.0 += 1;

                    entry.1 += meta.size;
                    if meta.modified > entry.2 {
                        entry.2 = meta.modified;
                    }
                }
            }
        }
    }

    Some(BuiltIndex::with_objects(to_sorted_vec(modules), keys))
}

async fn build_raw_index(storage: &Storage) -> Option<BuiltIndex> {
    let keys = list_keys(storage, "raw/").await?;
    // (count, size, modified, is_file)
    let mut groups: HashMap<String, (usize, u64, u64, bool)> = HashMap::new();

    for (key, meta) in &keys {
        if let Some(rest) = key.strip_prefix("raw/") {
            let is_root_file = !rest.contains('/');
            let group = rest.split('/').next().unwrap_or(rest).to_string();
            let entry = groups.entry(group).or_insert((0, 0, 0, is_root_file));
            entry.0 += 1;
            entry.1 += meta.size;
            if meta.modified > entry.2 {
                entry.2 = meta.modified;
            }
        }
    }

    let mut result: Vec<_> = groups
        .into_iter()
        .map(|(name, (versions, size, modified, is_file))| RepoInfo {
            name,
            versions,
            size,
            size_available: true,
            updated: if modified > 0 {
                format_timestamp(modified)
            } else {
                "N/A".to_string()
            },
            is_file,
        })
        .collect();

    // Directories first (alphabetical), then files (alphabetical)
    result.sort_by(|a, b| a.is_file.cmp(&b.is_file).then_with(|| a.name.cmp(&b.name)));
    Some(BuiltIndex::with_objects(result, keys))
}

/// Generic index builder: groups files under `prefix` by first path segment.
/// Only counts files matching `suffix` (e.g. ".gem", ".nupkg", ".tar.gz").
async fn build_generic_index(storage: &Storage, prefix: &str, suffix: &str) -> Option<BuiltIndex> {
    let keys = list_keys(storage, prefix).await?;
    let mut packages: HashMap<String, (usize, u64, u64)> = HashMap::new();

    for (key, meta) in &keys {
        if !key.ends_with(suffix) {
            continue;
        }
        if let Some(rest) = key.strip_prefix(prefix) {
            let name = rest.split('/').next().unwrap_or(rest).to_string();
            if name.is_empty() {
                continue;
            }
            let entry = packages.entry(name).or_insert((0, 0, 0));
            entry.0 += 1;
            entry.1 += meta.size;
            if meta.modified > entry.2 {
                entry.2 = meta.modified;
            }
        }
    }

    Some(BuiltIndex::with_objects(to_sorted_vec(packages), keys))
}

/// Gems index: keys like gems/gems/{name}-{version}.gem
/// Uses split_gem_filename to extract package name from flat file layout.
async fn build_gems_index(storage: &Storage) -> Option<BuiltIndex> {
    let keys = list_keys(storage, "gems/gems/").await?;
    let mut packages: HashMap<String, (usize, u64, u64)> = HashMap::new();

    for (key, meta) in &keys {
        if !key.ends_with(".gem") {
            continue;
        }
        if let Some(rest) = key.strip_prefix("gems/gems/") {
            let stem = rest.strip_suffix(".gem").unwrap_or(rest);
            let name = match crate::registry::gems::split_gem_filename(stem) {
                Some((n, _)) => n,
                None => stem.to_string(),
            };
            if name.is_empty() {
                continue;
            }
            let entry = packages.entry(name).or_insert((0, 0, 0));
            entry.0 += 1;
            entry.1 += meta.size;
            if meta.modified > entry.2 {
                entry.2 = meta.modified;
            }
        }
    }

    Some(BuiltIndex::with_objects(to_sorted_vec(packages), keys))
}

/// Conan index: keys like conan/{name}/{ver}/{user}/{chan}/revisions/{rev}/files/{file}
async fn build_conan_index(storage: &Storage) -> Option<BuiltIndex> {
    let keys = list_keys(storage, "conan/").await?;
    let mut packages: HashMap<String, (usize, u64, u64)> = HashMap::new();

    for (key, meta) in &keys {
        if let Some(rest) = key.strip_prefix("conan/") {
            // First segment is the package name
            let name = rest.split('/').next().unwrap_or(rest).to_string();
            if name.is_empty() {
                continue;
            }
            let entry = packages.entry(name).or_insert((0, 0, 0));
            entry.0 += 1;
            entry.1 += meta.size;
            if meta.modified > entry.2 {
                entry.2 = meta.modified;
            }
        }
    }

    Some(BuiltIndex::with_objects(to_sorted_vec(packages), keys))
}

/// Convert HashMap to sorted Vec<RepoInfo>
fn to_sorted_vec(map: HashMap<String, (usize, u64, u64)>) -> Vec<RepoInfo> {
    let mut result: Vec<_> = map
        .into_iter()
        .map(|(name, (versions, size, modified))| RepoInfo {
            name,
            versions,
            size,
            size_available: true,
            updated: if modified > 0 {
                format_timestamp(modified)
            } else {
                "N/A".to_string()
            },
            is_file: false,
        })
        .collect();

    result.sort_by(|a, b| a.name.cmp(&b.name));
    result
}

/// Pagination helper
pub fn paginate<T: Clone>(data: &[T], page: usize, limit: usize) -> (Vec<T>, usize) {
    let total = data.len();
    let start = page.saturating_sub(1) * limit;

    if start >= total {
        return (Vec::new(), total);
    }

    let end = (start + limit).min(total);
    (data[start..end].to_vec(), total)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn put_hosted_npm_generation(
        storage: &Storage,
        repository: &str,
        package: &str,
        versions: &[(&str, &[u8])],
        dist_tags: &[(&str, &str)],
    ) -> crate::npm_layout::HostedPackumentPointer {
        use base64::Engine as _;
        use sha2::Digest as _;

        let mut manifests = serde_json::Map::new();
        for (version, blob) in versions {
            let manifest = serde_json::json!({
                "name": package,
                "version": version,
                "dist": {
                    "integrity": format!(
                        "sha512-{}",
                        base64::engine::general_purpose::STANDARD
                            .encode(sha2::Sha512::digest(blob))
                    )
                }
            });
            let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
            let blob_key = crate::npm_layout::hosted_blob_key_from_manifest(
                repository,
                package,
                &manifest_bytes,
            )
            .unwrap();
            storage.put(&blob_key, blob).await.unwrap();
            storage
                .put(
                    &format!("npm/repositories/{repository}/{package}/versions/{version}.json"),
                    &manifest_bytes,
                )
                .await
                .unwrap();
            manifests.insert((*version).to_string(), manifest);
        }
        let tags = dist_tags
            .iter()
            .map(|(tag, version)| {
                (
                    (*tag).to_string(),
                    serde_json::Value::String((*version).to_string()),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        let packument = serde_json::json!({
            "name": package,
            "versions": manifests,
            "dist-tags": tags,
        });
        let full = serde_json::to_vec(&packument).unwrap();
        let pointer = crate::registry::write_hosted_packument_generation_documents(
            storage, repository, package, &packument, &full,
        )
        .await
        .unwrap();
        crate::registry::commit_hosted_packument_pointer(storage, repository, package, &pointer)
            .await
            .unwrap();
        pointer
    }

    #[test]
    fn index_retry_backoff_is_capped_and_jitter_stays_in_window() {
        assert_eq!(
            (0..6).map(index_retry_ceiling_secs).collect::<Vec<_>>(),
            vec![30, 60, 120, 240, 300, 300]
        );
        for attempt in 0..6 {
            let ceiling = index_retry_ceiling_secs(attempt);
            let floor = ceiling / 2;
            for _ in 0..32 {
                let delay = index_retry_delay(attempt).as_secs();
                assert!((floor..=ceiling).contains(&delay));
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn degraded_npm_index_retries_once_due_and_recovers() {
        let (_dir, inner) = temp_storage();
        put_hosted_npm_generation(
            &inner,
            "npm-private",
            "pkg",
            &[("1.0.0", b"blob")],
            &[("latest", "1.0.0")],
        )
        .await;
        let current_key = crate::npm_layout::hosted_packument_current_key("npm-private", "pkg");
        let backend =
            crate::test_helpers::FaultInjectBackend::new(inner).fail_get_times(current_key, 1);
        let list_attempts = backend.list_attempts();
        let storage = Storage::from_backend(Arc::new(backend));
        let index = Arc::new(RepoIndex::new());
        let cancel = tokio_util::sync::CancellationToken::new();
        let handle = index
            .start_background(storage.clone(), [RegistryType::Npm], cancel.clone())
            .unwrap();

        for _ in 0..100 {
            if list_attempts.lock().len() == 1 && index.status("npm") == Some(IndexStatus::Degraded)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(list_attempts.lock().len(), 1);
        assert_eq!(index.status("npm"), Some(IndexStatus::Degraded));

        // Request-path notifications must neither cause a scan nor move the
        // already-scheduled deadline.
        for _ in 0..8 {
            let _ = index.get("npm", &storage).await;
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_secs(14)).await;
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert_eq!(list_attempts.lock().len(), 1);

        // Equal jitter is bounded by the 30-second first-attempt ceiling.
        tokio::time::advance(Duration::from_secs(16)).await;
        for _ in 0..100 {
            if index.status("npm") == Some(IndexStatus::Ready) {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(list_attempts.lock().len(), 2);
        assert_eq!(index.status("npm"), Some(IndexStatus::Ready));
        let rows = index.get("npm", &storage).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].versions, 1);

        cancel.cancel();
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn failed_full_index_rebuild_does_not_loop_every_second() {
        let (_dir, inner) = temp_storage();
        let backend = crate::test_helpers::FaultInjectBackend::new(inner).fail_list("npm/");
        let list_attempts = backend.list_attempts();
        let storage = Storage::from_backend(Arc::new(backend));
        let index = Arc::new(RepoIndex::new());
        let cancel = tokio_util::sync::CancellationToken::new();
        let handle = index
            .start_background(storage.clone(), [RegistryType::Npm], cancel.clone())
            .unwrap();

        for _ in 0..100 {
            if list_attempts.lock().len() == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(list_attempts.lock().len(), 1);
        assert_eq!(index.status("npm"), Some(IndexStatus::Degraded));

        for _ in 0..8 {
            let _ = index.get("npm", &storage).await;
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_secs(14)).await;
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            list_attempts.lock().len(),
            1,
            "Notify and the old one-second dirty loop must not bypass backoff"
        );

        tokio::time::advance(Duration::from_secs(16)).await;
        for _ in 0..100 {
            if list_attempts.lock().len() == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(list_attempts.lock().len(), 2);

        cancel.cancel();
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn real_invalidation_bypasses_failed_generation_retry_deadline() {
        let (_dir, inner) = temp_storage();
        let backend = crate::test_helpers::FaultInjectBackend::new(inner).fail_list("npm/");
        let list_attempts = backend.list_attempts();
        let storage = Storage::from_backend(Arc::new(backend));
        let index = Arc::new(RepoIndex::new());
        let cancel = tokio_util::sync::CancellationToken::new();
        let handle = index
            .start_background(storage, [RegistryType::Npm], cancel.clone())
            .unwrap();

        for _ in 0..100 {
            if list_attempts.lock().len() == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(list_attempts.lock().len(), 1);

        index.invalidate("npm");
        for _ in 0..100 {
            if list_attempts.lock().len() == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            list_attempts.lock().len(),
            2,
            "a newer real generation must not wait behind old failure backoff"
        );

        cancel.cancel();
        handle.await.unwrap();
    }

    #[test]
    fn try_accept_reindex_first_call_accepted() {
        let idx = RepoIndex::new();
        assert!(idx.try_accept_reindex(1000, 10).is_ok());
    }

    #[test]
    fn try_accept_reindex_debounces_within_window() {
        let idx = RepoIndex::new();
        assert!(idx.try_accept_reindex(1000, 10).is_ok());
        // 3s later, still inside the 10s window -> rejected with remaining secs.
        assert_eq!(idx.try_accept_reindex(1003, 10), Err(7));
    }

    #[test]
    fn try_accept_reindex_allows_after_window() {
        let idx = RepoIndex::new();
        assert!(idx.try_accept_reindex(1000, 10).is_ok());
        assert!(idx.try_accept_reindex(1010, 10).is_ok());
    }

    #[test]
    fn rejected_reindex_does_not_advance_window() {
        let idx = RepoIndex::new();
        assert!(idx.try_accept_reindex(1000, 10).is_ok());
        // A rejected call must NOT record its timestamp, otherwise a tight loop
        // would keep sliding the window forward and never accept.
        assert!(idx.try_accept_reindex(1005, 10).is_err());
        assert!(idx.try_accept_reindex(1010, 10).is_ok());
    }

    #[test]
    fn invalidate_all_marks_every_index_dirty() {
        let idx = RepoIndex::new();
        // Clear dirty on every index (they start dirty).
        for ri in idx.indexes.values() {
            ri.set(BuiltIndex::repos(Vec::new()), 1);
            assert!(!ri.is_dirty());
        }
        idx.invalidate_all();
        for ri in idx.indexes.values() {
            assert!(ri.is_dirty());
        }
    }

    #[test]
    fn test_paginate_first_page() {
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let (page, total) = paginate(&data, 1, 3);
        assert_eq!(page, vec![1, 2, 3]);
        assert_eq!(total, 10);
    }

    #[test]
    fn test_paginate_second_page() {
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let (page, total) = paginate(&data, 2, 3);
        assert_eq!(page, vec![4, 5, 6]);
        assert_eq!(total, 10);
    }

    #[test]
    fn test_paginate_last_page_partial() {
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let (page, total) = paginate(&data, 4, 3);
        assert_eq!(page, vec![10]);
        assert_eq!(total, 10);
    }

    #[test]
    fn test_paginate_beyond_range() {
        let data = vec![1, 2, 3];
        let (page, total) = paginate(&data, 5, 3);
        assert!(page.is_empty());
        assert_eq!(total, 3);
    }

    #[test]
    fn test_paginate_empty_data() {
        let data: Vec<i32> = vec![];
        let (page, total) = paginate(&data, 1, 10);
        assert!(page.is_empty());
        assert_eq!(total, 0);
    }

    #[test]
    fn test_paginate_page_zero() {
        // page 0 with saturating_sub becomes 0, so start = 0
        let data = vec![1, 2, 3];
        let (page, _) = paginate(&data, 0, 2);
        assert_eq!(page, vec![1, 2]);
    }

    #[test]
    fn test_paginate_large_limit() {
        let data = vec![1, 2, 3];
        let (page, total) = paginate(&data, 1, 100);
        assert_eq!(page, vec![1, 2, 3]);
        assert_eq!(total, 3);
    }

    #[test]
    fn test_registry_index_new() {
        let idx = RegistryIndex::new();
        assert_eq!(idx.count(), 0);
        assert!(idx.is_dirty());
    }

    #[test]
    fn test_registry_index_invalidate() {
        let idx = RegistryIndex::new();
        // Initially dirty
        assert!(idx.is_dirty());

        // Set data clears dirty
        idx.set(
            BuiltIndex::repos(vec![RepoInfo {
                name: "test".to_string(),
                versions: 1,
                size: 100,
                updated: "2026-01-01".to_string(),
                ..Default::default()
            }]),
            1,
        );
        assert!(!idx.is_dirty());
        assert_eq!(idx.count(), 1);

        // Invalidate makes it dirty again
        idx.invalidate();
        assert!(idx.is_dirty());
    }

    #[test]
    fn stale_generation_publish_cannot_clear_newer_invalidation() {
        let idx = RegistryIndex::new();
        let rebuild_generation = idx.requested_generation.load(Ordering::Acquire);

        // Simulate a publish arriving while LIST is in flight.
        idx.invalidate();
        idx.set(BuiltIndex::repos(Vec::new()), rebuild_generation);

        assert!(
            idx.is_dirty(),
            "the racing invalidation must remain pending"
        );
        assert_eq!(idx.status(), IndexStatus::Warming);

        let current_generation = idx.requested_generation.load(Ordering::Acquire);
        idx.set(BuiltIndex::repos(Vec::new()), current_generation);
        assert!(!idx.is_dirty());
        assert_eq!(idx.status(), IndexStatus::Ready);
    }

    #[tokio::test]
    async fn background_worker_wakes_for_registry_activated_after_start() {
        let (_dir, storage) = temp_storage();
        let index = Arc::new(RepoIndex::new());
        let cancel = tokio_util::sync::CancellationToken::new();
        let worker = index
            .start_background(storage.clone(), [], cancel.clone())
            .expect("first worker starts");

        // `get` is request-path-only: it activates Maven but does not scan.
        assert!(index.get("maven", &storage).await.is_empty());
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            index.get_strict("maven", &storage),
        )
        .await
        .expect("background worker did not lose activation wakeup")
        .expect("empty Maven index rebuild succeeds");
        assert_eq!(index.status("maven"), Some(IndexStatus::Ready));

        cancel.cancel();
        worker.await.unwrap();
    }

    #[test]
    fn start_background_without_runtime_is_a_retryable_noop() {
        let (_dir, storage) = temp_storage();
        let index = Arc::new(RepoIndex::new());
        assert!(
            index
                .start_background(
                    storage,
                    [RegistryType::Maven],
                    tokio_util::sync::CancellationToken::new(),
                )
                .is_none(),
            "a synchronous caller must not panic or consume the one-start guard"
        );
        assert!(!index.background_started.load(Ordering::Acquire));
    }

    #[test]
    fn test_registry_index_get_cached() {
        let idx = RegistryIndex::new();
        idx.set(
            BuiltIndex::repos(vec![
                RepoInfo {
                    name: "a".to_string(),
                    versions: 2,
                    size: 200,
                    updated: "today".to_string(),
                    ..Default::default()
                },
                RepoInfo {
                    name: "b".to_string(),
                    versions: 1,
                    size: 100,
                    updated: "yesterday".to_string(),
                    ..Default::default()
                },
            ]),
            1,
        );

        let cached = idx.get_cached();
        assert_eq!(cached.len(), 2);
        assert_eq!(cached[0].name, "a");
    }

    #[test]
    fn unavailable_size_is_not_exported_as_zero() {
        let idx = RegistryIndex::new();
        idx.set(
            BuiltIndex::repos(vec![RepoInfo {
                name: "npm-package".to_string(),
                versions: 1,
                size: 0,
                size_available: false,
                ..Default::default()
            }]),
            1,
        );
        assert_eq!(idx.total_size(), None);
    }

    #[test]
    fn npm_size_series_is_omitted_even_for_an_empty_ready_index() {
        let index = RepoIndex::new();
        let npm = index.indexes.get(&RegistryType::Npm).unwrap();
        npm.set(BuiltIndex::repos(Vec::new()), 1);
        assert!(!index.sizes().contains_key(&RegistryType::Npm));
    }

    #[tokio::test]
    async fn strict_reader_receives_published_degraded_snapshot() {
        let index = RepoIndex::new();
        let npm = index.indexes.get(&RegistryType::Npm).unwrap();
        npm.set(
            BuiltIndex::with_objects_and_status(
                vec![RepoInfo {
                    name: "repositories/npm-private/pkg".to_string(),
                    versions: 1,
                    size: 0,
                    size_available: false,
                    updated: "N/A".to_string(),
                    is_file: false,
                }],
                Vec::new(),
                true,
            ),
            1,
        );

        let (_dir, storage) = temp_storage();
        let snapshot = index.get_strict("npm", &storage).await.unwrap();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(index.status("npm"), Some(IndexStatus::Degraded));
    }

    #[test]
    fn test_registry_index_default() {
        let idx = RegistryIndex::default();
        assert_eq!(idx.count(), 0);
    }

    #[test]
    fn test_repo_index_new() {
        let idx = RepoIndex::new();
        let counts = idx.counts();
        for rt in RegistryType::all() {
            assert_eq!(
                counts.get(rt).copied().unwrap_or(0),
                0,
                "non-zero for {}",
                rt
            );
        }
    }

    #[test]
    fn test_repo_index_invalidate() {
        let idx = RepoIndex::new();
        // Should not panic for any registry (all 13 + unknown)
        for rt in RegistryType::all() {
            idx.invalidate(rt.as_str());
        }
        idx.invalidate("unknown"); // should be a no-op
    }

    #[test]
    fn test_repo_index_default() {
        let idx = RepoIndex::default();
        let counts = idx.counts();
        for rt in RegistryType::all() {
            assert_eq!(
                counts.get(rt).copied().unwrap_or(0),
                0,
                "non-zero for {}",
                rt
            );
        }
    }

    #[test]
    fn test_to_sorted_vec() {
        let mut map = std::collections::HashMap::new();
        map.insert("zebra".to_string(), (3usize, 100u64, 0u64));
        map.insert("alpha".to_string(), (1, 50, 1700000000));

        let result = to_sorted_vec(map);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].name, "alpha");
        assert_eq!(result[0].versions, 1);
        assert_eq!(result[0].size, 50);
        assert_ne!(result[0].updated, "N/A");
        assert_eq!(result[1].name, "zebra");
        assert_eq!(result[1].versions, 3);
        assert_eq!(result[1].updated, "N/A"); // modified = 0
    }

    // ── #588: dashboard stats must reflect real on-disk data ──────────────
    // count = primary artifacts only (no checksum sidecars / metadata / digest
    // manifests); size = actual on-disk bytes (du), never a manifest "virtual"
    // size. Seed a Storage and exercise the real build_*_index path (PM-4).

    fn temp_storage() -> (tempfile::TempDir, crate::Storage) {
        let dir = tempfile::TempDir::new().unwrap();
        let s = crate::Storage::new_local(dir.path().to_str().unwrap());
        (dir, s)
    }

    #[tokio::test]
    async fn pypi_index_excludes_checksum_sidecars_from_count() {
        let (_d, s) = temp_storage();
        s.put("pypi/six/six-1.16.0-py3-none-any.whl", &[0u8; 100])
            .await
            .unwrap();
        s.put("pypi/six/six-1.16.0-py3-none-any.whl.sha256", b"deadbeef")
            .await
            .unwrap();

        let repos = build_pypi_index(&s).await.expect("index built").repos;
        assert_eq!(repos.len(), 1);
        // ONE artifact, not two — the .sha256 sidecar is not an artifact (#588).
        assert_eq!(repos[0].versions, 1, "checksum sidecar must not be counted");
        // ...but its bytes still count toward size, so size == on-disk du.
        assert_eq!(repos[0].size, 100 + 8);
    }

    #[tokio::test]
    async fn maven_index_counts_only_primary_artifacts() {
        let (_d, s) = temp_storage();
        let base = "maven/com/example/app/1.0";
        s.put(&format!("{base}/app-1.0.jar"), &[0u8; 200])
            .await
            .unwrap();
        s.put(&format!("{base}/app-1.0.pom"), &[0u8; 50])
            .await
            .unwrap();
        for ext in ["jar.sha1", "jar.md5", "jar.sha256", "jar.sha512"] {
            s.put(&format!("{base}/app-1.0.{ext}"), b"x").await.unwrap();
        }
        s.put(&format!("{base}/maven-metadata.xml"), &[0u8; 30])
            .await
            .unwrap();

        let repos = build_maven_index(&s).await.expect("index built");
        let total: usize = repos.iter().map(|r| r.versions).sum();
        // jar + pom = 2 primary; the 4 checksums + metadata.xml are NOT counted
        // (old code reported 7) (#588).
        assert_eq!(total, 2, "only primary artifacts counted, got {total}");
        // size still sums every file on disk (du).
        let size: u64 = repos.iter().map(|r| r.size).sum();
        assert_eq!(size, 200 + 50 + 4 + 30);
    }

    #[tokio::test]
    async fn maven_stats_count_excludes_metadata_only_dir() {
        // Real layout (what a `mvn deploy` / proxy produces): the
        // artifact-level `maven-metadata.xml` lands in the PARENT dir of the
        // version dirs, so the on-disk tree has two directories for one jar:
        //   maven/com/example/a/            <- metadata only (zero artifacts)
        //   maven/com/example/a/1.0/        <- the jar (one artifact)
        // The metadata-only dir is not a repository. `/api/ui/stats` and
        // `nora_artifacts_total` (both read RegistryIndex::count via counts())
        // must report maven:1 for the single pushed jar, NOT 2.
        let (_d, s) = temp_storage();
        s.put("maven/com/example/a/1.0/a-1.0.jar", &[0u8; 200])
            .await
            .unwrap();
        s.put("maven/com/example/a/maven-metadata.xml", &[0u8; 30])
            .await
            .unwrap();

        // Exercise the exact prod path /api/ui/stats walks: RepoIndex::get(...)
        // reads the background-built cache, then counts() reads it.
        let idx = RepoIndex::new();
        assert!(idx.rebuild_for_test(RegistryType::Maven, &s).await);
        let repos = idx.get("maven", &s).await;
        // Two on-disk dir buckets, but only one carries an artifact.
        assert_eq!(repos.len(), 2, "both dirs are indexed buckets");
        assert_eq!(
            idx.counts().get(&RegistryType::Maven).copied().unwrap_or(0),
            1,
            "count must exclude the versions:0 metadata-only dir"
        );
        // Size still sums every on-disk file (du), metadata bytes included.
        let size: u64 = repos.iter().map(|r| r.size).sum();
        assert_eq!(size, 200 + 30, "metadata bytes still count toward size==du");
    }

    #[tokio::test]
    async fn maven_index_keeps_named_repository_paths_isolated() {
        let (_d, storage) = temp_storage();
        storage
            .put(
                "maven/repositories/releases/com/example/a/1.0/a-1.0.jar",
                b"release",
            )
            .await
            .unwrap();
        storage
            .put(
                "maven/repositories/open/com/example/a/1.0/a-1.0.jar",
                b"open",
            )
            .await
            .unwrap();

        let repos = build_maven_index(&storage).await.expect("index built");

        assert!(repos
            .iter()
            .any(|entry| entry.name == "repositories/releases/com/example/a/1.0"));
        assert!(repos
            .iter()
            .any(|entry| entry.name == "repositories/open/com/example/a/1.0"));
        assert_eq!(repos.iter().map(|entry| entry.versions).sum::<usize>(), 2);
    }

    #[tokio::test]
    async fn npm_index_keeps_hosted_and_proxy_repositories_isolated() {
        use sha2::Digest as _;

        let (_d, storage) = temp_storage();
        put_hosted_npm_generation(
            &storage,
            "npm-private",
            "@scope/pkg",
            &[("1.0.0", b"hosted")],
            &[("latest", "1.0.0")],
        )
        .await;
        storage
            .put(
                "npm/repositories/npm-registry/proxy/tarballs/@scope/pkg/pkg-1.0.0.tgz",
                b"proxy",
            )
            .await
            .unwrap();
        put_hosted_npm_generation(
            &storage,
            "npm-private",
            "proxy",
            &[("1.0.0", b"hosted-package-named-proxy")],
            &[("latest", "1.0.0")],
        )
        .await;
        let orphan_digest = hex::encode(sha2::Sha512::digest(b"precommit-orphan"));
        storage
            .put(
                &format!("npm/repositories/npm-private/orphan/blobs/sha512/{orphan_digest}.tgz"),
                b"precommit-orphan",
            )
            .await
            .unwrap();

        let backend = crate::test_helpers::FaultInjectBackend::new(storage.clone());
        let get_attempts = backend.get_attempts();
        let list_attempts = backend.list_attempts();
        let counted = Storage::from_backend(Arc::new(backend));
        let built = build_npm_index(&counted, &HashMap::new())
            .await
            .expect("index built");
        let repos = built.repos;

        assert!(!built.degraded);
        assert_eq!(repos.len(), 3);
        assert!(repos
            .iter()
            .any(|entry| entry.name == "repositories/npm-private/@scope/pkg"));
        assert!(repos
            .iter()
            .any(|entry| entry.name == "repositories/npm-private/proxy"));
        assert!(repos
            .iter()
            .any(|entry| entry.name == "repositories/npm-registry/@scope/pkg"));
        assert_eq!(repos.iter().map(|entry| entry.versions).sum::<usize>(), 3);
        assert!(repos
            .iter()
            .all(|entry| entry.size == 0 && !entry.size_available));
        assert!(
            !repos
                .iter()
                .any(|entry| entry.name == "repositories/npm-private/orphan"),
            "hosted pre-commit tarballs must not enter the index"
        );
        assert_eq!(list_attempts.lock().as_slice(), &["npm/".to_string()]);
        let gets = get_attempts.lock();
        assert_eq!(gets.len(), 4, "pointer + active full per hosted package");
        assert!(gets.iter().all(|key| !key.contains("/versions/")));
    }

    #[tokio::test]
    async fn npm_index_uses_current_generation_not_split_tombstones() {
        let (_d, storage) = temp_storage();
        put_hosted_npm_generation(
            &storage,
            "npm-private",
            "@scope/pkg",
            &[("1.0.0", b"shared"), ("2.0.0", b"shared")],
            &[("latest", "2.0.0")],
        )
        .await;
        let initial = build_npm_index(&storage, &HashMap::new()).await.unwrap();
        assert!(!initial.degraded);
        assert_eq!(initial.repos[0].versions, 2);
        assert_eq!(
            initial
                .npm_hosted
                .values()
                .next()
                .unwrap()
                .dependencies
                .len(),
            4,
            "current/full/install plus one deduplicated shared blob"
        );

        // Publish a generation that removes 1.0.0. The old split manifest is
        // deliberately retained, and the active split is removed: neither
        // path can override the current full generation.
        put_hosted_npm_generation(
            &storage,
            "npm-private",
            "@scope/pkg",
            &[("2.0.0", b"shared")],
            &[("latest", "2.0.0")],
        )
        .await;
        storage
            .delete("npm/repositories/npm-private/@scope/pkg/versions/2.0.0.json")
            .await
            .unwrap();
        storage
            .put(
                &crate::npm_layout::hosted_packument_retired_key("npm-private", "@scope/pkg"),
                b"stale-invalid-retired",
            )
            .await
            .unwrap();
        let current = build_npm_index(&storage, &initial.npm_hosted)
            .await
            .unwrap();
        assert!(!current.degraded);
        assert_eq!(current.repos.len(), 1);
        assert_eq!(current.repos[0].versions, 1);

        // A retired package has no current authority and must disappear even
        // while all old generation and split objects remain in storage.
        storage
            .delete(&crate::npm_layout::hosted_packument_current_key(
                "npm-private",
                "@scope/pkg",
            ))
            .await
            .unwrap();
        storage
            .put(
                &crate::npm_layout::hosted_packument_retired_key("npm-private", "@scope/pkg"),
                crate::npm_layout::HOSTED_PACKUMENT_RETIRED_V1,
            )
            .await
            .unwrap();
        let retired = build_npm_index(&storage, &current.npm_hosted)
            .await
            .unwrap();
        assert!(!retired.degraded);
        assert!(retired.repos.is_empty());
    }

    #[tokio::test]
    async fn npm_index_invalid_or_unreadable_retired_authority_is_degraded() {
        let (_d, storage) = temp_storage();
        let retired_key = crate::npm_layout::hosted_packument_retired_key("npm-private", "pkg");
        storage.put(&retired_key, b"wrong").await.unwrap();

        let invalid = build_npm_index(&storage, &HashMap::new()).await.unwrap();
        assert!(invalid.degraded);
        assert!(invalid.repos.is_empty());

        let unavailable_storage = Storage::from_backend(Arc::new(
            crate::test_helpers::FaultInjectBackend::new(storage).fail_get(retired_key),
        ));
        let unavailable = build_npm_index(&unavailable_storage, &HashMap::new())
            .await
            .unwrap();
        assert!(unavailable.degraded);
        assert!(unavailable.repos.is_empty());
    }

    #[tokio::test]
    async fn npm_index_degrades_per_package_and_revalidates_lkg_authority() {
        let (_d, storage) = temp_storage();
        put_hosted_npm_generation(
            &storage,
            "npm-private",
            "bad",
            &[("1.0.0", b"bad-v1")],
            &[("latest", "1.0.0")],
        )
        .await;
        put_hosted_npm_generation(
            &storage,
            "npm-private",
            "good",
            &[("1.0.0", b"good-v1")],
            &[("latest", "1.0.0")],
        )
        .await;
        let initial = build_npm_index(&storage, &HashMap::new()).await.unwrap();
        let bad_pointer =
            crate::registry::read_hosted_packument_pointer(&storage, "npm-private", "bad")
                .await
                .unwrap()
                .unwrap();
        let bad_full_key = crate::npm_layout::hosted_packument_full_key(
            "npm-private",
            "bad",
            &bad_pointer.generation,
        );

        put_hosted_npm_generation(
            &storage,
            "npm-private",
            "good",
            &[("1.0.0", b"good-v1"), ("2.0.0", b"good-v2")],
            &[("latest", "2.0.0")],
        )
        .await;

        let without_lkg_storage = Storage::from_backend(Arc::new(
            crate::test_helpers::FaultInjectBackend::new(storage.clone()).fail_get(&bad_full_key),
        ));
        let without_lkg = build_npm_index(&without_lkg_storage, &HashMap::new())
            .await
            .unwrap();
        assert!(without_lkg.degraded);
        assert!(without_lkg
            .repos
            .iter()
            .all(|row| row.name != "repositories/npm-private/bad"));
        assert_eq!(
            without_lkg
                .repos
                .iter()
                .find(|row| row.name == "repositories/npm-private/good")
                .unwrap()
                .versions,
            2
        );

        let with_lkg_storage = Storage::from_backend(Arc::new(
            crate::test_helpers::FaultInjectBackend::new(storage.clone()).fail_get(&bad_full_key),
        ));
        let with_lkg = build_npm_index(&with_lkg_storage, &initial.npm_hosted)
            .await
            .unwrap();
        assert!(with_lkg.degraded);
        assert_eq!(
            with_lkg
                .repos
                .iter()
                .find(|row| row.name == "repositories/npm-private/bad")
                .unwrap()
                .versions,
            1
        );
        assert_eq!(
            with_lkg
                .repos
                .iter()
                .find(|row| row.name == "repositories/npm-private/good")
                .unwrap()
                .versions,
            2
        );
        // A new current pointer cannot borrow the prior generation's LKG.
        let changed_pointer = put_hosted_npm_generation(
            &storage,
            "npm-private",
            "bad",
            &[("2.0.0", b"bad-v2")],
            &[("latest", "2.0.0")],
        )
        .await;
        let changed_full_key = crate::npm_layout::hosted_packument_full_key(
            "npm-private",
            "bad",
            &changed_pointer.generation,
        );
        let changed_storage = Storage::from_backend(Arc::new(
            crate::test_helpers::FaultInjectBackend::new(storage.clone())
                .fail_get(changed_full_key),
        ));
        let changed = build_npm_index(&changed_storage, &initial.npm_hosted)
            .await
            .unwrap();
        assert!(changed.degraded);
        assert!(changed
            .repos
            .iter()
            .all(|row| row.name != "repositories/npm-private/bad"));

        // Restore the old pointer, then change a dependency identity. Even an
        // identical pointer fingerprint is insufficient for LKG reuse.
        crate::registry::commit_hosted_packument_pointer(
            &storage,
            "npm-private",
            "bad",
            &bad_pointer,
        )
        .await
        .unwrap();
        storage
            .delete(&crate::npm_layout::hosted_packument_install_v1_key(
                "npm-private",
                "bad",
                &bad_pointer.generation,
            ))
            .await
            .unwrap();
        let dependency_changed = build_npm_index(&storage, &initial.npm_hosted)
            .await
            .unwrap();
        assert!(dependency_changed.degraded);
        assert!(dependency_changed
            .repos
            .iter()
            .all(|row| row.name != "repositories/npm-private/bad"));

        // A package root without current/retired is transitional, not a clean
        // absence. It becomes cleanly absent only after an exact retired marker.
        storage
            .delete(&crate::npm_layout::hosted_packument_current_key(
                "npm-private",
                "bad",
            ))
            .await
            .unwrap();
        storage
            .put(
                &crate::npm_layout::hosted_package_key("npm-private", "bad"),
                br#"{"name":"bad"}"#,
            )
            .await
            .unwrap();
        let transitional = build_npm_index(&storage, &initial.npm_hosted)
            .await
            .unwrap();
        assert!(transitional.degraded);
        assert!(transitional
            .repos
            .iter()
            .all(|row| row.name != "repositories/npm-private/bad"));
        storage
            .put(
                &crate::npm_layout::hosted_packument_retired_key("npm-private", "bad"),
                crate::npm_layout::HOSTED_PACKUMENT_RETIRED_V1,
            )
            .await
            .unwrap();
        let retired = build_npm_index(&storage, &initial.npm_hosted)
            .await
            .unwrap();
        assert!(!retired.degraded);
        assert!(retired
            .repos
            .iter()
            .all(|row| row.name != "repositories/npm-private/bad"));
    }

    #[tokio::test]
    async fn npm_index_rejects_missing_blobs_and_invalid_dist_tags() {
        use sha2::Digest as _;

        let (_d, storage) = temp_storage();
        put_hosted_npm_generation(
            &storage,
            "npm-private",
            "missing-blob",
            &[("1.0.0", b"missing")],
            &[("latest", "1.0.0")],
        )
        .await;
        put_hosted_npm_generation(
            &storage,
            "npm-private",
            "invalid-tag",
            &[("1.0.0", b"present")],
            &[("latest", "9.9.9")],
        )
        .await;
        let digest = hex::encode(sha2::Sha512::digest(b"missing"));
        storage
            .delete(&crate::npm_layout::hosted_blob_key_for_digest(
                "npm-private",
                "missing-blob",
                &digest,
            ))
            .await
            .unwrap();

        let built = build_npm_index(&storage, &HashMap::new()).await.unwrap();
        assert!(built.degraded);
        assert!(built.repos.is_empty());
    }

    #[tokio::test]
    async fn docker_index_real_size_not_virtual_and_single_count() {
        let (_d, s) = temp_storage();
        // Manifest DECLARES huge layer sizes (virtual) but the actual blob
        // files on disk are tiny — the index must report the on-disk size.
        let manifest = serde_json::json!({
            "schemaVersion": 2,
            "config": { "size": 1_000_000, "digest": "sha256:cfg" },
            "layers": [ { "size": 9_000_000, "digest": "sha256:lyr" } ]
        })
        .to_string();
        // Same image pushed by tag AND its content-addressed digest manifest.
        s.put(
            "docker/library/app/manifests/latest.json",
            manifest.as_bytes(),
        )
        .await
        .unwrap();
        s.put(
            "docker/library/app/manifests/sha256:abc123.json",
            manifest.as_bytes(),
        )
        .await
        .unwrap();
        // Real blobs on disk (tiny).
        s.put("docker/library/app/blobs/sha256:cfg", &[0u8; 120])
            .await
            .unwrap();
        s.put("docker/library/app/blobs/sha256:lyr", &[0u8; 340])
            .await
            .unwrap();

        let repos = build_docker_index(&s).await.expect("index built").repos;
        assert_eq!(repos.len(), 1);
        // Count = 1 tag, NOT 2 (the digest manifest is not a separate image).
        assert_eq!(repos[0].versions, 1, "tag + digest manifest double-counted");
        // Size = actual on-disk bytes (2 manifests + 2 blobs), NOT the
        // declared 10_000_000 virtual size.
        let on_disk = (manifest.len() as u64) * 2 + 120 + 340;
        assert_eq!(
            repos[0].size, on_disk,
            "size must be on-disk du, not virtual"
        );
        assert!(repos[0].size < 10_000_000, "must not report virtual size");
    }

    /// #738 A/B: an index rebuild must read size/mtime from the listing
    /// (`list_with_meta`) and NEVER issue a per-key `stat()` — which on S3 is a
    /// HEAD per key (N+1). A backend that counts `stat()` calls and serves the
    /// metadata via `list_with_meta` proves the rebuild now does ZERO stats
    /// while still computing the correct on-disk size (the old path did one HEAD per key).
    #[tokio::test]
    async fn docker_index_uses_list_with_meta_not_per_key_stat() {
        use crate::storage::{FileMeta, StorageBackend};
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        struct CountingBackend {
            entries: Vec<(String, FileMeta)>,
            stat_calls: Arc<AtomicUsize>,
        }

        #[async_trait::async_trait]
        impl StorageBackend for CountingBackend {
            async fn stat(&self, _key: &str) -> crate::storage::Result<Option<FileMeta>> {
                self.stat_calls.fetch_add(1, Ordering::SeqCst);
                Ok(None)
            }
            async fn list(&self, prefix: &str) -> crate::storage::Result<Vec<String>> {
                Ok(self
                    .entries
                    .iter()
                    .filter(|(k, _)| k.starts_with(prefix))
                    .map(|(k, _)| k.clone())
                    .collect())
            }
            async fn list_with_meta(
                &self,
                prefix: &str,
            ) -> crate::storage::Result<Vec<(String, FileMeta)>> {
                Ok(self
                    .entries
                    .iter()
                    .filter(|(k, _)| k.starts_with(prefix))
                    .cloned()
                    .collect())
            }
            async fn put(&self, _k: &str, _d: &[u8]) -> crate::storage::Result<()> {
                Ok(())
            }
            async fn get(&self, _k: &str) -> crate::storage::Result<axum::body::Bytes> {
                Err(crate::storage::StorageError::NotFound)
            }
            async fn delete(&self, _k: &str) -> crate::storage::Result<()> {
                Ok(())
            }
            async fn health_check(&self) -> bool {
                true
            }
            fn backend_name(&self) -> &'static str {
                "counting-test"
            }
            async fn put_from_path(
                &self,
                _k: &str,
                _s: &std::path::Path,
            ) -> crate::storage::Result<()> {
                Ok(())
            }
            async fn get_reader(
                &self,
                _k: &str,
            ) -> crate::storage::Result<(
                u64,
                std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send + Unpin>>,
            )> {
                Err(crate::storage::StorageError::NotFound)
            }
        }

        let stat_calls = Arc::new(AtomicUsize::new(0));
        let entries = vec![
            (
                "docker/library/app/manifests/latest.json".to_string(),
                FileMeta {
                    size: 100,
                    modified: 5,
                },
            ),
            (
                "docker/library/app/blobs/sha256:lyr".to_string(),
                FileMeta {
                    size: 340,
                    modified: 9,
                },
            ),
        ];
        let storage = Storage::from_backend(Arc::new(CountingBackend {
            entries,
            stat_calls: Arc::clone(&stat_calls),
        }));

        let repos = build_docker_index(&storage)
            .await
            .expect("index built")
            .repos;

        // After the fix: the rebuild made ZERO per-key stat() calls.
        assert_eq!(
            stat_calls.load(Ordering::SeqCst),
            0,
            "#738: index rebuild must not stat() per key — size/mtime come from list_with_meta"
        );
        // Correctness preserved: size = sum of listed sizes (100 + 340), 1 tag.
        assert_eq!(repos.len(), 1);
        assert_eq!(repos[0].versions, 1);
        assert_eq!(repos[0].size, 440);
    }
}
