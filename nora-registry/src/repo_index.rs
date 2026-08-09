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
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use tokio::sync::{Mutex as AsyncMutex, Notify};
use tracing::info;

/// Repository info for UI display
#[derive(Debug, Clone, Serialize, Default)]
pub struct RepoInfo {
    pub name: String,
    pub versions: usize,
    pub size: u64,
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

struct BuiltIndex {
    repos: Vec<RepoInfo>,
    objects: Vec<IndexedObject>,
}

impl BuiltIndex {
    #[cfg(test)]
    fn repos(repos: Vec<RepoInfo>) -> Self {
        Self {
            repos,
            objects: Vec::new(),
        }
    }

    fn with_objects(repos: Vec<RepoInfo>, keys: Vec<(String, FileMeta)>) -> Self {
        let mut objects: Vec<_> = keys
            .into_iter()
            .map(|(key, meta)| IndexedObject { key, meta })
            .collect();
        objects.sort_by(|left, right| left.key.cmp(&right.key));
        Self { repos, objects }
    }
}

/// Index for a single registry type
pub struct RegistryIndex {
    data: RwLock<Arc<Vec<RepoInfo>>>,
    objects: RwLock<Arc<Vec<IndexedObject>>>,
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
            data: RwLock::new(Arc::new(Vec::new())),
            objects: RwLock::new(Arc::new(Vec::new())),
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
        Arc::clone(&self.data.read())
    }

    fn get_objects(&self) -> Arc<Vec<IndexedObject>> {
        Arc::clone(&self.objects.read())
    }

    fn set(&self, built: BuiltIndex, generation: u64) {
        *self.data.write() = Arc::new(built.repos);
        *self.objects.write() = Arc::new(built.objects);
        self.published_generation
            .store(generation, Ordering::Release);
        let status = if generation < self.requested_generation.load(Ordering::Acquire) {
            0
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
        // maven:2 for a single pushed jar). Size is unaffected: `total_size`
        // sums every bucket, so `storage_bytes` stays == on-disk `du`.
        self.data.read().iter().filter(|r| r.versions > 0).count()
    }

    /// Sum of artifact bytes in this registry's cached index (no rebuild).
    pub fn total_size(&self) -> u64 {
        self.data.read().iter().map(|r| r.size).sum()
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
            loop {
                let Some(repo_index) = weak.upgrade() else {
                    return;
                };
                let notified = Arc::clone(&repo_index.notify).notified_owned();
                let active = repo_index.active.read().clone();
                let mut pending_after_pass = false;
                for registry in RegistryType::all() {
                    if !active.contains(registry) {
                        continue;
                    }
                    if cancel.is_cancelled() {
                        return;
                    }
                    repo_index.rebuild_one(*registry, &storage).await;
                    if repo_index
                        .indexes
                        .get(registry)
                        .is_some_and(RegistryIndex::is_dirty)
                    {
                        pending_after_pass = true;
                    }
                }

                drop(repo_index);
                if pending_after_pass {
                    tokio::select! {
                        _ = cancel.cancelled() => return,
                        _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {},
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

    async fn rebuild_one(&self, registry: RegistryType, storage: &Storage) -> bool {
        let Some(index) = self.indexes.get(&registry) else {
            return false;
        };
        if !index.is_dirty() {
            return true;
        }
        let _guard = index.rebuild_lock.lock().await;
        if !index.is_dirty() {
            return true;
        }
        let generation = index.requested_generation.load(Ordering::Acquire);
        match build_index(registry, storage).await {
            Some(built) => {
                info!(
                    registry = registry.as_str(),
                    count = built.repos.len(),
                    generation,
                    "Index rebuilt"
                );
                index.set(built, generation);
                true
            }
            None => {
                index.fail(generation);
                tracing::warn!(
                    registry = registry.as_str(),
                    generation,
                    "index rebuild failed; retaining last published snapshot"
                );
                false
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
        self.rebuild_one(registry, storage).await
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
            .map(|(rt, idx)| (*rt, idx.total_size()))
            .collect()
    }
}

async fn build_index(reg_type: RegistryType, storage: &Storage) -> Option<BuiltIndex> {
    if reg_type == RegistryType::Maven {
        return build_maven_index_with_objects(storage).await;
    }
    match reg_type {
        RegistryType::Docker => build_docker_index(storage).await,
        RegistryType::Maven => unreachable!("handled above"),
        RegistryType::Npm => build_npm_index(storage).await,
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
        Err(e) => {
            tracing::warn!(prefix, error = %e, "index rebuild: storage list failed");
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

async fn build_npm_index(storage: &Storage) -> Option<BuiltIndex> {
    let keys = list_keys(storage, "npm/").await?;
    let mut packages: HashMap<String, (usize, u64, u64)> = HashMap::new();
    let by_key: HashMap<&str, &crate::storage::FileMeta> = keys
        .iter()
        .map(|(key, meta)| (key.as_str(), meta))
        .collect();

    // A hosted version manifest is the publish commit point: a staged tarball
    // without it is an invisible orphan and must not become a UI artifact.
    // Proxy cache has no hosted manifest, so its concrete tarballs remain the
    // countable unit. Groups own no objects and never appear here.
    for (key, meta) in &keys {
        let Some(parsed) = crate::npm_layout::parse_npm_object_key(key) else {
            continue;
        };
        let name = format!("repositories/{}/{}", parsed.repository, parsed.package);
        match parsed.kind {
            crate::npm_layout::NpmObjectKind::HostedVersion(_) => {
                let manifest = match storage.get(key).await {
                    Ok(manifest) => manifest,
                    Err(error) => {
                        tracing::warn!(
                            key,
                            error = %error,
                            "npm index: cannot read hosted version manifest"
                        );
                        return None;
                    }
                };
                let Some(blob_key) = crate::npm_layout::hosted_blob_key_from_manifest(
                    &parsed.repository,
                    &parsed.package,
                    &manifest,
                ) else {
                    tracing::warn!(
                        key,
                        "npm index: hosted manifest has no valid blob reference"
                    );
                    return None;
                };
                let blob = by_key.get(blob_key.as_str()).copied();
                let entry = packages.entry(name).or_insert((0, 0, 0));
                entry.0 += 1;
                entry.1 += meta.size + blob.map(|value| value.size).unwrap_or(0);
                entry.2 = entry
                    .2
                    .max(meta.modified)
                    .max(blob.map(|value| value.modified).unwrap_or(0));
            }
            crate::npm_layout::NpmObjectKind::ProxyTarball(_) => {
                let entry = packages.entry(name).or_insert((0, 0, 0));
                entry.0 += 1;
                entry.1 += meta.size;
                entry.2 = entry.2.max(meta.modified);
            }
            _ => {}
        }
    }

    Some(BuiltIndex::with_objects(to_sorted_vec(packages), keys))
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
        use base64::Engine as _;
        use sha2::Digest as _;

        let (_d, storage) = temp_storage();
        let hosted_manifest = |name: &str, version: &str, blob: &[u8]| {
            serde_json::to_vec(&serde_json::json!({
                "name": name,
                "version": version,
                "dist": {
                    "integrity": format!(
                        "sha512-{}",
                        base64::engine::general_purpose::STANDARD
                            .encode(sha2::Sha512::digest(blob))
                    )
                }
            }))
            .unwrap()
        };
        let scoped_manifest = hosted_manifest("@scope/pkg", "1.0.0", b"hosted");
        let scoped_blob = crate::npm_layout::hosted_blob_key_from_manifest(
            "npm-private",
            "@scope/pkg",
            &scoped_manifest,
        )
        .unwrap();
        storage.put(&scoped_blob, b"hosted").await.unwrap();
        storage
            .put(
                "npm/repositories/npm-private/@scope/pkg/versions/1.0.0.json",
                &scoped_manifest,
            )
            .await
            .unwrap();
        storage
            .put(
                "npm/repositories/npm-registry/proxy/tarballs/@scope/pkg/pkg-1.0.0.tgz",
                b"proxy",
            )
            .await
            .unwrap();
        let proxy_manifest = hosted_manifest("proxy", "1.0.0", b"hosted-package-named-proxy");
        let hosted_proxy_blob = crate::npm_layout::hosted_blob_key_from_manifest(
            "npm-private",
            "proxy",
            &proxy_manifest,
        )
        .unwrap();
        storage
            .put(&hosted_proxy_blob, b"hosted-package-named-proxy")
            .await
            .unwrap();
        storage
            .put(
                "npm/repositories/npm-private/proxy/versions/1.0.0.json",
                &proxy_manifest,
            )
            .await
            .unwrap();
        let orphan_manifest = hosted_manifest("orphan", "1.0.0", b"precommit-orphan");
        let orphan_blob = crate::npm_layout::hosted_blob_key_from_manifest(
            "npm-private",
            "orphan",
            &orphan_manifest,
        )
        .unwrap();
        storage
            .put(&orphan_blob, b"precommit-orphan")
            .await
            .unwrap();

        let repos = build_npm_index(&storage).await.expect("index built").repos;

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
        assert!(
            !repos
                .iter()
                .any(|entry| entry.name == "repositories/npm-private/orphan"),
            "hosted pre-commit tarballs must not enter the index"
        );
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
            async fn total_size(&self) -> u64 {
                0
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
