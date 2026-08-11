// Copyright (c) 2026 The NORA Authors
// SPDX-License-Identifier: MIT

//! In-memory repository index rebuilt by one background worker.
//!
//! Design:
//! - Request handlers only read the last published snapshot
//! - Invalidation increments a generation, so a write racing a rebuild cannot be lost
//! - One worker rebuilds active registries sequentially, bounding storage scan concurrency
//! - Protocol callers may wait for the generation visible when their request began

mod persistent_builder;
mod redb_store;

pub(crate) use persistent_builder::{PersistentIndexPhase, PersistentIndexProgress};
pub(crate) use redb_store::{
    preflight_database, ChangeEvent, ChildPreflight, PersistentIndex, StoreError, StoredNpmPackage,
    StoredNpmVersion, MAX_QUERY_EXAMINED,
};

use crate::config::Config;
use crate::registry_type::RegistryType;
use crate::storage::{
    FileMeta, Storage, StorageMutation, StorageMutationObserver, StorageMutationOutcome,
};
use crate::ui::components::format_timestamp;
use crate::validation::ends_with_ci;
use arc_swap::ArcSwapOption;
use parking_lot::{Mutex, RwLock};
use rand::Rng as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, Mutex as AsyncMutex, Notify, Semaphore};
use tokio::time::Instant;
use tracing::info;
use utoipa::ToSchema;

const INDEX_RETRY_BASE_SECS: u64 = 30;
const INDEX_RETRY_MAX_SECS: u64 = 300;
const SEMANTIC_CHANGE_CONCURRENCY: usize = 8;
// Version 2 establishes the process-clean proof required for immediate warm
// Ready. Version 1 binaries could persist `clean_shutdown=true` despite an
// in-memory reconciliation gap, so every legacy PVC must perform one
// authoritative S2 before its projection can be reused.
const PERSISTENT_TOPOLOGY_SCHEMA: u64 = 2;

fn index_error_class(error: &StoreError) -> &'static str {
    match error {
        StoreError::AlreadyOpen => "already_open",
        StoreError::Schema(_) => "schema",
        StoreError::WriterUnavailable => "writer_unavailable",
        StoreError::Superseded => "superseded",
        StoreError::TransactionTooLarge => "transaction_too_large",
        StoreError::DiskAdmission(_) => "disk_admission",
        StoreError::Serialization(_) => "serialization",
        StoreError::Database(_) => "database",
        StoreError::Io(_) => "io",
        StoreError::PreflightUnavailable(_) => "preflight_unavailable",
    }
}

fn reconcile_error_class(error: &persistent_builder::ReconcileError) -> &'static str {
    match error {
        persistent_builder::ReconcileError::Store(error) => index_error_class(error),
        persistent_builder::ReconcileError::Storage(_) => "storage",
        persistent_builder::ReconcileError::Serialization(_) => "serialization",
        persistent_builder::ReconcileError::NpmAuthority { reason, .. } => reason,
    }
}

fn persistent_config_digest(config: &Config) -> Result<String, StoreError> {
    persistent_config_digest_with_schema(config, PERSISTENT_TOPOLOGY_SCHEMA)
}

fn persistent_config_digest_with_schema(
    config: &Config,
    topology_schema: u64,
) -> Result<String, StoreError> {
    // Credentials are skipped by the config serializers. The digest binds a
    // reusable DB to its storage identity and Maven/npm topology without ever
    // storing or logging the source document.
    let topology = serde_json::json!({
        "schema": topology_schema,
        "storage": {
            "mode": &config.storage.mode,
            "local_path": &config.storage.path,
            "s3_endpoint": &config.storage.s3_url,
            "bucket": &config.storage.bucket,
            "region": &config.storage.s3_region,
            "virtual_hosted": config.storage.s3_virtual_hosted,
        },
        "maven": &config.maven,
        "npm": &config.npm,
    });
    let bytes = serde_json::to_vec(&topology)?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

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
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Default)]
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

#[derive(Debug, Clone)]
pub(crate) struct LogicalIndexedObject {
    pub(crate) path: String,
    pub(crate) meta: FileMeta,
}

#[derive(Debug, Clone)]
pub(crate) struct PersistentRepoPage {
    pub(crate) items: Vec<RepoInfo>,
    pub(crate) next_after: Option<Vec<u8>>,
    pub(crate) generation: u64,
    pub(crate) config_digest: String,
}

#[derive(Debug, Clone)]
pub(crate) struct PersistentMavenPage {
    pub(crate) items: Vec<RepoInfo>,
    pub(crate) next_after: Option<String>,
    pub(crate) generation: u64,
    pub(crate) config_digest: String,
    pub(crate) has_direct_files: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct PersistentMavenFilePage {
    pub(crate) items: Vec<(String, FileMeta)>,
    pub(crate) next_after: Option<String>,
    pub(crate) generation: u64,
    pub(crate) config_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PersistentObjectRevision {
    pub(crate) db_uuid: String,
    pub(crate) active_slot: Option<redb_store::Slot>,
    pub(crate) generation: u64,
    pub(crate) watermark: u64,
    pub(crate) config_digest: String,
}

impl PersistentObjectRevision {
    fn from_meta(meta: &redb_store::MetaState) -> Self {
        Self {
            db_uuid: meta.db_uuid.clone(),
            active_slot: meta.active_slot,
            generation: meta.generation,
            watermark: meta.active_watermark(),
            config_digest: meta.config_digest.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PersistentObjectPage {
    pub(crate) items: Vec<IndexedObject>,
    pub(crate) next_after: Option<Vec<u8>>,
    pub(crate) revision: PersistentObjectRevision,
}

#[derive(Debug, Clone)]
pub(crate) struct RepoQuery {
    pub(crate) registry: String,
    pub(crate) after: Option<Vec<u8>>,
    pub(crate) filter: Option<String>,
    pub(crate) limit: usize,
    pub(crate) max_examined: usize,
    pub(crate) deadline: Duration,
    pub(crate) name_prefix: Option<String>,
    pub(crate) before_name: Option<String>,
    pub(crate) allowed_repositories: Option<Vec<String>>,
}

/// Minimal hosted npm search document derived from the same validated full
/// generation as the repository row. Request handlers filter and render this
/// projection in memory; split version/tag objects are never a search oracle.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    persistent: Option<Arc<PersistentRuntime>>,
}

struct PersistentRuntime {
    /// Replaceable derived-index handle. S3 remains available while this is
    /// `None`; index-backed UI is 503 and mutations are fail-closed until the
    /// background recovery loop reopens/reseeds and reconciles it.
    index: ArcSwapOption<PersistentIndex>,
    index_path: std::path::PathBuf,
    storage: Storage,
    config_digest: String,
    reconcile_interval: Duration,
    maven_enabled: bool,
    maven_named: bool,
    npm_enabled: bool,
    requested_sequence: AtomicU64,
    published_sequence: AtomicU64,
    request_epoch: AtomicU64,
    resolved_epoch: AtomicU64,
    /// Highest request epoch that only a complete authoritative S3 reconcile
    /// may resolve. Zero means no such recovery is pending. Unlike a boolean,
    /// the monotonic epoch cannot be cleared by an older async completion.
    reconcile_required_through: AtomicU64,
    status: AtomicU8,
    notify: Notify,
    /// Serializes mutation admission, writer-handle replacement, and
    /// readiness publication. S3 preparation remains concurrent; only the
    /// small O(1) fence state is protected by this non-async lock.
    fence: Mutex<PersistentFenceState>,
    fence_notify: Notify,
    fence_cancel: tokio_util::sync::CancellationToken,
    fence_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    fence_started: AtomicBool,
    #[cfg(test)]
    physical_admission_barrier: Mutex<Option<(Arc<std::sync::Barrier>, Arc<std::sync::Barrier>)>>,
    #[cfg(test)]
    publication_barrier: Mutex<Option<(Arc<std::sync::Barrier>, Arc<std::sync::Barrier>)>>,
    #[cfg(test)]
    semantic_registration_barrier:
        Mutex<Option<(Arc<tokio::sync::Barrier>, Arc<tokio::sync::Barrier>)>>,
    #[cfg(test)]
    reconcile_retry_scheduled: Notify,
    #[cfg(test)]
    reconcile_retry_delay_millis: AtomicU64,
    #[cfg(test)]
    background_waiting: Notify,
    #[cfg(test)]
    shutdown_waiting: Notify,
    semantic_tasks: tokio_util::task::TaskTracker,
    semantic_abort_handles: Mutex<Vec<tokio::task::AbortHandle>>,
    semantic_permits: Arc<Semaphore>,
    background_started: AtomicBool,
    initial_reconciled: AtomicBool,
    projection_usable: AtomicBool,
    progress: persistent_builder::ReconcileProgress,
    maven_artifacts: AtomicU64,
    maven_bytes: AtomicU64,
    npm_versions: AtomicU64,
    npm_bytes: AtomicU64,
}

struct PhysicalReceipt {
    ticket: u64,
    index: Arc<PersistentIndex>,
    receive: oneshot::Receiver<Result<u64, StoreError>>,
}

#[derive(Default)]
struct PersistentFenceState {
    /// Number of semantic repairs that were admitted but have not completed
    /// (including aborted tasks whose drop guard has not run yet).
    semantic_inflight: u64,
    /// Monotonic in-memory ticket for physical writer admissions. The newest
    /// FIFO writer receipt fences every older receipt, so only one receiver is
    /// retained regardless of mutation volume.
    physical_admitted: u64,
    physical_acked: u64,
    latest_physical_receipt: Option<PhysicalReceipt>,
}

fn persistent_reconcile_retry_delay(_runtime: &PersistentRuntime, attempt: u32) -> Duration {
    #[cfg(test)]
    {
        let millis = _runtime
            .reconcile_retry_delay_millis
            .load(Ordering::Acquire);
        if millis != 0 {
            return Duration::from_millis(millis);
        }
    }
    index_retry_delay(attempt)
}

#[cfg(test)]
fn wait_test_fence_barrier(
    barrier: &Mutex<Option<(Arc<std::sync::Barrier>, Arc<std::sync::Barrier>)>>,
) {
    if let Some((captured, release)) = barrier.lock().clone() {
        captured.wait();
        release.wait();
    }
}

#[cfg(test)]
fn wait_test_fence_barrier_in_place(
    barrier: &Mutex<Option<(Arc<std::sync::Barrier>, Arc<std::sync::Barrier>)>>,
) {
    if let Some((captured, release)) = barrier.lock().clone() {
        // This hook deliberately holds the short publication fence while the
        // test swaps writers. Tell Tokio that the synchronous barrier blocks
        // so it can replace this worker under a parallel test load.
        tokio::task::block_in_place(|| {
            captured.wait();
            release.wait();
        });
    }
}

fn publish_persistent_meta_locked(
    runtime: &PersistentRuntime,
    meta: &redb_store::MetaState,
    _fence: &PersistentFenceState,
) {
    runtime
        .maven_artifacts
        .store(meta.totals.maven_artifacts, Ordering::Release);
    runtime
        .maven_bytes
        .store(meta.totals.maven_bytes, Ordering::Release);
    runtime
        .npm_versions
        .store(meta.totals.npm_versions, Ordering::Release);
    runtime
        .npm_bytes
        .store(meta.totals.npm_bytes, Ordering::Release);
    crate::metrics::INDEX_GENERATION.set(i64::try_from(meta.generation).unwrap_or(i64::MAX));
    crate::metrics::INDEX_PENDING_CHANGES.set(
        i64::try_from(
            meta.accepted_change_seq
                .saturating_sub(meta.active_watermark()),
        )
        .unwrap_or(i64::MAX),
    );
}

fn publish_index_database_bytes(runtime: &PersistentRuntime) {
    // Keep potentially slow PVC metadata I/O outside the fence critical
    // section. This gauge is observational and need not be transactionally
    // coupled to generation publication.
    let database_bytes = std::fs::metadata(&runtime.index_path).map_or(0, |meta| meta.len());
    crate::metrics::INDEX_DATABASE_BYTES.set(i64::try_from(database_bytes).unwrap_or(i64::MAX));
}

fn current_persistent_index(
    runtime: &PersistentRuntime,
) -> Result<Arc<PersistentIndex>, StoreError> {
    runtime
        .index
        .load_full()
        .filter(|index| index.writer_healthy())
        .ok_or(StoreError::WriterUnavailable)
}

fn is_current_persistent_index(
    runtime: &PersistentRuntime,
    candidate: &Arc<PersistentIndex>,
) -> bool {
    runtime
        .index
        .load_full()
        .is_some_and(|current| Arc::ptr_eq(&current, candidate))
}

fn is_current_persistent_index_locked(
    runtime: &PersistentRuntime,
    candidate: &Arc<PersistentIndex>,
    _fence: &PersistentFenceState,
) -> bool {
    is_current_persistent_index(runtime, candidate)
}

fn current_request_epoch_locked(runtime: &PersistentRuntime, _fence: &PersistentFenceState) -> u64 {
    runtime.request_epoch.load(Ordering::Acquire).max(1)
}

fn require_full_reconcile_locked(runtime: &PersistentRuntime, fence: &PersistentFenceState) {
    let epoch = current_request_epoch_locked(runtime, fence);
    runtime
        .reconcile_required_through
        .fetch_max(epoch, Ordering::AcqRel);
    runtime.notify.notify_one();
}

#[cfg(test)]
fn require_full_reconcile(runtime: &PersistentRuntime) {
    let fence = runtime.fence.lock();
    require_full_reconcile_locked(runtime, &fence);
}

fn full_reconcile_required_locked(
    runtime: &PersistentRuntime,
    _fence: &PersistentFenceState,
) -> bool {
    runtime.reconcile_required_through.load(Ordering::Acquire) != 0
}

fn full_reconcile_required(runtime: &PersistentRuntime) -> bool {
    let fence = runtime.fence.lock();
    full_reconcile_required_locked(runtime, &fence)
}

/// Clear only requirements that existed before the completed authoritative
/// scan. A concurrent/newer failure retains its larger epoch and therefore
/// cannot be erased by this older completion.
fn resolve_full_reconcile_through_locked(
    runtime: &PersistentRuntime,
    observed_epoch: u64,
    _fence: &PersistentFenceState,
) -> bool {
    loop {
        let required = runtime.reconcile_required_through.load(Ordering::Acquire);
        if required == 0 {
            return true;
        }
        if required > observed_epoch {
            return false;
        }
        if runtime
            .reconcile_required_through
            .compare_exchange(required, 0, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return true;
        }
    }
}

fn persistent_projection_index(
    runtime: &PersistentRuntime,
) -> Result<Arc<PersistentIndex>, StoreError> {
    if !runtime.projection_usable.load(Ordering::Acquire) {
        return Err(StoreError::WriterUnavailable);
    }
    current_persistent_index(runtime)
}

async fn recover_persistent_index(
    runtime: &PersistentRuntime,
) -> Result<Arc<PersistentIndex>, StoreError> {
    if let Ok(index) = current_persistent_index(runtime) {
        return Ok(index);
    }

    runtime.progress.reset(PersistentIndexPhase::Recovering);

    let old = {
        let mut fence = runtime.fence.lock();
        runtime.projection_usable.store(false, Ordering::Release);
        store_persistent_status_locked(runtime, IndexStatus::Degraded, &fence);
        fence.latest_physical_receipt = None;
        fence.physical_admitted = 0;
        fence.physical_acked = 0;
        runtime.index.swap(None)
    };
    if let Some(old) = old {
        old.shutdown().await;
        drop(old);
    }

    let index = PersistentIndex::open_after_child_preflight(
        &runtime.index_path,
        runtime.config_digest.clone(),
    )
    .await?;
    let meta = index.meta().await?;
    let compatible = meta.active_slot.is_some()
        && meta.config_digest == runtime.config_digest
        && (!runtime.maven_enabled || meta.completeness.maven)
        && (!runtime.npm_enabled || meta.completeness.npm);
    {
        let fence = runtime.fence.lock();
        runtime
            .requested_sequence
            .store(meta.accepted_change_seq, Ordering::Release);
        runtime.published_sequence.store(
            if compatible {
                meta.active_watermark()
            } else {
                0
            },
            Ordering::Release,
        );
        runtime.resolved_epoch.store(0, Ordering::Release);
        runtime.initial_reconciled.store(false, Ordering::Release);
        require_full_reconcile_locked(runtime, &fence);
        publish_persistent_meta_locked(runtime, &meta, &fence);
        runtime.index.store(Some(Arc::clone(&index)));
        runtime
            .projection_usable
            .store(compatible, Ordering::Release);
        store_persistent_status_locked(runtime, IndexStatus::Warming, &fence);
    }
    publish_index_database_bytes(runtime);
    runtime.fence_notify.notify_one();
    Ok(index)
}

fn publish_pending_change_count_locked(runtime: &PersistentRuntime, _fence: &PersistentFenceState) {
    let sequence_pending = runtime
        .requested_sequence
        .load(Ordering::Acquire)
        .saturating_sub(runtime.published_sequence.load(Ordering::Acquire));
    let epoch_pending = runtime
        .request_epoch
        .load(Ordering::Acquire)
        .saturating_sub(runtime.resolved_epoch.load(Ordering::Acquire));
    crate::metrics::INDEX_PENDING_CHANGES
        .set(i64::try_from(sequence_pending.max(epoch_pending)).unwrap_or(i64::MAX));
}

fn store_persistent_status_locked(
    runtime: &PersistentRuntime,
    status: IndexStatus,
    _fence: &PersistentFenceState,
) {
    let value = match status {
        IndexStatus::Warming => 0,
        IndexStatus::Ready => 1,
        IndexStatus::Degraded => 2,
    };
    runtime.status.store(value, Ordering::Release);
    crate::metrics::INDEX_STATE.set(i64::from(value));
}

struct SemanticInflightGuard {
    runtime: Arc<PersistentRuntime>,
    armed: bool,
}

impl SemanticInflightGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for SemanticInflightGuard {
    fn drop(&mut self) {
        let mut fence = self.runtime.fence.lock();
        fence.semantic_inflight = fence.semantic_inflight.saturating_sub(1);
        if self.armed {
            // A cancelled or unwound typed repair may have stopped before its
            // durable registration or publication. Never infer completion
            // from the inflight count alone: force authoritative anti-entropy.
            store_persistent_status_locked(&self.runtime, IndexStatus::Degraded, &fence);
            require_full_reconcile_locked(&self.runtime, &fence);
        }
        publish_pending_change_count_locked(&self.runtime, &fence);
        drop(fence);
        if self.armed {
            tracing::warn!(
                "semantic derived-index repair ended abnormally; authoritative reconciliation required"
            );
            self.runtime.notify.notify_one();
        }
        self.runtime.fence_notify.notify_one();
    }
}

fn spawn_semantic_task(
    runtime: &Arc<PersistentRuntime>,
    task: impl std::future::Future<Output = ()> + Send + 'static,
) {
    let handle = runtime.semantic_tasks.spawn(task);
    let abort = handle.abort_handle();
    drop(handle);
    let mut handles = runtime.semantic_abort_handles.lock();
    handles.retain(|handle| !handle.is_finished());
    handles.push(abort);
}

fn ensure_persistent_fence(runtime: &Arc<PersistentRuntime>) {
    if runtime
        .fence_started
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        runtime.fence_notify.notify_one();
        return;
    }
    let task_runtime = Arc::clone(runtime);
    let handle = tokio::spawn(async move {
        run_persistent_fence(task_runtime).await;
    });
    *runtime.fence_task.lock() = Some(handle);
    runtime.fence_notify.notify_one();
}

async fn settle_persistent_fence(runtime: &Arc<PersistentRuntime>) {
    let candidate = {
        let fence = runtime.fence.lock();
        if fence.semantic_inflight != 0
            || fence.physical_acked < fence.physical_admitted
            || fence.latest_physical_receipt.is_some()
            || !runtime.initial_reconciled.load(Ordering::Acquire)
            || !runtime.projection_usable.load(Ordering::Acquire)
            || full_reconcile_required_locked(runtime, &fence)
        {
            return;
        }
        let Ok(index) = current_persistent_index(runtime) else {
            store_persistent_status_locked(runtime, IndexStatus::Degraded, &fence);
            require_full_reconcile_locked(runtime, &fence);
            return;
        };
        (
            index,
            current_request_epoch_locked(runtime, &fence),
            fence.physical_admitted,
        )
    };

    let (index, observed_epoch, observed_physical) = candidate;
    let meta = match index.meta().await {
        Ok(meta) => meta,
        Err(error) => {
            let fence = runtime.fence.lock();
            if is_current_persistent_index_locked(runtime, &index, &fence) {
                store_persistent_status_locked(runtime, IndexStatus::Degraded, &fence);
                require_full_reconcile_locked(runtime, &fence);
            }
            tracing::warn!(
                error_class = %index_error_class(&error),
                "derived-index fence could not read the writer watermark"
            );
            return;
        }
    };

    {
        let fence = runtime.fence.lock();
        if !is_current_persistent_index_locked(runtime, &index, &fence)
            || fence.semantic_inflight != 0
            || fence.physical_acked < fence.physical_admitted
            || fence.latest_physical_receipt.is_some()
            || fence.physical_admitted != observed_physical
            || current_request_epoch_locked(runtime, &fence) != observed_epoch
            || full_reconcile_required_locked(runtime, &fence)
        {
            return;
        }
        runtime
            .requested_sequence
            .fetch_max(meta.accepted_change_seq, Ordering::AcqRel);
        let active_watermark = meta.active_watermark();
        let caught_up = !meta.global_dirty
            && meta.accepted_change_seq <= active_watermark
            && runtime.requested_sequence.load(Ordering::Acquire) <= active_watermark;
        if caught_up {
            runtime
                .published_sequence
                .fetch_max(active_watermark, Ordering::AcqRel);
            runtime
                .resolved_epoch
                .fetch_max(observed_epoch, Ordering::AcqRel);
            publish_persistent_meta_locked(runtime, &meta, &fence);
            publish_pending_change_count_locked(runtime, &fence);
            store_persistent_status_locked(runtime, IndexStatus::Ready, &fence);
        } else {
            publish_pending_change_count_locked(runtime, &fence);
            store_persistent_status_locked(runtime, IndexStatus::Warming, &fence);
        }
    }
    publish_index_database_bytes(runtime);
}

async fn run_persistent_fence(runtime: Arc<PersistentRuntime>) {
    loop {
        if runtime.fence_cancel.is_cancelled() {
            return;
        }

        loop {
            let receipt = {
                let mut fence = runtime.fence.lock();
                fence.latest_physical_receipt.take()
            };
            let Some(receipt) = receipt else {
                break;
            };

            // CANCEL-SAFETY: dropping the receipt receiver does not cancel the
            // already-admitted FIFO writer command. This branch is used only
            // during shutdown; the next startup reconcile repairs any gap.
            let result = tokio::select! {
                _ = runtime.fence_cancel.cancelled() => return,
                result = receipt.receive => result,
            };
            let mut fence = runtime.fence.lock();
            if !is_current_persistent_index_locked(runtime.as_ref(), &receipt.index, &fence) {
                continue;
            }
            fence.physical_acked = fence.physical_acked.max(receipt.ticket);
            match result {
                Ok(Ok(sequence)) => {
                    runtime
                        .requested_sequence
                        .fetch_max(sequence, Ordering::AcqRel);
                }
                Ok(Err(error)) => {
                    store_persistent_status_locked(runtime.as_ref(), IndexStatus::Degraded, &fence);
                    require_full_reconcile_locked(runtime.as_ref(), &fence);
                    tracing::warn!(
                        error_class = %index_error_class(&error),
                        "physical derived-index invalidation failed in the writer"
                    );
                }
                Err(_) => {
                    store_persistent_status_locked(runtime.as_ref(), IndexStatus::Degraded, &fence);
                    require_full_reconcile_locked(runtime.as_ref(), &fence);
                    tracing::warn!(
                        "physical derived-index invalidation receipt closed before acknowledgement"
                    );
                }
            }
            publish_pending_change_count_locked(runtime.as_ref(), &fence);
        }

        settle_persistent_fence(&runtime).await;
        // `notify_one` drives new coordinator work. Shutdown waiters need a
        // non-stored progress edge after the coordinator has acknowledged a
        // receipt and republished readiness; otherwise both sides can sleep on
        // the same Notify until the entire shutdown deadline expires.
        runtime.fence_notify.notify_waiters();

        // CANCEL-SAFETY: cancellation exits permanently. A coalesced Notify
        // permit may be consumed only on the branch that immediately loops
        // and drains the newest receipt/state.
        tokio::select! {
            _ = runtime.fence_cancel.cancelled() => return,
            _ = runtime.fence_notify.notified() => {},
        }
    }
}

async fn process_persistent_change(
    runtime: Arc<PersistentRuntime>,
    event_epoch: u64,
    event: ChangeEvent,
) {
    let index = match current_persistent_index(&runtime) {
        Ok(index) => index,
        Err(error) => {
            let fence = runtime.fence.lock();
            store_persistent_status_locked(&runtime, IndexStatus::Degraded, &fence);
            require_full_reconcile_locked(&runtime, &fence);
            tracing::warn!(
                error_class = %index_error_class(&error),
                "derived-index mutation deferred until writer recovery"
            );
            return;
        }
    };
    #[cfg(test)]
    let registration_barrier = { runtime.semantic_registration_barrier.lock().clone() };
    #[cfg(test)]
    if let Some((captured, release)) = registration_barrier {
        captured.wait().await;
        release.wait().await;
    }
    match index.register_change(event.clone()).await {
        Ok(sequence) => {
            {
                let fence = runtime.fence.lock();
                if !is_current_persistent_index_locked(&runtime, &index, &fence) {
                    require_full_reconcile_locked(&runtime, &fence);
                    tracing::debug!(
                        event_epoch,
                        "discarded derived-index registration completed by a superseded writer"
                    );
                    return;
                }
                #[cfg(test)]
                wait_test_fence_barrier_in_place(&runtime.publication_barrier);
                runtime
                    .requested_sequence
                    .fetch_max(sequence, Ordering::AcqRel);
                publish_pending_change_count_locked(&runtime, &fence);
            }
            if runtime.initial_reconciled.load(Ordering::Acquire)
                && !matches!(
                    event,
                    ChangeEvent::GlobalDirty | ChangeEvent::PhysicalDirty { .. }
                )
            {
                match persistent_builder::apply_change(
                    Arc::clone(&index),
                    runtime.storage.clone(),
                    sequence,
                    event,
                )
                .await
                {
                    Ok(meta) => {
                        {
                            let fence = runtime.fence.lock();
                            if !is_current_persistent_index_locked(&runtime, &index, &fence) {
                                require_full_reconcile_locked(&runtime, &fence);
                                tracing::debug!(
                                    event_epoch,
                                    "discarded incremental repair completed by a superseded writer"
                                );
                                return;
                            }
                            publish_persistent_meta_locked(&runtime, &meta, &fence);
                            runtime
                                .published_sequence
                                .fetch_max(meta.active_watermark(), Ordering::AcqRel);
                            publish_pending_change_count_locked(&runtime, &fence);
                        }
                        publish_index_database_bytes(&runtime);
                        return;
                    }
                    Err(error) => {
                        tracing::warn!(
                            error_class = %reconcile_error_class(&error),
                            "incremental index repair deferred to full reconciliation"
                        );
                    }
                }
            }
            let fence = runtime.fence.lock();
            require_full_reconcile_locked(&runtime, &fence);
        }
        Err(error) => {
            let fence = runtime.fence.lock();
            if !is_current_persistent_index_locked(&runtime, &index, &fence) {
                require_full_reconcile_locked(&runtime, &fence);
                return;
            }
            store_persistent_status_locked(&runtime, IndexStatus::Degraded, &fence);
            require_full_reconcile_locked(&runtime, &fence);
            tracing::error!(
                error_class = %index_error_class(&error),
                "failed to persist derived-index invalidation"
            );
        }
    }
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
            persistent: None,
        }
    }

    pub async fn open_persistent(
        config: &Config,
        enabled: &HashSet<RegistryType>,
        storage: Storage,
    ) -> Result<Arc<Self>, StoreError> {
        if config.storage.mode != crate::config::StorageMode::S3 {
            return Ok(Arc::new(Self::new()));
        }
        if !enabled.contains(&RegistryType::Maven) && !enabled.contains(&RegistryType::Npm) {
            return Ok(Arc::new(Self::new()));
        }
        let digest = persistent_config_digest(config)?;
        let persistent = match PersistentIndex::open_after_child_preflight(
            &config.index.path,
            digest.clone(),
        )
        .await
        {
            Ok(index) => Some(index),
            Err(StoreError::AlreadyOpen) => return Err(StoreError::AlreadyOpen),
            Err(error) => {
                tracing::error!(
                    error_class = %index_error_class(&error),
                    "persistent index unavailable at startup; protocol listener will start while recovery retries"
                );
                None
            }
        };
        let meta = match &persistent {
            Some(index) => Some(index.meta().await?),
            None => None,
        };
        Self::from_persistent(config, enabled, storage, persistent, meta, digest).await
    }

    async fn from_persistent(
        config: &Config,
        enabled: &HashSet<RegistryType>,
        storage: Storage,
        persistent: Option<Arc<PersistentIndex>>,
        meta: Option<redb_store::MetaState>,
        digest: String,
    ) -> Result<Arc<Self>, StoreError> {
        let maven_enabled = enabled.contains(&RegistryType::Maven);
        let npm_enabled = enabled.contains(&RegistryType::Npm);
        let persisted_is_usable = meta.as_ref().is_some_and(|meta| {
            meta.active_slot.is_some()
                && meta.config_digest == digest
                && (!maven_enabled || meta.completeness.maven)
                && (!npm_enabled || meta.completeness.npm)
        });
        let persisted_is_clean = persisted_is_usable
            && persistent
                .as_ref()
                .is_some_and(|index| index.startup_clean())
            && meta.as_ref().is_some_and(|meta| {
                !meta.global_dirty && meta.accepted_change_seq == meta.active_watermark()
            });
        let accepted_change_seq = meta.as_ref().map_or(0, |meta| meta.accepted_change_seq);
        let active_watermark = meta
            .as_ref()
            .map_or(0, redb_store::MetaState::active_watermark);
        let totals = meta
            .as_ref()
            .map_or_else(redb_store::RegistryTotals::default, |meta| meta.totals);
        let index_available = persistent.is_some();

        let mut index = Self::new();
        index.persistent = Some(Arc::new(PersistentRuntime {
            index: ArcSwapOption::from(persistent),
            index_path: std::path::PathBuf::from(&config.index.path),
            storage,
            config_digest: digest,
            reconcile_interval: Duration::from_secs(config.index.reconcile_interval_secs),
            maven_enabled,
            maven_named: !config.maven.repositories.is_empty(),
            npm_enabled,
            requested_sequence: AtomicU64::new(accepted_change_seq),
            published_sequence: AtomicU64::new(if persisted_is_usable {
                active_watermark
            } else {
                0
            }),
            request_epoch: AtomicU64::new(1),
            resolved_epoch: AtomicU64::new(u64::from(persisted_is_clean)),
            // A child-preflighted clean database is already the durable result
            // of every acknowledged local mutation through its watermark. It
            // can publish immediately; periodic anti-entropy still checks for
            // out-of-band S3 changes. Every weaker startup state reconciles
            // fail-closed before publishing Ready.
            reconcile_required_through: AtomicU64::new(u64::from(
                (maven_enabled || npm_enabled) && !persisted_is_clean,
            )),
            status: AtomicU8::new(0),
            notify: Notify::new(),
            fence: Mutex::new(PersistentFenceState::default()),
            fence_notify: Notify::new(),
            fence_cancel: tokio_util::sync::CancellationToken::new(),
            fence_task: Mutex::new(None),
            fence_started: AtomicBool::new(false),
            #[cfg(test)]
            physical_admission_barrier: Mutex::new(None),
            #[cfg(test)]
            publication_barrier: Mutex::new(None),
            #[cfg(test)]
            semantic_registration_barrier: Mutex::new(None),
            #[cfg(test)]
            reconcile_retry_scheduled: Notify::new(),
            #[cfg(test)]
            reconcile_retry_delay_millis: AtomicU64::new(0),
            #[cfg(test)]
            background_waiting: Notify::new(),
            #[cfg(test)]
            shutdown_waiting: Notify::new(),
            semantic_tasks: tokio_util::task::TaskTracker::new(),
            semantic_abort_handles: Mutex::new(Vec::new()),
            semantic_permits: Arc::new(Semaphore::new(SEMANTIC_CHANGE_CONCURRENCY)),
            background_started: AtomicBool::new(false),
            initial_reconciled: AtomicBool::new(persisted_is_clean),
            projection_usable: AtomicBool::new(persisted_is_usable),
            progress: persistent_builder::ReconcileProgress::new(if persisted_is_clean {
                PersistentIndexPhase::Idle
            } else if index_available {
                PersistentIndexPhase::Preparing
            } else {
                PersistentIndexPhase::Recovering
            }),
            maven_artifacts: AtomicU64::new(totals.maven_artifacts),
            maven_bytes: AtomicU64::new(totals.maven_bytes),
            npm_versions: AtomicU64::new(totals.npm_versions),
            npm_bytes: AtomicU64::new(totals.npm_bytes),
        }));
        if let Some(runtime) = &index.persistent {
            let fence = runtime.fence.lock();
            if let Some(meta) = &meta {
                publish_persistent_meta_locked(runtime, meta, &fence);
            }
            store_persistent_status_locked(
                runtime,
                if persisted_is_clean {
                    IndexStatus::Ready
                } else if index_available {
                    IndexStatus::Warming
                } else {
                    IndexStatus::Degraded
                },
                &fence,
            );
            if persisted_is_clean {
                tracing::info!(
                    generation = meta.as_ref().map_or(0, |meta| meta.generation),
                    accepted_change_seq,
                    "published clean persistent Maven/npm generation without startup S3 reconcile"
                );
            }
        }
        let index = Arc::new(index);
        if let Some(runtime) = &index.persistent {
            ensure_persistent_fence(runtime);
        }
        Ok(index)
    }

    #[cfg(test)]
    pub(crate) async fn open_persistent_for_test(
        config: &Config,
        enabled: &HashSet<RegistryType>,
        storage: Storage,
    ) -> Result<Arc<Self>, StoreError> {
        if !enabled.contains(&RegistryType::Maven) && !enabled.contains(&RegistryType::Npm) {
            return Ok(Arc::new(Self::new()));
        }
        let digest = persistent_config_digest(config)?;
        let persistent = PersistentIndex::open(&config.index.path, digest.clone())?;
        let meta = persistent.meta().await?;
        Self::from_persistent(
            config,
            enabled,
            storage,
            Some(persistent),
            Some(meta),
            digest,
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn reconcile_persistent_for_test(&self) -> Result<(), StoreError> {
        let runtime = self
            .persistent
            .as_ref()
            .ok_or(StoreError::WriterUnavailable)?;
        let index = current_persistent_index(runtime)?;
        let meta = persistent_builder::reconcile_with_progress(
            index,
            runtime.storage.clone(),
            runtime.config_digest.clone(),
            runtime.maven_enabled,
            runtime.npm_enabled,
            &runtime.progress,
        )
        .await
        .map_err(|error| match error {
            persistent_builder::ReconcileError::Store(error) => error,
            other => StoreError::Database(other.to_string()),
        })?;
        {
            let fence = runtime.fence.lock();
            publish_persistent_meta_locked(runtime, &meta, &fence);
            runtime
                .requested_sequence
                .store(meta.accepted_change_seq, Ordering::Release);
            runtime
                .published_sequence
                .store(meta.active_watermark(), Ordering::Release);
            let observed_epoch = current_request_epoch_locked(runtime, &fence);
            runtime
                .resolved_epoch
                .store(observed_epoch, Ordering::Release);
            runtime
                .reconcile_required_through
                .store(0, Ordering::Release);
            runtime.initial_reconciled.store(true, Ordering::Release);
            runtime.projection_usable.store(true, Ordering::Release);
            store_persistent_status_locked(runtime, IndexStatus::Ready, &fence);
            runtime.progress.set_phase(PersistentIndexPhase::Idle);
        }
        publish_index_database_bytes(runtime);
        runtime.fence_notify.notify_one();
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn persistent_meta_for_test(
        &self,
    ) -> Result<redb_store::MetaState, StoreError> {
        let runtime = self
            .persistent
            .as_ref()
            .ok_or(StoreError::WriterUnavailable)?;
        current_persistent_index(runtime)?.meta().await
    }

    #[cfg(test)]
    pub(crate) async fn stop_persistent_writer_for_test(&self) -> Result<(), StoreError> {
        let runtime = self
            .persistent
            .as_ref()
            .ok_or(StoreError::WriterUnavailable)?;
        let index = current_persistent_index(runtime)?;
        index.shutdown().await;
        Ok(())
    }

    pub fn persistent_status(&self) -> Option<IndexStatus> {
        let persistent = self.persistent.as_ref()?;
        let _fence = persistent.fence.lock();
        if current_persistent_index(persistent).is_err() {
            return Some(IndexStatus::Degraded);
        }
        Some(IndexStatus::from_u8(
            persistent.status.load(Ordering::Acquire),
        ))
    }

    /// Whether the persistent Maven/npm projection can currently serve UI
    /// queries. This is intentionally weaker than protocol readiness: an
    /// unclean or reconciling index may still expose its last-good generation.
    pub fn persistent_projection_available(&self) -> bool {
        let Some(persistent) = &self.persistent else {
            return true;
        };
        let _fence = persistent.fence.lock();
        persistent.projection_usable.load(Ordering::Acquire)
            && current_persistent_index(persistent).is_ok()
    }

    pub(crate) fn persistent_index_progress(&self) -> PersistentIndexProgress {
        self.persistent.as_ref().map_or(
            PersistentIndexProgress {
                phase: PersistentIndexPhase::Idle,
                maven_objects: 0,
                npm_objects: 0,
                npm_packages: 0,
            },
            |runtime| runtime.progress.snapshot(),
        )
    }

    pub(crate) fn has_persistent(&self) -> bool {
        self.persistent.is_some()
    }

    pub fn persistent_writer_healthy(&self) -> bool {
        self.persistent
            .as_ref()
            .is_none_or(|runtime| current_persistent_index(runtime).is_ok())
    }

    pub fn persistent_protocol_ready(&self) -> bool {
        let Some(runtime) = &self.persistent else {
            return true;
        };
        if !runtime.maven_enabled && !runtime.npm_enabled {
            return true;
        }
        let fence = runtime.fence.lock();
        current_persistent_index(runtime).is_ok()
            && runtime.status.load(Ordering::Acquire) == 1
            && !full_reconcile_required_locked(runtime, &fence)
            && fence.semantic_inflight == 0
            && fence.physical_acked >= fence.physical_admitted
            && fence.latest_physical_receipt.is_none()
            && runtime.resolved_epoch.load(Ordering::Acquire)
                >= runtime.request_epoch.load(Ordering::Acquire)
            && runtime.published_sequence.load(Ordering::Acquire)
                >= runtime.requested_sequence.load(Ordering::Acquire)
    }

    pub async fn shutdown_persistent(&self) {
        self.shutdown_persistent_until(Instant::now() + Duration::from_secs(30), true)
            .await;
    }

    pub(crate) async fn shutdown_persistent_until(&self, deadline: Instant, allow_clean: bool) {
        if let Some(runtime) = &self.persistent {
            runtime.semantic_tasks.close();
            let remaining = deadline.saturating_duration_since(Instant::now());
            if tokio::time::timeout(remaining, runtime.semantic_tasks.wait())
                .await
                .is_err()
            {
                tracing::error!(
                    active = runtime.semantic_tasks.len(),
                    "persistent-index semantic tasks did not drain before writer shutdown"
                );
                let handles = std::mem::take(&mut *runtime.semantic_abort_handles.lock());
                for handle in handles {
                    handle.abort();
                }
                let remaining = deadline.saturating_duration_since(Instant::now());
                let _ = tokio::time::timeout(remaining, runtime.semantic_tasks.wait()).await;
            }
            // Resolve a quiescent all-acked state synchronously. Relying only
            // on a previously queued Notify can otherwise consume the entire
            // shutdown deadline after the background S2 task has already
            // stopped and no future event can wake the coordinator.
            settle_persistent_fence(runtime).await;
            let runtime_caught_up = loop {
                if self.persistent_protocol_ready() {
                    break true;
                }
                let may_settle = {
                    let fence = runtime.fence.lock();
                    current_persistent_index(runtime).is_ok()
                        && runtime.initial_reconciled.load(Ordering::Acquire)
                        && runtime.projection_usable.load(Ordering::Acquire)
                        && !full_reconcile_required_locked(runtime, &fence)
                        && fence.semantic_inflight == 0
                        && (fence.physical_acked < fence.physical_admitted
                            || fence.latest_physical_receipt.is_some())
                };
                if !may_settle || Instant::now() >= deadline {
                    break false;
                }
                let notified = runtime.fence_notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.persistent_protocol_ready() {
                    break true;
                }
                #[cfg(test)]
                runtime.shutdown_waiting.notify_waiters();
                let remaining = deadline.saturating_duration_since(Instant::now());
                if tokio::time::timeout(remaining, &mut notified)
                    .await
                    .is_err()
                {
                    break false;
                }
            };
            let mark_clean = if allow_clean && runtime_caught_up {
                match current_persistent_index(runtime) {
                    Ok(index) => index.meta().await.is_ok_and(|meta| {
                        !meta.global_dirty && meta.accepted_change_seq == meta.active_watermark()
                    }),
                    Err(_) => false,
                }
            } else {
                false
            };
            if !mark_clean {
                tracing::warn!(
                    allow_clean,
                    "persistent index cannot prove a clean process and durable caught-up fence; preserving an unclean startup requirement"
                );
            }
            runtime.fence_cancel.cancel();
            let fence_task = runtime.fence_task.lock().take();
            if let Some(mut handle) = fence_task {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if tokio::time::timeout(remaining, &mut handle).await.is_err() {
                    handle.abort();
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    let _ = tokio::time::timeout(remaining, handle).await;
                }
            }
            let index = {
                let mut fence = runtime.fence.lock();
                fence.latest_physical_receipt = None;
                runtime.index.swap(None)
            };
            if let Some(index) = index {
                if mark_clean {
                    index.shutdown_until(deadline).await;
                } else {
                    index.shutdown_unclean_until(deadline).await;
                }
            }
        }
    }

    pub(crate) async fn persistent_repo_page(
        &self,
        mut query: RepoQuery,
    ) -> Result<PersistentRepoPage, StoreError> {
        let runtime = self
            .persistent
            .as_ref()
            .ok_or(StoreError::WriterUnavailable)?;
        let index = persistent_projection_index(runtime)?;
        query.max_examined = MAX_QUERY_EXAMINED;
        query.deadline = Duration::from_millis(250);
        let (items, next_after, generation) = index.query_repos(query).await?;
        Ok(PersistentRepoPage {
            items,
            next_after,
            generation,
            config_digest: runtime.config_digest.clone(),
        })
    }

    pub(crate) async fn persistent_maven_children(
        &self,
        prefixes: Vec<String>,
        logical_path: String,
        after: Option<String>,
        limit: usize,
    ) -> Result<PersistentMavenPage, StoreError> {
        let runtime = self
            .persistent
            .as_ref()
            .ok_or(StoreError::WriterUnavailable)?;
        let index = persistent_projection_index(runtime)?;
        let (items, next_after, generation, has_direct_files) = index
            .maven_children_page(prefixes, logical_path, after, limit)
            .await?;
        Ok(PersistentMavenPage {
            items,
            next_after,
            generation,
            config_digest: runtime.config_digest.clone(),
            has_direct_files,
        })
    }

    pub(crate) async fn persistent_maven_files(
        &self,
        prefixes: Vec<String>,
        logical_path: String,
        after: Option<String>,
        limit: usize,
    ) -> Result<PersistentMavenFilePage, StoreError> {
        let runtime = self
            .persistent
            .as_ref()
            .ok_or(StoreError::WriterUnavailable)?;
        let index = persistent_projection_index(runtime)?;
        let (items, next_after, generation) = index
            .maven_files_page(prefixes, logical_path, after, limit)
            .await?;
        Ok(PersistentMavenFilePage {
            items,
            next_after,
            generation,
            config_digest: runtime.config_digest.clone(),
        })
    }

    /// Read at most `max_examined` Maven objects from one or more physical
    /// prefixes. Prefix order implements Nexus-compatible first-member-wins
    /// for groups. Every redb transaction is page-scoped and ends before the
    /// next await.
    pub(crate) async fn persistent_logical_objects(
        &self,
        prefixes: &[String],
        max_examined: usize,
    ) -> Result<(Vec<LogicalIndexedObject>, u64, bool), StoreError> {
        if max_examined == 0 || max_examined > MAX_QUERY_EXAMINED {
            return Err(StoreError::TransactionTooLarge);
        }
        let runtime = self
            .persistent
            .as_ref()
            .ok_or(StoreError::WriterUnavailable)?;
        let index = persistent_projection_index(runtime)?;
        let before = index.meta().await?;
        let Some(slot) = before.active_slot else {
            return Ok((Vec::new(), before.generation, false));
        };
        let mut selected = BTreeMap::<String, FileMeta>::new();
        let mut examined = 0usize;
        let mut truncated = false;
        for prefix in prefixes {
            let mut after = None;
            loop {
                let remaining = max_examined.saturating_sub(examined);
                if remaining == 0 {
                    truncated = true;
                    break;
                }
                let page_limit = remaining.min(redb_store::MAX_TX_ROWS);
                let (rows, next) = index
                    .scan_objects(slot, prefix.as_bytes().to_vec(), after, page_limit)
                    .await?;
                if rows.is_empty() {
                    break;
                }
                examined = examined.saturating_add(rows.len());
                for (key, meta) in rows {
                    if let Some(path) = key.strip_prefix(prefix) {
                        selected.entry(path.to_string()).or_insert(meta);
                    }
                }
                if examined >= max_examined {
                    truncated = next.is_some();
                    break;
                }
                let Some(next) = next else { break };
                after = Some(next);
            }
            if truncated {
                break;
            }
        }
        let after = index.meta().await?;
        if after.generation != before.generation || after.active_slot != before.active_slot {
            return Err(StoreError::Superseded);
        }
        Ok((
            selected
                .into_iter()
                .map(|(path, meta)| LogicalIndexedObject { path, meta })
                .collect(),
            before.generation,
            truncated,
        ))
    }

    /// Read one bounded page from the raw S3-derived inventory. Long-running
    /// maintenance consumers compare `revision` between pages and stop if an
    /// incremental repair or full generation flip changed their source set.
    pub(crate) async fn persistent_object_page(
        &self,
        prefix: &str,
        after: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<PersistentObjectPage, StoreError> {
        let runtime = self
            .persistent
            .as_ref()
            .ok_or(StoreError::WriterUnavailable)?;
        let index = persistent_projection_index(runtime)?;
        let before = index.meta().await?;
        let Some(slot) = before.active_slot else {
            return Ok(PersistentObjectPage {
                items: Vec::new(),
                next_after: None,
                revision: PersistentObjectRevision::from_meta(&before),
            });
        };
        let (rows, next_after) = index
            .scan_objects(slot, prefix.as_bytes().to_vec(), after, limit)
            .await?;
        let after_meta = index.meta().await?;
        if after_meta.active_slot != before.active_slot
            || after_meta.generation != before.generation
            || after_meta.active_watermark() != before.active_watermark()
        {
            return Err(StoreError::Superseded);
        }
        Ok(PersistentObjectPage {
            items: rows
                .into_iter()
                .map(|(key, meta)| IndexedObject { key, meta })
                .collect(),
            next_after,
            revision: PersistentObjectRevision::from_meta(&before),
        })
    }

    pub(crate) async fn persistent_object_revision(
        &self,
    ) -> Result<PersistentObjectRevision, StoreError> {
        let runtime = self
            .persistent
            .as_ref()
            .ok_or(StoreError::WriterUnavailable)?;
        let meta = persistent_projection_index(runtime)?.meta().await?;
        Ok(PersistentObjectRevision::from_meta(&meta))
    }

    pub(crate) async fn persistent_npm_package(
        &self,
        repository: &str,
        package: &str,
        after: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<
        (
            Option<StoredNpmPackage>,
            Vec<StoredNpmVersion>,
            Option<Vec<u8>>,
            u64,
            String,
        ),
        StoreError,
    > {
        let runtime = self
            .persistent
            .as_ref()
            .ok_or(StoreError::WriterUnavailable)?;
        let index = persistent_projection_index(runtime)?;
        let (package_row, versions, next_after, generation) = index
            .npm_package_page(repository, package, after, limit)
            .await?;
        Ok((
            package_row,
            versions,
            next_after,
            generation,
            runtime.config_digest.clone(),
        ))
    }

    fn request_persistent_change(&self, event: ChangeEvent) {
        let Some(runtime) = self.persistent.clone() else {
            return;
        };
        let permit = tokio::runtime::Handle::try_current().ok().and_then(|_| {
            Arc::clone(&runtime.semantic_permits)
                .try_acquire_owned()
                .ok()
        });
        let event_epoch = {
            let mut fence = runtime.fence.lock();
            let event_epoch = runtime.request_epoch.fetch_add(1, Ordering::AcqRel) + 1;
            store_persistent_status_locked(&runtime, IndexStatus::Warming, &fence);
            if permit.is_some() {
                fence.semantic_inflight = fence.semantic_inflight.saturating_add(1);
            } else {
                // Bound memory by collapsing overload/no-runtime into the one
                // monotonic authoritative-reconcile fence.
                require_full_reconcile_locked(&runtime, &fence);
            }
            publish_pending_change_count_locked(&runtime, &fence);
            event_epoch
        };
        runtime.fence_notify.notify_one();
        let Some(permit) = permit else {
            return;
        };
        let task_runtime = Arc::clone(&runtime);
        let mut inflight = SemanticInflightGuard {
            runtime: Arc::clone(&task_runtime),
            armed: true,
        };
        spawn_semantic_task(&runtime, async move {
            let _permit = permit;
            process_persistent_change(task_runtime, event_epoch, event).await;
            inflight.disarm();
        });
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
            if self.persistent.is_some()
                && matches!(registry, RegistryType::Maven | RegistryType::Npm)
            {
                continue;
            }
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

    /// Start the Maven/npm persistent reconciliation worker. A successful run
    /// publishes one complete S2 generation; failures retain the previous
    /// slot and retry with bounded backoff. The periodic delay starts after a
    /// completed run, so a slow scan never creates catch-up storms.
    pub fn start_persistent_background(
        self: &Arc<Self>,
        storage: Storage,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Option<tokio::task::JoinHandle<()>> {
        let runtime = self.persistent.clone()?;
        if !runtime.maven_enabled && !runtime.npm_enabled {
            return None;
        }
        if runtime
            .background_started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            runtime.notify.notify_one();
            return None;
        }

        Some(tokio::spawn(async move {
            let mut next_periodic = if runtime.initial_reconciled.load(Ordering::Acquire) {
                Instant::now() + runtime.reconcile_interval
            } else {
                Instant::now()
            };
            let mut retry_attempt = 0u32;
            let mut retry_not_before = None::<Instant>;
            loop {
                if cancel.is_cancelled() {
                    return;
                }
                if let Some(deadline) = retry_not_before {
                    if deadline > Instant::now() {
                        // A Notify only records/coalesces more work. It must not
                        // bypass a recovery/reconcile failure backoff and turn
                        // request traffic into a LIST/reopen storm.
                        tokio::select! {
                            _ = cancel.cancelled() => return,
                            _ = tokio::time::sleep_until(deadline) => {},
                        }
                    }
                    retry_not_before = None;
                }
                let forced = full_reconcile_required(&runtime);
                if forced || Instant::now() >= next_periodic {
                    let index = match recover_persistent_index(&runtime).await {
                        Ok(index) => index,
                        Err(error) => {
                            runtime
                                .progress
                                .set_phase(PersistentIndexPhase::RetryWaiting);
                            {
                                let fence = runtime.fence.lock();
                                require_full_reconcile_locked(&runtime, &fence);
                                store_persistent_status_locked(
                                    &runtime,
                                    IndexStatus::Degraded,
                                    &fence,
                                );
                            }
                            let delay = persistent_reconcile_retry_delay(&runtime, retry_attempt);
                            retry_attempt = retry_attempt.saturating_add(1);
                            next_periodic = Instant::now() + delay;
                            retry_not_before = Some(next_periodic);
                            tracing::error!(
                                error_class = %index_error_class(&error),
                                retry_after_secs = delay.as_secs(),
                                "persistent index recovery failed; protocol S3 reads remain available"
                            );
                            continue;
                        }
                    };
                    let observed_epoch = {
                        let fence = runtime.fence.lock();
                        current_request_epoch_locked(&runtime, &fence)
                    };
                    let had_generation = index
                        .meta()
                        .await
                        .is_ok_and(|meta| meta.active_slot.is_some());
                    if !had_generation {
                        let fence = runtime.fence.lock();
                        store_persistent_status_locked(&runtime, IndexStatus::Warming, &fence);
                    }
                    let reconcile_started = std::time::Instant::now();
                    match persistent_builder::reconcile_with_progress(
                        Arc::clone(&index),
                        storage.clone(),
                        runtime.config_digest.clone(),
                        runtime.maven_enabled,
                        runtime.npm_enabled,
                        &runtime.progress,
                    )
                    .await
                    {
                        Ok(meta) => {
                            let unchanged = {
                                let fence = runtime.fence.lock();
                                if !is_current_persistent_index_locked(&runtime, &index, &fence) {
                                    runtime
                                        .progress
                                        .set_phase(PersistentIndexPhase::RetryWaiting);
                                    require_full_reconcile_locked(&runtime, &fence);
                                    store_persistent_status_locked(
                                        &runtime,
                                        IndexStatus::Warming,
                                        &fence,
                                    );
                                    continue;
                                }
                                publish_persistent_meta_locked(&runtime, &meta, &fence);
                                let active_watermark = meta.active_watermark();
                                runtime
                                    .requested_sequence
                                    .fetch_max(meta.accepted_change_seq, Ordering::AcqRel);
                                runtime
                                    .published_sequence
                                    .store(active_watermark, Ordering::Release);
                                let scan_caught_up = observed_epoch
                                    == current_request_epoch_locked(&runtime, &fence)
                                    && fence.semantic_inflight == 0
                                    && fence.physical_acked >= fence.physical_admitted
                                    && fence.latest_physical_receipt.is_none()
                                    && runtime.requested_sequence.load(Ordering::Acquire)
                                        <= active_watermark
                                    && !meta.global_dirty;
                                let requirement_resolved = scan_caught_up
                                    && resolve_full_reconcile_through_locked(
                                        &runtime,
                                        observed_epoch,
                                        &fence,
                                    );
                                let unchanged = requirement_resolved
                                    && observed_epoch
                                        == current_request_epoch_locked(&runtime, &fence)
                                    && !full_reconcile_required_locked(&runtime, &fence);
                                runtime.initial_reconciled.store(true, Ordering::Release);
                                runtime.projection_usable.store(true, Ordering::Release);
                                runtime.progress.set_phase(PersistentIndexPhase::Idle);
                                publish_pending_change_count_locked(&runtime, &fence);
                                // The fence coordinator owns the final ordered
                                // meta barrier and is the only runtime path
                                // that publishes Ready.
                                store_persistent_status_locked(
                                    &runtime,
                                    IndexStatus::Warming,
                                    &fence,
                                );
                                unchanged
                            };
                            publish_index_database_bytes(&runtime);
                            runtime.fence_notify.notify_one();
                            crate::metrics::INDEX_RECONCILE_TOTAL
                                .with_label_values(&["success", "none"])
                                .inc();
                            crate::metrics::INDEX_RECONCILE_DURATION_SECONDS
                                .with_label_values(&["success"])
                                .observe(reconcile_started.elapsed().as_secs_f64());
                            crate::metrics::INDEX_LAST_SUCCESS_TIMESTAMP.set(
                                i64::try_from(
                                    std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap_or_default()
                                        .as_secs(),
                                )
                                .unwrap_or(i64::MAX),
                            );
                            retry_attempt = 0;
                            retry_not_before = None;
                            next_periodic = Instant::now() + runtime.reconcile_interval;
                            info!(
                                generation = meta.generation,
                                accepted_change_seq = meta.accepted_change_seq,
                                pending_change = !unchanged,
                                "persistent Maven/npm index generation published"
                            );
                            if !unchanged {
                                continue;
                            }
                        }
                        Err(persistent_builder::ReconcileError::Store(StoreError::Superseded)) => {
                            let full_required = {
                                let fence = runtime.fence.lock();
                                store_persistent_status_locked(
                                    &runtime,
                                    IndexStatus::Warming,
                                    &fence,
                                );
                                full_reconcile_required_locked(&runtime, &fence)
                            };
                            runtime.fence_notify.notify_one();
                            tracing::debug!(
                                "persistent shadow generation superseded; waiting for the mutation fence to settle"
                            );
                            if full_required {
                                next_periodic = Instant::now() + Duration::from_secs(1);
                                retry_not_before = Some(next_periodic);
                            } else {
                                next_periodic = Instant::now() + runtime.reconcile_interval;
                            }
                            crate::metrics::INDEX_RECONCILE_TOTAL
                                .with_label_values(&["superseded", "superseded"])
                                .inc();
                            crate::metrics::INDEX_RECONCILE_DURATION_SECONDS
                                .with_label_values(&["superseded"])
                                .observe(reconcile_started.elapsed().as_secs_f64());
                        }
                        Err(error) => {
                            {
                                let fence = runtime.fence.lock();
                                require_full_reconcile_locked(&runtime, &fence);
                                store_persistent_status_locked(
                                    &runtime,
                                    IndexStatus::Degraded,
                                    &fence,
                                );
                            }
                            let delay = persistent_reconcile_retry_delay(&runtime, retry_attempt);
                            retry_attempt = retry_attempt.saturating_add(1);
                            next_periodic = Instant::now() + delay;
                            retry_not_before = Some(next_periodic);
                            #[cfg(test)]
                            runtime.reconcile_retry_scheduled.notify_one();
                            tracing::warn!(
                                error_class = %reconcile_error_class(&error),
                                retry_after_secs = delay.as_secs(),
                                "persistent index reconcile failed; last-good generation retained"
                            );
                            let error_class = reconcile_error_class(&error);
                            crate::metrics::INDEX_RECONCILE_TOTAL
                                .with_label_values(&["error", error_class])
                                .inc();
                            crate::metrics::INDEX_RECONCILE_DURATION_SECONDS
                                .with_label_values(&["error"])
                                .observe(reconcile_started.elapsed().as_secs_f64());
                        }
                    }
                }

                #[cfg(test)]
                runtime.background_waiting.notify_one();
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    _ = runtime.notify.notified() => {},
                    _ = tokio::time::sleep_until(next_periodic) => {},
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
            if self.persistent.is_some() && matches!(rt, RegistryType::Maven | RegistryType::Npm) {
                self.request_persistent_change(ChangeEvent::GlobalDirty);
            } else if let Some(idx) = self.indexes.get(&rt) {
                idx.invalidate();
                self.activate(rt);
                self.notify.notify_one();
            }
        }
    }

    fn invalidate_memory_snapshot(&self, registry: RegistryType) {
        if self.persistent.is_some() && matches!(registry, RegistryType::Maven | RegistryType::Npm)
        {
            return;
        }
        if let Some(index) = self.indexes.get(&registry) {
            index.invalidate();
            self.activate(registry);
            self.notify.notify_one();
        }
    }

    pub fn invalidate_maven_path(&self, repository: &str, path: &str) {
        self.invalidate_memory_snapshot(RegistryType::Maven);
        self.request_persistent_change(ChangeEvent::MavenPathChanged {
            repository: repository.to_string(),
            path: path.to_string(),
        });
    }

    /// Publish the narrowest semantic invalidation available for a completed
    /// proxy-cache write. Maven storage keys encode both the repository and
    /// logical path, so a single cached artifact/metadata bundle does not need
    /// to force a full S3 reconciliation. Other formats retain their existing
    /// registry-wide behavior.
    pub fn invalidate_cached_path(&self, registry: &str, key: &str) {
        if registry == "maven" {
            let named_layout = self
                .persistent
                .as_ref()
                .is_some_and(|runtime| runtime.maven_named);
            if named_layout {
                if let Some(named) = key.strip_prefix("maven/repositories/") {
                    if let Some((repository, path)) = named.split_once('/') {
                        self.invalidate_maven_path(repository, path);
                        return;
                    }
                }
            } else if let Some(path) = key.strip_prefix("maven/") {
                self.invalidate_maven_path("", path);
                return;
            }
        }
        self.invalidate(registry);
    }

    pub fn invalidate_maven_ga(&self, repository: &str, ga_path: &str) {
        self.invalidate_memory_snapshot(RegistryType::Maven);
        self.request_persistent_change(ChangeEvent::MavenGaChanged {
            repository: repository.to_string(),
            ga_path: ga_path.to_string(),
        });
    }

    pub fn invalidate_npm_hosted(&self, repository: &str, package: &str) {
        self.invalidate_memory_snapshot(RegistryType::Npm);
        self.request_persistent_change(ChangeEvent::NpmHostedChanged {
            repository: repository.to_string(),
            package: package.to_string(),
        });
    }

    pub fn invalidate_npm_proxy(&self, repository: &str, package: &str) {
        self.invalidate_memory_snapshot(RegistryType::Npm);
        self.request_persistent_change(ChangeEvent::NpmProxyChanged {
            repository: repository.to_string(),
            package: package.to_string(),
        });
    }

    /// Invalidate every registry index so each rebuilds from storage on next read.
    /// Backs the admin reindex endpoint for the "rescan all paths" case.
    pub fn invalidate_all(&self) {
        for (registry, idx) in &self.indexes {
            if self.persistent.is_some()
                && matches!(registry, RegistryType::Maven | RegistryType::Npm)
            {
                continue;
            }
            idx.invalidate();
            self.activate(*registry);
        }
        self.notify.notify_one();
        self.request_persistent_change(ChangeEvent::GlobalDirty);
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
        if self.persistent.is_some() && matches!(reg_type, RegistryType::Maven | RegistryType::Npm)
        {
            return Arc::new(Vec::new());
        }
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
        if matches!(reg_type, RegistryType::Maven | RegistryType::Npm) {
            if let Some(runtime) = &self.persistent {
                if !self.persistent_protocol_ready() {
                    return Err(());
                }
                let index = persistent_projection_index(runtime).map_err(|_| ())?;
                let mut rows = Vec::new();
                let mut after = None;
                let mut generation = None;
                loop {
                    let remaining = MAX_QUERY_EXAMINED.saturating_sub(rows.len());
                    if remaining == 0 {
                        return Err(());
                    }
                    let (mut page, next, observed_generation) = index
                        .query_repos(RepoQuery {
                            registry: registry.to_string(),
                            after,
                            filter: None,
                            limit: remaining.min(100),
                            max_examined: remaining,
                            deadline: Duration::from_millis(250),
                            name_prefix: None,
                            before_name: None,
                            allowed_repositories: None,
                        })
                        .await
                        .map_err(|_| ())?;
                    if generation.is_some_and(|value| value != observed_generation) {
                        return Err(());
                    }
                    generation = Some(observed_generation);
                    rows.append(&mut page);
                    let Some(next) = next else { break };
                    after = Some(next);
                }
                if generation != index.meta().await.ok().map(|meta| meta.generation)
                    || !self.persistent_protocol_ready()
                {
                    return Err(());
                }
                return Ok(Arc::new(rows));
            }
        }
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
        if let Some(runtime) = &self.persistent {
            if !self.persistent_protocol_ready() {
                return Err(());
            }
            let index = persistent_projection_index(runtime).map_err(|_| ())?;
            let (documents, generation, truncated) = index
                .list_npm_search_documents(MAX_QUERY_EXAMINED)
                .await
                .map_err(|_| ())?;
            if truncated
                || generation == 0
                || generation != index.meta().await.map_err(|_| ())?.generation
                || !self.persistent_protocol_ready()
            {
                return Err(());
            }
            return Ok(Arc::new(documents));
        }
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
        if self.persistent.is_some() && matches!(registry, RegistryType::Maven | RegistryType::Npm)
        {
            return self.persistent_status();
        }
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
        let mut counts = self
            .indexes
            .iter()
            .map(|(rt, idx)| (*rt, idx.count()))
            .collect::<HashMap<_, _>>();
        if let Some(runtime) = &self.persistent {
            counts.insert(
                RegistryType::Maven,
                usize::try_from(runtime.maven_artifacts.load(Ordering::Acquire))
                    .unwrap_or(usize::MAX),
            );
            counts.insert(
                RegistryType::Npm,
                usize::try_from(runtime.npm_versions.load(Ordering::Acquire)).unwrap_or(usize::MAX),
            );
        }
        counts
    }

    /// Get total artifact bytes per registry from the cached index (no rebuild).
    pub fn sizes(&self) -> HashMap<RegistryType, u64> {
        let mut sizes = self
            .indexes
            .iter()
            .filter(|(rt, _)| **rt != RegistryType::Npm)
            .filter_map(|(rt, idx)| idx.total_size().map(|size| (*rt, size)))
            .collect::<HashMap<_, _>>();
        if let Some(runtime) = &self.persistent {
            sizes.insert(
                RegistryType::Maven,
                runtime.maven_bytes.load(Ordering::Acquire),
            );
            sizes.insert(RegistryType::Npm, runtime.npm_bytes.load(Ordering::Acquire));
        }
        sizes
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

impl StorageMutationObserver for RepoIndex {
    fn observe(&self, mutation: StorageMutation) {
        if !mutation.key.starts_with("maven/") && !mutation.key.starts_with("npm/") {
            return;
        }
        let Some(runtime) = &self.persistent else {
            return;
        };
        let mut fence = runtime.fence.lock();
        runtime.request_epoch.fetch_add(1, Ordering::AcqRel);
        store_persistent_status_locked(runtime, IndexStatus::Warming, &fence);
        #[cfg(test)]
        wait_test_fence_barrier(&runtime.physical_admission_barrier);
        let index = match current_persistent_index(runtime) {
            Ok(index) => index,
            Err(error) => {
                store_persistent_status_locked(runtime, IndexStatus::Degraded, &fence);
                require_full_reconcile_locked(runtime, &fence);
                publish_pending_change_count_locked(runtime, &fence);
                drop(fence);
                runtime.fence_notify.notify_one();
                tracing::warn!(
                    error_class = %index_error_class(&error),
                    "physical index invalidation retained in memory until writer recovery"
                );
                return;
            }
        };
        match index.try_register_change(ChangeEvent::PhysicalDirty { key: mutation.key }) {
            Ok(receive) => {
                // The newest receipt fences every earlier command in the
                // single FIFO redb writer. Replacing it keeps admission O(1)
                // while the one coordinator prevents Ready until the fence is
                // acknowledged and all semantic repairs have quiesced.
                fence.physical_admitted = fence.physical_admitted.saturating_add(1);
                let ticket = fence.physical_admitted;
                fence.latest_physical_receipt = Some(PhysicalReceipt {
                    ticket,
                    index,
                    receive,
                });
                if mutation.outcome == StorageMutationOutcome::Unknown {
                    require_full_reconcile_locked(runtime, &fence);
                }
            }
            Err(error) => {
                require_full_reconcile_locked(runtime, &fence);
                tracing::warn!(
                    error_class = %index_error_class(&error),
                    mutation_kind = ?mutation.kind,
                    mutation_outcome = ?mutation.outcome,
                    "redb invalidation queue saturated; bounded anti-entropy reconcile required"
                );
            }
        }
        publish_pending_change_count_locked(runtime, &fence);
        drop(fence);
        runtime.fence_notify.notify_one();
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
    for field in [
        "description",
        "keywords",
        "publisher",
        "maintainers",
        "license",
        "author",
        "homepage",
        "repository",
    ] {
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

    async fn reopen_persistent_with_startup_state(
        config: &Config,
        enabled: &HashSet<RegistryType>,
        storage: Storage,
        startup_clean: bool,
    ) -> Arc<RepoIndex> {
        let digest = persistent_config_digest(config).unwrap();
        let persistent = PersistentIndex::open_with_startup_state_for_test(
            &config.index.path,
            digest.clone(),
            startup_clean,
        )
        .unwrap();
        let meta = persistent.meta().await.unwrap();
        RepoIndex::from_persistent(
            config,
            enabled,
            storage,
            Some(persistent),
            Some(meta),
            digest,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn persistent_database_is_not_opened_without_maven_or_npm() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage = Storage::new_local(storage_dir.path().to_str().unwrap());
        let index_dir = tempfile::tempdir().unwrap();
        let path = index_dir.path().join("unused.redb");
        let mut config = Config::default();
        config.index.path = path.to_string_lossy().into_owned();
        let index = RepoIndex::open_persistent_for_test(&config, &HashSet::new(), storage)
            .await
            .unwrap();
        assert!(!index.has_persistent());
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn clean_warm_start_publishes_last_good_without_immediate_s3_reconcile() {
        let storage_dir = tempfile::tempdir().unwrap();
        let authoritative = Storage::new_local(storage_dir.path().to_str().unwrap());
        authoritative
            .put("maven/com/acme/app/1.0/app-1.0.jar", b"jar")
            .await
            .unwrap();
        let index_dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.maven.repositories.clear();
        config.index.path = index_dir
            .path()
            .join("index.redb")
            .to_string_lossy()
            .into_owned();
        config.index.reconcile_interval_secs = 1;
        let enabled = HashSet::from([RegistryType::Maven]);

        let first = RepoIndex::open_persistent_for_test(&config, &enabled, authoritative.clone())
            .await
            .unwrap();
        first.reconcile_persistent_for_test().await.unwrap();
        let generation = first.persistent_meta_for_test().await.unwrap().generation;
        first.shutdown_persistent().await;
        drop(first);
        assert_eq!(
            preflight_database(std::path::Path::new(&config.index.path)),
            ChildPreflight::Healthy,
            "only a caught-up mutation fence may persist a clean shutdown"
        );

        let (list_signal, mut list_attempted) = tokio::sync::mpsc::unbounded_channel();
        let backend = Arc::new(
            crate::test_helpers::FaultInjectBackend::new(authoritative)
                .signal_list_attempts("maven/", list_signal),
        );
        let list_attempts = backend.list_attempts();
        let storage = Storage::from_backend(backend);
        let reopened =
            reopen_persistent_with_startup_state(&config, &enabled, storage.clone(), true).await;

        assert_eq!(reopened.persistent_status(), Some(IndexStatus::Ready));
        assert!(reopened.persistent_protocol_ready());
        assert_eq!(
            reopened
                .persistent_meta_for_test()
                .await
                .unwrap()
                .generation,
            generation
        );
        let runtime = reopened.persistent.as_ref().unwrap();
        assert!(runtime.initial_reconciled.load(Ordering::Acquire));
        assert!(!full_reconcile_required(runtime));

        let cancel = tokio_util::sync::CancellationToken::new();
        let idle = runtime.background_waiting.notified();
        let handle = reopened
            .start_persistent_background(storage, cancel.clone())
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), idle)
            .await
            .expect("clean-start worker must reach its periodic idle state");
        assert!(list_attempts.lock().is_empty());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), list_attempted.recv())
                .await
                .is_err(),
            "a clean warm start must not perform an immediate root LIST"
        );

        tokio::time::timeout(Duration::from_secs(2), list_attempted.recv())
            .await
            .expect("periodic anti-entropy must still start at its configured interval")
            .expect("LIST signal channel must remain open");

        cancel.cancel();
        handle.await.unwrap();
        reopened.shutdown_persistent().await;
    }

    #[tokio::test]
    async fn abnormal_process_shutdown_forces_unclean_preflight_and_immediate_reconcile() {
        let storage_dir = tempfile::tempdir().unwrap();
        let authoritative = Storage::new_local(storage_dir.path().to_str().unwrap());
        authoritative
            .put("maven/com/acme/app/1.0/app-1.0.jar", b"jar")
            .await
            .unwrap();
        let index_dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.maven.repositories.clear();
        config.index.path = index_dir
            .path()
            .join("index.redb")
            .to_string_lossy()
            .into_owned();
        config.index.reconcile_interval_secs = 3600;
        let enabled = HashSet::from([RegistryType::Maven]);

        let first = RepoIndex::open_persistent_for_test(&config, &enabled, authoritative.clone())
            .await
            .unwrap();
        first.reconcile_persistent_for_test().await.unwrap();
        first
            .shutdown_persistent_until(Instant::now() + Duration::from_secs(5), false)
            .await;
        drop(first);
        assert_eq!(
            preflight_database(std::path::Path::new(&config.index.path)),
            ChildPreflight::HealthyAfterIntegrity,
            "an abnormal process-session boundary must never persist the fast-ready clean proof"
        );

        let (list_signal, mut list_attempted) = tokio::sync::mpsc::unbounded_channel();
        let storage = Storage::from_backend(Arc::new(
            crate::test_helpers::FaultInjectBackend::new(authoritative)
                .signal_list_attempts("maven/", list_signal),
        ));
        let reopened =
            reopen_persistent_with_startup_state(&config, &enabled, storage.clone(), false).await;
        assert_eq!(reopened.persistent_status(), Some(IndexStatus::Warming));
        assert!(!reopened.persistent_protocol_ready());

        let cancel = tokio_util::sync::CancellationToken::new();
        let handle = reopened
            .start_persistent_background(storage, cancel.clone())
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), list_attempted.recv())
            .await
            .expect("unclean startup must issue an immediate authoritative LIST")
            .expect("LIST signal channel must remain open");
        tokio::time::timeout(Duration::from_secs(2), async {
            while !reopened.persistent_protocol_ready() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("authoritative S2 must restore Ready");

        cancel.cancel();
        handle.await.unwrap();
        reopened.shutdown_persistent().await;
    }

    #[tokio::test]
    async fn physical_receipt_progress_wakes_shutdown_before_shared_deadline() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage = Storage::new_local(storage_dir.path().to_str().unwrap());
        storage
            .put("maven/com/acme/app/1.0/app-1.0.jar", b"jar")
            .await
            .unwrap();
        let index_dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.maven.repositories.clear();
        config.index.path = index_dir
            .path()
            .join("index.redb")
            .to_string_lossy()
            .into_owned();
        let enabled = HashSet::from([RegistryType::Maven]);
        let index = RepoIndex::open_persistent_for_test(&config, &enabled, storage)
            .await
            .unwrap();
        index.reconcile_persistent_for_test().await.unwrap();
        let runtime = Arc::clone(index.persistent.as_ref().unwrap());
        let persistent = current_persistent_index(&runtime).unwrap();
        let sequence = persistent.meta().await.unwrap().accepted_change_seq;
        let (send, receive) = oneshot::channel();
        {
            let mut fence = runtime.fence.lock();
            fence.physical_admitted = 1;
            fence.latest_physical_receipt = Some(PhysicalReceipt {
                ticket: 1,
                index: persistent,
                receive,
            });
            store_persistent_status_locked(&runtime, IndexStatus::Warming, &fence);
        }
        runtime.fence_notify.notify_one();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let receipt_taken = {
                    let fence = runtime.fence.lock();
                    fence.latest_physical_receipt.is_none()
                        && fence.physical_acked < fence.physical_admitted
                };
                if receipt_taken {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("coordinator must take the delayed receipt before shutdown starts");

        let shutdown_waiting = runtime.shutdown_waiting.notified();
        tokio::pin!(shutdown_waiting);
        shutdown_waiting.as_mut().enable();
        let shutting_down = Arc::clone(&index);
        let shutdown = tokio::spawn(async move {
            shutting_down
                .shutdown_persistent_until(Instant::now() + Duration::from_secs(2), true)
                .await;
        });
        tokio::time::timeout(Duration::from_secs(1), &mut shutdown_waiting)
            .await
            .expect("shutdown must be waiting for the coordinator's receipt progress edge");
        send.send(Ok(sequence)).unwrap();
        tokio::time::timeout(Duration::from_millis(500), shutdown)
            .await
            .expect("receipt settlement must wake shutdown without consuming its full deadline")
            .unwrap();
        drop(index);

        assert_eq!(
            preflight_database(std::path::Path::new(&config.index.path)),
            ChildPreflight::Healthy,
            "acknowledged receipt and caught-up durable meta should still close cleanly"
        );
    }

    #[tokio::test]
    async fn clean_file_with_durable_dirty_state_still_reconciles_fail_closed() {
        let storage_dir = tempfile::tempdir().unwrap();
        let authoritative = Storage::new_local(storage_dir.path().to_str().unwrap());
        authoritative
            .put("maven/com/acme/app/1.0/app-1.0.jar", b"jar")
            .await
            .unwrap();
        let index_dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.maven.repositories.clear();
        config.index.path = index_dir
            .path()
            .join("index.redb")
            .to_string_lossy()
            .into_owned();
        config.index.reconcile_interval_secs = 3600;
        let enabled = HashSet::from([RegistryType::Maven]);

        let first = RepoIndex::open_persistent_for_test(&config, &enabled, authoritative.clone())
            .await
            .unwrap();
        first.reconcile_persistent_for_test().await.unwrap();
        current_persistent_index(first.persistent.as_ref().unwrap())
            .unwrap()
            .register_change(ChangeEvent::GlobalDirty)
            .await
            .unwrap();
        first.shutdown_persistent().await;
        drop(first);
        assert_eq!(
            preflight_database(std::path::Path::new(&config.index.path)),
            ChildPreflight::HealthyAfterIntegrity,
            "durable dirty state must prevent a clean preflight classification"
        );

        let (list_signal, mut list_attempted) = tokio::sync::mpsc::unbounded_channel();
        let storage = Storage::from_backend(Arc::new(
            crate::test_helpers::FaultInjectBackend::new(authoritative)
                .signal_list_attempts("maven/", list_signal),
        ));
        let reopened =
            reopen_persistent_with_startup_state(&config, &enabled, storage.clone(), true).await;
        assert_eq!(reopened.persistent_status(), Some(IndexStatus::Warming));
        assert!(!reopened.persistent_protocol_ready());
        assert!(full_reconcile_required(
            reopened.persistent.as_ref().unwrap()
        ));

        let cancel = tokio_util::sync::CancellationToken::new();
        let handle = reopened
            .start_persistent_background(storage, cancel.clone())
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), list_attempted.recv())
            .await
            .expect("durable dirty state must force an immediate authoritative LIST")
            .expect("LIST signal channel must remain open");
        tokio::time::timeout(Duration::from_secs(2), async {
            while !reopened.persistent_protocol_ready() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the authoritative reconcile must restore Ready");

        cancel.cancel();
        handle.await.unwrap();
        reopened.shutdown_persistent().await;
    }

    #[tokio::test]
    async fn legacy_clean_marker_requires_one_authoritative_reconcile() {
        let storage_dir = tempfile::tempdir().unwrap();
        let authoritative = Storage::new_local(storage_dir.path().to_str().unwrap());
        authoritative
            .put("maven/com/acme/app/1.0/app-1.0.jar", b"jar")
            .await
            .unwrap();
        let index_dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.maven.repositories.clear();
        config.index.path = index_dir
            .path()
            .join("index.redb")
            .to_string_lossy()
            .into_owned();
        config.index.reconcile_interval_secs = 3600;
        let enabled = HashSet::from([RegistryType::Maven]);

        let legacy_digest = persistent_config_digest_with_schema(&config, 1).unwrap();
        let legacy_persistent =
            PersistentIndex::open(&config.index.path, legacy_digest.clone()).unwrap();
        let legacy_meta = legacy_persistent.meta().await.unwrap();
        let legacy = RepoIndex::from_persistent(
            &config,
            &enabled,
            authoritative.clone(),
            Some(legacy_persistent),
            Some(legacy_meta),
            legacy_digest,
        )
        .await
        .unwrap();
        legacy.reconcile_persistent_for_test().await.unwrap();
        legacy.shutdown_persistent().await;
        drop(legacy);
        assert_eq!(
            preflight_database(std::path::Path::new(&config.index.path)),
            ChildPreflight::Healthy,
            "the legacy fixture intentionally carries its old clean marker"
        );

        let (list_signal, mut list_attempted) = tokio::sync::mpsc::unbounded_channel();
        let storage = Storage::from_backend(Arc::new(
            crate::test_helpers::FaultInjectBackend::new(authoritative)
                .signal_list_attempts("maven/", list_signal),
        ));
        let reopened =
            reopen_persistent_with_startup_state(&config, &enabled, storage.clone(), true).await;
        assert_eq!(reopened.persistent_status(), Some(IndexStatus::Warming));
        assert!(!reopened.persistent_protocol_ready());

        let cancel = tokio_util::sync::CancellationToken::new();
        let handle = reopened
            .start_persistent_background(storage, cancel.clone())
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), list_attempted.recv())
            .await
            .expect("topology proof version change must force one immediate S3 reconcile")
            .expect("LIST signal channel must remain open");
        tokio::time::timeout(Duration::from_secs(2), async {
            while !reopened.persistent_protocol_ready() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("legacy PVC must become Ready only after current S2 publication");

        cancel.cancel();
        handle.await.unwrap();
        reopened.shutdown_persistent().await;
    }

    #[tokio::test]
    async fn persistent_object_revision_detects_reseed_with_reused_counters() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage = Storage::new_local(storage_dir.path().to_str().unwrap());
        storage
            .put("maven/com/acme/app/1.0/app-1.0.jar", b"artifact")
            .await
            .unwrap();
        storage
            .put("maven/com/acme/app/2.0/app-2.0.jar", b"artifact-two")
            .await
            .unwrap();
        let first_dir = tempfile::tempdir().unwrap();
        let second_dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.maven.repositories.clear();
        config.index.path = first_dir
            .path()
            .join("index.redb")
            .to_string_lossy()
            .into_owned();
        let enabled = HashSet::from([RegistryType::Maven]);
        let index = RepoIndex::open_persistent_for_test(&config, &enabled, storage.clone())
            .await
            .unwrap();
        index.reconcile_persistent_for_test().await.unwrap();
        let before = index
            .persistent_object_page("maven/", None, 1)
            .await
            .unwrap();
        assert_eq!(before.items.len(), 1);
        let next_after = before
            .next_after
            .clone()
            .expect("the first page must have a continuation");

        let runtime = index.persistent.as_ref().unwrap();
        let replacement = PersistentIndex::open(
            second_dir.path().join("index.redb"),
            runtime.config_digest.clone(),
        )
        .unwrap();
        let replacement_meta = persistent_builder::reconcile(
            Arc::clone(&replacement),
            storage,
            runtime.config_digest.clone(),
            true,
            false,
        )
        .await
        .unwrap();
        assert_eq!(replacement_meta.generation, before.revision.generation);
        assert_eq!(
            replacement_meta.active_watermark(),
            before.revision.watermark
        );
        assert_eq!(replacement_meta.active_slot, before.revision.active_slot);

        let replaced = {
            let _fence = runtime.fence.lock();
            runtime.index.swap(Some(replacement))
        };
        let after_page = index
            .persistent_object_page("maven/", Some(next_after), 1)
            .await
            .unwrap();
        assert_eq!(after_page.items.len(), 1);
        let after = after_page.revision;
        assert_ne!(
            after, before.revision,
            "a new database identity must supersede pages even when counters and slot repeat"
        );
        assert_ne!(after.db_uuid, before.revision.db_uuid);

        if let Some(replaced) = replaced {
            replaced.shutdown().await;
        }
        index.shutdown_persistent().await;
    }

    #[tokio::test]
    async fn completed_maven_cache_write_repairs_physical_event_incrementally() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage = Storage::new_local(storage_dir.path().to_str().unwrap());
        let artifact = "maven/com/acme/app/1.0/app-1.0.jar";
        storage.put(artifact, b"old").await.unwrap();
        let index_dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.maven.repositories.clear();
        config.index.path = index_dir
            .path()
            .join("index.redb")
            .to_string_lossy()
            .into_owned();
        let enabled = HashSet::from([RegistryType::Maven]);
        let index = RepoIndex::open_persistent_for_test(&config, &enabled, storage.clone())
            .await
            .unwrap();
        index.reconcile_persistent_for_test().await.unwrap();
        storage.set_mutation_observer(index.clone());

        storage.put(artifact, b"replacement").await.unwrap();
        assert!(!index.persistent_protocol_ready());
        index.invalidate_cached_path("maven", artifact);
        tokio::time::timeout(Duration::from_secs(2), async {
            while !index.persistent_protocol_ready() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("typed cache invalidation catches up without a full reconcile");

        let runtime = index.persistent.as_ref().unwrap();
        let persistent = current_persistent_index(runtime).unwrap();
        let meta = persistent.meta().await.unwrap();
        assert_eq!(meta.generation, 2);
        assert!(!meta.global_dirty);
        assert_eq!(meta.active_watermark(), meta.accepted_change_seq);
        assert_eq!(
            persistent
                .get_object_in_slot(meta.active_slot.unwrap(), artifact)
                .await
                .unwrap()
                .unwrap()
                .size,
            b"replacement".len() as u64
        );
        storage.clear_mutation_observer();
        index.shutdown_persistent().await;
    }

    #[tokio::test]
    async fn immutable_already_exists_retry_repairs_missing_redb_row_incrementally() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage = Storage::new_local(storage_dir.path().to_str().unwrap());
        let artifact = "maven/com/acme/app/1.0/app-1.0.jar";
        storage
            .put(artifact, b"committed-before-crash")
            .await
            .unwrap();
        let index_dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.maven.repositories.clear();
        config.index.path = index_dir
            .path()
            .join("index.redb")
            .to_string_lossy()
            .into_owned();
        let enabled = HashSet::from([RegistryType::Maven]);
        let index = RepoIndex::open_persistent_for_test(&config, &enabled, storage.clone())
            .await
            .unwrap();
        index.reconcile_persistent_for_test().await.unwrap();
        storage.set_mutation_observer(index.clone());

        assert!(matches!(
            storage.put_if_absent(artifact, b"retry-candidate").await,
            Err(crate::storage::StorageError::AlreadyExists)
        ));
        index.invalidate_cached_path("maven", artifact);
        tokio::time::timeout(Duration::from_secs(2), async {
            while !index.persistent_protocol_ready() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("AlreadyExists retry must repair the exact existing S3 row");

        let runtime = index.persistent.as_ref().unwrap();
        let persistent = current_persistent_index(runtime).unwrap();
        let meta = persistent.meta().await.unwrap();
        assert_eq!(meta.generation, 2);
        assert_eq!(meta.active_watermark(), meta.accepted_change_seq);
        assert_eq!(
            persistent
                .get_object_in_slot(meta.active_slot.unwrap(), artifact)
                .await
                .unwrap()
                .unwrap()
                .size,
            b"committed-before-crash".len() as u64,
            "redb must index the authoritative winner, not retry bytes"
        );
        storage.clear_mutation_observer();
        index.shutdown_persistent().await;
    }

    #[tokio::test]
    async fn semantic_overload_collapses_to_one_bounded_full_reconcile_fence() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage = Storage::new_local(storage_dir.path().to_str().unwrap());
        let index_dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.maven.repositories.clear();
        config.index.path = index_dir
            .path()
            .join("index.redb")
            .to_string_lossy()
            .into_owned();
        let enabled = HashSet::from([RegistryType::Maven]);
        let index = RepoIndex::open_persistent_for_test(&config, &enabled, storage)
            .await
            .unwrap();
        index.reconcile_persistent_for_test().await.unwrap();
        let runtime = index.persistent.as_ref().unwrap();
        let permits = (0..SEMANTIC_CHANGE_CONCURRENCY)
            .map(|_| {
                runtime
                    .semantic_permits
                    .clone()
                    .try_acquire_owned()
                    .unwrap()
            })
            .collect::<Vec<_>>();

        for sequence in 0..100 {
            index.invalidate_maven_path("", &format!("com/acme/app/{sequence}.jar"));
        }
        assert_eq!(
            runtime.semantic_tasks.len(),
            0,
            "overflow must not create a waiter or Tokio task per event"
        );
        assert!(full_reconcile_required(runtime));
        assert!(!index.persistent_protocol_ready());

        drop(permits);
        index.shutdown_persistent().await;
        assert_eq!(
            preflight_database(std::path::Path::new(&config.index.path)),
            ChildPreflight::HealthyAfterIntegrity,
            "an in-memory full-reconcile requirement must survive restart as an unclean DB"
        );
    }

    #[tokio::test]
    async fn physical_mutation_burst_creates_no_per_event_tokio_tasks() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage = Storage::new_local(storage_dir.path().to_str().unwrap());
        let index_dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.maven.repositories.clear();
        config.index.path = index_dir
            .path()
            .join("index.redb")
            .to_string_lossy()
            .into_owned();
        let enabled = HashSet::from([RegistryType::Maven]);
        let index = RepoIndex::open_persistent_for_test(&config, &enabled, storage)
            .await
            .unwrap();
        index.reconcile_persistent_for_test().await.unwrap();
        let runtime = index.persistent.as_ref().unwrap();

        for sequence in 0..100 {
            index.observe(StorageMutation {
                key: format!("maven/com/acme/app/{sequence}.jar"),
                kind: crate::storage::StorageMutationKind::Put,
                outcome: StorageMutationOutcome::Confirmed,
            });
        }
        assert_eq!(
            runtime.semantic_tasks.len(),
            0,
            "physical invalidation admission must not create Tokio tasks"
        );
        let persistent = current_persistent_index(runtime).unwrap();
        let meta = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let meta = persistent.meta().await.unwrap();
                if meta.accepted_change_seq == 100 {
                    break meta;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the bounded writer queue must durably register the burst");
        assert_eq!(meta.accepted_change_seq, 100);
        assert!(meta.global_dirty);
        assert!(!index.persistent_protocol_ready());

        index.shutdown_persistent().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn physical_admission_and_readiness_publication_share_one_fence() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage = Storage::new_local(storage_dir.path().to_str().unwrap());
        let artifact = "maven/com/acme/app/1.0/app-1.0.jar";
        storage.put(artifact, b"jar").await.unwrap();
        let index_dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.maven.repositories.clear();
        config.index.path = index_dir
            .path()
            .join("index.redb")
            .to_string_lossy()
            .into_owned();
        let enabled = HashSet::from([RegistryType::Maven]);
        let index = RepoIndex::open_persistent_for_test(&config, &enabled, storage)
            .await
            .unwrap();
        index.reconcile_persistent_for_test().await.unwrap();
        let runtime = index.persistent.as_ref().unwrap();
        let captured = Arc::new(std::sync::Barrier::new(2));
        let release = Arc::new(std::sync::Barrier::new(2));
        *runtime.physical_admission_barrier.lock() =
            Some((Arc::clone(&captured), Arc::clone(&release)));

        let observer = {
            let index = Arc::clone(&index);
            std::thread::spawn(move || {
                index.observe(StorageMutation {
                    key: artifact.to_string(),
                    kind: crate::storage::StorageMutationKind::Put,
                    outcome: StorageMutationOutcome::Confirmed,
                });
            })
        };
        tokio::task::spawn_blocking(move || captured.wait())
            .await
            .unwrap();

        let readiness = {
            let index = Arc::clone(&index);
            tokio::task::spawn_blocking(move || index.persistent_protocol_ready())
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !readiness.is_finished(),
            "readiness must not pass between epoch publication and physical writer admission"
        );

        tokio::task::spawn_blocking(move || release.wait())
            .await
            .unwrap();
        observer.join().unwrap();
        assert!(!readiness.await.unwrap());
        *runtime.physical_admission_barrier.lock() = None;

        index.invalidate_cached_path("maven", artifact);
        tokio::time::timeout(Duration::from_secs(2), async {
            while !index.persistent_protocol_ready() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the paired semantic repair must close the admitted physical fence");
        assert!(!full_reconcile_required(runtime));

        index.shutdown_persistent().await;
    }

    #[tokio::test]
    async fn out_of_order_semantic_repairs_settle_ready_without_periodic_reconcile() {
        let storage_dir = tempfile::tempdir().unwrap();
        let authoritative = Storage::new_local(storage_dir.path().to_str().unwrap());
        let repository = "npm-private";
        let first = "first-pkg";
        let second = "second-pkg";
        let first_current = crate::npm_layout::hosted_packument_current_key(repository, first);
        let captured = Arc::new(tokio::sync::Barrier::new(2));
        let release = Arc::new(tokio::sync::Barrier::new(2));
        let backend = Arc::new(
            crate::test_helpers::FaultInjectBackend::new(authoritative.clone()).barrier_get(
                first_current,
                Arc::clone(&captured),
                Arc::clone(&release),
            ),
        );
        let list_attempts = backend.list_attempts();
        let storage = Storage::from_backend(backend);
        let index_dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.maven.repositories.clear();
        config.index.path = index_dir
            .path()
            .join("index.redb")
            .to_string_lossy()
            .into_owned();
        let enabled = HashSet::from([RegistryType::Npm]);
        let index = RepoIndex::open_persistent_for_test(&config, &enabled, storage)
            .await
            .unwrap();
        index.reconcile_persistent_for_test().await.unwrap();
        list_attempts.lock().clear();
        put_hosted_npm_generation(
            &authoritative,
            repository,
            first,
            &[("1.0.0", b"first")],
            &[("latest", "1.0.0")],
        )
        .await;
        put_hosted_npm_generation(
            &authoritative,
            repository,
            second,
            &[("1.0.0", b"second")],
            &[("latest", "1.0.0")],
        )
        .await;

        index.invalidate_npm_hosted(repository, first);
        captured.wait().await;
        let initial_generation = index.persistent_meta_for_test().await.unwrap().generation;
        index.invalidate_npm_hosted(repository, second);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if index.persistent_meta_for_test().await.unwrap().generation > initial_generation {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the newer unrelated repair must complete while the older one is blocked");

        release.wait().await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while !index.persistent_protocol_ready() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("quiescent clean meta must resolve all epochs independent of completion order");
        assert!(
            !list_attempts.lock().iter().any(|prefix| prefix == "npm/"),
            "settling out-of-order repairs must not wait for or trigger a root S3 reconcile"
        );
        let meta = index.persistent_meta_for_test().await.unwrap();
        assert!(!meta.global_dirty);
        assert_eq!(meta.active_watermark(), meta.accepted_change_seq);

        index.shutdown_persistent().await;
    }

    #[tokio::test]
    async fn acknowledged_storage_mutation_never_waits_for_an_unavailable_index_writer() {
        let storage_dir = tempfile::tempdir().unwrap();
        let authoritative = Storage::new_local(storage_dir.path().to_str().unwrap());
        let storage = Storage::from_backend(Arc::new(
            crate::test_helpers::FaultInjectBackend::new(authoritative),
        ));
        let index_dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.maven.repositories.clear();
        config.index.path = index_dir
            .path()
            .join("index.redb")
            .to_string_lossy()
            .into_owned();
        let enabled = HashSet::from([RegistryType::Maven]);
        let index = RepoIndex::open_persistent_for_test(&config, &enabled, storage.clone())
            .await
            .unwrap();
        index.reconcile_persistent_for_test().await.unwrap();
        storage.set_mutation_observer(index.clone());

        let runtime = index.persistent.as_ref().unwrap();
        let writer = current_persistent_index(runtime).unwrap();
        writer.shutdown().await;
        assert!(!writer.writer_healthy());

        tokio::time::timeout(
            Duration::from_secs(1),
            storage.put("maven/com/acme/app/1.0/app-1.0.jar", b"body"),
        )
        .await
        .expect("derived index must not enter acknowledged storage-write latency")
        .unwrap();
        assert_eq!(
            storage
                .get("maven/com/acme/app/1.0/app-1.0.jar")
                .await
                .unwrap()
                .as_ref(),
            b"body"
        );
        assert_eq!(index.persistent_status(), Some(IndexStatus::Degraded));

        storage.clear_mutation_observer();
        index.shutdown_persistent().await;
    }

    #[tokio::test]
    async fn aborted_semantic_repair_forces_immediate_authoritative_reconcile() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage = Storage::new_local(storage_dir.path().to_str().unwrap());
        let index_dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.maven.repositories.clear();
        config.index.path = index_dir
            .path()
            .join("index.redb")
            .to_string_lossy()
            .into_owned();
        let enabled = HashSet::from([RegistryType::Maven]);
        let index = RepoIndex::open_persistent_for_test(&config, &enabled, storage)
            .await
            .unwrap();
        index.reconcile_persistent_for_test().await.unwrap();
        assert!(index.persistent_protocol_ready());

        let runtime = index.persistent.as_ref().unwrap();
        let captured = Arc::new(tokio::sync::Barrier::new(2));
        let release = Arc::new(tokio::sync::Barrier::new(2));
        *runtime.semantic_registration_barrier.lock() = Some((Arc::clone(&captured), release));
        let reconcile_notified = runtime.notify.notified();
        tokio::pin!(reconcile_notified);

        index.invalidate("maven");
        tokio::time::timeout(Duration::from_secs(1), captured.wait())
            .await
            .expect("semantic task must reach the pre-registration barrier");
        runtime
            .semantic_abort_handles
            .lock()
            .last()
            .expect("semantic task abort handle")
            .abort();
        tokio::time::timeout(Duration::from_secs(1), &mut reconcile_notified)
            .await
            .expect("abnormal task drop must wake authoritative reconciliation");
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if runtime.fence.lock().semantic_inflight == 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("aborted task must release its inflight ticket");

        assert!(full_reconcile_required(runtime));
        assert_eq!(index.persistent_status(), Some(IndexStatus::Degraded));
        assert!(!index.persistent_protocol_ready());

        *runtime.semantic_registration_barrier.lock() = None;
        index.reconcile_persistent_for_test().await.unwrap();
        assert!(index.persistent_protocol_ready());
        index.shutdown_persistent().await;
    }

    #[tokio::test]
    async fn persistent_reconcile_notifications_do_not_bypass_failure_backoff() {
        let storage_dir = tempfile::tempdir().unwrap();
        let authoritative = Storage::new_local(storage_dir.path().to_str().unwrap());
        let (list_signal, mut list_attempted) = tokio::sync::mpsc::unbounded_channel();
        let backend = Arc::new(
            crate::test_helpers::FaultInjectBackend::new(authoritative)
                .fail_list("maven/")
                .signal_list_attempts("maven/", list_signal),
        );
        let list_attempts = backend.list_attempts();
        let storage = Storage::from_backend(backend);
        let index_dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.maven.repositories.clear();
        config.index.path = index_dir
            .path()
            .join("index.redb")
            .to_string_lossy()
            .into_owned();
        let enabled = HashSet::from([RegistryType::Maven]);
        let index = RepoIndex::open_persistent_for_test(&config, &enabled, storage.clone())
            .await
            .unwrap();
        let runtime = index.persistent.as_ref().unwrap();
        runtime
            .reconcile_retry_delay_millis
            .store(200, Ordering::Release);
        let cancel = tokio_util::sync::CancellationToken::new();
        let handle = index
            .start_persistent_background(storage, cancel.clone())
            .unwrap();

        tokio::time::timeout(Duration::from_secs(1), list_attempted.recv())
            .await
            .expect("first LIST attempt must be admitted")
            .expect("LIST attempt channel must remain open");
        assert_eq!(list_attempts.lock().len(), 1);
        runtime.reconcile_retry_scheduled.notified().await;
        assert_eq!(index.persistent_status(), Some(IndexStatus::Degraded));

        for _ in 0..32 {
            runtime.notify.notify_one();
            tokio::task::yield_now().await;
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(50), list_attempted.recv())
                .await
                .is_err(),
            "request notifications must not bypass the retry deadline"
        );
        assert_eq!(
            list_attempts.lock().len(),
            1,
            "request notifications must not turn a failed reconcile into a LIST storm"
        );

        tokio::time::timeout(Duration::from_secs(1), list_attempted.recv())
            .await
            .expect("second LIST attempt must run after the retry deadline")
            .expect("LIST attempt channel must remain open");
        assert_eq!(list_attempts.lock().len(), 2);

        cancel.cancel();
        handle.await.unwrap();
        index.shutdown_persistent().await;
    }

    #[tokio::test]
    async fn incompatible_pvc_generation_stays_hidden_until_current_topology_reconciles() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage = Storage::new_local(storage_dir.path().to_str().unwrap());
        storage
            .put("maven/com/acme/app/1.0/app-1.0.jar", b"jar")
            .await
            .unwrap();
        let index_dir = tempfile::tempdir().unwrap();
        let index_path = index_dir.path().join("index.redb");
        let mut first_config = Config::default();
        first_config.maven.repositories.clear();
        first_config.index.path = index_path.to_string_lossy().into_owned();
        let enabled = HashSet::from([RegistryType::Maven]);
        let first = RepoIndex::open_persistent_for_test(&first_config, &enabled, storage.clone())
            .await
            .unwrap();
        first.reconcile_persistent_for_test().await.unwrap();
        first.shutdown_persistent().await;
        drop(first);

        let mut changed_config = first_config.clone();
        changed_config.maven.repositories = vec![crate::config::MavenRepository::Hosted {
            name: "releases".to_string(),
            version_policy: crate::config::MavenVersionPolicy::Mixed,
            write_policy: crate::config::MavenWritePolicy::AllowOnce,
        }];
        let reopened =
            reopen_persistent_with_startup_state(&changed_config, &enabled, storage, true).await;
        assert!(reopened.persistent_writer_healthy());
        assert!(matches!(
            reopened
                .persistent_maven_children(
                    vec!["maven/repositories/releases/".to_string()],
                    String::new(),
                    None,
                    10,
                )
                .await,
            Err(StoreError::WriterUnavailable)
        ));

        reopened.reconcile_persistent_for_test().await.unwrap();
        assert!(reopened
            .persistent_maven_children(
                vec!["maven/repositories/releases/".to_string()],
                String::new(),
                None,
                10,
            )
            .await
            .is_ok());
        reopened.shutdown_persistent().await;
    }

    #[tokio::test]
    async fn stopped_writer_is_reopened_and_reconciled_without_stopping_protocol_storage() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage = Storage::new_local(storage_dir.path().to_str().unwrap());
        let artifact = "maven/com/acme/app/1.0/app-1.0.jar";
        storage.put(artifact, b"jar").await.unwrap();
        let index_dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.maven.repositories.clear();
        config.index.path = index_dir
            .path()
            .join("index.redb")
            .to_string_lossy()
            .into_owned();
        config.index.reconcile_interval_secs = 3600;
        let enabled = HashSet::from([RegistryType::Maven]);
        let index = RepoIndex::open_persistent_for_test(&config, &enabled, storage.clone())
            .await
            .unwrap();
        index.reconcile_persistent_for_test().await.unwrap();
        let runtime = index.persistent.as_ref().unwrap();
        let old = current_persistent_index(runtime).unwrap();
        let old_generation = old.meta().await.unwrap().generation;
        old.shutdown().await;
        drop(old);
        assert!(!index.persistent_writer_healthy());
        assert_eq!(storage.get(artifact).await.unwrap().as_ref(), b"jar");

        let cancel = tokio_util::sync::CancellationToken::new();
        let handle = index
            .start_persistent_background(storage, cancel.clone())
            .unwrap();
        require_full_reconcile(runtime);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if index.persistent_protocol_ready() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("writer recovery must reopen and publish a current generation");
        let recovered = current_persistent_index(runtime).unwrap();
        assert!(recovered.writer_healthy());
        assert!(recovered.meta().await.unwrap().generation > old_generation);

        cancel.cancel();
        handle.await.unwrap();
        index.shutdown_persistent().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn superseded_writer_completion_cannot_publish_sequence_into_reseeded_runtime() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage = Storage::new_local(storage_dir.path().to_str().unwrap());
        let artifact = "maven/com/acme/app/1.0/app-1.0.jar";
        storage.put(artifact, b"jar").await.unwrap();
        let index_dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.maven.repositories.clear();
        config.index.path = index_dir
            .path()
            .join("old.redb")
            .to_string_lossy()
            .into_owned();
        let enabled = HashSet::from([RegistryType::Maven]);
        let repo_index = RepoIndex::open_persistent_for_test(&config, &enabled, storage.clone())
            .await
            .unwrap();
        repo_index.reconcile_persistent_for_test().await.unwrap();
        let runtime = Arc::clone(repo_index.persistent.as_ref().unwrap());
        let old = current_persistent_index(&runtime).unwrap();
        let captured = Arc::new(std::sync::Barrier::new(2));
        let release = Arc::new(std::sync::Barrier::new(2));
        *runtime.publication_barrier.lock() = Some((Arc::clone(&captured), Arc::clone(&release)));

        let event_epoch = {
            let _fence = runtime.fence.lock();
            runtime.request_epoch.fetch_add(1, Ordering::AcqRel) + 1
        };
        let task = tokio::spawn(process_persistent_change(
            Arc::clone(&runtime),
            event_epoch,
            ChangeEvent::MavenPathChanged {
                repository: String::new(),
                path: "com/acme/app/1.0/app-1.0.jar".to_string(),
            },
        ));
        tokio::task::spawn_blocking(move || captured.wait())
            .await
            .unwrap();

        let fresh = PersistentIndex::open(
            index_dir.path().join("fresh.redb"),
            runtime.config_digest.clone(),
        )
        .unwrap();
        let replacement = {
            let runtime = Arc::clone(&runtime);
            let fresh = Arc::clone(&fresh);
            tokio::task::spawn_blocking(move || {
                let mut fence = runtime.fence.lock();
                runtime.index.store(Some(fresh));
                runtime.requested_sequence.store(0, Ordering::Release);
                runtime.published_sequence.store(0, Ordering::Release);
                runtime.resolved_epoch.store(0, Ordering::Release);
                fence.latest_physical_receipt = None;
                fence.physical_admitted = 0;
                fence.physical_acked = 0;
            })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !replacement.is_finished(),
            "writer replacement must wait while current-handle publication holds the fence"
        );
        tokio::task::spawn_blocking(move || release.wait())
            .await
            .unwrap();
        replacement.await.unwrap();
        *runtime.publication_barrier.lock() = None;
        task.await.unwrap();

        assert_eq!(runtime.requested_sequence.load(Ordering::Acquire), 0);
        assert_eq!(runtime.published_sequence.load(Ordering::Acquire), 0);
        assert_eq!(fresh.meta().await.unwrap().accepted_change_seq, 0);
        assert!(full_reconcile_required(&runtime));

        old.shutdown().await;
        fresh.shutdown().await;
    }

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

        let npm_index = &index.indexes[&RegistryType::Npm];
        loop {
            let changed = npm_index.changed.notified();
            if npm_index.published_generation.load(Ordering::Acquire) >= 1
                && index.status("npm") == Some(IndexStatus::Degraded)
            {
                break;
            }
            changed.await;
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
        loop {
            let changed = npm_index.changed.notified();
            if index.status("npm") == Some(IndexStatus::Ready) {
                break;
            }
            changed.await;
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

        let npm_index = &index.indexes[&RegistryType::Npm];
        loop {
            let changed = npm_index.changed.notified();
            if npm_index.failed_generation.load(Ordering::Acquire) >= 1 {
                break;
            }
            changed.await;
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

        let changed = npm_index.changed.notified();
        tokio::time::advance(Duration::from_secs(16)).await;
        changed.await;
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
                FileMeta::local(100, 5),
            ),
            (
                "docker/library/app/blobs/sha256:lyr".to_string(),
                FileMeta::local(340, 9),
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
