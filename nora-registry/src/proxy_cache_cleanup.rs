// Copyright (c) 2026 The NORA Authors
// SPDX-License-Identifier: MIT

//! Bounded, access-aware cleanup for rebuildable Maven/npm proxy caches.
//!
//! The cleanup namespace is deliberately narrower than retention: only
//! configured named Maven proxy repositories and explicit npm `/proxy/`
//! layouts are eligible. Hosted, group, imported, migrated and legacy Maven
//! objects are never candidates.

use prometheus::{
    register_histogram, register_int_counter_vec, register_int_gauge, Histogram, IntCounterVec,
    IntGauge,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncReadExt;
use tokio::sync::{Mutex, Notify};
use tracing::{info, warn};

use crate::config::{Config, MavenRepository, NpmRepository, ProxyCacheCleanupConfig};
use crate::npm_layout::{parse_npm_object_key, NpmObjectKind};
use crate::repo_index::{IndexStatus, IndexedObject, RepoIndex};
use crate::storage::{FileMeta, Storage, StorageError};
use crate::{acquire_publish_lock, PublishLocks};

const ACCESS_SCHEMA_V1: u8 = 1;
const SESSION_SCHEMA_V1: u8 = 1;
const ACCESS_PREFIX: &str = ".nora-proxy-access/v1";
const SESSION_KEY: &str = ".nora-proxy-access/session-v1.json";
const TOUCH_CAPACITY: usize = 16_384;
const TOUCH_COALESCE: Duration = Duration::from_secs(3_600);
const TOUCH_RETRY: Duration = Duration::from_secs(30);
const CLEANUP_INDEX_PAGE_SIZE: usize = 1_000;

static CLEANUP_DELETED: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "nora_proxy_cache_cleanup_deleted_total",
        "Proxy-cache payloads deleted after age, idle and identity revalidation",
        &["registry"]
    )
    .expect("proxy cache cleanup deleted metric")
});

static CLEANUP_BYTES: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "nora_proxy_cache_cleanup_bytes_freed_total",
        "Proxy-cache payload bytes deleted",
        &["registry"]
    )
    .expect("proxy cache cleanup bytes metric")
});

static CLEANUP_SKIPPED: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "nora_proxy_cache_cleanup_skipped_total",
        "Proxy-cache cleanup candidates kept for a bounded reason",
        &["reason"]
    )
    .expect("proxy cache cleanup skipped metric")
});

static CLEANUP_DURATION: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram!(
        "nora_proxy_cache_cleanup_duration_seconds",
        "Duration of proxy-cache cleanup runs",
        vec![0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0]
    )
    .expect("proxy cache cleanup duration metric")
});

static CLEANUP_LAST_RUN: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "nora_proxy_cache_cleanup_last_run_timestamp",
        "Unix timestamp of the last completed proxy-cache cleanup run"
    )
    .expect("proxy cache cleanup last run metric")
});

static TOUCH_QUEUE_DEPTH: LazyLock<IntGauge> = LazyLock::new(|| {
    register_int_gauge!(
        "nora_proxy_cache_touch_queue_depth",
        "Distinct proxy-cache access markers waiting for background persistence"
    )
    .expect("proxy cache touch queue depth metric")
});

static TOUCH_FAILURES: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec!(
        "nora_proxy_cache_touch_failures_total",
        "Proxy-cache access-marker failures by bounded error class",
        &["reason"]
    )
    .expect("proxy cache touch failures metric")
});

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct AccessMarker {
    schema: u8,
    payload_key: String,
    payload_sha256: String,
    last_accessed_at_unix: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct SessionMarker {
    schema: u8,
    state: SessionState,
    session_id: String,
    started_at_unix: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    closed_at_unix: Option<u64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum SessionState {
    Active,
    Clean,
    Disabled,
}

#[derive(Debug)]
struct PendingTouch {
    last_accessed_at_unix: u64,
    due: Instant,
}

#[derive(Default)]
struct TouchState {
    pending: HashMap<String, PendingTouch>,
    force_flush: bool,
}

struct AccessInner {
    storage: Storage,
    publish_locks: PublishLocks,
    state: Mutex<TouchState>,
    changed: Notify,
    space_available: Notify,
    recovery_needed: AtomicBool,
    worker_drained: AtomicBool,
    session_id: String,
    session_started_at_unix: u64,
}

/// Process-local handle for durable, bounded proxy-cache access tracking.
///
/// A session marker is persisted before HTTP starts. If the process exits
/// before every queued touch is durable, the marker survives; the next process
/// baselines every in-scope payload and performs no deletion in that run.
#[derive(Clone)]
pub struct ProxyCacheAccess {
    inner: Arc<AccessInner>,
}

impl ProxyCacheAccess {
    pub async fn start_session(
        storage: Storage,
        publish_locks: PublishLocks,
    ) -> Result<Self, StorageError> {
        let started_at_unix = now_unix_secs();
        let recovery_needed = match storage.get(SESSION_KEY).await {
            Ok(bytes) => serde_json::from_slice::<SessionMarker>(&bytes).map_or(true, |marker| {
                !trusted_clean_session(&marker, started_at_unix)
            }),
            // Absence is the first enablement or a transition from a version
            // that did not persist clean tracking state. Baseline before any
            // deletion; markerless payload handling alone cannot prove that
            // stale markers did not survive an earlier disabled interval.
            Err(StorageError::NotFound) => true,
            Err(error) => return Err(error),
        };
        let session_id = uuid::Uuid::new_v4().to_string();
        let marker = SessionMarker {
            schema: SESSION_SCHEMA_V1,
            state: SessionState::Active,
            session_id: session_id.clone(),
            started_at_unix,
            closed_at_unix: None,
        };
        let bytes = serde_json::to_vec(&marker).map_err(|_| StorageError::IntegrityViolation)?;
        put_and_verify(&storage, SESSION_KEY, &bytes).await?;
        Ok(Self {
            inner: Arc::new(AccessInner {
                storage,
                publish_locks,
                state: Mutex::new(TouchState::default()),
                changed: Notify::new(),
                space_available: Notify::new(),
                recovery_needed: AtomicBool::new(recovery_needed),
                worker_drained: AtomicBool::new(false),
                session_id,
                session_started_at_unix: started_at_unix,
            }),
        })
    }

    /// Invalidate a previously clean tracking checkpoint when NORA starts with
    /// cleanup disabled. A later re-enable must baseline because reads during
    /// this disabled runtime were not tracked.
    pub async fn mark_tracking_disabled(storage: &Storage) -> Result<(), StorageError> {
        match storage.get(SESSION_KEY).await {
            Err(StorageError::NotFound) => return Ok(()),
            Err(error) => return Err(error),
            Ok(_) => {}
        }
        let marker = SessionMarker {
            schema: SESSION_SCHEMA_V1,
            state: SessionState::Disabled,
            session_id: uuid::Uuid::new_v4().to_string(),
            started_at_unix: now_unix_secs(),
            closed_at_unix: None,
        };
        let bytes = serde_json::to_vec(&marker).map_err(|_| StorageError::IntegrityViolation)?;
        put_and_verify(storage, SESSION_KEY, &bytes).await
    }

    /// Record an access while the caller holds the exact payload publish lock.
    /// Marker I/O remains in the tracked background worker; this method only
    /// inserts or coalesces an in-memory intent and applies bounded backpressure.
    pub async fn record_locked(&self, key: &str) {
        let timestamp = now_unix_secs();
        loop {
            let mut state = self.inner.state.lock().await;
            if let Some(pending) = state.pending.get_mut(key) {
                pending.last_accessed_at_unix = pending.last_accessed_at_unix.max(timestamp);
                return;
            }
            if state.pending.len() < TOUCH_CAPACITY {
                state.pending.insert(
                    key.to_string(),
                    PendingTouch {
                        last_accessed_at_unix: timestamp,
                        due: Instant::now() + TOUCH_COALESCE,
                    },
                );
                TOUCH_QUEUE_DEPTH.set(state.pending.len() as i64);
                drop(state);
                self.inner.changed.notify_one();
                return;
            }

            // Never drop a last-access event. Ask the worker to flush early and
            // wait asynchronously for a bounded slot instead.
            state.force_flush = true;
            let available = self.inner.space_available.notified();
            drop(state);
            self.inner.changed.notify_one();
            available.await;
        }
    }

    pub async fn has_pending(&self, key: &str) -> bool {
        self.inner.state.lock().await.pending.contains_key(key)
    }

    pub fn recovery_needed(&self) -> bool {
        self.inner.recovery_needed.load(Ordering::Acquire)
    }

    fn mark_recovered(&self) {
        self.inner.recovery_needed.store(false, Ordering::Release);
    }

    pub fn spawn_worker(
        &self,
        cancel: tokio_util::sync::CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let access = self.clone();
        tokio::spawn(async move {
            access.inner.worker_drained.store(false, Ordering::Release);
            let mut draining = false;
            loop {
                let (keys, next_due) = {
                    let mut state = access.inner.state.lock().await;
                    if state.pending.is_empty() {
                        if draining {
                            access.inner.worker_drained.store(true, Ordering::Release);
                            TOUCH_QUEUE_DEPTH.set(0);
                            return;
                        }
                        (Vec::new(), None)
                    } else {
                        let now = Instant::now();
                        let flush_all = draining || state.force_flush;
                        state.force_flush = false;
                        let keys = state
                            .pending
                            .iter()
                            .filter(|(_, touch)| flush_all || touch.due <= now)
                            .map(|(key, _)| key.clone())
                            .collect::<Vec<_>>();
                        let next_due = state.pending.values().map(|touch| touch.due).min();
                        (keys, next_due)
                    }
                };

                if !keys.is_empty() {
                    let mut all_flushed = true;
                    for key in keys {
                        all_flushed &= access.flush_one(&key).await;
                    }
                    if draining && !all_flushed {
                        // Leave the durable session marker in place. The next
                        // process will baseline all in-scope payloads before
                        // any deletion; retrying in a cancelled tight loop
                        // would only amplify a storage outage.
                        return;
                    }
                    continue;
                }

                if draining {
                    tokio::task::yield_now().await;
                    continue;
                }

                match next_due {
                    Some(deadline) => {
                        tokio::select! {
                            _ = cancel.cancelled() => draining = true,
                            _ = access.inner.changed.notified() => {},
                            _ = tokio::time::sleep_until(deadline.into()) => {},
                        }
                    }
                    None => {
                        tokio::select! {
                            _ = cancel.cancelled() => draining = true,
                            _ = access.inner.changed.notified() => {},
                        }
                    }
                }
            }
        })
    }

    async fn flush_one(&self, key: &str) -> bool {
        let lock = acquire_publish_lock(&self.inner.publish_locks, key);
        let _guard = lock.lock().await;
        let timestamp = {
            let state = self.inner.state.lock().await;
            match state.pending.get(key) {
                Some(touch) => touch.last_accessed_at_unix,
                None => return true,
            }
        };

        let marker_key = access_marker_key(key);
        let previous = match read_access_marker(&self.inner.storage, &marker_key).await {
            Ok(previous) => previous,
            Err(_) => {
                // Marker uncertainty is not absence. Blind overwrite could
                // move a newer last-access timestamp backwards.
                TOUCH_FAILURES.with_label_values(&["marker_read"]).inc();
                self.retry_touch(key).await;
                return false;
            }
        };
        let (digest, last_accessed_at_unix) = match previous {
            Some(marker) if marker.schema == ACCESS_SCHEMA_V1 && marker.payload_key == key => (
                marker.payload_sha256,
                marker.last_accessed_at_unix.max(timestamp),
            ),
            None => match streamed_payload_sha256(&self.inner.storage, key).await {
                Ok((_size, digest)) => (digest, timestamp),
                Err(StorageError::NotFound) => {
                    self.complete_touch(key).await;
                    return true;
                }
                Err(_) => {
                    TOUCH_FAILURES.with_label_values(&["payload_read"]).inc();
                    self.retry_touch(key).await;
                    return false;
                }
            },
            Some(_) => {
                TOUCH_FAILURES.with_label_values(&["marker_invalid"]).inc();
                self.retry_touch(key).await;
                return false;
            }
        };
        let marker = AccessMarker {
            schema: ACCESS_SCHEMA_V1,
            payload_key: key.to_string(),
            payload_sha256: digest,
            last_accessed_at_unix,
        };
        let bytes = match serde_json::to_vec(&marker) {
            Ok(bytes) => bytes,
            Err(_) => {
                TOUCH_FAILURES.with_label_values(&["serialize"]).inc();
                self.retry_touch(key).await;
                return false;
            }
        };
        match put_and_verify(&self.inner.storage, &marker_key, &bytes).await {
            Ok(()) => {
                self.complete_touch(key).await;
                true
            }
            Err(_) => {
                TOUCH_FAILURES.with_label_values(&["marker_write"]).inc();
                self.retry_touch(key).await;
                false
            }
        }
    }

    async fn complete_touch(&self, key: &str) {
        let mut state = self.inner.state.lock().await;
        if state.pending.remove(key).is_some() {
            TOUCH_QUEUE_DEPTH.set(state.pending.len() as i64);
            drop(state);
            self.inner.space_available.notify_waiters();
        }
    }

    async fn retry_touch(&self, key: &str) {
        let mut state = self.inner.state.lock().await;
        if let Some(touch) = state.pending.get_mut(key) {
            touch.due = Instant::now() + TOUCH_RETRY;
        }
    }

    /// Persist a clean tracking checkpoint only after the worker proved that
    /// no accepted touch remains. A failed/aborted shutdown deliberately
    /// leaves the active marker for next-start recovery.
    pub async fn close_clean_session(&self) {
        if self.recovery_needed()
            || !self.inner.worker_drained.load(Ordering::Acquire)
            || !self.inner.state.lock().await.pending.is_empty()
        {
            return;
        }
        let owns_active_session = match self.inner.storage.get(SESSION_KEY).await {
            Ok(bytes) => serde_json::from_slice::<SessionMarker>(&bytes).is_ok_and(|marker| {
                marker.schema == SESSION_SCHEMA_V1
                    && marker.state == SessionState::Active
                    && marker.session_id == self.inner.session_id
            }),
            Err(_) => false,
        };
        if !owns_active_session {
            TOUCH_FAILURES
                .with_label_values(&["session_ownership"])
                .inc();
            warn!("proxy-cache access session ownership changed; clean checkpoint not written");
            return;
        }
        let marker = SessionMarker {
            schema: SESSION_SCHEMA_V1,
            state: SessionState::Clean,
            session_id: self.inner.session_id.clone(),
            started_at_unix: self.inner.session_started_at_unix,
            closed_at_unix: Some(now_unix_secs()),
        };
        let Ok(bytes) = serde_json::to_vec(&marker) else {
            TOUCH_FAILURES.with_label_values(&["session_close"]).inc();
            return;
        };
        if put_and_verify(&self.inner.storage, SESSION_KEY, &bytes)
            .await
            .is_err()
        {
            TOUCH_FAILURES.with_label_values(&["session_close"]).inc();
            warn!(
                backend = self.inner.storage.backend_name(),
                "proxy-cache access session marker retained after shutdown"
            );
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CandidateKind {
    Maven,
    NpmPackument,
    NpmTarball,
}

#[derive(Debug, Clone)]
struct CleanupTarget {
    registry: &'static str,
    prefix: String,
    kind: CandidateKind,
}

#[derive(Debug, Default)]
pub struct ProxyCacheCleanupResult {
    pub scanned: usize,
    pub planned: usize,
    pub deleted: usize,
    pub bytes_freed: u64,
    pub baselined: usize,
    pub failures: usize,
    pub cancelled: bool,
    pub duration_secs: f64,
}

fn cleanup_targets(config: &Config) -> Vec<CleanupTarget> {
    let mut targets = Vec::new();
    if config.maven.enabled {
        for repository in &config.maven.repositories {
            if let MavenRepository::Proxy { name, .. } = repository {
                targets.push(CleanupTarget {
                    registry: "maven",
                    prefix: format!("maven/repositories/{name}/"),
                    kind: CandidateKind::Maven,
                });
            }
        }
    }

    if config.npm.enabled {
        let mut npm_proxies = config
            .npm
            .repositories
            .iter()
            .filter_map(|repository| match repository {
                NpmRepository::Proxy { name, .. } => Some(name.clone()),
                _ => None,
            })
            .collect::<HashSet<_>>();
        // The synthetic legacy proxy is active only when named repositories
        // are absent. A named hosted repository may legitimately be called
        // `npm-registry`; treating the legacy URL knob as authoritative in
        // that configuration could classify hosted package `proxy` as cache.
        if config.npm.repositories.is_empty() && config.npm.proxy.is_some() {
            npm_proxies.insert("npm-registry".to_string());
        }
        let mut npm_proxies = npm_proxies.into_iter().collect::<Vec<_>>();
        npm_proxies.sort();
        for name in npm_proxies {
            targets.push(CleanupTarget {
                registry: "npm",
                prefix: format!("npm/repositories/{name}/proxy/packuments/"),
                kind: CandidateKind::NpmPackument,
            });
            targets.push(CleanupTarget {
                registry: "npm",
                prefix: format!("npm/repositories/{name}/proxy/tarballs/"),
                kind: CandidateKind::NpmTarball,
            });
        }
    }
    targets
}

fn is_payload(kind: CandidateKind, key: &str) -> bool {
    match kind {
        CandidateKind::Maven => ![".md5", ".sha1", ".sha256", ".sha512"]
            .iter()
            .any(|suffix| key.ends_with(suffix)),
        CandidateKind::NpmPackument => parse_npm_object_key(key)
            .is_some_and(|parsed| parsed.kind == NpmObjectKind::ProxyPackument),
        CandidateKind::NpmTarball => parse_npm_object_key(key)
            .is_some_and(|parsed| matches!(parsed.kind, NpmObjectKind::ProxyTarball(_))),
    }
}

fn sidecar_keys(kind: CandidateKind, key: &str) -> Vec<String> {
    match kind {
        CandidateKind::Maven => [".md5", ".sha1", ".sha256", ".sha512"]
            .into_iter()
            .map(|suffix| format!("{key}{suffix}"))
            .collect(),
        CandidateKind::NpmPackument => vec![format!("{key}.meta")],
        CandidateKind::NpmTarball => Vec::new(),
    }
}

fn maven_bundle_key(key: &str) -> &str {
    [".md5", ".sha1", ".sha256", ".sha512"]
        .into_iter()
        .find_map(|suffix| key.strip_suffix(suffix))
        .unwrap_or(key)
}

fn record_cleanup_mutation(
    target: &CleanupTarget,
    key: &str,
    invalidated: &mut HashSet<&'static str>,
    invalidated_maven_paths: &mut HashSet<String>,
) {
    if target.kind == CandidateKind::Maven {
        invalidated_maven_paths.insert(maven_bundle_key(key).to_string());
    } else {
        invalidated.insert(target.registry);
    }
}

pub async fn run_proxy_cache_cleanup(
    storage: &Storage,
    publish_locks: &PublishLocks,
    access: &ProxyCacheAccess,
    config: &Config,
    policy: &ProxyCacheCleanupConfig,
    repo_index: &RepoIndex,
    cancel: &tokio_util::sync::CancellationToken,
) -> ProxyCacheCleanupResult {
    let started = Instant::now();
    let now = now_unix_secs();
    let recovery = access.recovery_needed();
    let mut result = ProxyCacheCleanupResult::default();
    let mut recovery_complete = true;
    let mut invalidated = HashSet::new();
    let mut invalidated_maven_paths = HashSet::new();
    // Persistent Maven/npm share one atomic generation. Capture the destructive
    // admission gate once: this run's own first DELETE deliberately makes the
    // runtime Warming, while per-page revision checks plus exact-key S3
    // revalidation remain the safety oracle for the rest of the same batch.
    let persistent_ready = repo_index.has_persistent() && repo_index.persistent_protocol_ready();

    for target in cleanup_targets(config) {
        if cancel.is_cancelled() {
            result.cancelled = true;
            recovery_complete = false;
            break;
        }
        // A Degraded snapshot is never a destructive oracle; exact-key checks
        // below still revalidate every candidate from a Ready snapshot.
        let target_ready = if repo_index.has_persistent() {
            persistent_ready
        } else {
            repo_index.status(target.registry) == Some(IndexStatus::Ready)
        };
        if !target_ready {
            result.failures += 1;
            recovery_complete = false;
            CLEANUP_SKIPPED
                .with_label_values(&["index_unavailable"])
                .inc();
            warn!(
                registry = target.registry,
                "proxy-cache cleanup index is not Ready; target kept"
            );
            continue;
        }
        if repo_index.has_persistent() {
            // redb is an ordered derived inventory, so maintenance reads one
            // bounded page at a time. A changing revision stops this target;
            // exact-key checks make earlier decisions safe, and the next run
            // resumes from the newly published generation.
            let mut after = None;
            let mut revision = None;
            loop {
                let page = match repo_index
                    .persistent_object_page(&target.prefix, after, CLEANUP_INDEX_PAGE_SIZE)
                    .await
                {
                    Ok(page) => page,
                    Err(_) => {
                        result.failures += 1;
                        recovery_complete = false;
                        CLEANUP_SKIPPED
                            .with_label_values(&["index_unavailable"])
                            .inc();
                        warn!(
                            registry = target.registry,
                            "proxy-cache cleanup persistent index page unavailable; target kept"
                        );
                        break;
                    }
                };
                if revision
                    .as_ref()
                    .is_some_and(|expected| expected != &page.revision)
                {
                    result.failures += 1;
                    recovery_complete = false;
                    CLEANUP_SKIPPED.with_label_values(&["index_changed"]).inc();
                    warn!(
                        registry = target.registry,
                        "proxy-cache cleanup index changed between pages; target pass stopped"
                    );
                    break;
                }
                revision = Some(page.revision.clone());
                process_cleanup_entries(
                    &page.items,
                    storage,
                    publish_locks,
                    access,
                    policy,
                    &target,
                    recovery,
                    now,
                    cancel,
                    &mut result,
                    &mut recovery_complete,
                    &mut invalidated,
                    &mut invalidated_maven_paths,
                )
                .await;
                if result.cancelled {
                    break;
                }
                let Some(next_after) = page.next_after else {
                    match repo_index.persistent_object_revision().await {
                        Ok(current) if revision.as_ref() == Some(&current) => {}
                        _ => {
                            result.failures += 1;
                            recovery_complete = false;
                            CLEANUP_SKIPPED.with_label_values(&["index_changed"]).inc();
                            warn!(
                                registry = target.registry,
                                "proxy-cache cleanup index changed during target pass"
                            );
                        }
                    }
                    break;
                };
                after = Some(next_after);
            }
        } else {
            // Legacy formats still publish an immutable in-memory snapshot.
            // Reusing its Arc avoids a second O(objects) collection.
            let entries = repo_index.objects(target.registry);
            process_cleanup_entries(
                &entries,
                storage,
                publish_locks,
                access,
                policy,
                &target,
                recovery,
                now,
                cancel,
                &mut result,
                &mut recovery_complete,
                &mut invalidated,
                &mut invalidated_maven_paths,
            )
            .await;
        }
        if result.cancelled {
            break;
        }
    }

    if recovery && recovery_complete && !policy.dry_run {
        access.mark_recovered();
        info!(
            baselined = result.baselined,
            "proxy-cache crash recovery baseline completed; deletion deferred to a later run"
        );
    }
    for registry in invalidated {
        repo_index.invalidate(registry);
    }
    for key in invalidated_maven_paths {
        repo_index.invalidate_maven_storage_key(&key);
    }
    result.duration_secs = started.elapsed().as_secs_f64();
    CLEANUP_DURATION.observe(result.duration_secs);
    CLEANUP_LAST_RUN.set(now as i64);
    result
}

#[allow(clippy::too_many_arguments)]
async fn process_cleanup_entries(
    entries: &[IndexedObject],
    storage: &Storage,
    publish_locks: &PublishLocks,
    access: &ProxyCacheAccess,
    policy: &ProxyCacheCleanupConfig,
    target: &CleanupTarget,
    recovery: bool,
    now: u64,
    cancel: &tokio_util::sync::CancellationToken,
    result: &mut ProxyCacheCleanupResult,
    recovery_complete: &mut bool,
    invalidated: &mut HashSet<&'static str>,
    invalidated_maven_paths: &mut HashSet<String>,
) {
    for entry in entries {
        let key = &entry.key;
        if !key.starts_with(&target.prefix) || !is_payload(target.kind, key) {
            continue;
        }
        if cancel.is_cancelled() {
            result.cancelled = true;
            *recovery_complete = false;
            return;
        }
        result.scanned += 1;
        if recovery {
            match baseline_payload(storage, publish_locks, access, key, policy.dry_run).await {
                Ok(true) => result.baselined += 1,
                Ok(false) => {}
                Err(()) => {
                    result.failures += 1;
                    *recovery_complete = false;
                }
            }
            continue;
        }
        process_candidate(
            storage,
            publish_locks,
            access,
            policy,
            target,
            key,
            entry.meta.clone(),
            now,
            result,
            invalidated,
            invalidated_maven_paths,
        )
        .await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn process_candidate(
    storage: &Storage,
    publish_locks: &PublishLocks,
    access: &ProxyCacheAccess,
    policy: &ProxyCacheCleanupConfig,
    target: &CleanupTarget,
    key: &str,
    listed_meta: FileMeta,
    now: u64,
    result: &mut ProxyCacheCleanupResult,
    invalidated: &mut HashSet<&'static str>,
    invalidated_maven_paths: &mut HashSet<String>,
) {
    if !old_enough(listed_meta.modified, policy.min_cache_age_secs, now) {
        CLEANUP_SKIPPED.with_label_values(&["cache_age"]).inc();
        return;
    }
    let marker_key = access_marker_key(key);
    let marker_bytes = match storage.get(&marker_key).await {
        Ok(bytes) => bytes,
        Err(StorageError::NotFound) => {
            match baseline_payload(storage, publish_locks, access, key, policy.dry_run).await {
                Ok(true) => result.baselined += 1,
                Ok(false) => {}
                Err(()) => result.failures += 1,
            }
            return;
        }
        Err(_) => {
            result.failures += 1;
            CLEANUP_SKIPPED.with_label_values(&["marker_read"]).inc();
            return;
        }
    };
    let marker: AccessMarker = match serde_json::from_slice(&marker_bytes) {
        Ok(marker) => marker,
        Err(_) => {
            result.failures += 1;
            CLEANUP_SKIPPED.with_label_values(&["marker_invalid"]).inc();
            return;
        }
    };
    if marker.schema != ACCESS_SCHEMA_V1 || marker.payload_key != key {
        result.failures += 1;
        CLEANUP_SKIPPED.with_label_values(&["marker_invalid"]).inc();
        return;
    }
    if !old_enough(marker.last_accessed_at_unix, policy.min_idle_secs, now) {
        CLEANUP_SKIPPED.with_label_values(&["idle_age"]).inc();
        return;
    }
    let (initial_size, initial_digest) = match streamed_payload_sha256(storage, key).await {
        Ok(identity) => identity,
        Err(StorageError::NotFound) => return,
        Err(_) => {
            result.failures += 1;
            CLEANUP_SKIPPED.with_label_values(&["payload_read"]).inc();
            return;
        }
    };
    if initial_size != listed_meta.size {
        CLEANUP_SKIPPED
            .with_label_values(&["identity_changed"])
            .inc();
        return;
    }
    if marker.payload_sha256 != initial_digest {
        match baseline_payload(storage, publish_locks, access, key, policy.dry_run).await {
            Ok(true) => result.baselined += 1,
            Ok(false) => {}
            Err(()) => result.failures += 1,
        }
        return;
    }

    let lock = acquire_publish_lock(publish_locks, key);
    let _guard = lock.lock().await;
    if access.has_pending(key).await {
        CLEANUP_SKIPPED.with_label_values(&["touch_pending"]).inc();
        return;
    }
    let current_meta = match storage.stat(key).await {
        Ok(Some(meta)) => meta,
        Ok(None) => return,
        Err(_) => {
            result.failures += 1;
            CLEANUP_SKIPPED.with_label_values(&["stat_error"]).inc();
            return;
        }
    };
    if current_meta.size != listed_meta.size
        || current_meta.modified != listed_meta.modified
        || !old_enough(current_meta.modified, policy.min_cache_age_secs, now)
    {
        CLEANUP_SKIPPED
            .with_label_values(&["identity_changed"])
            .inc();
        return;
    }
    let (current_size, current_digest) = match streamed_payload_sha256(storage, key).await {
        Ok(identity) => identity,
        Err(StorageError::NotFound) => return,
        Err(_) => {
            result.failures += 1;
            CLEANUP_SKIPPED
                .with_label_values(&["payload_recheck"])
                .inc();
            return;
        }
    };
    if current_size != current_meta.size || current_digest != initial_digest {
        CLEANUP_SKIPPED
            .with_label_values(&["identity_changed"])
            .inc();
        return;
    }
    let current_marker_bytes = match storage.get(&marker_key).await {
        Ok(bytes) => bytes,
        Err(_) => {
            result.failures += 1;
            CLEANUP_SKIPPED.with_label_values(&["marker_recheck"]).inc();
            return;
        }
    };
    if current_marker_bytes != marker_bytes {
        CLEANUP_SKIPPED.with_label_values(&["marker_changed"]).inc();
        return;
    }

    result.planned += 1;
    if policy.dry_run {
        return;
    }
    for sidecar in sidecar_keys(target.kind, key) {
        match storage.stat(&sidecar).await {
            Ok(Some(_)) => {}
            Ok(None) => continue,
            Err(_) => {
                result.failures += 1;
                CLEANUP_SKIPPED.with_label_values(&["sidecar_stat"]).inc();
                return;
            }
        }
        match storage.delete(&sidecar).await {
            Ok(()) => {
                record_cleanup_mutation(target, &sidecar, invalidated, invalidated_maven_paths)
            }
            Err(StorageError::NotFound) => {}
            Err(_) => {
                result.failures += 1;
                CLEANUP_SKIPPED.with_label_values(&["sidecar_delete"]).inc();
                return;
            }
        }
    }
    match storage.delete(key).await {
        Ok(()) => record_cleanup_mutation(target, key, invalidated, invalidated_maven_paths),
        Err(StorageError::NotFound) => return,
        Err(_) => {
            result.failures += 1;
            CLEANUP_SKIPPED.with_label_values(&["payload_delete"]).inc();
            return;
        }
    }
    result.deleted += 1;
    result.bytes_freed = result.bytes_freed.saturating_add(current_meta.size);
    CLEANUP_DELETED.with_label_values(&[target.registry]).inc();
    CLEANUP_BYTES
        .with_label_values(&[target.registry])
        .inc_by(current_meta.size);
    if let Err(error) = storage.delete(&marker_key).await {
        if !matches!(error, StorageError::NotFound) {
            result.failures += 1;
            CLEANUP_SKIPPED.with_label_values(&["marker_delete"]).inc();
        }
    }
}

async fn baseline_payload(
    storage: &Storage,
    publish_locks: &PublishLocks,
    access: &ProxyCacheAccess,
    key: &str,
    dry_run: bool,
) -> Result<bool, ()> {
    let lock = acquire_publish_lock(publish_locks, key);
    let _guard = lock.lock().await;
    if access.has_pending(key).await {
        return Ok(false);
    }
    if dry_run {
        return Ok(false);
    }
    let digest = match streamed_payload_sha256(storage, key).await {
        Ok((_size, digest)) => digest,
        Err(StorageError::NotFound) => return Ok(false),
        Err(_) => return Err(()),
    };
    let marker_key = access_marker_key(key);
    let previous = read_access_marker(storage, &marker_key)
        .await
        .map_err(|_| ())?;
    // Capture baseline time only after the exact-key lock and streaming hash.
    // A long scan can otherwise shorten the idle window by hours, while a
    // touch flushed before this lock must never move backwards.
    let baseline_at = now_unix_secs();
    let last_accessed_at_unix = previous
        .filter(|marker| {
            marker.schema == ACCESS_SCHEMA_V1
                && marker.payload_key == key
                && marker.payload_sha256 == digest
        })
        .map_or(baseline_at, |marker| {
            marker.last_accessed_at_unix.max(baseline_at)
        });
    let marker = AccessMarker {
        schema: ACCESS_SCHEMA_V1,
        payload_key: key.to_string(),
        payload_sha256: digest,
        last_accessed_at_unix,
    };
    let bytes = serde_json::to_vec(&marker).map_err(|_| ())?;
    put_and_verify(storage, &marker_key, &bytes)
        .await
        .map_err(|_| ())?;
    Ok(true)
}

pub fn spawn_proxy_cache_cleanup_scheduler(
    storage: Storage,
    publish_locks: PublishLocks,
    repo_index: Arc<RepoIndex>,
    config: Arc<Config>,
    access: ProxyCacheAccess,
    cleanup_lock: Arc<tokio::sync::Mutex<()>>,
    cancel: tokio_util::sync::CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // Avoid repeating the measured boot contention between object-store
        // scans. Ready and Degraded both mean the first index attempt settled.
        loop {
            let maven_warming =
                config.maven.enabled && repo_index.status("maven") == Some(IndexStatus::Warming);
            let npm_warming =
                config.npm.enabled && repo_index.status("npm") == Some(IndexStatus::Warming);
            if !maven_warming && !npm_warming {
                break;
            }
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_secs(1)) => {},
            }
        }

        let mut boot_run = true;
        loop {
            if !boot_run {
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    _ = tokio::time::sleep(Duration::from_secs(config.proxy_cache_cleanup.interval_secs)) => {},
                }
            }
            let guard = if boot_run {
                boot_run = false;
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    guard = cleanup_lock.lock() => Ok(guard),
                }
            } else {
                cleanup_lock.try_lock()
            };
            let Ok(guard) = guard else {
                info!("Proxy-cache cleanup: cleanup lock held, skipping run");
                continue;
            };
            let result = run_proxy_cache_cleanup(
                &storage,
                &publish_locks,
                &access,
                &config,
                &config.proxy_cache_cleanup,
                &repo_index,
                &cancel,
            )
            .await;
            info!(
                dry_run = config.proxy_cache_cleanup.dry_run,
                scanned = result.scanned,
                baselined = result.baselined,
                planned = result.planned,
                deleted = result.deleted,
                bytes_freed = result.bytes_freed,
                failures = result.failures,
                cancelled = result.cancelled,
                duration_seconds = result.duration_secs,
                "Proxy-cache cleanup run finished"
            );
            drop(guard);
            if result.cancelled {
                return;
            }
        }
    })
}

fn access_marker_key(payload_key: &str) -> String {
    let digest = sha256_hex(payload_key.as_bytes());
    format!("{ACCESS_PREFIX}/{}/{}.json", &digest[..2], digest)
}

fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

/// Hash one payload with fixed memory. The reader-reported size is verified
/// against the bytes observed so a truncated stream is never accepted as an
/// object identity.
async fn streamed_payload_sha256(
    storage: &Storage,
    key: &str,
) -> Result<(u64, String), StorageError> {
    let (expected_size, mut reader) = storage.get_reader(key).await?;
    let mut digest = Sha256::new();
    let mut observed_size = 0u64;
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        observed_size = observed_size
            .checked_add(read as u64)
            .ok_or(StorageError::IntegrityViolation)?;
        digest.update(&buffer[..read]);
    }
    if observed_size != expected_size {
        return Err(StorageError::IntegrityViolation);
    }
    Ok((observed_size, hex::encode(digest.finalize())))
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn old_enough(timestamp: u64, threshold: u64, now: u64) -> bool {
    timestamp <= now && now - timestamp >= threshold
}

fn trusted_clean_session(marker: &SessionMarker, now: u64) -> bool {
    marker.schema == SESSION_SCHEMA_V1
        && marker.state == SessionState::Clean
        && marker
            .closed_at_unix
            .is_some_and(|closed| marker.started_at_unix <= closed && closed <= now)
}

async fn read_access_marker(
    storage: &Storage,
    key: &str,
) -> Result<Option<AccessMarker>, StorageError> {
    match storage.get(key).await {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|_| StorageError::IntegrityViolation),
        Err(StorageError::NotFound) => Ok(None),
        Err(error) => Err(error),
    }
}

async fn put_and_verify(storage: &Storage, key: &str, bytes: &[u8]) -> Result<(), StorageError> {
    match storage.put(key, bytes).await {
        Ok(()) => Ok(()),
        Err(write_error) => match storage.get(key).await {
            Ok(stored) if stored.as_ref() == bytes => Ok(()),
            Ok(_) => Err(StorageError::IntegrityViolation),
            Err(_) => Err(write_error),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{MavenVersionPolicy, NpmWritePolicy};
    use crate::test_helpers::FaultInjectBackend;
    use std::path::Path;

    fn test_policy() -> ProxyCacheCleanupConfig {
        ProxyCacheCleanupConfig {
            enabled: true,
            dry_run: false,
            interval_secs: 60,
            min_cache_age_secs: 10,
            min_idle_secs: 10,
        }
    }

    fn test_config() -> Config {
        let mut config = Config::default();
        config.maven.proxies.clear();
        config.maven.repositories = vec![
            MavenRepository::Hosted {
                name: "hosted".to_string(),
                version_policy: MavenVersionPolicy::Mixed,
                write_policy: crate::config::MavenWritePolicy::AllowOnce,
            },
            MavenRepository::Proxy {
                name: "central".to_string(),
                url: "https://repo.example.invalid".to_string(),
                auth: None,
                version_policy: MavenVersionPolicy::Mixed,
                metadata_ttl: None,
                negative_ttl: 300,
            },
            MavenRepository::Group {
                name: "public".to_string(),
                members: vec!["hosted".to_string(), "central".to_string()],
            },
        ];
        config.npm.proxy = None;
        config.npm.repositories = vec![
            NpmRepository::Hosted {
                name: "npm-private".to_string(),
                write_policy: NpmWritePolicy::AllowOnce,
            },
            NpmRepository::Proxy {
                name: "npm-public".to_string(),
                url: "https://registry.example.invalid".to_string(),
                auth: None,
                metadata_ttl: None,
                negative_ttl: 300,
            },
        ];
        config.proxy_cache_cleanup = test_policy();
        config
    }

    fn test_locks() -> PublishLocks {
        Arc::new(parking_lot::Mutex::new(HashMap::new()))
    }

    fn make_old(root: &Path, key: &str, age: Duration) {
        std::fs::File::options()
            .write(true)
            .open(root.join(key))
            .unwrap()
            .set_modified(SystemTime::now() - age)
            .unwrap();
    }

    async fn write_marker(storage: &Storage, key: &str, last_accessed_at_unix: u64) {
        let payload = storage.get(key).await.unwrap();
        let marker = AccessMarker {
            schema: ACCESS_SCHEMA_V1,
            payload_key: key.to_string(),
            payload_sha256: sha256_hex(&payload),
            last_accessed_at_unix,
        };
        storage
            .put(
                &access_marker_key(key),
                &serde_json::to_vec(&marker).unwrap(),
            )
            .await
            .unwrap();
    }

    #[test]
    fn marker_key_is_hidden_deterministic_and_does_not_embed_payload_path() {
        let payload = "npm/repositories/npm-registry/proxy/packuments/@scope/pkg.json";
        let marker = access_marker_key(payload);
        assert!(marker.starts_with(".nora-proxy-access/v1/"));
        assert!(!marker.contains("scope"));
        assert_eq!(marker, access_marker_key(payload));
    }

    #[test]
    fn candidate_classification_never_treats_sidecars_as_payloads() {
        assert!(is_payload(CandidateKind::Maven, "x.jar"));
        assert!(!is_payload(CandidateKind::Maven, "x.jar.sha256"));
        assert!(is_payload(
            CandidateKind::NpmPackument,
            "npm/repositories/proxy/proxy/packuments/pkg.json"
        ));
        assert!(!is_payload(
            CandidateKind::NpmPackument,
            "npm/repositories/proxy/proxy/packuments/pkg.json.meta"
        ));
        assert!(is_payload(
            CandidateKind::NpmTarball,
            "npm/repositories/proxy/proxy/tarballs/pkg/pkg-1.0.0.tgz"
        ));
        assert!(!is_payload(
            CandidateKind::NpmTarball,
            "npm/repositories/proxy/proxy/tarballs/proxy-1.0.0.tgz"
        ));
    }

    #[test]
    fn future_timestamps_are_never_old_enough() {
        assert!(!old_enough(101, 1, 100));
        assert!(old_enough(90, 10, 100));
    }

    #[test]
    fn only_temporally_valid_clean_session_is_trusted() {
        let mut marker = SessionMarker {
            schema: SESSION_SCHEMA_V1,
            state: SessionState::Clean,
            session_id: "session".to_string(),
            started_at_unix: 90,
            closed_at_unix: Some(95),
        };
        assert!(trusted_clean_session(&marker, 100));
        marker.closed_at_unix = Some(101);
        assert!(!trusted_clean_session(&marker, 100));
        marker.closed_at_unix = None;
        assert!(!trusted_clean_session(&marker, 100));
    }

    #[test]
    fn targets_come_only_from_configured_proxy_repositories() {
        let targets = cleanup_targets(&test_config());
        let prefixes = targets
            .iter()
            .map(|target| target.prefix.as_str())
            .collect::<Vec<_>>();
        assert!(prefixes.contains(&"maven/repositories/central/"));
        assert!(prefixes.contains(&"npm/repositories/npm-public/proxy/packuments/"));
        assert!(prefixes.contains(&"npm/repositories/npm-public/proxy/tarballs/"));
        assert!(!prefixes.iter().any(
            |prefix| prefix.starts_with("maven/") && !prefix.starts_with("maven/repositories/")
        ));
        assert!(!prefixes.iter().any(|prefix| prefix.contains("hosted")));
        assert!(!prefixes.iter().any(|prefix| prefix.contains("npm-private")));
        assert!(!prefixes.iter().any(|prefix| prefix.contains("negative")));
    }

    #[test]
    fn legacy_npm_target_is_inactive_when_named_repositories_exist() {
        let mut config = test_config();
        config.npm.proxy = Some("https://legacy.example.invalid".to_string());
        config.npm.repositories = vec![NpmRepository::Hosted {
            name: "npm-registry".to_string(),
            write_policy: NpmWritePolicy::AllowOnce,
        }];

        let targets = cleanup_targets(&config);

        assert!(!targets
            .iter()
            .any(|target| target.prefix.contains("npm-registry/proxy/")));
    }

    #[tokio::test]
    async fn markerless_old_payload_is_baselined_and_never_deleted_same_run() {
        let root = tempfile::tempdir().unwrap();
        let storage = Storage::new_local(root.path().to_str().unwrap());
        let locks = test_locks();
        let access = ProxyCacheAccess::start_session(storage.clone(), locks.clone())
            .await
            .unwrap();
        let key = "maven/repositories/central/com/acme/demo/1.0/demo-1.0.jar";
        storage.put(key, b"artifact").await.unwrap();
        make_old(root.path(), key, Duration::from_secs(100));

        let result = run_test_cleanup(
            &storage,
            &locks,
            &access,
            &test_config(),
            &test_policy(),
            &tokio_util::sync::CancellationToken::new(),
        )
        .await;

        assert_eq!(result.baselined, 1);
        assert_eq!(result.deleted, 0);
        assert!(storage.get(key).await.is_ok());
        let marker = read_access_marker(&storage, &access_marker_key(key))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(marker.payload_sha256, sha256_hex(b"artifact"));
    }

    #[tokio::test]
    async fn cleanup_hashes_payload_with_streaming_reader_not_full_get() {
        let root = tempfile::tempdir().unwrap();
        let inner = Storage::new_local(root.path().to_str().unwrap());
        let locks = test_locks();
        let access = ProxyCacheAccess::start_session(inner.clone(), locks.clone())
            .await
            .unwrap();
        let key = "maven/repositories/central/com/acme/demo/1.0/demo-1.0.jar";
        inner.put(key, b"artifact").await.unwrap();
        make_old(root.path(), key, Duration::from_secs(100));
        write_marker(&inner, key, now_unix_secs() - 100).await;
        let storage = Storage::from_backend(Arc::new(
            FaultInjectBackend::new(inner.clone()).fail_get(key),
        ));

        let result = run_test_cleanup(
            &storage,
            &locks,
            &access,
            &test_config(),
            &test_policy(),
            &tokio_util::sync::CancellationToken::new(),
        )
        .await;

        assert_eq!(result.deleted, 1);
        assert!(matches!(inner.get(key).await, Err(StorageError::NotFound)));
    }

    #[tokio::test]
    async fn eligible_maven_bundle_deletes_only_payload_and_cache_sidecars() {
        let root = tempfile::tempdir().unwrap();
        let storage = Storage::new_local(root.path().to_str().unwrap());
        let locks = test_locks();
        let access = ProxyCacheAccess::start_session(storage.clone(), locks.clone())
            .await
            .unwrap();
        let key = "maven/repositories/central/com/acme/demo/1.0/demo-1.0.jar";
        let hosted = "maven/repositories/hosted/com/acme/demo/1.0/demo-1.0.jar";
        let legacy = "maven/com/acme/demo/1.0/demo-1.0.jar";
        storage.put(key, b"proxy").await.unwrap();
        for sidecar in sidecar_keys(CandidateKind::Maven, key) {
            storage.put(&sidecar, b"sum").await.unwrap();
        }
        storage.put(hosted, b"hosted").await.unwrap();
        storage.put(legacy, b"legacy").await.unwrap();
        make_old(root.path(), key, Duration::from_secs(100));
        write_marker(&storage, key, now_unix_secs() - 100).await;

        let result = run_test_cleanup(
            &storage,
            &locks,
            &access,
            &test_config(),
            &test_policy(),
            &tokio_util::sync::CancellationToken::new(),
        )
        .await;

        assert_eq!(result.deleted, 1);
        assert!(matches!(
            storage.get(key).await,
            Err(StorageError::NotFound)
        ));
        for sidecar in sidecar_keys(CandidateKind::Maven, key) {
            assert!(matches!(
                storage.get(&sidecar).await,
                Err(StorageError::NotFound)
            ));
        }
        assert!(storage.get(hosted).await.is_ok());
        assert!(storage.get(legacy).await.is_ok());
    }

    #[tokio::test]
    async fn eligible_npm_packument_deletes_validator_but_not_negative_cache() {
        let root = tempfile::tempdir().unwrap();
        let storage = Storage::new_local(root.path().to_str().unwrap());
        let locks = test_locks();
        let access = ProxyCacheAccess::start_session(storage.clone(), locks.clone())
            .await
            .unwrap();
        let key = "npm/repositories/npm-public/proxy/packuments/pkg.json";
        let validator = format!("{key}.meta");
        let negative = "npm/repositories/npm-public/proxy/negative/pkg";
        storage.put(key, br#"{"name":"pkg"}"#).await.unwrap();
        storage.put(&validator, b"validator").await.unwrap();
        storage.put(negative, b"negative").await.unwrap();
        make_old(root.path(), key, Duration::from_secs(100));
        write_marker(&storage, key, now_unix_secs() - 100).await;

        let result = run_test_cleanup(
            &storage,
            &locks,
            &access,
            &test_config(),
            &test_policy(),
            &tokio_util::sync::CancellationToken::new(),
        )
        .await;

        assert_eq!(result.deleted, 1);
        assert!(matches!(
            storage.get(key).await,
            Err(StorageError::NotFound)
        ));
        assert!(matches!(
            storage.get(&validator).await,
            Err(StorageError::NotFound)
        ));
        assert!(storage.get(negative).await.is_ok());
    }

    #[tokio::test]
    async fn both_payload_age_and_idle_age_are_required() {
        let root = tempfile::tempdir().unwrap();
        let storage = Storage::new_local(root.path().to_str().unwrap());
        let locks = test_locks();
        let access = ProxyCacheAccess::start_session(storage.clone(), locks.clone())
            .await
            .unwrap();
        let recent_payload = "npm/repositories/npm-public/proxy/tarballs/pkg/pkg-1.0.0.tgz";
        let recently_used = "npm/repositories/npm-public/proxy/tarballs/pkg/pkg-2.0.0.tgz";
        storage.put(recent_payload, b"one").await.unwrap();
        storage.put(recently_used, b"two").await.unwrap();
        make_old(root.path(), recently_used, Duration::from_secs(100));
        write_marker(&storage, recent_payload, now_unix_secs() - 100).await;
        write_marker(&storage, recently_used, now_unix_secs()).await;

        let result = run_test_cleanup(
            &storage,
            &locks,
            &access,
            &test_config(),
            &test_policy(),
            &tokio_util::sync::CancellationToken::new(),
        )
        .await;

        assert_eq!(result.deleted, 0);
        assert!(storage.get(recent_payload).await.is_ok());
        assert!(storage.get(recently_used).await.is_ok());
    }

    #[tokio::test]
    async fn recently_used_old_payload_is_not_streamed_or_hashed() {
        let root = tempfile::tempdir().unwrap();
        let inner = Storage::new_local(root.path().to_str().unwrap());
        let key = "npm/repositories/npm-public/proxy/tarballs/pkg/pkg-1.0.0.tgz";
        inner.put(key, b"large-tarball-placeholder").await.unwrap();
        make_old(root.path(), key, Duration::from_secs(100));
        write_marker(&inner, key, now_unix_secs()).await;
        let backend = FaultInjectBackend::new(inner.clone());
        let reader_attempts = backend.reader_attempts();
        let storage = Storage::from_backend(Arc::new(backend));
        let locks = test_locks();
        let access = ProxyCacheAccess::start_session(storage.clone(), locks.clone())
            .await
            .unwrap();

        let result = run_test_cleanup(
            &storage,
            &locks,
            &access,
            &test_config(),
            &test_policy(),
            &tokio_util::sync::CancellationToken::new(),
        )
        .await;

        assert_eq!(result.deleted, 0);
        assert!(!reader_attempts.lock().iter().any(|attempt| attempt == key));
        assert!(inner.get(key).await.is_ok());
    }

    #[tokio::test]
    async fn invalid_or_future_access_marker_is_fail_closed() {
        let root = tempfile::tempdir().unwrap();
        let storage = Storage::new_local(root.path().to_str().unwrap());
        let locks = test_locks();
        let access = ProxyCacheAccess::start_session(storage.clone(), locks.clone())
            .await
            .unwrap();
        let invalid = "npm/repositories/npm-public/proxy/tarballs/pkg/invalid.tgz";
        let future = "npm/repositories/npm-public/proxy/tarballs/pkg/future.tgz";
        storage.put(invalid, b"invalid").await.unwrap();
        storage.put(future, b"future").await.unwrap();
        make_old(root.path(), invalid, Duration::from_secs(100));
        make_old(root.path(), future, Duration::from_secs(100));
        storage
            .put(&access_marker_key(invalid), b"not-json")
            .await
            .unwrap();
        write_marker(&storage, future, now_unix_secs() + 100).await;

        let result = run_test_cleanup(
            &storage,
            &locks,
            &access,
            &test_config(),
            &test_policy(),
            &tokio_util::sync::CancellationToken::new(),
        )
        .await;

        assert!(result.failures >= 1);
        assert_eq!(result.deleted, 0);
        assert!(storage.get(invalid).await.is_ok());
        assert!(storage.get(future).await.is_ok());
    }

    #[tokio::test]
    async fn list_uncertainty_is_fail_closed() {
        let root = tempfile::tempdir().unwrap();
        let inner = Storage::new_local(root.path().to_str().unwrap());
        let locks = test_locks();
        let access = ProxyCacheAccess::start_session(inner.clone(), locks.clone())
            .await
            .unwrap();
        let key = "maven/repositories/central/com/acme/demo/1.0/demo-1.0.jar";
        inner.put(key, b"artifact").await.unwrap();
        make_old(root.path(), key, Duration::from_secs(100));
        write_marker(&inner, key, now_unix_secs() - 100).await;
        let storage = Storage::from_backend(Arc::new(
            FaultInjectBackend::new(inner.clone()).fail_list("maven/"),
        ));
        let index = Arc::new(RepoIndex::new());
        assert!(
            !index
                .rebuild_for_test(crate::registry_type::RegistryType::Maven, &storage)
                .await
        );
        assert!(
            index
                .rebuild_for_test(crate::registry_type::RegistryType::Npm, &storage)
                .await
        );

        let result = run_proxy_cache_cleanup(
            &storage,
            &locks,
            &access,
            &test_config(),
            &test_policy(),
            &index,
            &tokio_util::sync::CancellationToken::new(),
        )
        .await;

        assert!(result.failures >= 1);
        assert_eq!(result.deleted, 0);
        assert!(inner.get(key).await.is_ok());
    }

    #[tokio::test]
    async fn large_cardinality_cleanup_reuses_index_snapshot_without_listing() {
        const OBJECTS: usize = 100_000;
        let root = tempfile::tempdir().unwrap();
        let inner = Storage::new_local(root.path().to_str().unwrap());
        let backend = FaultInjectBackend::new(inner);
        let list_attempts = backend.list_attempts();
        let storage = Storage::from_backend(Arc::new(backend));
        let locks = test_locks();
        let access = ProxyCacheAccess::start_session(storage.clone(), locks.clone())
            .await
            .unwrap();
        access.mark_recovered();
        let index = RepoIndex::new();
        let modified = now_unix_secs();
        index.publish_objects_for_test(
            crate::registry_type::RegistryType::Maven,
            (0..OBJECTS)
                .map(|number| {
                    (
                        format!(
                            "maven/repositories/central/com/acme/demo/{number}/demo-{number}.jar"
                        ),
                        FileMeta::local(1, modified),
                    )
                })
                .collect(),
        );
        index.publish_objects_for_test(crate::registry_type::RegistryType::Npm, Vec::new());

        let result = run_proxy_cache_cleanup(
            &storage,
            &locks,
            &access,
            &test_config(),
            &test_policy(),
            &index,
            &tokio_util::sync::CancellationToken::new(),
        )
        .await;

        assert_eq!(result.scanned, OBJECTS);
        assert_eq!(result.deleted, 0);
        assert!(list_attempts.lock().is_empty());
    }

    #[tokio::test]
    async fn persistent_maven_cleanup_promotes_lower_group_member_without_full_reconcile() {
        let root = tempfile::tempdir().unwrap();
        let inner = Storage::new_local(root.path().to_str().unwrap());
        let backend = Arc::new(FaultInjectBackend::new(inner.clone()));
        let list_attempts = backend.list_attempts();
        let storage = Storage::from_backend(backend);
        let locks = test_locks();
        let access = ProxyCacheAccess::start_session(storage.clone(), locks.clone())
            .await
            .unwrap();
        access.mark_recovered();
        let key = "maven/repositories/central/com/acme/demo/1.0/demo-1.0.jar";
        let fallback_key = "maven/repositories/hosted/com/acme/demo/1.0/demo-1.0.jar";
        storage.put(key, b"upper").await.unwrap();
        storage.put(fallback_key, b"lower-member").await.unwrap();
        make_old(root.path(), key, Duration::from_secs(100));
        write_marker(&storage, key, now_unix_secs() - 100).await;

        let index_dir = tempfile::tempdir().unwrap();
        let mut config = test_config();
        let MavenRepository::Group { members, .. } = &mut config.maven.repositories[2] else {
            panic!("test topology must contain a Maven group");
        };
        *members = vec!["central".to_string(), "hosted".to_string()];
        config.index.path = index_dir
            .path()
            .join("index.redb")
            .to_string_lossy()
            .into_owned();
        let enabled = HashSet::from([
            crate::registry_type::RegistryType::Maven,
            crate::registry_type::RegistryType::Npm,
        ]);
        let index = RepoIndex::open_persistent_for_test(&config, &enabled, storage.clone())
            .await
            .unwrap();
        index.reconcile_persistent_for_test().await.unwrap();
        let prefixes = vec![
            "maven/repositories/central/".to_string(),
            "maven/repositories/hosted/".to_string(),
        ];
        let before = index
            .persistent_maven_files(prefixes.clone(), "com/acme/demo/1.0".to_string(), None, 10)
            .await
            .unwrap();
        assert_eq!(before.items.len(), 1);
        assert_eq!(before.items[0].1.size, b"upper".len() as u64);
        list_attempts.lock().clear();
        storage.set_mutation_observer(index.clone());

        let result = run_proxy_cache_cleanup(
            &storage,
            &locks,
            &access,
            &config,
            &test_policy(),
            &index,
            &tokio_util::sync::CancellationToken::new(),
        )
        .await;

        assert_eq!(result.scanned, 1);
        assert_eq!(result.deleted, 1);
        assert_eq!(result.failures, 0);
        assert!(matches!(inner.get(key).await, Err(StorageError::NotFound)));
        let deadline = Instant::now() + Duration::from_secs(10);
        while !index.persistent_protocol_ready() {
            assert!(
                Instant::now() < deadline,
                "exact Maven cleanup repair did not settle"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let after = index
            .persistent_maven_files(prefixes, "com/acme/demo/1.0".to_string(), None, 10)
            .await
            .unwrap();
        assert!(after.generation > before.generation);
        assert_eq!(after.items.len(), 1);
        assert_eq!(after.items[0].1.size, b"lower-member".len() as u64);
        let attempts = list_attempts.lock().clone();
        assert!(
            !attempts.iter().any(|prefix| prefix == "maven/"),
            "cleanup must not fall back to a root Maven inventory scan: {attempts:?}"
        );
        assert_eq!(
            attempts,
            vec!["maven/repositories/central/com/acme/demo/1.0/".to_string()],
            "the typed repair should reread only the changed bundle parent"
        );

        storage.clear_mutation_observer();
        index.shutdown_persistent().await;
    }

    #[tokio::test]
    async fn partial_maven_sidecar_delete_with_unknown_outcome_requires_full_reconcile() {
        let root = tempfile::tempdir().unwrap();
        let inner = Storage::new_local(root.path().to_str().unwrap());
        let key = "maven/repositories/central/com/acme/demo/1.0/demo-1.0.jar";
        let md5 = format!("{key}.md5");
        let sha1 = format!("{key}.sha1");
        let backend =
            Arc::new(FaultInjectBackend::new(inner.clone()).fail_delete_after(sha1.clone()));
        let list_attempts = backend.list_attempts();
        let storage = Storage::from_backend(backend);
        let locks = test_locks();
        let access = ProxyCacheAccess::start_session(storage.clone(), locks.clone())
            .await
            .unwrap();
        access.mark_recovered();
        storage.put(key, b"artifact").await.unwrap();
        storage.put(&md5, b"md5").await.unwrap();
        storage.put(&sha1, b"sha1").await.unwrap();
        make_old(root.path(), key, Duration::from_secs(100));
        write_marker(&storage, key, now_unix_secs() - 100).await;

        let index_dir = tempfile::tempdir().unwrap();
        let mut config = test_config();
        config.index.path = index_dir
            .path()
            .join("index.redb")
            .to_string_lossy()
            .into_owned();
        let enabled = HashSet::from([
            crate::registry_type::RegistryType::Maven,
            crate::registry_type::RegistryType::Npm,
        ]);
        let index = RepoIndex::open_persistent_for_test(&config, &enabled, storage.clone())
            .await
            .unwrap();
        index.reconcile_persistent_for_test().await.unwrap();
        let before = index.persistent_meta_for_test().await.unwrap();
        list_attempts.lock().clear();
        storage.set_mutation_observer(index.clone());

        let result = run_proxy_cache_cleanup(
            &storage,
            &locks,
            &access,
            &config,
            &test_policy(),
            &index,
            &tokio_util::sync::CancellationToken::new(),
        )
        .await;

        assert_eq!(result.deleted, 0);
        assert!(result.failures >= 1);
        assert!(inner.get(key).await.is_ok(), "payload must be kept");
        assert!(matches!(inner.get(&md5).await, Err(StorageError::NotFound)));
        assert!(matches!(
            inner.get(&sha1).await,
            Err(StorageError::NotFound)
        ));

        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let meta = index.persistent_meta_for_test().await.unwrap();
            if meta.generation > before.generation {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "confirmed partial delete was not repaired semantically"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            !index.persistent_protocol_ready(),
            "ambiguous delete outcome must retain the authoritative reconcile fence"
        );
        assert!(
            !list_attempts.lock().iter().any(|prefix| prefix == "maven/"),
            "typed partial repair itself must remain narrow"
        );

        index.reconcile_persistent_for_test().await.unwrap();
        assert!(index.persistent_protocol_ready());
        assert!(
            list_attempts.lock().iter().any(|prefix| prefix == "maven/"),
            "Unknown outcome must be discharged only by authoritative S2"
        );

        storage.clear_mutation_observer();
        index.shutdown_persistent().await;
    }

    #[tokio::test]
    async fn marker_read_uncertainty_and_sidecar_delete_failure_keep_payload() {
        let root = tempfile::tempdir().unwrap();
        let inner = Storage::new_local(root.path().to_str().unwrap());
        let locks = test_locks();
        let access = ProxyCacheAccess::start_session(inner.clone(), locks.clone())
            .await
            .unwrap();
        let key = "maven/repositories/central/com/acme/demo/1.0/demo-1.0.jar";
        inner.put(key, b"artifact").await.unwrap();
        for sidecar in sidecar_keys(CandidateKind::Maven, key) {
            inner.put(&sidecar, b"sum").await.unwrap();
        }
        make_old(root.path(), key, Duration::from_secs(100));
        write_marker(&inner, key, now_unix_secs() - 100).await;
        let marker_key = access_marker_key(key);
        let unreadable = Storage::from_backend(Arc::new(
            FaultInjectBackend::new(inner.clone()).fail_get(&marker_key),
        ));
        let uncertain = run_test_cleanup(
            &unreadable,
            &locks,
            &access,
            &test_config(),
            &test_policy(),
            &tokio_util::sync::CancellationToken::new(),
        )
        .await;
        assert!(uncertain.failures >= 1);
        assert_eq!(uncertain.deleted, 0);
        assert!(inner.get(key).await.is_ok());

        let first_sidecar = sidecar_keys(CandidateKind::Maven, key)
            .into_iter()
            .next()
            .unwrap();
        let undeletable = Storage::from_backend(Arc::new(
            FaultInjectBackend::new(inner.clone()).fail_delete(&first_sidecar),
        ));
        let failed_delete = run_test_cleanup(
            &undeletable,
            &locks,
            &access,
            &test_config(),
            &test_policy(),
            &tokio_util::sync::CancellationToken::new(),
        )
        .await;
        assert!(failed_delete.failures >= 1);
        assert_eq!(failed_delete.deleted, 0);
        assert!(inner.get(key).await.is_ok());
    }

    #[tokio::test]
    async fn payload_delete_failure_keeps_payload_and_access_marker() {
        let root = tempfile::tempdir().unwrap();
        let inner = Storage::new_local(root.path().to_str().unwrap());
        let locks = test_locks();
        let access = ProxyCacheAccess::start_session(inner.clone(), locks.clone())
            .await
            .unwrap();
        let key = "npm/repositories/npm-public/proxy/tarballs/pkg/pkg-1.0.0.tgz";
        inner.put(key, b"tarball").await.unwrap();
        make_old(root.path(), key, Duration::from_secs(100));
        write_marker(&inner, key, now_unix_secs() - 100).await;
        let marker_key = access_marker_key(key);
        let storage = Storage::from_backend(Arc::new(
            FaultInjectBackend::new(inner.clone()).fail_delete(key),
        ));

        let result = run_test_cleanup(
            &storage,
            &locks,
            &access,
            &test_config(),
            &test_policy(),
            &tokio_util::sync::CancellationToken::new(),
        )
        .await;

        assert!(result.failures >= 1);
        assert_eq!(result.deleted, 0);
        assert!(inner.get(key).await.is_ok());
        assert!(inner.get(&marker_key).await.is_ok());
    }

    #[tokio::test]
    async fn dry_run_cleanup_pass_performs_no_candidate_mutations() {
        let root = tempfile::tempdir().unwrap();
        let inner = Storage::new_local(root.path().to_str().unwrap());
        let locks = test_locks();
        let access = ProxyCacheAccess::start_session(inner.clone(), locks.clone())
            .await
            .unwrap();
        let key = "npm/repositories/npm-public/proxy/packuments/pkg.json";
        inner.put(key, b"{\"name\":\"pkg\"}").await.unwrap();
        make_old(root.path(), key, Duration::from_secs(100));
        let backend = FaultInjectBackend::new(inner.clone());
        let writes = backend.write_attempts();
        let deletes = backend.delete_attempts();
        let storage = Storage::from_backend(Arc::new(backend));
        let mut policy = test_policy();
        policy.dry_run = true;

        let result = run_test_cleanup(
            &storage,
            &locks,
            &access,
            &test_config(),
            &policy,
            &tokio_util::sync::CancellationToken::new(),
        )
        .await;

        assert_eq!(result.deleted, 0);
        assert!(writes.lock().is_empty());
        assert!(deletes.lock().is_empty());
        assert!(inner.get(key).await.is_ok());
        assert!(matches!(
            inner.get(&access_marker_key(key)).await,
            Err(StorageError::NotFound)
        ));
    }

    #[tokio::test]
    async fn identity_change_while_waiting_for_lock_is_kept() {
        let root = tempfile::tempdir().unwrap();
        let inner = Storage::new_local(root.path().to_str().unwrap());
        let locks = test_locks();
        let access = ProxyCacheAccess::start_session(inner.clone(), locks.clone())
            .await
            .unwrap();
        let key = "maven/repositories/central/com/acme/demo/1.0/demo-1.0.jar";
        inner.put(key, b"old").await.unwrap();
        make_old(root.path(), key, Duration::from_secs(100));
        write_marker(&inner, key, now_unix_secs() - 100).await;
        let backend = FaultInjectBackend::new(inner.clone());
        let get_attempts = backend.get_attempts();
        let storage = Storage::from_backend(Arc::new(backend));
        let lock = acquire_publish_lock(&locks, key);
        let guard = lock.lock().await;
        let task_storage = storage.clone();
        let task_locks = locks.clone();
        let task_access = access.clone();
        let task_config = test_config();
        let task_policy = test_policy();
        let task = tokio::spawn(async move {
            run_test_cleanup(
                &task_storage,
                &task_locks,
                &task_access,
                &task_config,
                &task_policy,
                &tokio_util::sync::CancellationToken::new(),
            )
            .await
        });
        let marker_key = access_marker_key(key);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if get_attempts
                    .lock()
                    .iter()
                    .any(|attempt| attempt == &marker_key)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        inner.put(key, b"new").await.unwrap();
        drop(guard);

        let result = task.await.unwrap();
        assert_eq!(result.deleted, 0);
        assert_eq!(inner.get(key).await.unwrap().as_ref(), b"new");
    }

    #[tokio::test]
    async fn previous_unclean_session_forces_baseline_only_recovery_run() {
        let root = tempfile::tempdir().unwrap();
        let storage = Storage::new_local(root.path().to_str().unwrap());
        storage.put(SESSION_KEY, b"previous-session").await.unwrap();
        let locks = test_locks();
        let access = ProxyCacheAccess::start_session(storage.clone(), locks.clone())
            .await
            .unwrap();
        assert!(access.recovery_needed());
        let key = "npm/repositories/npm-public/proxy/packuments/pkg.json";
        storage.put(key, b"{\"name\":\"pkg\"}").await.unwrap();
        make_old(root.path(), key, Duration::from_secs(100));
        write_marker(&storage, key, now_unix_secs() - 100).await;

        let result = run_recovery_test_cleanup(
            &storage,
            &locks,
            &access,
            &test_config(),
            &test_policy(),
            &tokio_util::sync::CancellationToken::new(),
        )
        .await;

        assert_eq!(result.deleted, 0);
        assert_eq!(result.baselined, 1);
        assert!(!access.recovery_needed());
        assert!(storage.get(key).await.is_ok());
    }

    #[tokio::test]
    async fn recovery_dry_run_never_clears_crash_session_marker() {
        let root = tempfile::tempdir().unwrap();
        let storage = Storage::new_local(root.path().to_str().unwrap());
        storage.put(SESSION_KEY, b"previous-session").await.unwrap();
        let locks = test_locks();
        let access = ProxyCacheAccess::start_session(storage.clone(), locks.clone())
            .await
            .unwrap();
        let key = "npm/repositories/npm-public/proxy/packuments/pkg.json";
        storage.put(key, br#"{"name":"pkg"}"#).await.unwrap();
        make_old(root.path(), key, Duration::from_secs(100));
        write_marker(&storage, key, now_unix_secs() - 100).await;
        let mut policy = test_policy();
        policy.dry_run = true;

        let result = run_recovery_test_cleanup(
            &storage,
            &locks,
            &access,
            &test_config(),
            &policy,
            &tokio_util::sync::CancellationToken::new(),
        )
        .await;
        let cancel = tokio_util::sync::CancellationToken::new();
        let worker = access.spawn_worker(cancel.clone());
        cancel.cancel();
        worker.await.unwrap();
        access.close_clean_session().await;

        assert_eq!(result.deleted, 0);
        assert!(access.recovery_needed());
        assert!(storage.get(SESSION_KEY).await.is_ok());
        assert!(storage.get(key).await.is_ok());
    }

    #[tokio::test]
    async fn touch_worker_deduplicates_flushes_and_closes_clean_session() {
        let root = tempfile::tempdir().unwrap();
        let storage = Storage::new_local(root.path().to_str().unwrap());
        let locks = test_locks();
        let access = ProxyCacheAccess::start_session(storage.clone(), locks.clone())
            .await
            .unwrap();
        access.mark_recovered();
        let key = "npm/repositories/npm-public/proxy/tarballs/pkg/pkg-1.0.0.tgz";
        storage.put(key, b"tarball").await.unwrap();
        access.record_locked(key).await;
        access.record_locked(key).await;
        assert_eq!(access.inner.state.lock().await.pending.len(), 1);

        let cancel = tokio_util::sync::CancellationToken::new();
        let worker = access.spawn_worker(cancel.clone());
        cancel.cancel();
        worker.await.unwrap();

        assert!(access.inner.worker_drained.load(Ordering::Acquire));
        assert!(access.inner.state.lock().await.pending.is_empty());
        let marker = read_access_marker(&storage, &access_marker_key(key))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(marker.payload_sha256, sha256_hex(b"tarball"));
        access.close_clean_session().await;
        let session: SessionMarker =
            serde_json::from_slice(&storage.get(SESSION_KEY).await.unwrap()).unwrap();
        assert_eq!(session.state, SessionState::Clean);
    }

    #[tokio::test]
    async fn clean_restart_is_trusted_but_disabled_interval_forces_recovery() {
        let root = tempfile::tempdir().unwrap();
        let storage = Storage::new_local(root.path().to_str().unwrap());
        let locks = test_locks();
        let first = ProxyCacheAccess::start_session(storage.clone(), locks.clone())
            .await
            .unwrap();
        assert!(first.recovery_needed());
        first.mark_recovered();
        let cancel = tokio_util::sync::CancellationToken::new();
        let worker = first.spawn_worker(cancel.clone());
        cancel.cancel();
        worker.await.unwrap();
        first.close_clean_session().await;

        let second = ProxyCacheAccess::start_session(storage.clone(), locks.clone())
            .await
            .unwrap();
        assert!(!second.recovery_needed());
        ProxyCacheAccess::mark_tracking_disabled(&storage)
            .await
            .unwrap();
        let disabled: SessionMarker =
            serde_json::from_slice(&storage.get(SESSION_KEY).await.unwrap()).unwrap();
        assert_eq!(disabled.state, SessionState::Disabled);

        let reenabled = ProxyCacheAccess::start_session(storage, locks)
            .await
            .unwrap();
        assert!(reenabled.recovery_needed());
    }

    #[tokio::test]
    async fn touch_worker_hashes_payload_with_streaming_reader_not_full_get() {
        let root = tempfile::tempdir().unwrap();
        let inner = Storage::new_local(root.path().to_str().unwrap());
        let key = "npm/repositories/npm-public/proxy/tarballs/pkg/pkg-1.0.0.tgz";
        inner.put(key, b"tarball").await.unwrap();
        let storage = Storage::from_backend(Arc::new(
            FaultInjectBackend::new(inner.clone()).fail_get(key),
        ));
        let locks = test_locks();
        let access = ProxyCacheAccess::start_session(storage, locks)
            .await
            .unwrap();
        access.record_locked(key).await;
        let cancel = tokio_util::sync::CancellationToken::new();
        let worker = access.spawn_worker(cancel.clone());
        cancel.cancel();
        worker.await.unwrap();

        assert!(access.inner.worker_drained.load(Ordering::Acquire));
        let marker = read_access_marker(&inner, &access_marker_key(key))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(marker.payload_sha256, sha256_hex(b"tarball"));
    }

    #[tokio::test]
    async fn saturated_touch_queue_applies_backpressure_without_losing_events() {
        let root = tempfile::tempdir().unwrap();
        let storage = Storage::new_local(root.path().to_str().unwrap());
        let locks = test_locks();
        let access = ProxyCacheAccess::start_session(storage, locks)
            .await
            .unwrap();
        {
            let mut state = access.inner.state.lock().await;
            for index in 0..TOUCH_CAPACITY {
                state.pending.insert(
                    format!("npm/repositories/proxy/proxy/tarballs/pkg/missing-{index}.tgz"),
                    PendingTouch {
                        last_accessed_at_unix: now_unix_secs(),
                        due: Instant::now() + TOUCH_COALESCE,
                    },
                );
            }
            TOUCH_QUEUE_DEPTH.set(state.pending.len() as i64);
        }
        let cancel = tokio_util::sync::CancellationToken::new();
        let worker = access.spawn_worker(cancel.clone());
        let final_key = "npm/repositories/proxy/proxy/tarballs/pkg/final.tgz";

        tokio::time::timeout(Duration::from_secs(10), access.record_locked(final_key))
            .await
            .expect("producer should resume after the worker frees one bounded slot");
        assert!(access
            .inner
            .state
            .lock()
            .await
            .pending
            .contains_key(final_key));

        cancel.cancel();
        worker.await.unwrap();
        assert!(access.inner.worker_drained.load(Ordering::Acquire));
        assert!(access.inner.state.lock().await.pending.is_empty());
    }

    #[tokio::test]
    async fn touch_worker_resolves_post_commit_unknown_outcome_by_readback() {
        let root = tempfile::tempdir().unwrap();
        let inner = Storage::new_local(root.path().to_str().unwrap());
        let key = "npm/repositories/npm-public/proxy/tarballs/pkg/pkg-1.0.0.tgz";
        inner.put(key, b"tarball").await.unwrap();
        let marker_key = access_marker_key(key);
        let storage = Storage::from_backend(Arc::new(
            FaultInjectBackend::new(inner.clone()).fail_put_after(&marker_key),
        ));
        let locks = test_locks();
        let access = ProxyCacheAccess::start_session(storage.clone(), locks)
            .await
            .unwrap();
        access.record_locked(key).await;
        let cancel = tokio_util::sync::CancellationToken::new();
        let worker = access.spawn_worker(cancel.clone());
        cancel.cancel();
        worker.await.unwrap();

        assert!(access.inner.worker_drained.load(Ordering::Acquire));
        assert!(read_access_marker(&inner, &marker_key)
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn touch_marker_read_uncertainty_never_blind_overwrites() {
        let root = tempfile::tempdir().unwrap();
        let inner = Storage::new_local(root.path().to_str().unwrap());
        let key = "npm/repositories/npm-public/proxy/tarballs/pkg/pkg-1.0.0.tgz";
        inner.put(key, b"tarball").await.unwrap();
        let future = now_unix_secs() + 100;
        write_marker(&inner, key, future).await;
        let marker_key = access_marker_key(key);
        let backend = FaultInjectBackend::new(inner.clone()).fail_get(&marker_key);
        let writes = backend.write_attempts();
        let storage = Storage::from_backend(Arc::new(backend));
        let locks = test_locks();
        let access = ProxyCacheAccess::start_session(storage, locks)
            .await
            .unwrap();
        writes.lock().clear();
        access.record_locked(key).await;
        let cancel = tokio_util::sync::CancellationToken::new();
        let worker = access.spawn_worker(cancel.clone());
        cancel.cancel();
        worker.await.unwrap();

        assert!(!access.inner.worker_drained.load(Ordering::Acquire));
        assert!(access.inner.state.lock().await.pending.contains_key(key));
        assert!(!writes.lock().iter().any(|attempt| attempt == &marker_key));
        let marker = read_access_marker(&inner, &marker_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(marker.last_accessed_at_unix, future);
    }

    #[tokio::test]
    async fn baseline_preserves_a_newer_durable_touch_timestamp() {
        let root = tempfile::tempdir().unwrap();
        let storage = Storage::new_local(root.path().to_str().unwrap());
        let locks = test_locks();
        let access = ProxyCacheAccess::start_session(storage.clone(), locks.clone())
            .await
            .unwrap();
        let key = "npm/repositories/npm-public/proxy/tarballs/pkg/pkg-1.0.0.tgz";
        storage.put(key, b"tarball").await.unwrap();
        let newer_touch = now_unix_secs() + 100;
        write_marker(&storage, key, newer_touch).await;

        assert!(baseline_payload(&storage, &locks, &access, key, false)
            .await
            .unwrap());

        let marker = read_access_marker(&storage, &access_marker_key(key))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(marker.last_accessed_at_unix, newer_touch);
    }

    #[tokio::test]
    async fn failed_touch_flush_keeps_session_marker_for_crash_recovery() {
        let root = tempfile::tempdir().unwrap();
        let inner = Storage::new_local(root.path().to_str().unwrap());
        let key = "npm/repositories/npm-public/proxy/tarballs/pkg/pkg-1.0.0.tgz";
        inner.put(key, b"tarball").await.unwrap();
        let marker_key = access_marker_key(key);
        let storage = Storage::from_backend(Arc::new(
            FaultInjectBackend::new(inner.clone()).fail_put(&marker_key),
        ));
        let locks = test_locks();
        let access = ProxyCacheAccess::start_session(storage.clone(), locks)
            .await
            .unwrap();
        access.record_locked(key).await;
        let cancel = tokio_util::sync::CancellationToken::new();
        let worker = access.spawn_worker(cancel.clone());
        cancel.cancel();
        worker.await.unwrap();
        access.close_clean_session().await;

        assert!(!access.inner.worker_drained.load(Ordering::Acquire));
        assert!(inner.get(SESSION_KEY).await.is_ok());
        assert!(access.inner.state.lock().await.pending.contains_key(key));
    }

    async fn settled_test_index(storage: &Storage) -> Arc<RepoIndex> {
        let index = Arc::new(RepoIndex::new());
        assert!(
            index
                .rebuild_for_test(crate::registry_type::RegistryType::Maven, storage)
                .await
        );
        assert!(
            index
                .rebuild_for_test(crate::registry_type::RegistryType::Npm, storage)
                .await
        );
        index
    }

    async fn run_test_cleanup(
        storage: &Storage,
        publish_locks: &PublishLocks,
        access: &ProxyCacheAccess,
        config: &Config,
        policy: &ProxyCacheCleanupConfig,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> ProxyCacheCleanupResult {
        access.mark_recovered();
        let index = settled_test_index(storage).await;
        run_proxy_cache_cleanup(
            storage,
            publish_locks,
            access,
            config,
            policy,
            &index,
            cancel,
        )
        .await
    }

    async fn run_recovery_test_cleanup(
        storage: &Storage,
        publish_locks: &PublishLocks,
        access: &ProxyCacheAccess,
        config: &Config,
        policy: &ProxyCacheCleanupConfig,
        cancel: &tokio_util::sync::CancellationToken,
    ) -> ProxyCacheCleanupResult {
        let index = settled_test_index(storage).await;
        run_proxy_cache_cleanup(
            storage,
            publish_locks,
            access,
            config,
            policy,
            &index,
            cancel,
        )
        .await
    }

    #[tokio::test]
    async fn scheduler_runs_boot_pass_after_target_indexes_settle() {
        let root = tempfile::tempdir().unwrap();
        let inner = Storage::new_local(root.path().to_str().unwrap());
        let backend = FaultInjectBackend::new(inner);
        let list_attempts = backend.list_attempts();
        let storage = Storage::from_backend(Arc::new(backend));
        let locks = test_locks();
        let index = settled_test_index(&storage).await;
        list_attempts.lock().clear();
        let access = ProxyCacheAccess::start_session(storage.clone(), locks.clone())
            .await
            .unwrap();
        let cleanup_lock = Arc::new(tokio::sync::Mutex::new(()));
        let cancel = tokio_util::sync::CancellationToken::new();
        let handle = spawn_proxy_cache_cleanup_scheduler(
            storage,
            locks,
            index,
            Arc::new(test_config()),
            access.clone(),
            cleanup_lock.clone(),
            cancel.clone(),
        );

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if !access.recovery_needed() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(list_attempts.lock().is_empty());
        cancel.cancel();
        handle.await.unwrap();
    }

    #[tokio::test]
    async fn scheduler_cancels_while_waiting_for_initial_indexes() {
        let root = tempfile::tempdir().unwrap();
        let inner = Storage::new_local(root.path().to_str().unwrap());
        let backend = FaultInjectBackend::new(inner);
        let list_attempts = backend.list_attempts();
        let storage = Storage::from_backend(Arc::new(backend));
        let locks = test_locks();
        let access = ProxyCacheAccess::start_session(storage.clone(), locks.clone())
            .await
            .unwrap();
        let cancel = tokio_util::sync::CancellationToken::new();
        let handle = spawn_proxy_cache_cleanup_scheduler(
            storage,
            locks,
            Arc::new(RepoIndex::new()),
            Arc::new(test_config()),
            access,
            Arc::new(tokio::sync::Mutex::new(())),
            cancel.clone(),
        );
        tokio::task::yield_now().await;
        cancel.cancel();

        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap();
        assert!(list_attempts.lock().is_empty());
    }

    #[tokio::test]
    async fn scheduler_boot_wait_for_cleanup_lock_is_cancellation_safe() {
        let root = tempfile::tempdir().unwrap();
        let inner = Storage::new_local(root.path().to_str().unwrap());
        let backend = FaultInjectBackend::new(inner);
        let list_attempts = backend.list_attempts();
        let storage = Storage::from_backend(Arc::new(backend));
        let locks = test_locks();
        let index = settled_test_index(&storage).await;
        list_attempts.lock().clear();
        let access = ProxyCacheAccess::start_session(storage.clone(), locks.clone())
            .await
            .unwrap();
        let cleanup_lock = Arc::new(tokio::sync::Mutex::new(()));
        let held = cleanup_lock.lock().await;
        let cancel = tokio_util::sync::CancellationToken::new();
        let handle = spawn_proxy_cache_cleanup_scheduler(
            storage,
            locks,
            index,
            Arc::new(test_config()),
            access,
            Arc::clone(&cleanup_lock),
            cancel.clone(),
        );
        tokio::task::yield_now().await;
        assert!(list_attempts.lock().is_empty());
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .unwrap()
            .unwrap();
        drop(held);
    }
}
