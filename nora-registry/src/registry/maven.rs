// Copyright (c) 2026 The NORA Authors
// SPDX-License-Identifier: MIT

//! Maven registry — Maven 2 repository layout with checksums, immutability,
//! and automatic `maven-metadata.xml` generation.
//!
//! Implements:
//!   GET  /maven2/{*path}  — download artifact, checksum, or metadata
//!   PUT  /maven2/{*path}  — upload artifact (with auto-checksum + metadata update)

use crate::activity_log::{ActionType, ActivityEntry};
use crate::audit::AuditEntry;
use crate::auth::{enforce_namespace_scope, NamespaceAuthority};
use crate::config::{
    MavenProxy, MavenProxyEntry, MavenRepository, MavenVersionPolicy, MavenWritePolicy,
};
use crate::registry::{circuit_open_response, method_not_allowed, proxy_fetch, ProxyError};
use crate::registry_type::RegistryType;
use crate::storage::StorageError;
use crate::validation::ends_with_ci;
use crate::AppState;
use axum::{
    body::{to_bytes, Bytes},
    extract::{Path, State},
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Extension, Router,
};
use quick_xml::{events::Event, Reader};
use sha2::Digest;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::{Duration, Instant};

const MAVEN_NEGATIVE_CACHE_MAX_ENTRIES: usize = 10_000;
const MAVEN_REVALIDATION_CACHE_MAX_ENTRIES: usize = 10_000;

#[derive(Clone, Copy, Debug)]
struct MavenPolicyBlock;

/// Marks a response whose failure originated in Nora's local storage. Group
/// resolution may fail over after an upstream outage, but must never treat a
/// local read failure as an authoritative member miss.
#[derive(Clone, Copy, Debug)]
struct MavenStorageFailure;

fn storage_failure_response(operation: &'static str, key: &str, error: &StorageError) -> Response {
    let mut response = crate::registry::storage_error_response("maven", operation, key, error);
    response.extensions_mut().insert(MavenStorageFailure);
    response
}

/// Build the storage key for a Maven artifact at repo-relative `path`.
///
/// Single source of truth for the `maven/<path>` layout so that anything writing
/// Maven objects (the handlers here, and `nora import` — review R7, contract
/// `import-key-format-equals-handler-key-format`) produces byte-identical keys
/// that GC/retention/UI browse walk as strings.
pub(crate) fn storage_key(path: &str) -> String {
    format!("maven/{path}")
}

pub(crate) fn repository_storage_key(repository: &str, path: &str) -> String {
    format!("maven/repositories/{repository}/{path}")
}

pub fn routes() -> Router<AppState> {
    Router::new().route(
        "/maven2/{*path}",
        get(download_legacy)
            .put(upload_legacy)
            .fallback(|| async { method_not_allowed("GET, PUT") }),
    )
}

#[derive(Clone)]
struct DirectRepository {
    name: Option<String>,
    proxies: Vec<MavenProxyEntry>,
    metadata_ttl: i64,
    negative_ttl: i64,
    version_policy: MavenVersionPolicy,
    write_policy: MavenWritePolicy,
}

impl DirectRepository {
    fn legacy(state: &AppState) -> Self {
        Self {
            name: None,
            proxies: state.config.maven.proxies.clone(),
            metadata_ttl: state.config.maven.metadata_ttl,
            negative_ttl: 0,
            version_policy: MavenVersionPolicy::Mixed,
            write_policy: if state.config.maven.immutable_releases {
                MavenWritePolicy::AllowOnce
            } else {
                MavenWritePolicy::Allow
            },
        }
    }

    fn from_config(state: &AppState, repository: &MavenRepository) -> Option<Self> {
        match repository {
            MavenRepository::Hosted {
                name,
                version_policy,
                write_policy,
            } => Some(Self {
                name: Some(name.clone()),
                proxies: Vec::new(),
                metadata_ttl: state.config.maven.metadata_ttl,
                negative_ttl: 0,
                version_policy: *version_policy,
                write_policy: *write_policy,
            }),
            MavenRepository::Proxy {
                name,
                url,
                auth,
                version_policy,
                metadata_ttl,
                negative_ttl,
            } => Some(Self {
                name: Some(name.clone()),
                proxies: vec![MavenProxyEntry::Full(MavenProxy {
                    url: url.clone(),
                    auth: auth.clone(),
                })],
                metadata_ttl: metadata_ttl.unwrap_or(state.config.maven.metadata_ttl),
                negative_ttl: *negative_ttl,
                version_policy: *version_policy,
                write_policy: MavenWritePolicy::Deny,
            }),
            MavenRepository::Group { .. } => None,
        }
    }

    fn storage_key(&self, path: &str) -> String {
        self.name.as_deref().map_or_else(
            || storage_key(path),
            |name| repository_storage_key(name, path),
        )
    }

    fn storage_prefix(&self) -> String {
        self.name.as_deref().map_or_else(
            || "maven/".to_string(),
            |name| format!("maven/repositories/{name}/"),
        )
    }

    fn is_proxy(&self) -> bool {
        !self.proxies.is_empty()
    }

    fn tracks_proxy_cache(&self) -> bool {
        self.name.is_some() && self.is_proxy()
    }
}

// ============================================================================
// Path parsing
// ============================================================================

struct MavenCoordinates {
    group_path: String,
    artifact_id: String,
    version: String,
}

enum MavenPathKind {
    VersionFile(MavenCoordinates),
    #[allow(dead_code)]
    ArtifactMeta {
        group_path: String,
        artifact_id: String,
        filename: String,
    },
    Opaque,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MavenMetadataLevel {
    Group,
    Artifact,
    ArtifactAndGroup,
    Version,
}

fn classify_metadata_level(data: &[u8]) -> Option<MavenMetadataLevel> {
    let mut reader = Reader::from_reader(data);
    reader.config_mut().trim_text(true);

    let mut depth = 0;
    let mut root_seen = false;
    let mut root_closed = false;
    let mut has_artifact_id = false;
    let mut has_version = false;
    let mut has_plugins = false;

    loop {
        match reader.read_event() {
            Ok(Event::Start(element)) => {
                let name = element.local_name();
                if depth == 0 {
                    if root_seen || name.as_ref() != b"metadata" {
                        return None;
                    }
                    root_seen = true;
                } else if depth == 1 {
                    has_artifact_id |= name.as_ref() == b"artifactId";
                    has_version |= name.as_ref() == b"version";
                    has_plugins |= name.as_ref() == b"plugins";
                }
                depth += 1;
            }
            Ok(Event::Empty(element)) => {
                let name = element.local_name();
                if depth == 0 {
                    if root_seen || name.as_ref() != b"metadata" {
                        return None;
                    }
                    root_seen = true;
                    root_closed = true;
                } else if depth == 1 {
                    has_artifact_id |= name.as_ref() == b"artifactId";
                    has_version |= name.as_ref() == b"version";
                    has_plugins |= name.as_ref() == b"plugins";
                }
            }
            Ok(Event::End(element)) => {
                if depth == 0 {
                    return None;
                }
                depth -= 1;
                if depth == 0 {
                    if element.local_name().as_ref() != b"metadata" {
                        return None;
                    }
                    root_closed = true;
                }
            }
            Ok(Event::Eof) => break,
            Err(_) => return None,
            _ => {}
        }
    }

    if !root_seen || !root_closed || depth != 0 {
        return None;
    }

    if has_version {
        Some(MavenMetadataLevel::Version)
    } else if has_artifact_id && has_plugins {
        Some(MavenMetadataLevel::ArtifactAndGroup)
    } else if has_artifact_id {
        Some(MavenMetadataLevel::Artifact)
    } else {
        Some(MavenMetadataLevel::Group)
    }
}

fn metadata_document_key(checksum_key: &str) -> Option<&str> {
    [".md5", ".sha1", ".sha256", ".sha512"]
        .into_iter()
        .find_map(|suffix| checksum_key.strip_suffix(suffix))
}

fn checksum_suffix(path: &str) -> Option<&'static str> {
    [".md5", ".sha1", ".sha256", ".sha512"]
        .into_iter()
        .find(|suffix| path.ends_with(suffix))
        .map(|suffix| &suffix[1..])
}

fn classify_path(path: &str) -> MavenPathKind {
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    if segments.len() < 2 {
        return MavenPathKind::Opaque;
    }

    let last = segments[segments.len() - 1];

    if (last == "maven-metadata.xml" || last.starts_with("maven-metadata.xml."))
        && segments.len() >= 2
    {
        return MavenPathKind::ArtifactMeta {
            group_path: segments[..segments.len() - 2].join("/"),
            artifact_id: segments[segments.len() - 2].to_string(),
            filename: last.to_string(),
        };
    }

    if segments.len() >= 4 {
        return MavenPathKind::VersionFile(MavenCoordinates {
            group_path: segments[..segments.len() - 3].join("/"),
            artifact_id: segments[segments.len() - 3].to_string(),
            version: segments[segments.len() - 2].to_string(),
        });
    }

    MavenPathKind::Opaque
}

fn metadata_artifact_coordinates(path: &str) -> Option<(String, String)> {
    let document_path = metadata_document_key(path).unwrap_or(path);
    version_metadata_path(document_path)
        .map(|(group_path, artifact_id, _)| (group_path, artifact_id))
        .or_else(|| match classify_path(document_path) {
            MavenPathKind::ArtifactMeta {
                group_path,
                artifact_id,
                ..
            } => Some((group_path, artifact_id)),
            _ => None,
        })
}

fn invalidate_maven_document_index(
    state: &AppState,
    repository: &DirectRepository,
    document_path: &str,
    document: &[u8],
) {
    let server_managed_artifact_metadata = matches!(
        classify_path(document_path),
        MavenPathKind::ArtifactMeta { ref filename, .. } if filename == "maven-metadata.xml"
    ) && matches!(
        classify_metadata_level(document),
        Some(
            MavenMetadataLevel::Group
                | MavenMetadataLevel::Artifact
                | MavenMetadataLevel::ArtifactAndGroup
        )
    );
    if server_managed_artifact_metadata {
        if let Some((group_path, artifact_id)) = metadata_artifact_coordinates(document_path) {
            state.repo_index.invalidate_maven_ga(
                repository.name.as_deref().unwrap_or(""),
                &format!("{group_path}/{artifact_id}"),
            );
            return;
        }
    }
    state
        .repo_index
        .invalidate_maven_path(repository.name.as_deref().unwrap_or(""), document_path);
}

fn is_snapshot(version: &str) -> bool {
    version.ends_with("-SNAPSHOT")
}

fn version_allowed_by_policy(policy: MavenVersionPolicy, version: &str) -> bool {
    match policy {
        MavenVersionPolicy::Release => !is_snapshot(version),
        MavenVersionPolicy::Snapshot => is_snapshot(version),
        MavenVersionPolicy::Mixed => true,
    }
}

fn mutation_lock_key(repository: &DirectRepository, path: &str) -> String {
    let document_path = metadata_document_key(path).unwrap_or(path);
    if let Some((group_path, artifact_id, _)) = version_metadata_path(document_path) {
        return repository.storage_key(&format!("{group_path}/{artifact_id}/maven-metadata.xml"));
    }
    match classify_path(document_path) {
        MavenPathKind::VersionFile(coordinates) => repository.storage_key(&format!(
            "{}/{}/maven-metadata.xml",
            coordinates.group_path, coordinates.artifact_id
        )),
        MavenPathKind::ArtifactMeta {
            group_path,
            artifact_id,
            ..
        } => repository.storage_key(&format!("{group_path}/{artifact_id}/maven-metadata.xml")),
        MavenPathKind::Opaque => repository.storage_key(document_path),
    }
}

fn insert_negative_cache_entry(
    cache: &mut HashMap<String, Instant>,
    key: String,
    ttl_seconds: i64,
    max_entries: usize,
) {
    let now = Instant::now();
    cache.retain(|_, created| now.duration_since(*created).as_secs() < ttl_seconds.max(0) as u64);
    if !cache.contains_key(&key) && cache.len() >= max_entries {
        if let Some(oldest) = cache
            .iter()
            .min_by_key(|(_, created)| **created)
            .map(|(key, _)| key.clone())
        {
            cache.remove(&oldest);
        }
    }
    cache.insert(key, now);
}

fn revalidation_cache_fresh(
    cache: &mut HashMap<String, (Instant, [u8; 32])>,
    key: &str,
    expected_fingerprint: [u8; 32],
    metadata_ttl: i64,
) -> bool {
    if metadata_ttl <= 0 {
        cache.remove(key);
        return false;
    }
    let now = Instant::now();
    match cache.get(key).copied() {
        Some((validated, stored_fingerprint))
            if now.duration_since(validated).as_secs() < metadata_ttl as u64
                && stored_fingerprint == expected_fingerprint =>
        {
            true
        }
        Some(_) => {
            cache.remove(key);
            false
        }
        None => false,
    }
}

fn revalidation_fingerprint(data: &[u8]) -> [u8; 32] {
    sha2::Sha256::digest(data).into()
}

fn record_revalidation(
    cache: &mut HashMap<String, (Instant, [u8; 32])>,
    key: String,
    fingerprint: [u8; 32],
    max_entries: usize,
) {
    if !cache.contains_key(&key) && cache.len() >= max_entries {
        if let Some(oldest) = cache
            .iter()
            .min_by_key(|(_, (validated, _))| *validated)
            .map(|(key, _)| key.clone())
        {
            cache.remove(&oldest);
        }
    }
    cache.insert(key, (Instant::now(), fingerprint));
}

/// Whether a Maven path points at a MUTABLE resource that must be revalidated when proxied:
/// `maven-metadata.xml` (and its checksums) is rewritten as versions are deployed, and SNAPSHOT
/// version files are republished in place. Release artifacts are immutable.
fn is_mutable_maven_path(path: &str) -> bool {
    if ends_with_ci(path, "maven-metadata.xml")
        || ends_with_ci(path, "maven-metadata.xml.sha1")
        || ends_with_ci(path, "maven-metadata.xml.md5")
        || ends_with_ci(path, "maven-metadata.xml.sha256")
        || ends_with_ci(path, "maven-metadata.xml.sha512")
    {
        return true;
    }
    matches!(classify_path(path), MavenPathKind::VersionFile(c) if is_snapshot(&c.version))
}

fn spawn_proxy_cache(state: &AppState, path: &str, key: String, data: Bytes) {
    if is_mutable_maven_path(path) {
        state.spawn_cache("maven", key, data);
    } else {
        state.spawn_cache_immutable("maven", key, data);
    }
}

async fn record_still_cached_maven_access(state: &AppState, key: &str, expected: &[u8]) -> bool {
    let lock = state.publish_lock(key);
    let _guard = lock.lock().await;
    match state.storage.get(key).await {
        Ok(current) if current.as_ref() == expected => {
            state.record_proxy_cache_access_locked(key).await;
            true
        }
        Ok(_) | Err(StorageError::NotFound) => false,
        Err(_) => {
            tracing::warn!(
                key,
                backend = state.storage.backend_name(),
                error_class = "cache_recheck_failed",
                "Maven stale cache access could not be revalidated"
            );
            false
        }
    }
}

/// True when a URL points at Maven Central (one of its canonical hosts) — the only
/// Maven upstream with a per-artifact date source (its search API). A private mirror
/// (Nexus/Artifactory) returns false, so its coordinates are never sent to the public
/// search.maven.org (#68/#733).
fn url_is_maven_central(u: &str) -> bool {
    u.contains("repo1.maven.org")
        || u.contains("repo.maven.apache.org")
        || u.contains("search.maven.org")
        || u.contains("central.sonatype")
}

/// True when a configured Maven proxy points at Maven Central. Gates the search
/// query so internal coordinates are never sent to search.maven.org.
fn maven_upstream_is_central(proxies: &[MavenProxyEntry]) -> bool {
    proxies.iter().any(|p| url_is_maven_central(p.url()))
}

/// Best-effort upload timestamp for a Maven Central GAV via the Central search
/// API. Maven's repo protocol exposes no per-artifact date, so this is the only
/// source; any failure → `None` (the quarantine falls back to NORA's own clock).
async fn fetch_maven_central_date(
    client: &reqwest::Client,
    group: &str,
    artifact: &str,
    version: &str,
    timeout_secs: u64,
) -> Option<i64> {
    let url = format!(
        "https://search.maven.org/solrsearch/select?q=g:%22{}%22+AND+a:%22{}%22+AND+v:%22{}%22&core=gav&rows=1&wt=json",
        group, artifact, version
    );
    let resp = client
        .get(&url)
        .timeout(std::time::Duration::from_secs(timeout_secs))
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let json: serde_json::Value = resp.json().await.ok()?;
    let ts_ms = json
        .get("response")?
        .get("docs")?
        .as_array()?
        .first()?
        .get("timestamp")?
        .as_i64()?;
    Some(ts_ms / 1000)
}

// ============================================================================
// Download
// ============================================================================

async fn download_legacy(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Path(path): Path<String>,
) -> Response {
    if let Some(repository) = state.config.maven.default_repository.clone() {
        return download_configured(state, headers, &repository, path).await;
    }
    let repository = DirectRepository::legacy(&state);
    download_direct(state, headers, repository, path).await
}

pub(crate) async fn download_named(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Path((repository, path)): Path<(String, String)>,
) -> Response {
    download_configured(state, headers, &repository, path).await
}

async fn download_configured(
    state: AppState,
    headers: axum::http::HeaderMap,
    repository: &str,
    path: String,
) -> Response {
    let Some(config) = state.config.maven.repository(repository).cloned() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    match config {
        MavenRepository::Group { members, .. } => {
            download_group(state, headers, &members, path).await
        }
        direct => {
            let direct =
                DirectRepository::from_config(&state, &direct).expect("non-group repository");
            download_direct(state, headers, direct, path).await
        }
    }
}

async fn download_direct(
    state: AppState,
    headers: axum::http::HeaderMap,
    repository: DirectRepository,
    path: String,
) -> Response {
    // Checksum sidecars are derived views of the exact bytes we would return for the
    // base object. Never trust a cached or upstream sidecar independently: it can be
    // missing or stale after a mutable metadata/SNAPSHOT update.
    if let Some(suffix) = checksum_suffix(&path) {
        let Some(document_path) = metadata_document_key(&path) else {
            return StatusCode::BAD_REQUEST.into_response();
        };
        let response = Box::pin(download_direct(
            state.clone(),
            headers,
            repository.clone(),
            document_path.to_string(),
        ))
        .await;
        if response.status() != StatusCode::OK {
            return response;
        }
        let prelock_base = match to_bytes(response.into_body(), usize::MAX).await {
            Ok(base) => base,
            Err(error) => {
                tracing::error!(%error, path = %document_path, "Failed to read Maven base object");
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
        };
        let mutation_key = mutation_lock_key(&repository, document_path);
        let lock = state.publish_lock(&mutation_key);
        let _guard = lock.lock().await;
        let document_key = repository.storage_key(document_path);
        let cache_lock = (mutation_key != document_key).then(|| state.publish_lock(&document_key));
        let _cache_guard = match cache_lock.as_ref() {
            Some(lock) => Some(lock.lock().await),
            None => None,
        };
        let latest = match state.storage.get(&document_key).await {
            Ok(latest) => Some(latest),
            Err(StorageError::NotFound) if !is_mutable_maven_path(document_path) => None,
            Err(StorageError::NotFound) => return StatusCode::NOT_FOUND.into_response(),
            Err(error) => {
                tracing::error!(
                    %error,
                    key = %document_key,
                    "Failed to re-read Maven checksum base object under mutation lock"
                );
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
        };
        let checksum_source = latest.as_ref().unwrap_or(&prelock_base);
        let Some(checksum) = checksum_hex(suffix, checksum_source) else {
            return StatusCode::BAD_REQUEST.into_response();
        };
        let key = repository.storage_key(&path);
        // If the immutable base has not landed yet (or cleanup removed it
        // between the recursive response and this lock), return the derived
        // checksum but do not leave an orphan sidecar behind.
        if latest.is_some() {
            match state.storage.put(&key, checksum.as_bytes()).await {
                Ok(()) => invalidate_maven_document_index(
                    &state,
                    &repository,
                    document_path,
                    checksum_source,
                ),
                Err(error) => {
                    tracing::warn!(%error, %key, "Failed to refresh derived Maven checksum");
                }
            }
        }
        return with_content_type(&path, Bytes::from(checksum)).into_response();
    }

    let key = repository.storage_key(&path);
    if repository.is_proxy() {
        let requested_version = version_metadata_path(&path)
            .map(|(_, _, version)| version)
            .or_else(|| match classify_path(&path) {
                MavenPathKind::VersionFile(coords) => Some(coords.version),
                _ => None,
            });
        if let Some(version) = requested_version {
            let allowed = match repository.version_policy {
                MavenVersionPolicy::Release => !is_snapshot(&version),
                MavenVersionPolicy::Snapshot => is_snapshot(&version),
                MavenVersionPolicy::Mixed => true,
            };
            if !allowed {
                return StatusCode::NOT_FOUND.into_response();
            }
        }
    }

    let artifact_name = path
        .split('/')
        .rev()
        .take(3)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>()
        .join("/");

    // Classify path for curation (used in both pre-download and integrity checks)
    let curation_coords = if let MavenPathKind::VersionFile(coords) = classify_path(&path) {
        let maven_name = format!(
            "{}:{}",
            coords.group_path.replace('/', "."),
            coords.artifact_id
        );
        Some((maven_name, coords.version))
    } else {
        None
    };

    // #733 serve-local: an internal-namespace artifact is operator-owned — skip curation and
    // serve any local copy (fresh below, or stale before the proxy loop); the upstream branch is
    // blocked separately (never proxy an internal name).
    let internal = curation_coords
        .as_ref()
        .map(|(n, _)| {
            crate::curation::is_internal_namespace(
                &state.curation().curation_engine,
                crate::curation::RegistryType::Maven,
                n,
            )
        })
        .unwrap_or(false);

    // Release date for the digest-quarantine first-seen clock (#748/#750), hoisted
    // to function scope so the serve gate below can use it. Maven's repo layout
    // exposes no per-artifact upload date, so for a Maven Central upstream we query
    // the Central search API (gated to a Central proxy to avoid leaking coordinates);
    // other proxies have no date (None → NORA's own clock). Hosted-only uses mtime.
    //
    // #754: the Central query only happens on a cache MISS. On a cache hit the digest
    // is already recorded (quarantine `record` is idempotent → the date is ignored),
    // so a cheap local stat skips the upstream round-trip — a cache hit never pays it.
    let mut cached_meta = match state.storage.stat(&key).await {
        Ok(meta) => meta,
        Err(error) => {
            return storage_failure_response("stat", &key, &error);
        }
    };
    let already_cached = cached_meta.is_some();
    let publish_date: Option<i64> =
        if let Some((ref maven_name, ref maven_version)) = curation_coords {
            if !repository.is_proxy() {
                crate::curation::extract_mtime_as_publish_date(&state.storage, &key).await
            } else if !already_cached
                && !internal
                && state.config.server.trust_upstream_dates
                && maven_upstream_is_central(&repository.proxies)
            {
                // #68/#733: never send an internal-namespace GAV to the hardcoded public
                // search.maven.org — that would leak operator-internal coordinates.
                match maven_name.split_once(':') {
                    Some((group, artifact)) => {
                        fetch_maven_central_date(
                            &state.http_client,
                            group,
                            artifact,
                            maven_version,
                            state.config.maven.proxy_timeout,
                        )
                        .await
                    }
                    None => None,
                }
            } else {
                None
            }
        } else {
            None
        };

    // Curation check — only for versioned artifact files, not metadata
    if let Some((ref maven_name, ref maven_version)) = curation_coords {
        if !internal {
            if let Some(response) = crate::curation::check_download(
                &state.curation().curation_engine,
                state.bypass_token().as_deref(),
                &headers,
                crate::curation::RegistryType::Maven,
                maven_name,
                Some(maven_version),
                publish_date,
            ) {
                return response;
            }
        }
    }

    // Resolved here, not at the serve site, so the range branch below can see it.
    let (q_mode, q_secs) = crate::digest_quarantine::resolve_global(
        state.config.curation.maven.quarantine.as_ref().or(state
            .config
            .curation
            .quarantine
            .as_ref()),
        state
            .config
            .curation
            .maven
            .quarantine_ttl
            .as_deref()
            .or(state.config.curation.quarantine_ttl.as_deref()),
    );

    // Resumable download (#657): serve the requested bytes from the backend and skip
    // the full read below. Release artifacts only — maven-metadata.xml and SNAPSHOT
    // paths are mutable and need the freshness check. A partial read cannot be hashed,
    // so neither the quarantine gate nor the curation integrity check can run on it:
    // the range serve stands down while quarantine holds artifacts, and integrity is
    // the client's own checksum, as docker does.
    if !is_mutable_maven_path(&path)
        && matches!(q_mode, crate::digest_quarantine::QuarantineMode::Off)
    {
        if let Some(meta) = cached_meta.as_ref() {
            let range_lock = repository
                .tracks_proxy_cache()
                .then(|| state.publish_lock(&key));
            let _range_guard = match range_lock.as_ref() {
                Some(lock) => Some(lock.lock().await),
                None => None,
            };
            let range_meta = if repository.tracks_proxy_cache() {
                match state.storage.stat(&key).await {
                    Ok(meta) => meta,
                    Err(error) => return storage_failure_response("stat", &key, &error),
                }
            } else {
                Some(meta.clone())
            };
            if let Some(range_meta) = range_meta {
                if let Some(response) = crate::registry::range::range_response(
                    &state.storage,
                    &[&key],
                    &headers,
                    range_meta.size,
                    maven_content_type(&path),
                    &[(
                        header::CACHE_CONTROL,
                        "public, max-age=31536000, immutable".to_string(),
                    )],
                )
                .await
                {
                    if response.status() == StatusCode::PARTIAL_CONTENT {
                        if repository.tracks_proxy_cache() {
                            state.record_proxy_cache_access_locked(&key).await;
                        }
                        state.metrics.record_download("maven");
                        state.metrics.record_cache_hit("maven");
                    }
                    return response;
                }
            }
        }
    }

    // Read the cached artifact eagerly — kept for the freshness check and the stale-on-error fallback.
    // Only an explicit miss may fall through to another group member or an upstream.
    // Treating corruption/transient storage failure as absence would let a lower-priority
    // proxy silently replace authoritative hosted bytes.
    let cache_lock = repository
        .tracks_proxy_cache()
        .then(|| state.publish_lock(&key));
    let cache_guard = match cache_lock.as_ref() {
        Some(lock) => Some(lock.lock().await),
        None => None,
    };
    if repository.tracks_proxy_cache() {
        cached_meta = match state.storage.stat(&key).await {
            Ok(meta) => meta,
            Err(error) => return storage_failure_response("stat", &key, &error),
        };
    }
    let cached = match state.storage.get(&key).await {
        Ok(data) => Some(data),
        Err(StorageError::NotFound) => None,
        Err(error) => {
            tracing::error!(%error, %key, "Failed to read Maven artifact from storage");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };
    if cached.is_none() && repository.negative_ttl > 0 {
        let now = std::time::Instant::now();
        let mut cache = state.maven_negative_cache.lock();
        match cache.get(&key).copied() {
            Some(created)
                if now.duration_since(created).as_secs() < repository.negative_ttl as u64 =>
            {
                return StatusCode::NOT_FOUND.into_response();
            }
            Some(_) => {
                cache.remove(&key);
            }
            None => {}
        }
    }

    // maven-metadata.xml and SNAPSHOT artifacts are MUTABLE (rewritten as versions deploy); a
    // proxied mutable path must be revalidated against upstream unless within a positive
    // metadata_ttl window — otherwise newly deployed versions / SNAPSHOT updates never appear.
    // Release artifacts are immutable and served from cache; a hosted artifact is authoritative.
    let cache_fresh = match &cached {
        None => false,
        Some(_) if !is_mutable_maven_path(&path) => true,
        Some(data) => {
            let modified = cached_meta.as_ref().map(|m| m.modified);
            crate::cache_ttl::mutable_ref_fresh(
                repository.is_proxy(),
                repository.metadata_ttl,
                modified,
            ) || {
                // Metadata bodies are unbounded; hash before taking the process-wide cache
                // mutex so unrelated repository reads cannot be stalled by the digest work.
                let fingerprint = revalidation_fingerprint(data);
                revalidation_cache_fresh(
                    &mut state.maven_revalidation_cache.lock(),
                    &key,
                    fingerprint,
                    repository.metadata_ttl,
                )
            }
        }
    };

    if let Some(ref data) = cached {
        if cache_fresh {
            // Curation integrity verification (issue #189)
            if let Some((ref maven_name, ref maven_version)) = curation_coords {
                if let Some(response) = crate::curation::verify_integrity(
                    &state.curation().curation_engine,
                    crate::curation::RegistryType::Maven,
                    maven_name,
                    Some(maven_version),
                    data,
                ) {
                    return response;
                }
            }

            state.metrics.record_download("maven");
            state.metrics.record_cache_hit("maven");
            state.activity.push(ActivityEntry::new(
                ActionType::CacheHit,
                artifact_name.clone(),
                crate::registry_type::RegistryType::Maven,
                "CACHE",
            ));
            state
                .audit
                .log(AuditEntry::new("cache_hit", "api", "", "maven", ""));
            // Quarantine only real version artifacts (.jar/.pom/.sha1, immutable
            // per version). maven-metadata.xml is mutable (curation_coords=None) —
            // never quarantine it or its digest would change forever.
            if curation_coords.is_some() {
                if let Some(resp) = crate::digest_quarantine::proxy_gate_dated(
                    &state.digest_store,
                    "maven",
                    data,
                    &q_mode,
                    q_secs,
                    "cache",
                    publish_date,
                ) {
                    return resp;
                }
            }
            if repository.tracks_proxy_cache() {
                state.record_proxy_cache_access_locked(&key).await;
            }
            let mut response = with_content_type(&path, data.clone()).into_response();
            if !is_mutable_maven_path(&path) {
                response
                    .headers_mut()
                    .insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
            }
            return response;
        }
    }

    // #68 namespace isolation: the maven-metadata.xml (ArtifactMeta) path is not
    // covered by the VersionFile check_download above. An internal group:artifact's
    // metadata must never be fetched upstream (dependency confusion): serve any local
    // copy (deployed/cached metadata; the fresh path already returned above) and block
    // only when nothing is hosted locally — never proxy.
    let metadata_coordinates = metadata_artifact_coordinates(&path);
    if let Some((group_path, artifact_id)) = metadata_coordinates {
        let maven_name = format!("{}:{}", group_path.replace('/', "."), artifact_id);
        if crate::curation::is_internal_namespace(
            &state.curation().curation_engine,
            crate::curation::RegistryType::Maven,
            &maven_name,
        ) {
            if let Some(ref data) = cached {
                state.metrics.record_download("maven");
                state.metrics.record_cache_hit("maven");
                if repository.tracks_proxy_cache() {
                    state.record_proxy_cache_access_locked(&key).await;
                }
                return with_content_type(&path, data.clone()).into_response();
            }
            return crate::curation::check_namespace_isolation(
                &state.curation().curation_engine,
                crate::curation::RegistryType::Maven,
                &maven_name,
            )
            .unwrap_or_else(|| StatusCode::NOT_FOUND.into_response());
        }
    }

    // #733: an internal-namespace VersionFile artifact with no fresh copy — serve any stale
    // local copy, else block; never proxy upstream. (The ArtifactMeta path is handled above.)
    if internal {
        if let Some(ref data) = cached {
            state.metrics.record_download("maven");
            state.metrics.record_cache_hit("maven");
            if repository.tracks_proxy_cache() {
                state.record_proxy_cache_access_locked(&key).await;
            }
            return with_content_type(&path, data.clone()).into_response();
        }
        if let Some((ref maven_name, _)) = curation_coords {
            return crate::curation::check_namespace_isolation(
                &state.curation().curation_engine,
                crate::curation::RegistryType::Maven,
                maven_name,
            )
            .unwrap_or_else(|| StatusCode::NOT_FOUND.into_response());
        }
        return StatusCode::NOT_FOUND.into_response();
    }

    // Upstream I/O must not hold a cache-object lock. Any stale fallback below
    // reacquires the lock and verifies the same bytes still exist before it is
    // recorded and served.
    drop(cache_guard);

    let metadata_request = if version_metadata_path(&path).is_some() {
        None
    } else {
        match classify_path(&path) {
            MavenPathKind::ArtifactMeta {
                group_path,
                artifact_id,
                ..
            } => Some((
                group_path,
                artifact_id,
                metadata_document_key(&path).unwrap_or(&path).to_string(),
                checksum_suffix(&path),
            )),
            _ => None,
        }
    };

    let mut unavailable = None;
    let mut upstream_rejection = None;
    for proxy in &repository.proxies {
        let upstream_path = metadata_request
            .as_ref()
            .map(|(_, _, document_path, _)| document_path.as_str())
            .unwrap_or(&path);
        let url = format!("{}/{}", proxy.url().trim_end_matches('/'), upstream_path);

        match proxy_fetch(
            &state.http_client,
            &url,
            Duration::from_secs(state.config.maven.proxy_timeout),
            proxy.auth(),
            &state.circuit_breaker,
            RegistryType::Maven,
        )
        .await
        {
            Ok(data) => {
                state.maven_negative_cache.lock().remove(&key);
                state.metrics.record_download("maven");
                state.metrics.record_cache_miss("maven");
                state.activity.push(ActivityEntry::new(
                    ActionType::ProxyFetch,
                    artifact_name,
                    crate::registry_type::RegistryType::Maven,
                    "PROXY",
                ));
                state
                    .audit
                    .log(AuditEntry::new("proxy_fetch", "api", "", "maven", ""));

                let response_data =
                    if let Some((group_path, artifact_id, document_path, requested_checksum)) =
                        &metadata_request
                    {
                        let metadata = match merge_and_cache_proxy_metadata(
                            &state,
                            &repository,
                            group_path,
                            artifact_id,
                            document_path,
                            &data,
                        )
                        .await
                        {
                            Ok(metadata) => metadata,
                            Err(error) => {
                                return storage_failure_response("read", document_path, &error)
                            }
                        };
                        match requested_checksum {
                            Some(suffix) => match checksum_hex(suffix, &metadata) {
                                Some(checksum) => Bytes::from(checksum),
                                None => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
                            },
                            None => metadata,
                        }
                    } else {
                        spawn_proxy_cache(&state, &path, key.clone(), Bytes::from(data.clone()));
                        Bytes::from(data.clone())
                    };

                // Quarantine only real version artifacts; never maven-metadata.xml.
                if curation_coords.is_some() {
                    let (q_mode, q_secs) = crate::digest_quarantine::resolve_global(
                        state.config.curation.maven.quarantine.as_ref().or(state
                            .config
                            .curation
                            .quarantine
                            .as_ref()),
                        state
                            .config
                            .curation
                            .maven
                            .quarantine_ttl
                            .as_deref()
                            .or(state.config.curation.quarantine_ttl.as_deref()),
                    );
                    if let Some(resp) = crate::digest_quarantine::proxy_gate_dated(
                        &state.digest_store,
                        "maven",
                        &data,
                        &q_mode,
                        q_secs,
                        &url,
                        publish_date,
                    ) {
                        return resp;
                    }
                }
                return with_content_type(&path, response_data).into_response();
            }
            Err(ProxyError::NotFound) => {
                tracing::debug!(upstream = %proxy.url(), path = %path, "Maven proxy returned not found, trying next");
                continue;
            }
            Err(ProxyError::CircuitOpen(reg)) => {
                unavailable = Some(circuit_open_response(&reg));
                continue;
            }
            // `proxy_fetch` reserves Upstream(404) for a policy/WAF block that
            // happened to be disguised as 404. It is an outage, not an
            // authoritative miss, so keep it out of the negative-cache path.
            Err(ProxyError::Upstream(404)) => {
                tracing::warn!(
                    upstream = %proxy.url(),
                    path = %path,
                    "Maven upstream policy block was disguised as not found"
                );
                let mut response =
                    (StatusCode::BAD_GATEWAY, "Maven upstream policy blocked").into_response();
                response.extensions_mut().insert(MavenPolicyBlock);
                unavailable = Some(response);
                continue;
            }
            Err(ProxyError::Upstream(code)) if (400..500).contains(&code) => {
                tracing::debug!(
                    status = code,
                    upstream = %proxy.url(),
                    path = %path,
                    "Maven proxy rejected request, trying next"
                );
                let status = StatusCode::from_u16(code).unwrap_or(StatusCode::BAD_GATEWAY);
                upstream_rejection =
                    Some((status, "Maven upstream rejected request").into_response());
                continue;
            }
            Err(e @ (ProxyError::Upstream(_) | ProxyError::Network(_))) => {
                tracing::debug!(error = ?e, upstream = %proxy.url(), path = %path, "Maven proxy fetch failed, trying next");
                unavailable =
                    Some((StatusCode::BAD_GATEWAY, "Maven upstream unavailable").into_response());
                continue;
            }
        }
    }

    // Authentication, authorization, throttling and other upstream client
    // errors are authoritative responses. They must neither serve stale bytes
    // nor enter the negative cache.
    if let Some(response) = upstream_rejection {
        return response;
    }

    // Only transport/5xx/circuit failures may use stale-on-error. An authoritative
    // upstream 404 must not resurrect a removed mutable version or metadata document.
    if unavailable.is_some() {
        if let Some(ref data) = cached {
            if repository.tracks_proxy_cache()
                && !record_still_cached_maven_access(&state, &key, data).await
            {
                return StatusCode::BAD_GATEWAY.into_response();
            }
            tracing::warn!(registry = "maven", path = %path, "Maven upstream failed, serving stale cached artifact");
            // Quarantine still applies to a version artifact served from a stale cache:
            // a held SNAPSHOT must not be released just because the upstream went down
            // (mutable SNAPSHOT artifacts reach this path; immutable releases serve from
            // the fresh-cache branch above, which is already gated).
            if curation_coords.is_some() {
                let (q_mode, q_secs) = crate::digest_quarantine::resolve_global(
                    state.config.curation.maven.quarantine.as_ref().or(state
                        .config
                        .curation
                        .quarantine
                        .as_ref()),
                    state
                        .config
                        .curation
                        .maven
                        .quarantine_ttl
                        .as_deref()
                        .or(state.config.curation.quarantine_ttl.as_deref()),
                );
                if let Some(resp) = crate::digest_quarantine::proxy_gate_dated(
                    &state.digest_store,
                    "maven",
                    data,
                    &q_mode,
                    q_secs,
                    "cache-stale",
                    publish_date,
                ) {
                    return resp;
                }
            }
            let mut response = with_content_type(&path, data.clone()).into_response();
            response.headers_mut().insert(
                axum::http::header::HeaderName::from_static("x-nora-stale"),
                axum::http::header::HeaderValue::from_static("true"),
            );
            return response;
        }
    }

    if let Some(response) = unavailable {
        return response;
    }

    if repository.is_proxy() {
        if cached.is_none() && repository.negative_ttl > 0 {
            insert_negative_cache_entry(
                &mut state.maven_negative_cache.lock(),
                key,
                repository.negative_ttl,
                MAVEN_NEGATIVE_CACHE_MAX_ENTRIES,
            );
        }
        tracing::warn!(registry = "maven", path = %path, "Proxy failed, returning 404");
    }
    StatusCode::NOT_FOUND.into_response()
}

async fn download_group(
    state: AppState,
    headers: axum::http::HeaderMap,
    members: &[String],
    path: String,
) -> Response {
    let metadata = matches!(classify_path(&path), MavenPathKind::ArtifactMeta { .. });
    if !metadata {
        let mut unavailable = None;
        for member in members {
            let Some(config) = state.config.maven.repository(member).cloned() else {
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            };
            let Some(repository) = DirectRepository::from_config(&state, &config) else {
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            };
            let response =
                download_direct(state.clone(), headers.clone(), repository, path.clone()).await;
            if response.extensions().get::<MavenStorageFailure>().is_some() {
                return response;
            }
            if response.extensions().get::<MavenPolicyBlock>().is_some() {
                return response;
            }
            match response.status() {
                StatusCode::NOT_FOUND => {}
                StatusCode::BAD_GATEWAY
                | StatusCode::SERVICE_UNAVAILABLE
                | StatusCode::GATEWAY_TIMEOUT => unavailable = Some(response),
                _ => return response,
            }
        }
        return unavailable.unwrap_or_else(|| StatusCode::NOT_FOUND.into_response());
    }

    let document_path = metadata_document_key(&path).unwrap_or(&path);
    let requested_checksum = checksum_suffix(&path);
    let internal_metadata_name = metadata_artifact_coordinates(document_path)
        .map(|(group_path, artifact_id)| {
            format!("{}:{}", group_path.replace('/', "."), artifact_id)
        })
        .filter(|name| {
            crate::curation::is_internal_namespace(
                &state.curation().curation_engine,
                crate::registry_type::RegistryType::Maven,
                name,
            )
        });
    let mut documents = Vec::new();
    let mut unavailable = None;

    for member in members {
        let Some(config) = state.config.maven.repository(member).cloned() else {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        };
        // An internal coordinate must never consult an external proxy. For group metadata,
        // querying every member would otherwise turn the proxy's intentional namespace-
        // isolation refusal into the group's response and discard valid hosted metadata.
        // Hosted members remain authoritative; if none contains the document, the group
        // returns the normal client-facing isolation refusal below.
        if internal_metadata_name.is_some() && matches!(config, MavenRepository::Proxy { .. }) {
            continue;
        }
        let Some(repository) = DirectRepository::from_config(&state, &config) else {
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        };
        let response = download_direct(
            state.clone(),
            headers.clone(),
            repository,
            document_path.to_string(),
        )
        .await;
        if response.extensions().get::<MavenStorageFailure>().is_some() {
            return response;
        }
        if response.extensions().get::<MavenPolicyBlock>().is_some() {
            return response;
        }
        match response.status() {
            StatusCode::OK => match to_bytes(response.into_body(), usize::MAX).await {
                Ok(body) => documents.push(body),
                Err(_) => return StatusCode::BAD_GATEWAY.into_response(),
            },
            StatusCode::NOT_FOUND => {}
            // A group is an availability boundary: one broken member must not hide
            // metadata that another member can serve. Preserve the member error and
            // return it only when no member produced a document. Policy blocks remain
            // immediate via MavenPolicyBlock above.
            _ => unavailable = Some(response),
        }
    }

    let Some(first) = documents.first() else {
        if let Some(name) = internal_metadata_name {
            return crate::curation::check_namespace_isolation(
                &state.curation().curation_engine,
                crate::registry_type::RegistryType::Maven,
                &name,
            )
            .unwrap_or_else(|| StatusCode::NOT_FOUND.into_response());
        }
        return unavailable.unwrap_or_else(|| StatusCode::NOT_FOUND.into_response());
    };
    let merged = merge_group_metadata(document_path, &documents)
        .unwrap_or_else(|| Bytes::copy_from_slice(first));
    let response_data = match requested_checksum {
        Some(suffix) => match checksum_hex(suffix, &merged) {
            Some(checksum) => Bytes::from(checksum),
            None => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        },
        None => merged,
    };
    with_content_type(&path, response_data).into_response()
}

// ============================================================================
// Upload
// ============================================================================

async fn upload_legacy(
    State(state): State<AppState>,
    Path(path): Path<String>,
    Extension(authority): Extension<NamespaceAuthority>,
    body: Bytes,
) -> Response {
    let authorize = move |namespace: &str| enforce_namespace_scope(&authority, namespace).is_ok();
    if let Some(repository) = state.config.maven.default_repository.clone() {
        return upload_configured(state, &repository, path, body, authorize).await;
    }
    let repository = DirectRepository::legacy(&state);
    upload_direct(state, repository, path, body, authorize).await
}

pub(crate) async fn upload_named<F>(
    State(state): State<AppState>,
    Path((repository, path)): Path<(String, String)>,
    body: Bytes,
    authorize: F,
) -> Response
where
    F: FnOnce(&str) -> bool,
{
    upload_configured(state, &repository, path, body, authorize).await
}

async fn upload_configured<F>(
    state: AppState,
    repository: &str,
    path: String,
    body: Bytes,
    authorize: F,
) -> Response
where
    F: FnOnce(&str) -> bool,
{
    let Some(config) = state.config.maven.repository(repository).cloned() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(repository) = DirectRepository::from_config(&state, &config) else {
        return method_not_allowed("GET");
    };
    if repository.is_proxy() || repository.write_policy == MavenWritePolicy::Deny {
        return method_not_allowed("GET");
    }
    upload_direct(state, repository, path, body, authorize).await
}

async fn upload_direct<F>(
    state: AppState,
    repository: DirectRepository,
    path: String,
    body: Bytes,
    authorize: F,
) -> Response
where
    F: FnOnce(&str) -> bool,
{
    if !path.is_ascii() || path.contains("..") || path.contains('\0') || path.starts_with('/') {
        return (StatusCode::BAD_REQUEST, "Invalid path").into_response();
    }

    // Enforce OIDC namespace_scope on the artifact coordinate (group/artifactId).
    // An unrecognized (Opaque) path yields an empty coordinate → fail-closed (#583).
    let maven_namespace = version_metadata_path(&path)
        .map(|(group_path, artifact_id, _)| format!("{group_path}/{artifact_id}"))
        .unwrap_or_else(|| match classify_path(&path) {
            MavenPathKind::VersionFile(c) => format!("{}/{}", c.group_path, c.artifact_id),
            MavenPathKind::ArtifactMeta {
                group_path,
                artifact_id,
                ..
            } => format!("{}/{}", group_path, artifact_id),
            MavenPathKind::Opaque => String::new(),
        });
    if !authorize(&maven_namespace) {
        return StatusCode::FORBIDDEN.into_response();
    }

    let key = repository.storage_key(&path);
    let requested_version = version_metadata_path(&path)
        .map(|(_, _, version)| version)
        .or_else(|| match classify_path(&path) {
            MavenPathKind::VersionFile(coords) => Some(coords.version),
            _ => None,
        });
    if let Some(version) = requested_version {
        let allowed = match repository.version_policy {
            MavenVersionPolicy::Release => !is_snapshot(&version),
            MavenVersionPolicy::Snapshot => is_snapshot(&version),
            MavenVersionPolicy::Mixed => true,
        };
        if !allowed {
            return (
                StatusCode::BAD_REQUEST,
                format!(
                    "Version {} is not allowed by this repository's version policy",
                    version
                ),
            )
                .into_response();
        }
    }

    let artifact_name = path
        .split('/')
        .rev()
        .take(3)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>()
        .join("/");

    // Client checksum uploads are acknowledgements of the server's derived checksum,
    // not independent authoritative objects. Require the base object and validate
    // every supported algorithm against its actual bytes.
    if let Some(suffix) = checksum_suffix(&path) {
        let Some(document_path) = metadata_document_key(&path) else {
            return StatusCode::BAD_REQUEST.into_response();
        };
        let lock = state.publish_lock(&mutation_lock_key(&repository, document_path));
        let _guard = lock.lock().await;
        let document_key = repository.storage_key(document_path);
        let document = match state.storage.get(&document_key).await {
            Ok(document) => document,
            Err(StorageError::NotFound) => return StatusCode::NOT_FOUND.into_response(),
            Err(error) => {
                tracing::error!(
                    %error,
                    key = %document_key,
                    "Failed to read Maven checksum base object"
                );
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
        };
        let Some(expected) = checksum_hex(suffix, &document) else {
            return StatusCode::BAD_REQUEST.into_response();
        };
        let received = String::from_utf8_lossy(&body);
        let server_managed_metadata = matches!(
            classify_path(document_path),
            MavenPathKind::ArtifactMeta { filename, .. } if filename == "maven-metadata.xml"
        ) && matches!(
            classify_metadata_level(&document),
            Some(
                MavenMetadataLevel::Group
                    | MavenMetadataLevel::Artifact
                    | MavenMetadataLevel::ArtifactAndGroup
            )
        );
        if !server_managed_metadata && !expected.eq_ignore_ascii_case(received.trim()) {
            tracing::warn!(path = %path, "SECURITY: Maven checksum mismatch on upload");
            return (StatusCode::BAD_REQUEST, "Checksum mismatch").into_response();
        }
        match state.storage.put(&key, expected.as_bytes()).await {
            Ok(()) => {
                state.metrics.record_upload("maven");
                invalidate_maven_document_index(&state, &repository, document_path, &document);
                return StatusCode::CREATED.into_response();
            }
            Err(error) => {
                tracing::error!(%error, %key, "Failed to store derived Maven checksum");
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }
        }
    }

    match classify_path(&path) {
        MavenPathKind::VersionFile(coords) => {
            // Primary artifact upload (jar, pom, war, etc.)
            let snap = is_snapshot(&coords.version);

            // Lock on metadata key to serialize all uploads for the same artifact.
            // This prevents TOCTOU races on both immutability checks and
            // maven-metadata.xml generation (read-list-generate-write cycle).
            let metadata_lock_key = repository.storage_key(&format!(
                "{}/{}/maven-metadata.xml",
                coords.group_path, coords.artifact_id
            ));
            let lock = state.publish_lock(&metadata_lock_key);
            let _guard = lock.lock().await;

            let stored = if !snap && repository.write_policy == MavenWritePolicy::AllowOnce {
                state.storage.put_if_absent(&key, &body).await
            } else {
                state.storage.put(&key, &body).await
            };
            let exact_retry = match stored {
                Ok(()) => false,
                Err(StorageError::AlreadyExists) => {
                    let existing = match state.storage.get(&key).await {
                        Ok(existing) => existing,
                        Err(error) => {
                            tracing::error!(
                                %error,
                                %key,
                                "Failed to read existing immutable Maven artifact"
                            );
                            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                        }
                    };
                    if existing != body {
                        // Storage reports AlreadyExists to the physical observer
                        // even when no bytes changed. Pair it with the exact
                        // semantic bundle so this rejected request cannot leave
                        // the global index dirty and trigger a full S3 reconcile.
                        state
                            .repo_index
                            .invalidate_maven_path(repository.name.as_deref().unwrap_or(""), &path);
                        return (
                            StatusCode::CONFLICT,
                            format!(
                                "Version {}:{} is immutable (already deployed)",
                                coords.artifact_id, coords.version
                            ),
                        )
                            .into_response();
                    }
                    true
                }
                Err(error) => {
                    tracing::error!(%error, %key, "Failed to store Maven artifact");
                    return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                }
            };

            if let Err(error) = compute_and_store_checksums(&state.storage, &key, &body).await {
                tracing::error!(%error, %key, "Failed to store Maven artifact checksums");
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }

            // An exact retry after the immutable object was created must finish
            // any interrupted checksum/metadata work. Do not treat an ordinary
            // retry as a new deployment, though: preserve the current release
            // when metadata already records this version.
            let metadata_records_version = if exact_retry {
                match state.storage.get(&metadata_lock_key).await {
                    Ok(metadata) => parse_artifact_metadata(&metadata)
                        .is_some_and(|metadata| metadata.versions.contains(&coords.version)),
                    Err(StorageError::NotFound) => false,
                    Err(error) => {
                        tracing::error!(
                            %error,
                            key = %metadata_lock_key,
                            "Failed to inspect Maven metadata during exact retry"
                        );
                        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                    }
                }
            } else {
                false
            };
            if let Err(error) = update_artifact_metadata(
                &state,
                &repository.storage_prefix(),
                &coords.group_path,
                &coords.artifact_id,
                (!metadata_records_version).then_some(coords.version.as_str()),
            )
            .await
            {
                tracing::error!(
                    %error,
                    key = %metadata_lock_key,
                    "Failed to update Maven artifact metadata"
                );
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            }

            state.metrics.record_upload("maven");
            state
                .audit
                .log(AuditEntry::new("push", "api", &artifact_name, "maven", ""));
            state.activity.push(ActivityEntry::new(
                ActionType::Push,
                artifact_name,
                crate::registry_type::RegistryType::Maven,
                "LOCAL",
            ));
            state.repo_index.invalidate_maven_ga(
                repository.name.as_deref().unwrap_or(""),
                &format!("{}/{}", coords.group_path, coords.artifact_id),
            );

            StatusCode::CREATED.into_response()
        }

        MavenPathKind::ArtifactMeta {
            group_path,
            artifact_id,
            filename,
        } => {
            let Some(level) = classify_metadata_level(&body) else {
                return (StatusCode::BAD_REQUEST, "Invalid Maven metadata").into_response();
            };

            if filename == "maven-metadata.xml"
                && matches!(
                    level,
                    MavenMetadataLevel::Group
                        | MavenMetadataLevel::Artifact
                        | MavenMetadataLevel::ArtifactAndGroup
                )
            {
                let incoming_artifact = matches!(
                    level,
                    MavenMetadataLevel::Artifact | MavenMetadataLevel::ArtifactAndGroup
                )
                .then(|| parse_artifact_metadata(&body))
                .flatten();
                let incoming_plugins = matches!(
                    level,
                    MavenMetadataLevel::Group | MavenMetadataLevel::ArtifactAndGroup
                )
                .then(|| parse_group_plugins(&body))
                .flatten();
                if matches!(
                    level,
                    MavenMetadataLevel::Artifact | MavenMetadataLevel::ArtifactAndGroup
                ) && incoming_artifact.is_none()
                    || matches!(
                        level,
                        MavenMetadataLevel::Group | MavenMetadataLevel::ArtifactAndGroup
                    ) && incoming_plugins.is_none()
                {
                    return (StatusCode::BAD_REQUEST, "Invalid Maven metadata").into_response();
                }
                if incoming_artifact.as_ref().is_some_and(|metadata| {
                    !artifact_metadata_matches_path(metadata, &group_path, &artifact_id)
                }) {
                    return (
                        StatusCode::BAD_REQUEST,
                        "Maven metadata coordinates do not match the request path",
                    )
                        .into_response();
                }

                let lock = state.publish_lock(&key);
                let _guard = lock.lock().await;
                let current = match state.storage.get(&key).await {
                    Ok(current) => Some(current),
                    Err(StorageError::NotFound) => None,
                    Err(error) => {
                        tracing::error!(%error, %key, "Failed to read Maven artifact metadata");
                        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                    }
                };
                let current_artifact = current.as_deref().and_then(parse_artifact_metadata);
                let current_plugins = current.as_deref().and_then(parse_group_plugins);

                let artifact_xml = if incoming_artifact.is_some() || current_artifact.is_some() {
                    let versions = match stored_artifact_versions(
                        &state,
                        &repository.storage_prefix(),
                        &group_path,
                        &artifact_id,
                    )
                    .await
                    {
                        Ok(versions) => versions,
                        Err(error) => {
                            tracing::error!(
                                %error,
                                %key,
                                "Failed to list Maven artifact versions"
                            );
                            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                        }
                    };
                    let references_missing_version = incoming_artifact
                        .iter()
                        .flat_map(|metadata| {
                            metadata
                                .versions
                                .iter()
                                .chain(metadata.release.iter())
                                .chain(metadata.latest.iter())
                        })
                        .any(|version| !versions.contains(version));
                    if references_missing_version
                        || incoming_artifact
                            .as_ref()
                            .and_then(|metadata| metadata.release.as_deref())
                            .is_some_and(is_snapshot)
                    {
                        return (
                            StatusCode::BAD_REQUEST,
                            "Maven metadata references an unavailable or invalid version",
                        )
                            .into_response();
                    }
                    let last_updated = chrono::Utc::now().format("%Y%m%d%H%M%S").to_string();
                    let current_artifact_document =
                        current_artifact.as_ref().and(current.as_deref());
                    merge_hosted_artifact_metadata(
                        &group_path.replace('/', "."),
                        &artifact_id,
                        current_artifact_document,
                        incoming_artifact.as_ref(),
                        &versions,
                        None,
                        incoming_artifact.is_some().then_some(last_updated.as_str()),
                    )
                } else {
                    None
                };

                let mut plugins = Vec::new();
                let mut seen_prefixes = BTreeSet::new();
                if let Some(incoming) = incoming_plugins {
                    extend_plugins_first_wins(&mut plugins, &mut seen_prefixes, incoming);
                }
                if let Some(current) = current_plugins {
                    extend_plugins_first_wins(&mut plugins, &mut seen_prefixes, current);
                }
                let has_plugins = !plugins.is_empty();
                let Some(xml) = combine_metadata_sections(
                    artifact_xml,
                    has_plugins.then_some(plugins.as_slice()),
                ) else {
                    return (StatusCode::BAD_REQUEST, "Invalid Maven metadata").into_response();
                };

                if let Err(error) = state.storage.put(&key, xml.as_bytes()).await {
                    tracing::error!(%error, %key, "Failed to store Maven artifact metadata");
                    return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                }
                if let Err(error) =
                    compute_and_store_checksums(&state.storage, &key, xml.as_bytes()).await
                {
                    tracing::error!(%error, %key, "Failed to store Maven metadata checksums");
                    return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                }
                state.metrics.record_upload("maven");
                invalidate_maven_document_index(&state, &repository, &path, xml.as_bytes());
                return StatusCode::CREATED.into_response();
            }

            if level == MavenMetadataLevel::Version {
                let Some((path_group, path_artifact, path_version)) = version_metadata_path(&path)
                else {
                    return (StatusCode::BAD_REQUEST, "Invalid Maven metadata path")
                        .into_response();
                };
                let Some(metadata) = parse_version_metadata(&body) else {
                    return (StatusCode::BAD_REQUEST, "Invalid Maven metadata").into_response();
                };
                if metadata.group_id.as_deref() != Some(path_group.replace('/', ".").as_str())
                    || metadata.artifact_id.as_deref() != Some(path_artifact.as_str())
                    || metadata.version.as_deref() != Some(path_version.as_str())
                {
                    return (
                        StatusCode::BAD_REQUEST,
                        "Maven metadata coordinates do not match the request path",
                    )
                        .into_response();
                }
            }

            let lock = state.publish_lock(&mutation_lock_key(&repository, &path));
            let _guard = lock.lock().await;
            match state.storage.put(&key, &body).await {
                Ok(()) => {
                    if let Err(error) =
                        compute_and_store_checksums(&state.storage, &key, &body).await
                    {
                        tracing::error!(%error, %key, "Failed to store Maven metadata checksums");
                        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                    }
                    state.metrics.record_upload("maven");
                    invalidate_maven_document_index(&state, &repository, &path, &body);
                    StatusCode::CREATED.into_response()
                }
                Err(error) => {
                    tracing::error!(%error, %key, "Failed to store Maven metadata");
                    StatusCode::INTERNAL_SERVER_ERROR.into_response()
                }
            }
        }

        MavenPathKind::Opaque => match state.storage.put(&key, &body).await {
            Ok(()) => {
                state.metrics.record_upload("maven");
                state
                    .audit
                    .log(AuditEntry::new("push", "api", &artifact_name, "maven", ""));
                state.activity.push(ActivityEntry::new(
                    ActionType::Push,
                    artifact_name,
                    crate::registry_type::RegistryType::Maven,
                    "LOCAL",
                ));
                state
                    .repo_index
                    .invalidate_maven_path(repository.name.as_deref().unwrap_or(""), &path);
                StatusCode::CREATED.into_response()
            }
            Err(e) => {
                tracing::error!(error = %e, key = %key, "Failed to store Maven artifact");
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
        },
    }
}

// ============================================================================
// Checksum helpers
// ============================================================================

fn checksum_hex(suffix: &str, data: &[u8]) -> Option<String> {
    match suffix {
        "md5" => Some(hex::encode(md5::Md5::digest(data))),
        "sha1" => Some(hex::encode(sha1::Sha1::digest(data))),
        "sha256" => Some(hex::encode(sha2::Sha256::digest(data))),
        "sha512" => Some(hex::encode(sha2::Sha512::digest(data))),
        _ => None,
    }
}

async fn compute_and_store_checksums(
    storage: &crate::storage::Storage,
    key: &str,
    data: &[u8],
) -> crate::storage::Result<()> {
    for suffix in ["md5", "sha1", "sha256", "sha512"] {
        let ck = format!("{}.{}", key, suffix);
        let Some(hash) = checksum_hex(suffix, data) else {
            continue;
        };
        storage.put(&ck, hash.as_bytes()).await?;
    }
    Ok(())
}

// ============================================================================
// Metadata generation
// ============================================================================

#[derive(Clone, Default)]
struct ArtifactMetadata {
    group_id: Option<String>,
    artifact_id: Option<String>,
    latest: Option<String>,
    release: Option<String>,
    last_updated: Option<String>,
    versions: Vec<String>,
}

fn parse_artifact_metadata(data: &[u8]) -> Option<ArtifactMetadata> {
    if !matches!(
        classify_metadata_level(data),
        Some(MavenMetadataLevel::Artifact | MavenMetadataLevel::ArtifactAndGroup)
    ) {
        return None;
    }

    let mut reader = Reader::from_reader(data);
    reader.config_mut().trim_text(true);
    let mut metadata = ArtifactMetadata::default();
    let mut in_versions = false;
    let mut in_plugins = false;

    loop {
        match reader.read_event() {
            Ok(Event::Start(element)) if element.local_name().as_ref() == b"plugins" => {
                in_plugins = true;
            }
            Ok(Event::Start(element))
                if element.local_name().as_ref() == b"groupId" && !in_plugins =>
            {
                let text = reader.read_text(element.name()).ok()?.decode().ok()?;
                let group_id = quick_xml::escape::unescape(&text).ok()?.into_owned();
                if !group_id.is_empty() {
                    metadata.group_id = Some(group_id);
                }
            }
            Ok(Event::Start(element))
                if element.local_name().as_ref() == b"artifactId" && !in_plugins =>
            {
                let text = reader.read_text(element.name()).ok()?.decode().ok()?;
                let artifact_id = quick_xml::escape::unescape(&text).ok()?.into_owned();
                if !artifact_id.is_empty() {
                    metadata.artifact_id = Some(artifact_id);
                }
            }
            Ok(Event::Start(element)) if element.local_name().as_ref() == b"versions" => {
                in_versions = true;
            }
            Ok(Event::Start(element))
                if element.local_name().as_ref() == b"version" && in_versions =>
            {
                let text = reader.read_text(element.name()).ok()?.decode().ok()?;
                let version = quick_xml::escape::unescape(&text).ok()?.into_owned();
                if !version.is_empty() {
                    metadata.versions.push(version);
                }
            }
            Ok(Event::Start(element)) if element.local_name().as_ref() == b"latest" => {
                let text = reader.read_text(element.name()).ok()?.decode().ok()?;
                let latest = quick_xml::escape::unescape(&text).ok()?.into_owned();
                if !latest.is_empty() {
                    metadata.latest = Some(latest);
                }
            }
            Ok(Event::Start(element)) if element.local_name().as_ref() == b"release" => {
                let text = reader.read_text(element.name()).ok()?.decode().ok()?;
                let release = quick_xml::escape::unescape(&text).ok()?.into_owned();
                if !release.is_empty() {
                    metadata.release = Some(release);
                }
            }
            Ok(Event::Start(element)) if element.local_name().as_ref() == b"lastUpdated" => {
                let text = reader.read_text(element.name()).ok()?.decode().ok()?;
                let last_updated = quick_xml::escape::unescape(&text).ok()?.into_owned();
                if !last_updated.is_empty() {
                    metadata.last_updated = Some(last_updated);
                }
            }
            Ok(Event::End(element)) if element.local_name().as_ref() == b"versions" => {
                in_versions = false;
            }
            Ok(Event::End(element)) if element.local_name().as_ref() == b"plugins" => {
                in_plugins = false;
            }
            Ok(Event::Eof) => break,
            Err(_) => return None,
            _ => {}
        }
    }

    Some(metadata)
}

fn artifact_metadata_matches_path(
    metadata: &ArtifactMetadata,
    group_path: &str,
    artifact_id: &str,
) -> bool {
    metadata.group_id.as_deref() == Some(group_path.replace('/', ".").as_str())
        && metadata.artifact_id.as_deref() == Some(artifact_id)
}

async fn stored_artifact_versions(
    state: &AppState,
    storage_prefix: &str,
    group_path: &str,
    artifact_id: &str,
) -> Result<BTreeSet<String>, StorageError> {
    stored_artifact_versions_in_storage(&state.storage, storage_prefix, group_path, artifact_id)
        .await
}

async fn stored_artifact_versions_in_storage(
    storage: &crate::storage::Storage,
    storage_prefix: &str,
    group_path: &str,
    artifact_id: &str,
) -> Result<BTreeSet<String>, StorageError> {
    let prefix = format!("{storage_prefix}{group_path}/{artifact_id}/");
    let keys = storage.list(&prefix).await?;
    let mut versions = BTreeSet::new();

    for key in &keys {
        let relative = match key.strip_prefix(&prefix) {
            Some(relative) => relative,
            None => continue,
        };
        if let Some(version) = relative.split('/').next() {
            if !version.is_empty() && !version.starts_with("maven-metadata") {
                versions.insert(version.to_string());
            }
        }
    }

    Ok(versions)
}

fn push_unique(versions: &mut Vec<String>, known: &mut BTreeSet<String>, version: &str) {
    if !version.is_empty() && known.insert(version.to_string()) {
        versions.push(version.to_string());
    }
}

/// Merge authoritative hosted metadata. Version order follows deployment/client
/// order, `release` follows the current non-SNAPSHOT deployment rather than the
/// greatest version, and `latest` follows the current deployment unless a
/// validated client metadata document explicitly supplies it.
fn merge_hosted_artifact_metadata(
    group_id: &str,
    artifact_id: &str,
    current: Option<&[u8]>,
    incoming: Option<&ArtifactMetadata>,
    stored_versions: &BTreeSet<String>,
    deployed_version: Option<&str>,
    last_updated: Option<&str>,
) -> Option<String> {
    let current = match current {
        Some(data) => {
            let metadata = parse_artifact_metadata(data)?;
            if metadata.group_id.as_deref() != Some(group_id)
                || metadata.artifact_id.as_deref() != Some(artifact_id)
            {
                return None;
            }
            metadata
        }
        None => ArtifactMetadata::default(),
    };
    if incoming.is_some_and(|metadata| {
        metadata.group_id.as_deref() != Some(group_id)
            || metadata.artifact_id.as_deref() != Some(artifact_id)
    }) {
        return None;
    }

    let mut versions = Vec::new();
    let mut known = BTreeSet::new();
    for version in &current.versions {
        push_unique(&mut versions, &mut known, version);
    }
    if let Some(incoming) = incoming {
        for version in &incoming.versions {
            push_unique(&mut versions, &mut known, version);
        }
    }
    if let Some(version) = deployed_version {
        push_unique(&mut versions, &mut known, version);
    }
    let mut discovered: Vec<String> = stored_versions
        .iter()
        .filter(|version| !known.contains(*version))
        .cloned()
        .collect();
    sort_maven_versions(&mut discovered);
    for version in discovered {
        push_unique(&mut versions, &mut known, &version);
    }
    if versions.is_empty() {
        return None;
    }

    let release = incoming
        .and_then(|metadata| metadata.release.clone())
        .or_else(|| {
            deployed_version
                .filter(|version| !is_snapshot(version))
                .map(ToOwned::to_owned)
        })
        .or(current.release);
    let latest = incoming
        .and_then(|metadata| metadata.latest.clone())
        .or_else(|| deployed_version.map(ToOwned::to_owned))
        .or(current.latest);
    if release
        .as_ref()
        .is_some_and(|version| is_snapshot(version) || !known.contains(version))
        || latest
            .as_ref()
            .is_some_and(|version| !known.contains(version))
    {
        return None;
    }

    let incoming_last_updated = incoming.and_then(|metadata| metadata.last_updated.as_deref());
    Some(generate_metadata_xml_with_versioning(
        group_id,
        artifact_id,
        &versions,
        latest.as_deref(),
        release.as_deref(),
        last_updated
            .or(incoming_last_updated)
            .or(current.last_updated.as_deref()),
    ))
}

/// Merge one proxy document with versions already materialized in the same
/// repository. Explicit `latest`/`release` remain upstream-owned; local hosted
/// deployment semantics are handled by `merge_hosted_artifact_metadata`.
fn merge_artifact_metadata(
    group_id: &str,
    artifact_id: &str,
    base: Option<&[u8]>,
    stored_versions: &BTreeSet<String>,
    last_updated: Option<&str>,
    version_policy: MavenVersionPolicy,
) -> Option<String> {
    let metadata = match base {
        Some(data) => parse_artifact_metadata(data)?,
        None => ArtifactMetadata::default(),
    };
    let mut known = BTreeSet::new();
    let mut versions = Vec::new();
    for version in metadata
        .versions
        .iter()
        .filter(|version| version_allowed_by_policy(version_policy, version))
    {
        push_unique(&mut versions, &mut known, version);
    }
    let mut additional: Vec<String> = stored_versions
        .iter()
        .filter(|version| {
            version_allowed_by_policy(version_policy, version) && !known.contains(*version)
        })
        .cloned()
        .collect();
    sort_maven_versions(&mut additional);

    for version in additional {
        push_unique(&mut versions, &mut known, &version);
    }

    let latest = metadata
        .latest
        .filter(|version| {
            version_allowed_by_policy(version_policy, version) && known.contains(version)
        })
        .or_else(|| {
            versions
                .iter()
                .max_by(|left, right| compare_maven_versions(left, right))
                .cloned()
        });
    let release = match version_policy {
        MavenVersionPolicy::Snapshot => None,
        MavenVersionPolicy::Release | MavenVersionPolicy::Mixed => metadata
            .release
            .filter(|version| !is_snapshot(version) && known.contains(version))
            .or_else(|| {
                versions
                    .iter()
                    .filter(|version| !is_snapshot(version))
                    .max_by(|left, right| compare_maven_versions(left, right))
                    .cloned()
            }),
    };

    Some(generate_metadata_xml_with_versioning(
        group_id,
        artifact_id,
        &versions,
        latest.as_deref(),
        release.as_deref(),
        last_updated.or(metadata.last_updated.as_deref()),
    ))
}

fn merge_group_metadata(path: &str, documents: &[Bytes]) -> Option<Bytes> {
    let levels: Vec<MavenMetadataLevel> = documents
        .iter()
        .map(|document| classify_metadata_level(document))
        .collect::<Option<_>>()?;
    if levels
        .iter()
        .all(|level| *level == MavenMetadataLevel::Version)
    {
        return merge_group_version_metadata(path, documents);
    }
    if levels.contains(&MavenMetadataLevel::Version) {
        return None;
    }

    let artifact_documents: Vec<Bytes> = documents
        .iter()
        .zip(&levels)
        .filter(|(_, level)| {
            matches!(
                level,
                MavenMetadataLevel::Artifact | MavenMetadataLevel::ArtifactAndGroup
            )
        })
        .map(|(document, _)| document.clone())
        .collect();
    let artifact = if artifact_documents.is_empty() {
        None
    } else {
        let document = merge_group_artifact_metadata(path, &artifact_documents)?;
        Some(String::from_utf8(document.to_vec()).ok()?)
    };
    let plugins = merge_group_plugins(documents);

    combine_metadata_sections(artifact, plugins.as_deref()).map(Bytes::from)
}

fn merge_group_artifact_metadata(path: &str, documents: &[Bytes]) -> Option<Bytes> {
    let MavenPathKind::ArtifactMeta {
        group_path,
        artifact_id,
        ..
    } = classify_path(path)
    else {
        return None;
    };
    let parsed: Vec<ArtifactMetadata> = documents
        .iter()
        .map(|document| parse_artifact_metadata(document))
        .collect::<Option<_>>()?;
    if parsed
        .iter()
        .any(|metadata| !artifact_metadata_matches_path(metadata, &group_path, &artifact_id))
    {
        return None;
    }
    let mut versions: Vec<String> = parsed
        .iter()
        .flat_map(|metadata| metadata.versions.iter().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    sort_maven_versions(&mut versions);
    let latest = parsed
        .iter()
        .filter_map(|metadata| metadata.latest.as_ref())
        .max_by(|left, right| compare_maven_versions(left, right))
        .cloned();
    let release = parsed
        .iter()
        .filter_map(|metadata| metadata.release.as_ref())
        .filter(|version| !is_snapshot(version))
        .max_by(|left, right| compare_maven_versions(left, right))
        .cloned()
        .or_else(|| {
            versions
                .iter()
                .rev()
                .find(|version| !is_snapshot(version))
                .cloned()
        });
    let last_updated = parsed
        .iter()
        .filter_map(|metadata| metadata.last_updated.as_deref())
        .max();
    Some(Bytes::from(generate_metadata_xml_with_versioning(
        &group_path.replace('/', "."),
        &artifact_id,
        &versions,
        latest.as_deref(),
        release.as_deref(),
        last_updated,
    )))
}

#[derive(Clone, Default)]
struct MavenSnapshot {
    timestamp: Option<String>,
    build_number: Option<u64>,
    local_copy: Option<bool>,
}

#[derive(Clone, Default)]
struct MavenSnapshotVersion {
    classifier: Option<String>,
    extension: String,
    value: String,
    updated: Option<String>,
}

#[derive(Clone, Default)]
struct MavenVersionMetadata {
    group_id: Option<String>,
    artifact_id: Option<String>,
    version: Option<String>,
    snapshot: MavenSnapshot,
    last_updated: Option<String>,
    snapshot_versions: Vec<MavenSnapshotVersion>,
}

fn parse_version_metadata(data: &[u8]) -> Option<MavenVersionMetadata> {
    if classify_metadata_level(data) != Some(MavenMetadataLevel::Version) {
        return None;
    }

    let mut reader = Reader::from_reader(data);
    reader.config_mut().trim_text(true);
    let mut metadata = MavenVersionMetadata::default();
    let mut in_snapshot = false;
    let mut current_snapshot_version: Option<MavenSnapshotVersion> = None;

    loop {
        match reader.read_event() {
            Ok(Event::Start(element)) if element.local_name().as_ref() == b"snapshot" => {
                in_snapshot = true;
            }
            Ok(Event::Start(element)) if element.local_name().as_ref() == b"snapshotVersion" => {
                current_snapshot_version = Some(MavenSnapshotVersion::default());
            }
            Ok(Event::Start(element)) => {
                let name = element.local_name();
                let field = name.as_ref();
                if matches!(
                    field,
                    b"groupId"
                        | b"artifactId"
                        | b"version"
                        | b"timestamp"
                        | b"buildNumber"
                        | b"localCopy"
                        | b"lastUpdated"
                        | b"classifier"
                        | b"extension"
                        | b"value"
                        | b"updated"
                ) {
                    let text = reader.read_text(element.name()).ok()?.decode().ok()?;
                    let value = quick_xml::escape::unescape(&text).ok()?.into_owned();
                    if let Some(snapshot_version) = current_snapshot_version.as_mut() {
                        match field {
                            b"classifier" => {
                                if !value.is_empty() {
                                    snapshot_version.classifier = Some(value);
                                }
                            }
                            b"extension" => snapshot_version.extension = value,
                            b"value" => snapshot_version.value = value,
                            b"updated" if !value.is_empty() => {
                                snapshot_version.updated = Some(value);
                            }
                            _ => {}
                        }
                    } else if in_snapshot {
                        match field {
                            b"timestamp" => {
                                if !value.is_empty() {
                                    metadata.snapshot.timestamp = Some(value);
                                }
                            }
                            b"buildNumber" => {
                                metadata.snapshot.build_number = value.parse().ok();
                            }
                            b"localCopy" => {
                                metadata.snapshot.local_copy = value.parse().ok();
                            }
                            _ => {}
                        }
                    } else {
                        match field {
                            b"groupId" => metadata.group_id = Some(value),
                            b"artifactId" => metadata.artifact_id = Some(value),
                            b"version" => metadata.version = Some(value),
                            b"lastUpdated" if !value.is_empty() => {
                                metadata.last_updated = Some(value);
                            }
                            _ => {}
                        }
                    }
                }
            }
            Ok(Event::End(element)) if element.local_name().as_ref() == b"snapshot" => {
                in_snapshot = false;
            }
            Ok(Event::End(element)) if element.local_name().as_ref() == b"snapshotVersion" => {
                let snapshot_version = current_snapshot_version.take()?;
                if snapshot_version.extension.is_empty() || snapshot_version.value.is_empty() {
                    return None;
                }
                metadata.snapshot_versions.push(snapshot_version);
            }
            Ok(Event::Eof) => break,
            Err(_) => return None,
            _ => {}
        }
    }

    Some(metadata)
}

fn version_metadata_path(path: &str) -> Option<(String, String, String)> {
    let document_path = metadata_document_key(path).unwrap_or(path);
    let segments: Vec<&str> = document_path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();
    if segments.len() < 4 || segments.last().copied() != Some("maven-metadata.xml") {
        return None;
    }
    let version = segments[segments.len() - 2];
    if !is_snapshot(version) {
        return None;
    }
    Some((
        segments[..segments.len() - 3].join("/"),
        segments[segments.len() - 3].to_string(),
        version.to_string(),
    ))
}

fn merge_group_version_metadata(path: &str, documents: &[Bytes]) -> Option<Bytes> {
    let (group_path, artifact_id, version) = version_metadata_path(path)?;
    let group_id = group_path.replace('/', ".");
    let parsed: Vec<MavenVersionMetadata> = documents
        .iter()
        .map(|document| parse_version_metadata(document))
        .collect::<Option<_>>()?;
    if parsed.iter().any(|metadata| {
        metadata.group_id.as_deref() != Some(group_id.as_str())
            || metadata.artifact_id.as_deref() != Some(artifact_id.as_str())
            || metadata.version.as_deref() != Some(version.as_str())
    }) {
        return None;
    }

    let first = parsed.first()?;
    let selected_snapshot = parsed
        .iter()
        .skip(1)
        .fold(first, |selected, candidate| {
            let ordering = (
                candidate.snapshot.timestamp.as_deref().unwrap_or(""),
                candidate.snapshot.build_number.unwrap_or(0),
                candidate.last_updated.as_deref().unwrap_or(""),
            )
                .cmp(&(
                    selected.snapshot.timestamp.as_deref().unwrap_or(""),
                    selected.snapshot.build_number.unwrap_or(0),
                    selected.last_updated.as_deref().unwrap_or(""),
                ));
            if ordering.is_gt() {
                candidate
            } else {
                selected
            }
        })
        .snapshot
        .clone();
    let last_updated = parsed
        .iter()
        .filter_map(|metadata| metadata.last_updated.as_ref())
        .max()
        .cloned();
    let mut snapshot_versions: BTreeMap<(String, String), MavenSnapshotVersion> = BTreeMap::new();
    for metadata in &parsed {
        for candidate in &metadata.snapshot_versions {
            let key = (
                candidate.extension.clone(),
                candidate.classifier.clone().unwrap_or_default(),
            );
            let replace = snapshot_versions.get(&key).is_none_or(|current| {
                candidate.updated.as_deref().unwrap_or("")
                    > current.updated.as_deref().unwrap_or("")
            });
            if replace {
                snapshot_versions.insert(key, candidate.clone());
            }
        }
    }

    Some(Bytes::from(generate_version_metadata_xml(
        &group_id,
        &artifact_id,
        &version,
        &selected_snapshot,
        last_updated.as_deref(),
        snapshot_versions.values(),
    )))
}

fn generate_version_metadata_xml<'a>(
    group_id: &str,
    artifact_id: &str,
    version: &str,
    snapshot: &MavenSnapshot,
    last_updated: Option<&str>,
    snapshot_versions: impl Iterator<Item = &'a MavenSnapshotVersion>,
) -> String {
    let mut versioning = String::new();
    if snapshot.timestamp.is_some()
        || snapshot.build_number.is_some()
        || snapshot.local_copy.is_some()
    {
        versioning.push_str("    <snapshot>\n");
        if let Some(timestamp) = &snapshot.timestamp {
            versioning.push_str(&format!(
                "      <timestamp>{}</timestamp>\n",
                xml_escape(timestamp)
            ));
        }
        if let Some(build_number) = snapshot.build_number {
            versioning.push_str(&format!(
                "      <buildNumber>{build_number}</buildNumber>\n"
            ));
        }
        if let Some(local_copy) = snapshot.local_copy {
            versioning.push_str(&format!("      <localCopy>{local_copy}</localCopy>\n"));
        }
        versioning.push_str("    </snapshot>\n");
    }
    if let Some(last_updated) = last_updated {
        versioning.push_str(&format!(
            "    <lastUpdated>{}</lastUpdated>\n",
            xml_escape(last_updated)
        ));
    }
    let snapshot_versions: Vec<&MavenSnapshotVersion> = snapshot_versions.collect();
    if !snapshot_versions.is_empty() {
        versioning.push_str("    <snapshotVersions>\n");
        for snapshot_version in snapshot_versions {
            versioning.push_str("      <snapshotVersion>\n");
            if let Some(classifier) = &snapshot_version.classifier {
                versioning.push_str(&format!(
                    "        <classifier>{}</classifier>\n",
                    xml_escape(classifier)
                ));
            }
            versioning.push_str(&format!(
                "        <extension>{}</extension>\n        <value>{}</value>\n",
                xml_escape(&snapshot_version.extension),
                xml_escape(&snapshot_version.value)
            ));
            if let Some(updated) = &snapshot_version.updated {
                versioning.push_str(&format!(
                    "        <updated>{}</updated>\n",
                    xml_escape(updated)
                ));
            }
            versioning.push_str("      </snapshotVersion>\n");
        }
        versioning.push_str("    </snapshotVersions>\n");
    }

    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<metadata>\n  <groupId>{}</groupId>\n  <artifactId>{}</artifactId>\n  <version>{}</version>\n  <versioning>\n{}  </versioning>\n</metadata>\n",
        xml_escape(group_id),
        xml_escape(artifact_id),
        xml_escape(version),
        versioning,
    )
}

#[derive(Clone)]
struct MavenPlugin {
    name: String,
    prefix: String,
    artifact_id: String,
}

fn parse_group_plugins(document: &[u8]) -> Option<Vec<MavenPlugin>> {
    if !matches!(
        classify_metadata_level(document),
        Some(MavenMetadataLevel::Group | MavenMetadataLevel::ArtifactAndGroup)
    ) {
        return None;
    }
    let mut reader = Reader::from_reader(document);
    reader.config_mut().trim_text(true);
    let mut plugins = Vec::new();
    let mut current: Option<MavenPlugin> = None;

    loop {
        match reader.read_event() {
            Ok(Event::Start(element)) if element.local_name().as_ref() == b"plugin" => {
                current = Some(MavenPlugin {
                    name: String::new(),
                    prefix: String::new(),
                    artifact_id: String::new(),
                });
            }
            Ok(Event::Start(element)) if current.is_some() => {
                let field = element.local_name();
                if matches!(field.as_ref(), b"name" | b"prefix" | b"artifactId") {
                    let value = reader.read_text(element.name()).ok()?.decode().ok()?;
                    let value = quick_xml::escape::unescape(&value).ok()?.into_owned();
                    let plugin = current.as_mut()?;
                    match field.as_ref() {
                        b"name" => plugin.name = value,
                        b"prefix" => plugin.prefix = value,
                        b"artifactId" => plugin.artifact_id = value,
                        _ => {}
                    }
                }
            }
            Ok(Event::End(element)) if element.local_name().as_ref() == b"plugin" => {
                let plugin = current.take()?;
                if plugin.prefix.is_empty() || plugin.artifact_id.is_empty() {
                    return None;
                }
                plugins.push(plugin);
            }
            Ok(Event::Eof) => break,
            Err(_) => return None,
            _ => {}
        }
    }
    (!plugins.is_empty()).then_some(plugins)
}

fn extend_plugins_first_wins(
    plugins: &mut Vec<MavenPlugin>,
    seen_prefixes: &mut BTreeSet<String>,
    source: impl IntoIterator<Item = MavenPlugin>,
) {
    for plugin in source {
        if seen_prefixes.insert(plugin.prefix.clone()) {
            plugins.push(plugin);
        }
    }
}

fn merge_group_plugins(documents: &[Bytes]) -> Option<Vec<MavenPlugin>> {
    let mut plugins = Vec::new();
    let mut seen_prefixes = BTreeSet::new();
    let mut found_plugin_section = false;
    for document in documents {
        if matches!(
            classify_metadata_level(document),
            Some(MavenMetadataLevel::Group | MavenMetadataLevel::ArtifactAndGroup)
        ) {
            found_plugin_section = true;
            extend_plugins_first_wins(
                &mut plugins,
                &mut seen_prefixes,
                parse_group_plugins(document)?,
            );
        }
    }
    found_plugin_section.then_some(plugins)
}

fn plugins_section(plugins: &[MavenPlugin]) -> String {
    let entries = plugins
        .iter()
        .map(|plugin| {
            format!(
                "    <plugin>\n      <name>{}</name>\n      <prefix>{}</prefix>\n      <artifactId>{}</artifactId>\n    </plugin>",
                xml_escape(&plugin.name),
                xml_escape(&plugin.prefix),
                xml_escape(&plugin.artifact_id),
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!("  <plugins>\n{entries}\n  </plugins>\n")
}

fn combine_metadata_sections(
    artifact_metadata: Option<String>,
    plugins: Option<&[MavenPlugin]>,
) -> Option<String> {
    match (artifact_metadata, plugins) {
        (Some(mut artifact), Some(plugins)) => {
            let closing = artifact.rfind("</metadata>")?;
            artifact.insert_str(closing, &plugins_section(plugins));
            Some(artifact)
        }
        (Some(artifact), None) => Some(artifact),
        (None, Some(plugins)) => Some(format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<metadata>\n{}</metadata>\n",
            plugins_section(plugins)
        )),
        (None, None) => None,
    }
}

async fn merge_and_cache_proxy_metadata(
    state: &AppState,
    repository: &DirectRepository,
    group_path: &str,
    artifact_id: &str,
    document_path: &str,
    upstream: &[u8],
) -> crate::storage::Result<Bytes> {
    let storage_prefix = repository.storage_prefix();
    let key = format!("{storage_prefix}{document_path}");

    // Serialize the read -> merge -> write -> checksums cycle with the upload-side
    // regeneration (`update_artifact_metadata`) and any concurrent proxy merge: all of them
    // write the same `maven-metadata.xml` key and its four checksum sidecars. Without a shared
    // lock those five independent `put`s interleave, leaving a stored `.sha1`/`.md5`/… that
    // corresponds to different bytes than the stored `.xml` (checksum-mismatch window). The
    // upload path locks the artifact's `maven-metadata.xml` key; locking the document key here
    // — identical for artifact-level metadata — keeps `publish_lock serializes all writes to the
    // same artifact path` intact on the proxy path too (#886). Held across the writes below.
    let lock = state.publish_lock(&key);
    let _guard = lock.lock().await;

    let cached = match state.storage.get(&key).await {
        Ok(cached) => Some(cached),
        Err(StorageError::NotFound) => None,
        Err(error) => return Err(error),
    };
    let last_updated = [Some(upstream), cached.as_deref()]
        .into_iter()
        .flatten()
        .filter_map(parse_artifact_metadata)
        .filter_map(|metadata| metadata.last_updated)
        .max();
    let stored_versions =
        stored_artifact_versions(state, &storage_prefix, group_path, artifact_id).await?;
    let artifact = merge_artifact_metadata(
        &group_path.replace('/', "."),
        artifact_id,
        Some(upstream),
        &stored_versions,
        last_updated.as_deref(),
        repository.version_policy,
    );
    let plugin_documents: Vec<Bytes> = [Some(Bytes::copy_from_slice(upstream)), cached.clone()]
        .into_iter()
        .flatten()
        .collect();
    let plugins = merge_group_plugins(&plugin_documents);
    let data = combine_metadata_sections(artifact, plugins.as_deref())
        .map_or_else(|| Bytes::copy_from_slice(upstream), Bytes::from);

    if cached.as_deref() == Some(data.as_ref()) {
        if repository.metadata_ttl > 0 {
            // Keep hashing outside the cache mutex; the bounded map mutation below is the only
            // work that needs serialization.
            let fingerprint = revalidation_fingerprint(&data);
            record_revalidation(
                &mut state.maven_revalidation_cache.lock(),
                key,
                fingerprint,
                MAVEN_REVALIDATION_CACHE_MAX_ENTRIES,
            );
        } else {
            state.maven_revalidation_cache.lock().remove(&key);
        }
    } else {
        state.maven_revalidation_cache.lock().remove(&key);
        if let Err(error) = state.storage.put(&key, &data).await {
            tracing::warn!(key = %key, error = %error, "maven: failed to cache metadata");
        } else if let Err(error) = compute_and_store_checksums(&state.storage, &key, &data).await {
            tracing::warn!(key = %key, error = %error, "maven: failed to cache metadata checksums");
        } else {
            invalidate_maven_document_index(state, repository, document_path, &data);
        }
    }

    Ok(data)
}

async fn delete_existing_maven_object(
    storage: &crate::storage::Storage,
    key: &str,
) -> crate::storage::Result<Option<u64>> {
    match storage.get(key).await {
        Ok(data) => {
            let size = data.len() as u64;
            storage.delete(key).await?;
            Ok(Some(size))
        }
        Err(StorageError::NotFound) => Ok(None),
        Err(error) => Err(error),
    }
}

async fn delete_artifact_metadata_sidecars(
    storage: &crate::storage::Storage,
    metadata_key: &str,
) -> crate::storage::Result<(usize, u64)> {
    let mut removed_keys = 0;
    let mut removed_bytes = 0;
    for suffix in ["md5", "sha1", "sha256", "sha512"] {
        if let Some(size) =
            delete_existing_maven_object(storage, &format!("{metadata_key}.{suffix}")).await?
        {
            removed_keys += 1;
            removed_bytes += size;
        }
    }
    Ok((removed_keys, removed_bytes))
}

/// Regenerate one hosted GA's A-level metadata before retention deletes
/// `removed_version`. The caller must hold the GA metadata publish lock and
/// must not call this for proxy repositories.
///
/// Sidecars are removed before the base is replaced/deleted, so readers never
/// observe a stale checksum for the new document. Mixed G-level `<plugins>` are
/// preserved. The returned counters only include A-level objects permanently
/// deleted when no metadata sections remain; regenerated documents report zero.
pub(crate) async fn update_hosted_metadata_after_retention(
    storage: &crate::storage::Storage,
    storage_prefix: &str,
    group_path: &str,
    artifact_id: &str,
    removed_version: &str,
) -> crate::storage::Result<(usize, u64)> {
    let metadata_key = format!("{storage_prefix}{group_path}/{artifact_id}/maven-metadata.xml");
    let current = match storage.get(&metadata_key).await {
        Ok(current) => Some(current),
        Err(StorageError::NotFound) => None,
        Err(error) => return Err(error),
    };
    let current_artifact = current.as_deref().and_then(parse_artifact_metadata);
    let current_plugins = current.as_deref().and_then(parse_group_plugins);
    if current.is_some() && current_artifact.is_none() && current_plugins.is_none() {
        return Err(StorageError::IntegrityViolation);
    }
    if current_artifact
        .as_ref()
        .is_some_and(|metadata| !artifact_metadata_matches_path(metadata, group_path, artifact_id))
    {
        return Err(StorageError::IntegrityViolation);
    }

    let mut remaining =
        stored_artifact_versions_in_storage(storage, storage_prefix, group_path, artifact_id)
            .await?;
    remaining.remove(removed_version);

    let removed_sidecars = delete_artifact_metadata_sidecars(storage, &metadata_key).await?;
    if remaining.is_empty() {
        if let Some(plugins) = current_plugins.as_deref() {
            let document = combine_metadata_sections(None, Some(plugins))
                .ok_or(StorageError::IntegrityViolation)?;
            storage.put(&metadata_key, document.as_bytes()).await?;
            compute_and_store_checksums(storage, &metadata_key, document.as_bytes()).await?;
            return Ok((0, 0));
        }

        let mut removed_keys = removed_sidecars.0;
        let mut removed_bytes = removed_sidecars.1;
        if let Some(size) = delete_existing_maven_object(storage, &metadata_key).await? {
            removed_keys += 1;
            removed_bytes += size;
        }
        return Ok((removed_keys, removed_bytes));
    }

    let mut versions = Vec::new();
    let mut known = BTreeSet::new();
    if let Some(metadata) = current_artifact.as_ref() {
        for version in &metadata.versions {
            if remaining.contains(version) {
                push_unique(&mut versions, &mut known, version);
            }
        }
    }
    let mut additional: Vec<String> = remaining
        .iter()
        .filter(|version| !known.contains(*version))
        .cloned()
        .collect();
    sort_maven_versions(&mut additional);
    for version in additional {
        push_unique(&mut versions, &mut known, &version);
    }

    let latest = current_artifact
        .as_ref()
        .and_then(|metadata| metadata.latest.as_ref())
        .filter(|version| known.contains(*version))
        .cloned()
        .or_else(|| {
            versions
                .iter()
                .max_by(|left, right| compare_maven_versions(left, right))
                .cloned()
        });
    let release = current_artifact
        .as_ref()
        .and_then(|metadata| metadata.release.as_ref())
        .filter(|version| !is_snapshot(version) && known.contains(*version))
        .cloned()
        .or_else(|| {
            versions
                .iter()
                .filter(|version| !is_snapshot(version))
                .max_by(|left, right| compare_maven_versions(left, right))
                .cloned()
        });
    let last_updated = chrono::Utc::now().format("%Y%m%d%H%M%S").to_string();
    let artifact = generate_metadata_xml_with_versioning(
        &group_path.replace('/', "."),
        artifact_id,
        &versions,
        latest.as_deref(),
        release.as_deref(),
        Some(&last_updated),
    );
    let document = combine_metadata_sections(Some(artifact), current_plugins.as_deref())
        .ok_or(StorageError::IntegrityViolation)?;
    storage.put(&metadata_key, document.as_bytes()).await?;
    compute_and_store_checksums(storage, &metadata_key, document.as_bytes()).await?;
    Ok((0, 0))
}

async fn update_artifact_metadata(
    state: &AppState,
    storage_prefix: &str,
    group_path: &str,
    artifact_id: &str,
    deployed_version: Option<&str>,
) -> crate::storage::Result<()> {
    let versions = stored_artifact_versions(state, storage_prefix, group_path, artifact_id).await?;

    if versions.is_empty() {
        return Ok(());
    }

    let prefix = format!("{storage_prefix}{group_path}/{artifact_id}/");
    let metadata_key = format!("{}maven-metadata.xml", prefix);
    let current = match state.storage.get(&metadata_key).await {
        Ok(current) => Some(current),
        Err(StorageError::NotFound) => None,
        Err(error) => return Err(error),
    };
    let current_artifact_document = current
        .as_deref()
        .filter(|document| parse_artifact_metadata(document).is_some());
    let current_plugins = current.as_deref().and_then(parse_group_plugins);
    let group_id_dotted = group_path.replace('/', ".");
    // A completed exact retry passes no deployed version. Keep the existing
    // timestamp in that case so the retry remains byte-stable while still
    // repairing missing versions/checksums. A real deployment, or an exact
    // retry whose metadata is incomplete, passes the version and advances the
    // timestamp.
    let last_updated =
        deployed_version.map(|_| chrono::Utc::now().format("%Y%m%d%H%M%S").to_string());
    let artifact_xml = merge_hosted_artifact_metadata(
        &group_id_dotted,
        artifact_id,
        current_artifact_document,
        None,
        &versions,
        deployed_version,
        last_updated.as_deref(),
    )
    .unwrap_or_else(|| {
        let mut sorted: Vec<String> = versions.into_iter().collect();
        sort_maven_versions(&mut sorted);
        generate_metadata_xml(&group_id_dotted, artifact_id, &sorted)
    });
    let xml = combine_metadata_sections(Some(artifact_xml), current_plugins.as_deref())
        .expect("artifact metadata section is present");

    state.storage.put(&metadata_key, xml.as_bytes()).await?;
    compute_and_store_checksums(&state.storage, &metadata_key, xml.as_bytes()).await?;
    Ok(())
}

fn sort_maven_versions(versions: &mut [String]) {
    versions.sort_by(|a, b| compare_maven_versions(a, b));
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum MavenVersionItem {
    Numeric(String),
    Qualifier(String),
    List(Vec<MavenVersionItem>),
}

fn canonical_maven_qualifier(value: &str, followed_by_digit: bool) -> String {
    let value = if followed_by_digit && value.len() == 1 {
        match value {
            "a" => "alpha",
            "b" => "beta",
            "m" => "milestone",
            other => other,
        }
    } else {
        value
    };

    match value {
        "cr" | "rc" => "rc",
        "ga" | "final" | "release" => "",
        other => other,
    }
    .to_string()
}

fn compare_numeric_strings(left: &str, right: &str) -> std::cmp::Ordering {
    left.len().cmp(&right.len()).then_with(|| left.cmp(right))
}

fn maven_qualifier_key(value: &str) -> (u8, &str) {
    match value {
        "alpha" => (0, ""),
        "beta" => (1, ""),
        "milestone" => (2, ""),
        "rc" => (3, ""),
        "snapshot" => (4, ""),
        "" => (5, ""),
        "sp" => (6, ""),
        other => (7, other),
    }
}

impl MavenVersionItem {
    fn is_null(&self) -> bool {
        match self {
            Self::Numeric(value) => value == "0",
            Self::Qualifier(value) => value.is_empty(),
            Self::List(items) => items.is_empty(),
        }
    }

    fn compare_to(&self, other: Option<&Self>) -> std::cmp::Ordering {
        use MavenVersionItem::{List, Numeric, Qualifier};

        match (self, other) {
            (Numeric(left), Some(Numeric(right))) => compare_numeric_strings(left, right),
            (Numeric(_), Some(Qualifier(_) | List(_))) => std::cmp::Ordering::Greater,
            (Numeric(left), None) => compare_numeric_strings(left, "0"),

            (Qualifier(left), Some(Qualifier(right))) => {
                maven_qualifier_key(left).cmp(&maven_qualifier_key(right))
            }
            (Qualifier(_), Some(Numeric(_) | List(_))) => std::cmp::Ordering::Less,
            (Qualifier(left), None) => maven_qualifier_key(left).cmp(&maven_qualifier_key("")),

            (List(left), Some(List(right))) => {
                for index in 0..left.len().max(right.len()) {
                    let ordering = match (left.get(index), right.get(index)) {
                        (Some(left), right) => left.compare_to(right),
                        (None, Some(right)) => right.compare_to(None).reverse(),
                        (None, None) => std::cmp::Ordering::Equal,
                    };
                    if ordering != std::cmp::Ordering::Equal {
                        return ordering;
                    }
                }
                std::cmp::Ordering::Equal
            }
            (List(_), Some(Numeric(_))) => std::cmp::Ordering::Less,
            (List(_), Some(Qualifier(_))) => std::cmp::Ordering::Greater,
            (List(items), None) => items
                .iter()
                .map(|item| item.compare_to(None))
                .find(|ordering| *ordering != std::cmp::Ordering::Equal)
                .unwrap_or(std::cmp::Ordering::Equal),
        }
    }
}

fn normalize_maven_version_list(items: &mut Vec<MavenVersionItem>) {
    let mut index = items.len();
    while index > 0 {
        index -= 1;
        if items[index].is_null() {
            items.remove(index);
        } else if !matches!(items[index], MavenVersionItem::List(_)) {
            break;
        }
    }
}

fn maven_version_items(version: &str) -> MavenVersionItem {
    fn numeric_item(value: &str) -> MavenVersionItem {
        let normalized = value.trim_start_matches('0');
        MavenVersionItem::Numeric(if normalized.is_empty() {
            "0".to_string()
        } else {
            normalized.to_string()
        })
    }

    fn parsed_item(value: &str, is_digit: bool) -> MavenVersionItem {
        if is_digit {
            numeric_item(value)
        } else {
            MavenVersionItem::Qualifier(canonical_maven_qualifier(value, false))
        }
    }

    let version = version.to_ascii_lowercase();
    let mut lists = vec![Vec::new()];
    let mut is_digit = false;
    let mut start_index = 0;

    for (index, character) in version.char_indices() {
        if character == '.' {
            let item = if index == start_index {
                numeric_item("0")
            } else {
                parsed_item(&version[start_index..index], is_digit)
            };
            lists.last_mut().expect("root list exists").push(item);
            start_index = index + character.len_utf8();
        } else if character == '-' {
            let item = if index == start_index {
                numeric_item("0")
            } else {
                parsed_item(&version[start_index..index], is_digit)
            };
            lists.last_mut().expect("root list exists").push(item);
            start_index = index + character.len_utf8();
            lists.push(Vec::new());
        } else if character.is_ascii_digit() {
            if !is_digit && index > start_index {
                if !lists.last().expect("root list exists").is_empty() {
                    lists.push(Vec::new());
                }
                lists
                    .last_mut()
                    .expect("root list exists")
                    .push(MavenVersionItem::Qualifier(canonical_maven_qualifier(
                        &version[start_index..index],
                        true,
                    )));
                start_index = index;
                lists.push(Vec::new());
            }
            is_digit = true;
        } else {
            if is_digit && index > start_index {
                lists
                    .last_mut()
                    .expect("root list exists")
                    .push(numeric_item(&version[start_index..index]));
                start_index = index;
                lists.push(Vec::new());
            }
            is_digit = false;
        }
    }

    if version.len() > start_index {
        if !is_digit && !lists.last().expect("root list exists").is_empty() {
            lists.push(Vec::new());
        }
        lists
            .last_mut()
            .expect("root list exists")
            .push(parsed_item(&version[start_index..], is_digit));
    }

    while lists.len() > 1 {
        let mut child = lists.pop().expect("child list exists");
        normalize_maven_version_list(&mut child);
        lists
            .last_mut()
            .expect("parent list exists")
            .push(MavenVersionItem::List(child));
    }
    let mut root = lists.pop().expect("root list exists");
    normalize_maven_version_list(&mut root);
    MavenVersionItem::List(root)
}

/// Maven `ComparableVersion` ordering used by repository metadata, including
/// qualifier aliases, numeric normalization, and hyphen sub-list precedence.
fn compare_maven_versions(a: &str, b: &str) -> std::cmp::Ordering {
    let left = maven_version_items(a);
    let right = maven_version_items(b);
    left.compare_to(Some(&right))
}

/// Escape XML special characters in interpolated values.
fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn generate_metadata_xml(group_id: &str, artifact_id: &str, versions: &[String]) -> String {
    let latest = versions.last().map(String::as_str);
    let release = versions
        .iter()
        .rev()
        .find(|v| !v.ends_with("-SNAPSHOT"))
        .map(String::as_str);

    generate_metadata_xml_with_versioning(group_id, artifact_id, versions, latest, release, None)
}

fn generate_metadata_xml_with_versioning(
    group_id: &str,
    artifact_id: &str,
    versions: &[String],
    latest: Option<&str>,
    release: Option<&str>,
    last_updated: Option<&str>,
) -> String {
    let generated_last_updated;
    let last_updated = match last_updated {
        Some(last_updated) => last_updated,
        None => {
            generated_last_updated = chrono::Utc::now().format("%Y%m%d%H%M%S").to_string();
            &generated_last_updated
        }
    };

    let version_elements: String = versions
        .iter()
        .map(|v| format!("      <version>{}</version>", xml_escape(v)))
        .collect::<Vec<_>>()
        .join("\n");
    let latest_element = latest.map_or_else(String::new, |latest| {
        format!("    <latest>{}</latest>\n", xml_escape(latest))
    });
    let release_element = release.map_or_else(String::new, |release| {
        format!("    <release>{}</release>\n", xml_escape(release))
    });

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<metadata>
  <groupId>{}</groupId>
  <artifactId>{}</artifactId>
  <versioning>
{}{}
    <versions>
{}
    </versions>
    <lastUpdated>{}</lastUpdated>
  </versioning>
</metadata>
"#,
        xml_escape(group_id),
        xml_escape(artifact_id),
        latest_element,
        release_element,
        version_elements,
        last_updated
    )
}

// ============================================================================
// Content type
// ============================================================================

fn maven_content_type(path: &str) -> &'static str {
    if ends_with_ci(path, ".pom") {
        "application/xml"
    } else if ends_with_ci(path, ".jar") {
        "application/java-archive"
    } else if ends_with_ci(path, ".xml") {
        "application/xml"
    } else if ends_with_ci(path, ".sha1")
        || ends_with_ci(path, ".md5")
        || ends_with_ci(path, ".sha256")
        || ends_with_ci(path, ".sha512")
    {
        "text/plain"
    } else {
        "application/octet-stream"
    }
}

fn with_content_type(
    path: &str,
    data: Bytes,
) -> (StatusCode, [(header::HeaderName, &'static str); 2], Bytes) {
    let content_type = maven_content_type(path);

    // Metadata and SNAPSHOT assets are mutable; release artifacts are immutable.
    let cache_control = if is_mutable_maven_path(path) {
        "public, max-age=60, must-revalidate"
    } else {
        "public, max-age=31536000, immutable"
    };

    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, content_type),
            (header::CACHE_CONTROL, cache_control),
        ],
        data,
    )
}

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderMap;

    #[tokio::test]
    async fn named_proxy_cache_hits_queue_access_but_hosted_hits_do_not() {
        let mut ctx = crate::test_helpers::create_test_context_with_config(|config| {
            config.maven.proxies.clear();
            config.maven.repositories = vec![
                MavenRepository::Hosted {
                    name: "hosted".to_string(),
                    version_policy: MavenVersionPolicy::Mixed,
                    write_policy: MavenWritePolicy::AllowOnce,
                },
                MavenRepository::Proxy {
                    name: "central".to_string(),
                    url: "http://127.0.0.1:1".to_string(),
                    auth: None,
                    version_policy: MavenVersionPolicy::Mixed,
                    metadata_ttl: Some(300),
                    negative_ttl: 0,
                },
            ];
            config.maven.default_repository = Some("central".to_string());
        });
        let access = crate::proxy_cache_cleanup::ProxyCacheAccess::start_session(
            ctx.state.storage.clone(),
            ctx.state.publish_locks.clone(),
        )
        .await
        .unwrap();
        ctx.state.proxy_cache_access = Some(access.clone());
        let path = "com/acme/demo/1.0/demo-1.0.jar";
        let proxy_key = repository_storage_key("central", path);
        let hosted_key = repository_storage_key("hosted", path);
        ctx.state.storage.put(&proxy_key, b"proxy").await.unwrap();
        ctx.state.storage.put(&hosted_key, b"hosted").await.unwrap();

        let proxy = download_configured(
            ctx.state.clone(),
            HeaderMap::new(),
            "central",
            path.to_string(),
        )
        .await;
        assert_eq!(proxy.status(), StatusCode::OK);
        assert!(access.has_pending(&proxy_key).await);

        let hosted = download_configured(
            ctx.state.clone(),
            HeaderMap::new(),
            "hosted",
            path.to_string(),
        )
        .await;
        assert_eq!(hosted.status(), StatusCode::OK);
        assert!(!access.has_pending(&hosted_key).await);
    }

    #[tokio::test]
    async fn named_proxy_range_open_queues_access_under_payload_lock() {
        let mut ctx = crate::test_helpers::create_test_context_with_config(|config| {
            config.maven.proxies.clear();
            config.maven.repositories = vec![MavenRepository::Proxy {
                name: "central".to_string(),
                url: "http://127.0.0.1:1".to_string(),
                auth: None,
                version_policy: MavenVersionPolicy::Mixed,
                metadata_ttl: Some(300),
                negative_ttl: 0,
            }];
            config.maven.default_repository = Some("central".to_string());
        });
        let access = crate::proxy_cache_cleanup::ProxyCacheAccess::start_session(
            ctx.state.storage.clone(),
            ctx.state.publish_locks.clone(),
        )
        .await
        .unwrap();
        ctx.state.proxy_cache_access = Some(access.clone());
        let path = "com/acme/demo/1.0/demo-1.0.jar";
        let key = repository_storage_key("central", path);
        ctx.state.storage.put(&key, b"0123456789").await.unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(header::RANGE, HeaderValue::from_static("bytes=0-3"));

        let response =
            download_configured(ctx.state.clone(), headers, "central", path.to_string()).await;

        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        assert!(access.has_pending(&key).await);
    }

    #[test]
    fn test_url_is_maven_central() {
        // canonical Central hosts → true (date source available)
        assert!(url_is_maven_central("https://repo1.maven.org/maven2"));
        assert!(url_is_maven_central("https://repo.maven.apache.org/maven2"));
        assert!(url_is_maven_central("https://search.maven.org"));
        assert!(url_is_maven_central("https://central.sonatype.com"));
        // private mirrors → false: their coordinates must NEVER reach search.maven.org (#68/#733)
        assert!(!url_is_maven_central(
            "https://nexus.internal.corp/repository/maven"
        ));
        assert!(!url_is_maven_central("https://artifactory.acme.io/maven"));
        assert!(!url_is_maven_central("https://maven.pkg.github.com/acme"));
        assert!(!url_is_maven_central(""));
    }

    #[test]
    fn test_content_type_pom() {
        let (status, headers, _) =
            with_content_type("com/example/1.0/example-1.0.pom", Bytes::from("data"));
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers[0].1, "application/xml");
    }

    #[test]
    fn test_content_type_jar() {
        let (_, headers, _) =
            with_content_type("com/example/1.0/example-1.0.jar", Bytes::from("data"));
        assert_eq!(headers[0].1, "application/java-archive");
    }

    #[test]
    fn test_content_type_xml() {
        let (_, headers, _) =
            with_content_type("com/example/maven-metadata.xml", Bytes::from("data"));
        assert_eq!(headers[0].1, "application/xml");
    }

    #[test]
    fn test_content_type_sha1() {
        let (_, headers, _) =
            with_content_type("com/example/1.0/example-1.0.jar.sha1", Bytes::from("data"));
        assert_eq!(headers[0].1, "text/plain");
    }

    #[test]
    fn test_content_type_md5() {
        let (_, headers, _) =
            with_content_type("com/example/1.0/example-1.0.jar.md5", Bytes::from("data"));
        assert_eq!(headers[0].1, "text/plain");
    }

    #[test]
    fn test_content_type_sha256() {
        let (_, headers, _) = with_content_type(
            "com/example/1.0/example-1.0.jar.sha256",
            Bytes::from("data"),
        );
        assert_eq!(headers[0].1, "text/plain");
    }

    #[test]
    fn test_content_type_unknown() {
        let (_, headers, _) = with_content_type("some/random/file.bin", Bytes::from("data"));
        assert_eq!(headers[0].1, "application/octet-stream");
    }

    #[test]
    fn test_content_type_preserves_body() {
        let body = Bytes::from("test-jar-content");
        let (_, _, data) = with_content_type("test.jar", body.clone());
        assert_eq!(data, body);
    }

    #[test]
    fn test_is_mutable_maven_path() {
        // maven-metadata.xml and its checksums are mutable (rewritten as versions deploy).
        assert!(is_mutable_maven_path(
            "com/example/mylib/maven-metadata.xml"
        ));
        assert!(is_mutable_maven_path(
            "com/example/mylib/maven-metadata.xml.sha1"
        ));
        // SNAPSHOT version files are republished in place → mutable.
        assert!(is_mutable_maven_path(
            "com/example/mylib/1.0.0-SNAPSHOT/mylib-1.0.0-SNAPSHOT.jar"
        ));
        // Released artifacts are immutable.
        assert!(!is_mutable_maven_path(
            "com/example/mylib/1.0.0/mylib-1.0.0.jar"
        ));
        assert!(!is_mutable_maven_path(
            "com/example/mylib/1.0.0/mylib-1.0.0.pom"
        ));
    }

    #[tokio::test]
    async fn proxy_cache_uses_conditional_create_only_for_release_artifacts() {
        use crate::storage::Storage;
        use crate::test_helpers::{create_test_context, FaultInjectBackend};
        use std::sync::Arc;

        let ctx = create_test_context();
        let backend = FaultInjectBackend::new(ctx.state.storage.clone());
        let write_attempts = backend.write_attempts();
        let mut state = ctx.state.clone();
        state.storage = Storage::from_backend(Arc::new(backend));
        let release_key = "maven/releases/com/example/lib/1.0/lib-1.0.jar";
        let snapshot_key = "maven/snapshots/com/example/lib/1.0-SNAPSHOT/lib-1.0-SNAPSHOT.jar";

        spawn_proxy_cache(
            &state,
            "com/example/lib/1.0/lib-1.0.jar",
            release_key.to_string(),
            Bytes::from_static(b"release"),
        );
        spawn_proxy_cache(
            &state,
            "com/example/lib/1.0-SNAPSHOT/lib-1.0-SNAPSHOT.jar",
            snapshot_key.to_string(),
            Bytes::from_static(b"snapshot"),
        );

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if write_attempts.lock().len() == 2 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("background cache writes");

        let attempts = write_attempts.lock().clone();
        assert!(attempts.contains(&format!("create:{release_key}")));
        assert!(attempts.contains(&format!("put:{snapshot_key}")));
    }

    #[test]
    fn test_negative_cache_is_bounded_and_prunes_expired_entries() {
        let mut cache = HashMap::new();
        cache.insert(
            "expired".to_string(),
            Instant::now() - Duration::from_secs(10),
        );
        insert_negative_cache_entry(&mut cache, "first".to_string(), 1, 2);
        insert_negative_cache_entry(&mut cache, "second".to_string(), 1, 2);
        insert_negative_cache_entry(&mut cache, "third".to_string(), 1, 2);
        assert_eq!(cache.len(), 2);
        assert!(!cache.contains_key("expired"));
        assert!(cache.contains_key("third"));
    }

    #[test]
    fn test_revalidation_cache_is_bounded_identity_bound_and_disabled_at_zero_ttl() {
        let now = Instant::now();
        let old_fingerprint = sha2::Sha256::digest(b"old").into();
        let current_fingerprint = sha2::Sha256::digest(b"current").into();
        let mut cache = HashMap::from([
            (
                "old".to_string(),
                (now - Duration::from_secs(10), old_fingerprint),
            ),
            ("current".to_string(), (now, current_fingerprint)),
        ]);

        record_revalidation(
            &mut cache,
            "new".to_string(),
            revalidation_fingerprint(b"new"),
            2,
        );

        assert_eq!(cache.len(), 2);
        assert!(!cache.contains_key("old"));
        assert!(revalidation_cache_fresh(
            &mut cache,
            "current",
            revalidation_fingerprint(b"current"),
            300
        ));
        assert!(!revalidation_cache_fresh(
            &mut cache,
            "current",
            revalidation_fingerprint(b"changed"),
            300
        ));
        assert!(!cache.contains_key("current"));
        assert!(!revalidation_cache_fresh(
            &mut cache,
            "new",
            revalidation_fingerprint(b"new"),
            0
        ));
        assert!(!cache.contains_key("new"));

        let mut expired_cache = HashMap::from([(
            "expired-match".to_string(),
            (
                Instant::now() - Duration::from_secs(301),
                revalidation_fingerprint(b"same"),
            ),
        )]);
        assert!(!revalidation_cache_fresh(
            &mut expired_cache,
            "expired-match",
            revalidation_fingerprint(b"same"),
            300
        ));
        assert!(!expired_cache.contains_key("expired-match"));
    }

    #[test]
    fn test_all_metadata_checksum_cache_headers_revalidate() {
        for suffix in ["md5", "sha1", "sha256", "sha512"] {
            let (_, headers, _) = with_content_type(
                &format!("com/example/lib/maven-metadata.xml.{suffix}"),
                Bytes::new(),
            );
            assert_eq!(headers[1].1, "public, max-age=60, must-revalidate");
        }
    }

    #[test]
    fn test_snapshot_cache_headers_revalidate_and_release_is_immutable() {
        for path in [
            "com/example/lib/1.0-SNAPSHOT/lib-1.0-SNAPSHOT.jar",
            "com/example/lib/1.0-SNAPSHOT/lib-1.0-SNAPSHOT.jar.sha512",
        ] {
            let (_, headers, _) = with_content_type(path, Bytes::new());
            assert_eq!(headers[1].1, "public, max-age=60, must-revalidate");
        }
        let (_, headers, _) = with_content_type("com/example/lib/1.0/lib-1.0.jar", Bytes::new());
        assert_eq!(headers[1].1, "public, max-age=31536000, immutable");
    }

    // ── Path classification ─────────────────────────────────────────────

    #[test]
    fn test_classify_version_file() {
        match classify_path("com/example/mylib/1.0.0/mylib-1.0.0.jar") {
            MavenPathKind::VersionFile(c) => {
                assert_eq!(c.group_path, "com/example");
                assert_eq!(c.artifact_id, "mylib");
                assert_eq!(c.version, "1.0.0");
            }
            _ => panic!("expected VersionFile"),
        }
    }

    #[test]
    fn test_classify_version_checksum() {
        match classify_path("com/example/mylib/1.0.0/mylib-1.0.0.jar.sha1") {
            MavenPathKind::VersionFile(c) => {
                assert_eq!(c.version, "1.0.0");
                assert_eq!(
                    checksum_suffix("com/example/mylib/1.0.0/mylib-1.0.0.jar.sha1"),
                    Some("sha1")
                );
            }
            _ => panic!("expected VersionFile"),
        }
    }

    #[test]
    fn test_classify_artifact_metadata() {
        match classify_path("com/example/mylib/maven-metadata.xml") {
            MavenPathKind::ArtifactMeta {
                group_path,
                artifact_id,
                filename,
            } => {
                assert_eq!(group_path, "com/example");
                assert_eq!(artifact_id, "mylib");
                assert_eq!(filename, "maven-metadata.xml");
            }
            _ => panic!("expected ArtifactMeta"),
        }
    }

    #[test]
    fn test_classify_metadata_checksum() {
        match classify_path("com/example/mylib/maven-metadata.xml.sha256") {
            MavenPathKind::ArtifactMeta {
                artifact_id,
                filename,
                ..
            } => {
                assert_eq!(artifact_id, "mylib");
                assert_eq!(filename, "maven-metadata.xml.sha256");
            }
            _ => panic!("expected ArtifactMeta"),
        }
    }

    #[test]
    fn test_classify_deep_group() {
        match classify_path("org/apache/maven/plugins/maven-compiler-plugin/3.11.0/maven-compiler-plugin-3.11.0.jar") {
            MavenPathKind::VersionFile(c) => {
                assert_eq!(c.group_path, "org/apache/maven/plugins");
                assert_eq!(c.artifact_id, "maven-compiler-plugin");
                assert_eq!(c.version, "3.11.0");
            }
            _ => panic!("expected VersionFile"),
        }
    }

    #[test]
    fn test_classify_snapshot() {
        match classify_path("com/example/mylib/1.0-SNAPSHOT/mylib-1.0-SNAPSHOT.jar") {
            MavenPathKind::VersionFile(c) => {
                assert!(is_snapshot(&c.version));
            }
            _ => panic!("expected VersionFile"),
        }
    }

    #[test]
    fn test_classify_opaque_short_path() {
        assert!(matches!(classify_path("a"), MavenPathKind::Opaque));
    }

    // ── Checksum detection ──────────────────────────────────────────────

    #[test]
    fn test_checksum_suffix() {
        assert_eq!(checksum_suffix("foo.md5"), Some("md5"));
        assert_eq!(checksum_suffix("foo.sha1"), Some("sha1"));
        assert_eq!(checksum_suffix("foo.sha256"), Some("sha256"));
        assert_eq!(checksum_suffix("foo.sha512"), Some("sha512"));
        assert_eq!(checksum_suffix("foo.jar"), None);
        assert_eq!(checksum_suffix("foo.pom"), None);
    }

    // ── Version sorting ─────────────────────────────────────────────────

    #[test]
    fn test_sort_versions_lexicographic() {
        let mut v = vec!["1.0.0".into(), "0.9.0".into(), "1.1.0".into()];
        sort_maven_versions(&mut v);
        assert_eq!(v, vec!["0.9.0", "1.0.0", "1.1.0"]);
    }

    #[test]
    fn test_sort_snapshot_before_release() {
        let mut v = vec!["1.0.0-SNAPSHOT".into(), "1.0.0".into(), "0.9.0".into()];
        sort_maven_versions(&mut v);
        assert_eq!(v, vec!["0.9.0", "1.0.0-SNAPSHOT", "1.0.0"]);
    }

    #[test]
    fn test_sort_numeric_segments() {
        let mut v = vec!["10.0.0".into(), "9.0.0".into(), "2.1.0".into()];
        sort_maven_versions(&mut v);
        assert_eq!(v, vec!["2.1.0", "9.0.0", "10.0.0"]);
    }

    #[test]
    fn test_maven_version_qualifiers_and_equivalent_forms() {
        assert_eq!(
            compare_maven_versions("1", "1.0.0"),
            std::cmp::Ordering::Equal
        );
        assert_eq!(
            compare_maven_versions("1.0-1", "1.0.1"),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            compare_maven_versions("1-1", "1.1"),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            compare_maven_versions("1.0-final", "1.0-ga"),
            std::cmp::Ordering::Equal
        );
        assert_eq!(
            compare_maven_versions("1.0.0", "1.0.0-0"),
            std::cmp::Ordering::Equal
        );
        assert_eq!(
            compare_maven_versions("1.0-alpha", "1.0.alpha"),
            std::cmp::Ordering::Equal
        );
        assert_eq!(
            compare_maven_versions("1alpha", "1-alpha"),
            std::cmp::Ordering::Equal
        );
        assert_eq!(
            compare_maven_versions("1-alpha", "1.alpha"),
            std::cmp::Ordering::Equal
        );
        assert_eq!(
            compare_maven_versions("1.0alpha1", "1.0-alpha1"),
            std::cmp::Ordering::Equal
        );
        let mut versions = vec![
            "1.0-sp".into(),
            "1.0".into(),
            "1.0-SNAPSHOT".into(),
            "1.0-rc1".into(),
            "1.0-beta1".into(),
            "1.0-alpha1".into(),
        ];
        sort_maven_versions(&mut versions);
        assert_eq!(
            versions,
            vec![
                "1.0-alpha1",
                "1.0-beta1",
                "1.0-rc1",
                "1.0-SNAPSHOT",
                "1.0",
                "1.0-sp",
            ]
        );
    }

    #[test]
    fn test_maven_comparable_version_reference_order() {
        let versions = [
            "1-alpha2snapshot",
            "1-alpha2",
            "1-alpha-123",
            "1-beta-2",
            "1-beta123",
            "1-m2",
            "1-m11",
            "1-rc",
            "1-cr2",
            "1-rc123",
            "1-SNAPSHOT",
            "1",
            "1-sp",
            "1-sp2",
            "1-sp123",
            "1-abc",
            "1-def",
            "1-pom-1",
            "1-1-snapshot",
            "1-1",
            "1-2",
            "1-123",
        ];
        for (index, lower) in versions.iter().enumerate() {
            for higher in &versions[index + 1..] {
                assert_eq!(
                    compare_maven_versions(lower, higher),
                    std::cmp::Ordering::Less,
                    "expected {lower} < {higher}"
                );
            }
        }

        let numeric_versions = [
            "2.0", "2.0.a", "2-1", "2.0.2", "2.0.123", "2.1.0", "2.1-a", "2.1b", "2.1-c", "2.1-1",
            "2.1.0.1", "2.2", "2.123", "11.a2", "11.a11", "11.b2", "11.b11", "11.m2", "11.m11",
            "11", "11.a", "11b", "11c", "11m",
        ];
        for (index, lower) in numeric_versions.iter().enumerate() {
            for higher in &numeric_versions[index + 1..] {
                assert_eq!(
                    compare_maven_versions(lower, higher),
                    std::cmp::Ordering::Less,
                    "expected {lower} < {higher}"
                );
            }
        }

        for (left, right) in [
            ("1", "1.0"),
            ("1", "1-0"),
            ("1a", "1.0.0-a"),
            ("1x", "1.0.0-x"),
            ("1ga", "1"),
            ("1release", "1"),
            ("1cr", "1rc"),
            ("1a1", "1-alpha-1"),
            ("1b2", "1-beta-2"),
            ("1m3", "1-milestone-3"),
        ] {
            assert_eq!(
                compare_maven_versions(left, right),
                std::cmp::Ordering::Equal,
                "expected {left} == {right}"
            );
        }
    }

    // ── Metadata XML generation ─────────────────────────────────────────

    #[test]
    fn test_generate_metadata_xml() {
        let xml = generate_metadata_xml("com.example", "mylib", &["0.9.0".into(), "1.0.0".into()]);
        assert!(xml.contains("<groupId>com.example</groupId>"));
        assert!(xml.contains("<artifactId>mylib</artifactId>"));
        assert!(xml.contains("<latest>1.0.0</latest>"));
        assert!(xml.contains("<release>1.0.0</release>"));
        assert!(xml.contains("<version>0.9.0</version>"));
        assert!(xml.contains("<version>1.0.0</version>"));
        assert!(xml.contains("<lastUpdated>"));
    }

    #[test]
    fn test_generate_metadata_snapshot_only() {
        let xml = generate_metadata_xml("com.example", "mylib", &["1.0.0-SNAPSHOT".into()]);
        assert!(xml.contains("<latest>1.0.0-SNAPSHOT</latest>"));
        assert!(!xml.contains("<release>"));
    }

    #[test]
    fn test_merge_artifact_metadata_keeps_public_and_hosted_versions() {
        let upstream = br#"<?xml version="1.0" encoding="UTF-8"?>
<metadata>
  <groupId>com.example</groupId>
  <artifactId>library</artifactId>
  <versioning>
    <latest>2.0.0</latest>
    <release>2.0.0</release>
    <versions>
      <version>1.0.0</version>
      <version>2.0.0</version>
    </versions>
    <lastUpdated>20260728010000</lastUpdated>
  </versioning>
</metadata>
"#;
        let stored = BTreeSet::from(["0.5.0-internal".to_string(), "9.0.0-internal".to_string()]);

        let merged = merge_artifact_metadata(
            "com.example",
            "library",
            Some(upstream),
            &stored,
            None,
            MavenVersionPolicy::Mixed,
        )
        .unwrap();

        assert!(merged.contains("<version>1.0.0</version>"));
        assert!(merged.contains("<version>2.0.0</version>"));
        assert!(merged.contains("<version>0.5.0-internal</version>"));
        assert!(merged.contains("<version>9.0.0-internal</version>"));
        assert!(merged.contains("<latest>2.0.0</latest>"));
        assert!(merged.contains("<release>2.0.0</release>"));
        assert!(merged.contains("<lastUpdated>20260728010000</lastUpdated>"));

        let lower_only = BTreeSet::from(["0.5.0-internal".to_string()]);
        let merged = merge_artifact_metadata(
            "com.example",
            "library",
            Some(upstream),
            &lower_only,
            None,
            MavenVersionPolicy::Mixed,
        )
        .unwrap();
        assert!(merged.contains("<latest>2.0.0</latest>"));
        assert!(merged.contains("<release>2.0.0</release>"));
    }

    #[test]
    fn test_hosted_metadata_latest_and_release_follow_deployment_order() {
        let versions = BTreeSet::from(["1.0".to_string(), "2.0".to_string()]);
        let first = merge_hosted_artifact_metadata(
            "com.example",
            "library",
            None,
            None,
            &versions,
            Some("2.0"),
            Some("20260730010000"),
        )
        .unwrap();
        let second = merge_hosted_artifact_metadata(
            "com.example",
            "library",
            Some(first.as_bytes()),
            None,
            &versions,
            Some("1.0"),
            Some("20260730020000"),
        )
        .unwrap();
        assert!(second.contains("<release>1.0</release>"));
        assert!(second.contains("<latest>1.0</latest>"));
        assert!(second.find("<version>2.0</version>") < second.find("<version>1.0</version>"));
    }

    #[test]
    fn test_group_version_metadata_merges_newest_per_extension_and_classifier() {
        let older = Bytes::from_static(
            br#"<metadata><groupId>com.example</groupId><artifactId>library</artifactId><version>1.0-SNAPSHOT</version><versioning><snapshot><timestamp>20260730.010000</timestamp><buildNumber>1</buildNumber></snapshot><lastUpdated>20260730010000</lastUpdated><snapshotVersions><snapshotVersion><extension>jar</extension><value>1.0-20260730.010000-1</value><updated>20260730010000</updated></snapshotVersion><snapshotVersion><classifier>sources</classifier><extension>jar</extension><value>1.0-20260730.010000-1</value><updated>20260730010000</updated></snapshotVersion></snapshotVersions></versioning></metadata>"#,
        );
        let newer = Bytes::from_static(
            br#"<metadata><groupId>com.example</groupId><artifactId>library</artifactId><version>1.0-SNAPSHOT</version><versioning><snapshot><timestamp>20260730.020000</timestamp><buildNumber>2</buildNumber></snapshot><lastUpdated>20260730020000</lastUpdated><snapshotVersions><snapshotVersion><extension>jar</extension><value>1.0-20260730.020000-2</value><updated>20260730020000</updated></snapshotVersion><snapshotVersion><classifier>javadoc</classifier><extension>jar</extension><value>1.0-20260730.020000-2</value><updated>20260730020000</updated></snapshotVersion></snapshotVersions></versioning></metadata>"#,
        );
        let merged = merge_group_version_metadata(
            "com/example/library/1.0-SNAPSHOT/maven-metadata.xml",
            &[older, newer],
        )
        .unwrap();
        let merged = String::from_utf8(merged.to_vec()).unwrap();
        assert!(merged.contains("<timestamp>20260730.020000</timestamp>"));
        assert!(merged.contains("<buildNumber>2</buildNumber>"));
        assert!(merged.contains("1.0-20260730.020000-2"));
        assert!(merged.contains("<classifier>sources</classifier>"));
        assert!(merged.contains("<classifier>javadoc</classifier>"));
        assert_eq!(merged.matches("1.0-20260730.010000-1").count(), 1);
        assert_eq!(merged.matches("1.0-20260730.020000-2").count(), 2);
    }

    #[test]
    fn test_classify_artifact_level_metadata() {
        let xml = br#"
            <metadata xmlns="http://maven.apache.org/METADATA/1.1.0">
              <groupId>com.example</groupId>
              <artifactId>library</artifactId>
              <versioning>
                <versions><version>1.0.0</version></versions>
              </versioning>
            </metadata>
        "#;
        assert_eq!(
            classify_metadata_level(xml),
            Some(MavenMetadataLevel::Artifact)
        );
    }

    #[test]
    fn test_classify_version_level_metadata() {
        let xml = br#"
            <metadata>
              <groupId>com.example</groupId>
              <artifactId>library</artifactId>
              <version>1.0-SNAPSHOT</version>
              <versioning><snapshot><buildNumber>1</buildNumber></snapshot></versioning>
            </metadata>
        "#;
        assert_eq!(
            classify_metadata_level(xml),
            Some(MavenMetadataLevel::Version)
        );
    }

    #[test]
    fn test_classify_group_level_metadata() {
        let xml = br#"
            <metadata>
              <plugins>
                <plugin>
                  <prefix>example</prefix>
                  <artifactId>example-maven-plugin</artifactId>
                </plugin>
              </plugins>
            </metadata>
        "#;
        assert_eq!(
            classify_metadata_level(xml),
            Some(MavenMetadataLevel::Group)
        );
    }

    #[test]
    fn test_classify_combined_artifact_and_group_metadata() {
        let xml = br#"
            <metadata>
              <groupId>org.example</groupId>
              <artifactId>plugins</artifactId>
              <versioning>
                <latest>1.0</latest>
                <versions><version>1.0</version></versions>
              </versioning>
              <plugins>
                <plugin>
                  <prefix>example</prefix>
                  <artifactId>example-maven-plugin</artifactId>
                </plugin>
              </plugins>
            </metadata>
        "#;
        assert_eq!(
            classify_metadata_level(xml),
            Some(MavenMetadataLevel::ArtifactAndGroup)
        );
        assert_eq!(parse_artifact_metadata(xml).unwrap().versions, ["1.0"]);
        assert_eq!(parse_group_plugins(xml).unwrap().len(), 1);
    }

    #[test]
    fn test_reject_malformed_metadata() {
        assert_eq!(classify_metadata_level(b"<metadata>"), None);
        assert_eq!(classify_metadata_level(b"<project/>"), None);
    }
}

// ============================================================================
// Integration Tests
// ============================================================================

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod integration_tests {
    use super::{
        checksum_hex, compute_and_store_checksums, download_direct, merge_and_cache_proxy_metadata,
        parse_artifact_metadata, repository_storage_key, upload_direct, DirectRepository,
    };
    use crate::config::{MavenRepository, MavenVersionPolicy, MavenWritePolicy};
    use crate::storage::{
        FileMeta, Result as StorageResult, Storage, StorageBackend, StorageError,
    };
    use crate::test_helpers::{
        body_bytes, create_test_context, create_test_context_with_config, send, send_with_headers,
    };
    use axum::body::{Body, Bytes};
    use axum::http::{HeaderMap, Method, StatusCode};
    use parking_lot::Mutex;
    use std::collections::HashMap;
    use std::path::Path as FsPath;
    use std::pin::Pin;
    use std::sync::Arc;
    use tokio::io::AsyncRead;

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum InjectedOperation {
        Put,
        Get,
        List,
    }

    struct InjectedFailure {
        operation: InjectedOperation,
        key: String,
        skip_matches: usize,
    }

    #[derive(Default)]
    struct FaultInjectingBackend {
        objects: Mutex<HashMap<String, Bytes>>,
        failures: Mutex<Vec<InjectedFailure>>,
    }

    impl FaultInjectingBackend {
        fn fail_once(&self, operation: InjectedOperation, key: impl Into<String>) {
            self.fail_after(operation, key, 0);
        }

        fn fail_after(
            &self,
            operation: InjectedOperation,
            key: impl Into<String>,
            skip_matches: usize,
        ) {
            self.failures.lock().push(InjectedFailure {
                operation,
                key: key.into(),
                skip_matches,
            });
        }

        fn take_failure(&self, operation: InjectedOperation, key: &str) -> bool {
            let mut failures = self.failures.lock();
            let Some(index) = failures
                .iter()
                .position(|failure| failure.operation == operation && failure.key == key)
            else {
                return false;
            };
            if failures[index].skip_matches > 0 {
                failures[index].skip_matches -= 1;
                return false;
            }
            failures.remove(index);
            true
        }

        fn injected_error() -> StorageError {
            StorageError::Io(std::io::Error::other("injected Maven storage failure"))
        }
    }

    #[async_trait::async_trait]
    impl StorageBackend for FaultInjectingBackend {
        async fn put(&self, key: &str, data: &[u8]) -> StorageResult<()> {
            if self.take_failure(InjectedOperation::Put, key) {
                return Err(Self::injected_error());
            }
            self.objects
                .lock()
                .insert(key.to_string(), Bytes::copy_from_slice(data));
            Ok(())
        }

        async fn put_if_absent(&self, key: &str, data: &[u8]) -> StorageResult<()> {
            let mut objects = self.objects.lock();
            if objects.contains_key(key) {
                return Err(StorageError::AlreadyExists);
            }
            objects.insert(key.to_string(), Bytes::copy_from_slice(data));
            Ok(())
        }

        async fn get(&self, key: &str) -> StorageResult<Bytes> {
            if self.take_failure(InjectedOperation::Get, key) {
                return Err(Self::injected_error());
            }
            self.objects
                .lock()
                .get(key)
                .cloned()
                .ok_or(StorageError::NotFound)
        }

        async fn delete(&self, key: &str) -> StorageResult<()> {
            self.objects
                .lock()
                .remove(key)
                .map(|_| ())
                .ok_or(StorageError::NotFound)
        }

        async fn list(&self, prefix: &str) -> StorageResult<Vec<String>> {
            if self.take_failure(InjectedOperation::List, prefix) {
                return Err(Self::injected_error());
            }
            let mut keys: Vec<String> = self
                .objects
                .lock()
                .keys()
                .filter(|key| key.starts_with(prefix))
                .cloned()
                .collect();
            keys.sort();
            Ok(keys)
        }

        async fn stat(&self, key: &str) -> StorageResult<Option<FileMeta>> {
            Ok(self
                .objects
                .lock()
                .get(key)
                .map(|data| FileMeta::local(data.len() as u64, 1)))
        }

        async fn health_check(&self) -> bool {
            true
        }

        fn backend_name(&self) -> &'static str {
            "fault-injecting-maven-test"
        }

        async fn put_from_path(&self, _key: &str, _src: &FsPath) -> StorageResult<()> {
            Err(StorageError::Network(
                "put_from_path is not used by Maven fault tests".to_string(),
            ))
        }

        async fn get_reader(
            &self,
            _key: &str,
        ) -> StorageResult<(u64, Pin<Box<dyn AsyncRead + Send + Unpin>>)> {
            Err(StorageError::NotFound)
        }
    }

    async fn upload_with_state(
        state: &crate::AppState,
        path: &str,
        data: &'static [u8],
    ) -> axum::response::Response {
        super::upload_legacy(
            axum::extract::State(state.clone()),
            axum::extract::Path(path.to_string()),
            axum::Extension(crate::auth::NamespaceAuthority::Unrestricted),
            Bytes::from_static(data),
        )
        .await
    }

    fn named_repository_context() -> crate::test_helpers::TestContext {
        create_test_context_with_config(|config| {
            config.maven.repositories = vec![
                MavenRepository::Hosted {
                    name: "maven-releases".to_string(),
                    version_policy: MavenVersionPolicy::Release,
                    write_policy: MavenWritePolicy::Allow,
                },
                MavenRepository::Hosted {
                    name: "maven-snapshots".to_string(),
                    version_policy: MavenVersionPolicy::Snapshot,
                    write_policy: MavenWritePolicy::Allow,
                },
                MavenRepository::Hosted {
                    name: "maven-open".to_string(),
                    version_policy: MavenVersionPolicy::Release,
                    write_policy: MavenWritePolicy::Allow,
                },
                MavenRepository::Group {
                    name: "maven-public".to_string(),
                    members: vec![
                        "maven-open".to_string(),
                        "maven-releases".to_string(),
                        "maven-snapshots".to_string(),
                    ],
                },
            ];
            config.maven.default_repository = Some("maven-public".to_string());
        })
    }

    fn proxy_group_context(
        upstream_url: String,
        negative_ttl: i64,
    ) -> crate::test_helpers::TestContext {
        create_test_context_with_config(move |config| {
            config.maven.repositories = vec![
                MavenRepository::Proxy {
                    name: "proxy".to_string(),
                    url: upstream_url,
                    auth: None,
                    version_policy: MavenVersionPolicy::Release,
                    metadata_ttl: Some(0),
                    negative_ttl,
                },
                MavenRepository::Hosted {
                    name: "hosted".to_string(),
                    version_policy: MavenVersionPolicy::Release,
                    write_policy: MavenWritePolicy::Allow,
                },
                MavenRepository::Group {
                    name: "public".to_string(),
                    members: vec!["proxy".to_string(), "hosted".to_string()],
                },
            ];
        })
    }

    #[tokio::test]
    async fn test_named_allow_write_policy_replaces_release_and_derived_checksums() {
        let ctx = named_repository_context();
        let path = "com/example/redeploy/1.0/redeploy-1.0.jar";
        let uri = format!("/repository/maven-releases/{path}");

        for body in ["first", "replacement"] {
            assert_eq!(
                send(&ctx.app, Method::PUT, &uri, body).await.status(),
                StatusCode::CREATED
            );
        }

        let artifact = send(&ctx.app, Method::GET, &uri, "").await;
        assert_eq!(artifact.status(), StatusCode::OK);
        assert_eq!(body_bytes(artifact).await, "replacement");

        let checksum = send(&ctx.app, Method::GET, &format!("{uri}.sha256"), "").await;
        assert_eq!(checksum.status(), StatusCode::OK);
        assert_eq!(
            String::from_utf8_lossy(&body_bytes(checksum).await),
            checksum_hex("sha256", b"replacement").unwrap()
        );
    }

    #[tokio::test]
    async fn test_proxy_non_404_client_errors_propagate_and_never_negative_cache() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let artifact_path = "com/example/client-error/1.0/client-error-1.0.jar";
        let upstream_path = format!("/{artifact_path}");
        let metadata_path = "com/example/client-error/maven-metadata.xml";
        let upstream_metadata_path = format!("/{metadata_path}");
        let missing_metadata_path = "com/example/client-error-missing/maven-metadata.xml";
        let upstream_missing_metadata_path = format!("/{missing_metadata_path}");
        for status in [
            StatusCode::UNAUTHORIZED,
            StatusCode::FORBIDDEN,
            StatusCode::TOO_MANY_REQUESTS,
        ] {
            let upstream = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path(upstream_path.as_str()))
                .respond_with(ResponseTemplate::new(status.as_u16()))
                .mount(&upstream)
                .await;
            Mock::given(method("GET"))
                .and(path(upstream_metadata_path.as_str()))
                .respond_with(ResponseTemplate::new(status.as_u16()))
                .mount(&upstream)
                .await;
            Mock::given(method("GET"))
                .and(path(upstream_missing_metadata_path.as_str()))
                .respond_with(ResponseTemplate::new(status.as_u16()))
                .mount(&upstream)
                .await;
            let ctx = proxy_group_context(upstream.uri(), 60);
            assert_eq!(
                send(
                    &ctx.app,
                    Method::PUT,
                    &format!("/repository/hosted/{artifact_path}"),
                    "hosted-fallback-must-not-be-served",
                )
                .await
                .status(),
                StatusCode::CREATED
            );
            let metadata_key = repository_storage_key("proxy", metadata_path);
            ctx.state
                .storage
                .put(
                    &metadata_key,
                    br#"<metadata><groupId>com.example</groupId><artifactId>client-error</artifactId><versioning><versions><version>0.9</version></versions></versioning></metadata>"#,
                )
                .await
                .unwrap();

            let direct = send(
                &ctx.app,
                Method::GET,
                &format!("/repository/proxy/{artifact_path}"),
                "",
            )
            .await;
            assert_eq!(direct.status(), status);

            let group = send(
                &ctx.app,
                Method::GET,
                &format!("/repository/public/{artifact_path}"),
                "",
            )
            .await;
            assert_eq!(
                group.status(),
                status,
                "a group must not treat upstream {status} as a member miss"
            );

            let cache_key = repository_storage_key("proxy", artifact_path);
            assert!(
                !ctx.state
                    .maven_negative_cache
                    .lock()
                    .contains_key(&cache_key),
                "upstream {status} must not poison the Maven negative cache"
            );

            let stale = send(
                &ctx.app,
                Method::GET,
                &format!("/repository/proxy/{metadata_path}"),
                "",
            )
            .await;
            assert_eq!(
                stale.status(),
                status,
                "upstream {status} must propagate instead of serving stale metadata"
            );
            assert!(stale.headers().get("x-nora-stale").is_none());

            let grouped_metadata_without_fallback = send(
                &ctx.app,
                Method::GET,
                &format!("/repository/public/{missing_metadata_path}"),
                "",
            )
            .await;
            assert_eq!(
                grouped_metadata_without_fallback.status(),
                status,
                "a group must preserve upstream {status} when no member has metadata"
            );

            assert_eq!(
                send(
                    &ctx.app,
                    Method::PUT,
                    &format!("/repository/hosted/{metadata_path}"),
                    r#"<metadata><groupId>com.example</groupId><artifactId>client-error</artifactId><versioning><versions><version>1.0</version></versions></versioning></metadata>"#,
                )
                .await
                .status(),
                StatusCode::CREATED
            );
            let grouped_metadata = send(
                &ctx.app,
                Method::GET,
                &format!("/repository/public/{metadata_path}"),
                "",
            )
            .await;
            assert_eq!(
                grouped_metadata.status(),
                StatusCode::OK,
                "upstream {status} must not hide hosted group metadata"
            );
            assert!(String::from_utf8_lossy(&body_bytes(grouped_metadata).await).contains("1.0"));

            upstream.reset().await;
            Mock::given(method("GET"))
                .and(path(upstream_path.as_str()))
                .respond_with(ResponseTemplate::new(200).set_body_bytes("upstream"))
                .mount(&upstream)
                .await;
            let recovered = send(
                &ctx.app,
                Method::GET,
                &format!("/repository/proxy/{artifact_path}"),
                "",
            )
            .await;
            assert_eq!(recovered.status(), StatusCode::OK);
            assert_eq!(body_bytes(recovered).await, "upstream");
        }
    }

    #[tokio::test]
    async fn test_proxy_exact_404_negative_caches_and_group_falls_back() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let artifact_path = "com/example/missing/1.0/missing-1.0.jar";
        let upstream_path = format!("/{artifact_path}");
        let upstream = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(upstream_path.as_str()))
            .respond_with(ResponseTemplate::new(404))
            .mount(&upstream)
            .await;
        let ctx = proxy_group_context(upstream.uri(), 60);
        assert_eq!(
            send(
                &ctx.app,
                Method::PUT,
                &format!("/repository/hosted/{artifact_path}"),
                "hosted-fallback",
            )
            .await
            .status(),
            StatusCode::CREATED
        );

        let direct = send(
            &ctx.app,
            Method::GET,
            &format!("/repository/proxy/{artifact_path}"),
            "",
        )
        .await;
        assert_eq!(direct.status(), StatusCode::NOT_FOUND);

        let cache_key = repository_storage_key("proxy", artifact_path);
        assert!(ctx
            .state
            .maven_negative_cache
            .lock()
            .contains_key(&cache_key));

        upstream.reset().await;
        Mock::given(method("GET"))
            .and(path(upstream_path.as_str()))
            .respond_with(ResponseTemplate::new(200).set_body_bytes("late-upstream"))
            .mount(&upstream)
            .await;

        let group = send(
            &ctx.app,
            Method::GET,
            &format!("/repository/public/{artifact_path}"),
            "",
        )
        .await;
        assert_eq!(group.status(), StatusCode::OK);
        assert_eq!(body_bytes(group).await, "hosted-fallback");

        let still_cached = send(
            &ctx.app,
            Method::GET,
            &format!("/repository/proxy/{artifact_path}"),
            "",
        )
        .await;
        assert_eq!(still_cached.status(), StatusCode::NOT_FOUND);
        assert!(
            upstream.received_requests().await.unwrap().is_empty(),
            "the exact 404 must suppress upstream requests until its negative TTL expires"
        );
    }

    #[tokio::test]
    async fn test_proxy_policy_blocked_404_never_negative_caches() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let cases = [
            (
                "com/example/policy-blocked/1.0/policy-blocked-1.0.jar",
                "must-not-fall-through",
                "recovered-artifact",
            ),
            (
                "com/example/policy-blocked/maven-metadata.xml",
                "<metadata><groupId>com.example</groupId><artifactId>policy-blocked</artifactId><versioning><latest>1.0</latest><release>1.0</release><versions><version>1.0</version></versions><lastUpdated>20260731000000</lastUpdated></versioning></metadata>",
                "<metadata><groupId>com.example</groupId><artifactId>policy-blocked</artifactId><versioning><latest>2.0</latest><release>2.0</release><versions><version>2.0</version></versions><lastUpdated>20260731010000</lastUpdated></versioning></metadata>",
            ),
        ];
        let upstream = MockServer::start().await;
        for (artifact_path, _, _) in cases {
            Mock::given(method("GET"))
                .and(path(format!("/{artifact_path}")))
                .respond_with(ResponseTemplate::new(404).insert_header("x-amzn-waf-reason", "geo"))
                .mount(&upstream)
                .await;
        }
        let ctx = proxy_group_context(upstream.uri(), 60);

        for (artifact_path, hosted_body, _) in cases {
            let blocked = send(
                &ctx.app,
                Method::GET,
                &format!("/repository/proxy/{artifact_path}"),
                "",
            )
            .await;
            assert_eq!(blocked.status(), StatusCode::BAD_GATEWAY);

            assert_eq!(
                send(
                    &ctx.app,
                    Method::PUT,
                    &format!("/repository/hosted/{artifact_path}"),
                    hosted_body,
                )
                .await
                .status(),
                StatusCode::CREATED
            );
            let blocked_group = send(
                &ctx.app,
                Method::GET,
                &format!("/repository/public/{artifact_path}"),
                "",
            )
            .await;
            assert_eq!(blocked_group.status(), StatusCode::BAD_GATEWAY);

            let cache_key = repository_storage_key("proxy", artifact_path);
            assert!(
                !ctx.state
                    .maven_negative_cache
                    .lock()
                    .contains_key(&cache_key),
                "a policy/WAF 404 is an upstream failure, not a negative-cacheable miss"
            );
        }

        upstream.reset().await;
        for (artifact_path, _, recovered_body) in cases {
            Mock::given(method("GET"))
                .and(path(format!("/{artifact_path}")))
                .respond_with(
                    ResponseTemplate::new(200).set_body_bytes(recovered_body.as_bytes().to_vec()),
                )
                .mount(&upstream)
                .await;
        }
        for (artifact_path, _, recovered_body) in cases {
            let recovered = send(
                &ctx.app,
                Method::GET,
                &format!("/repository/proxy/{artifact_path}"),
                "",
            )
            .await;
            assert_eq!(recovered.status(), StatusCode::OK);
            let recovered = body_bytes(recovered).await;
            if artifact_path.ends_with("maven-metadata.xml") {
                let recovered = String::from_utf8_lossy(&recovered);
                assert!(recovered.contains("<version>2.0</version>"));
                assert!(recovered.contains("<lastUpdated>20260731010000</lastUpdated>"));
            } else {
                assert_eq!(recovered, recovered_body);
            }
        }
    }

    #[tokio::test]
    async fn test_named_repositories_isolate_same_path_and_group_preserves_order() {
        let ctx = named_repository_context();
        let path = "com/example/shared/1.0/shared-1.0.jar";

        for (repository, body) in [
            ("maven-releases", "main-bytes"),
            ("maven-open", "open-bytes"),
        ] {
            let response = send(
                &ctx.app,
                Method::PUT,
                &format!("/repository/{repository}/{path}"),
                body,
            )
            .await;
            assert_eq!(response.status(), StatusCode::CREATED);
        }

        let direct = send(
            &ctx.app,
            Method::GET,
            &format!("/repository/maven-releases/{path}"),
            "",
        )
        .await;
        assert_eq!(body_bytes(direct).await, "main-bytes");

        let redeploy = send(
            &ctx.app,
            Method::PUT,
            &format!("/repository/maven-releases/{path}"),
            "main-bytes-v2",
        )
        .await;
        assert_eq!(redeploy.status(), StatusCode::CREATED);
        let direct = send(
            &ctx.app,
            Method::GET,
            &format!("/repository/maven-releases/{path}"),
            "",
        )
        .await;
        assert_eq!(body_bytes(direct).await, "main-bytes-v2");

        let grouped = send(
            &ctx.app,
            Method::GET,
            &format!("/repository/maven-public/{path}"),
            "",
        )
        .await;
        assert_eq!(body_bytes(grouped).await, "open-bytes");

        let legacy = send(&ctx.app, Method::GET, &format!("/maven2/{path}"), "").await;
        assert_eq!(body_bytes(legacy).await, "open-bytes");
    }

    #[tokio::test]
    async fn test_named_repository_version_policies() {
        let ctx = named_repository_context();

        let release_to_snapshots = send(
            &ctx.app,
            Method::PUT,
            "/repository/maven-snapshots/com/example/lib/1.0/lib-1.0.jar",
            "release",
        )
        .await;
        assert_eq!(release_to_snapshots.status(), StatusCode::BAD_REQUEST);

        let snapshot_to_releases = send(
            &ctx.app,
            Method::PUT,
            "/repository/maven-releases/com/example/lib/1.0-SNAPSHOT/lib-1.0-SNAPSHOT.jar",
            "snapshot",
        )
        .await;
        assert_eq!(snapshot_to_releases.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_group_merges_artifact_metadata_and_checksum() {
        let ctx = named_repository_context();
        for (repository, version) in [
            ("maven-releases", "1.0"),
            ("maven-snapshots", "2.0-SNAPSHOT"),
        ] {
            let response = send(
                &ctx.app,
                Method::PUT,
                &format!("/repository/{repository}/com/example/lib/{version}/lib-{version}.jar"),
                Body::from(version.to_string()),
            )
            .await;
            assert_eq!(response.status(), StatusCode::CREATED);
        }

        let metadata = send(
            &ctx.app,
            Method::GET,
            "/repository/maven-public/com/example/lib/maven-metadata.xml",
            "",
        )
        .await;
        let metadata = body_bytes(metadata).await;
        let metadata_text = String::from_utf8(metadata.to_vec()).unwrap();
        assert!(metadata_text.contains("<version>1.0</version>"));
        assert!(metadata_text.contains("<version>2.0-SNAPSHOT</version>"));

        let checksum = send(
            &ctx.app,
            Method::GET,
            "/repository/maven-public/com/example/lib/maven-metadata.xml.sha1",
            "",
        )
        .await;
        let checksum = body_bytes(checksum).await;
        assert_eq!(
            String::from_utf8(checksum.to_vec()).unwrap(),
            hex::encode(sha1::Sha1::digest(&metadata))
        );
    }

    #[tokio::test]
    async fn test_group_artifact_metadata_uses_highest_member_release() {
        let ctx = named_repository_context();
        for (repository, version) in [
            ("maven-releases", "2.0"),
            ("maven-releases", "1.0"),
            ("maven-open", "1.5"),
        ] {
            let response = send(
                &ctx.app,
                Method::PUT,
                &format!(
                    "/repository/{repository}/com/example/group-order/{version}/group-order-{version}.jar"
                ),
                Body::from(version.to_string()),
            )
            .await;
            assert_eq!(response.status(), StatusCode::CREATED);
        }

        let metadata = body_bytes(
            send(
                &ctx.app,
                Method::GET,
                "/repository/maven-public/com/example/group-order/maven-metadata.xml",
                "",
            )
            .await,
        )
        .await;
        let metadata = String::from_utf8_lossy(&metadata);
        assert!(metadata.contains("<release>1.5</release>"));
        assert!(metadata.contains("<latest>1.5</latest>"));
        for version in ["1.0", "1.5", "2.0"] {
            assert!(metadata.contains(&format!("<version>{version}</version>")));
        }
    }

    #[tokio::test]
    async fn test_group_merges_version_metadata_and_checksum() {
        let ctx = create_test_context_with_config(|config| {
            config.maven.repositories = vec![
                MavenRepository::Hosted {
                    name: "snapshots-a".to_string(),
                    version_policy: MavenVersionPolicy::Snapshot,
                    write_policy: MavenWritePolicy::Allow,
                },
                MavenRepository::Hosted {
                    name: "snapshots-b".to_string(),
                    version_policy: MavenVersionPolicy::Snapshot,
                    write_policy: MavenWritePolicy::Allow,
                },
                MavenRepository::Group {
                    name: "snapshots".to_string(),
                    members: vec!["snapshots-a".to_string(), "snapshots-b".to_string()],
                },
            ];
        });
        let older = r#"<metadata><groupId>com.example</groupId><artifactId>library</artifactId><version>1.0-SNAPSHOT</version><versioning><snapshot><timestamp>20260730.010000</timestamp><buildNumber>1</buildNumber></snapshot><lastUpdated>20260730010000</lastUpdated><snapshotVersions><snapshotVersion><extension>jar</extension><value>1.0-20260730.010000-1</value><updated>20260730010000</updated></snapshotVersion><snapshotVersion><classifier>sources</classifier><extension>jar</extension><value>1.0-20260730.010000-1</value><updated>20260730010000</updated></snapshotVersion></snapshotVersions></versioning></metadata>"#;
        let newer = r#"<metadata><groupId>com.example</groupId><artifactId>library</artifactId><version>1.0-SNAPSHOT</version><versioning><snapshot><timestamp>20260730.020000</timestamp><buildNumber>2</buildNumber></snapshot><lastUpdated>20260730020000</lastUpdated><snapshotVersions><snapshotVersion><extension>jar</extension><value>1.0-20260730.020000-2</value><updated>20260730020000</updated></snapshotVersion></snapshotVersions></versioning></metadata>"#;
        let path = "com/example/library/1.0-SNAPSHOT/maven-metadata.xml";
        for (repository, body) in [("snapshots-a", older), ("snapshots-b", newer)] {
            let response = send(
                &ctx.app,
                Method::PUT,
                &format!("/repository/{repository}/{path}"),
                Body::from(body),
            )
            .await;
            assert_eq!(response.status(), StatusCode::CREATED);
        }

        let merged = body_bytes(
            send(
                &ctx.app,
                Method::GET,
                &format!("/repository/snapshots/{path}"),
                "",
            )
            .await,
        )
        .await;
        let merged_text = String::from_utf8_lossy(&merged);
        assert!(merged_text.contains("<timestamp>20260730.020000</timestamp>"));
        assert_eq!(merged_text.matches("1.0-20260730.010000-1").count(), 1);
        assert_eq!(merged_text.matches("1.0-20260730.020000-2").count(), 1);

        let checksum = body_bytes(
            send(
                &ctx.app,
                Method::GET,
                &format!("/repository/snapshots/{path}.sha512"),
                "",
            )
            .await,
        )
        .await;
        assert_eq!(
            String::from_utf8_lossy(&checksum),
            checksum_hex("sha512", &merged).unwrap()
        );
    }

    #[tokio::test]
    async fn test_group_internal_metadata_uses_hosted_and_never_queries_proxy() {
        use wiremock::MockServer;

        let upstream = MockServer::start().await;
        let upstream_url = upstream.uri();
        let ctx = create_test_context_with_config(move |config| {
            config.curation.internal_namespaces = vec!["com.internal.**".to_string()];
            config.maven.repositories = vec![
                MavenRepository::Proxy {
                    name: "central".to_string(),
                    url: upstream_url,
                    auth: None,
                    version_policy: MavenVersionPolicy::Mixed,
                    metadata_ttl: Some(0),
                    negative_ttl: 60,
                },
                MavenRepository::Hosted {
                    name: "snapshots".to_string(),
                    version_policy: MavenVersionPolicy::Snapshot,
                    write_policy: MavenWritePolicy::Allow,
                },
                MavenRepository::Group {
                    name: "public".to_string(),
                    // Proxy first makes the regression independent of member order.
                    members: vec!["central".to_string(), "snapshots".to_string()],
                },
            ];
        });
        let path = "com/internal/library/1.0-SNAPSHOT/maven-metadata.xml";
        let metadata = r#"<metadata><groupId>com.internal</groupId><artifactId>library</artifactId><version>1.0-SNAPSHOT</version><versioning><snapshot><timestamp>20260809.010203</timestamp><buildNumber>4</buildNumber></snapshot><lastUpdated>20260809010203</lastUpdated><snapshotVersions><snapshotVersion><extension>jar</extension><value>1.0-20260809.010203-4</value><updated>20260809010203</updated></snapshotVersion></snapshotVersions></versioning></metadata>"#;
        assert_eq!(
            send(
                &ctx.app,
                Method::PUT,
                &format!("/repository/snapshots/{path}"),
                metadata,
            )
            .await
            .status(),
            StatusCode::CREATED
        );

        let grouped = send(
            &ctx.app,
            Method::GET,
            &format!("/repository/public/{path}"),
            "",
        )
        .await;
        assert_eq!(grouped.status(), StatusCode::OK);
        let grouped = body_bytes(grouped).await;
        assert!(String::from_utf8_lossy(&grouped).contains("1.0-20260809.010203-4"));

        let checksum = send(
            &ctx.app,
            Method::GET,
            &format!("/repository/public/{path}.sha256"),
            "",
        )
        .await;
        assert_eq!(checksum.status(), StatusCode::OK);
        assert_eq!(
            String::from_utf8_lossy(&body_bytes(checksum).await),
            checksum_hex("sha256", &grouped).unwrap()
        );

        let missing = send(
            &ctx.app,
            Method::GET,
            "/repository/public/com/internal/missing/1.0-SNAPSHOT/maven-metadata.xml",
            "",
        )
        .await;
        assert_eq!(missing.status(), StatusCode::FORBIDDEN);
        assert!(
            upstream.received_requests().await.unwrap().is_empty(),
            "internal group metadata must never be requested from an external proxy"
        );
    }

    #[tokio::test]
    async fn test_group_merges_plugin_metadata() {
        let ctx = named_repository_context();
        for (repository, name, prefix, artifact_id) in [
            ("maven-open", "Open Plugin", "open", "open-maven-plugin"),
            ("maven-releases", "Main Plugin", "main", "main-maven-plugin"),
        ] {
            let metadata = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<metadata><plugins><plugin><name>{name}</name><prefix>{prefix}</prefix><artifactId>{artifact_id}</artifactId></plugin></plugins></metadata>"
            );
            let response = send(
                &ctx.app,
                Method::PUT,
                &format!("/repository/{repository}/org/example/plugins/maven-metadata.xml"),
                metadata,
            )
            .await;
            assert_eq!(response.status(), StatusCode::CREATED);
        }

        let response = send(
            &ctx.app,
            Method::GET,
            "/repository/maven-public/org/example/plugins/maven-metadata.xml",
            "",
        )
        .await;
        let metadata = String::from_utf8(body_bytes(response).await.to_vec()).unwrap();
        assert!(metadata.contains("<prefix>open</prefix>"));
        assert!(metadata.contains("<prefix>main</prefix>"));
    }

    #[tokio::test]
    async fn test_combined_artifact_and_plugin_metadata_survives_hosted_and_group_merge() {
        let ctx = create_test_context_with_config(|config| {
            config.maven.repositories = vec![
                MavenRepository::Hosted {
                    name: "first".to_string(),
                    version_policy: MavenVersionPolicy::Mixed,
                    write_policy: MavenWritePolicy::Allow,
                },
                MavenRepository::Hosted {
                    name: "second".to_string(),
                    version_policy: MavenVersionPolicy::Mixed,
                    write_policy: MavenWritePolicy::Allow,
                },
                MavenRepository::Group {
                    name: "public".to_string(),
                    members: vec!["first".to_string(), "second".to_string()],
                },
            ];
        });
        let path = "org/example/plugins/maven-metadata.xml";
        assert_eq!(
            send(
                &ctx.app,
                Method::PUT,
                "/repository/first/org/example/plugins/1.0/plugins-1.0.jar",
                "artifact",
            )
            .await
            .status(),
            StatusCode::CREATED
        );

        let combined = r#"<metadata>
  <groupId>org.example</groupId>
  <artifactId>plugins</artifactId>
  <versioning>
    <latest>1.0</latest>
    <release>1.0</release>
    <versions><version>1.0</version></versions>
    <lastUpdated>20260730010101</lastUpdated>
  </versioning>
  <plugins>
    <plugin><name>First</name><prefix>collision</prefix><artifactId>first-plugin</artifactId></plugin>
    <plugin><name>First only</name><prefix>first</prefix><artifactId>first-only-plugin</artifactId></plugin>
  </plugins>
</metadata>"#;
        assert_eq!(
            send(
                &ctx.app,
                Method::PUT,
                &format!("/repository/first/{path}"),
                combined,
            )
            .await
            .status(),
            StatusCode::CREATED
        );

        let second_plugins = r#"<metadata><plugins>
  <plugin><name>Second collision</name><prefix>collision</prefix><artifactId>second-plugin</artifactId></plugin>
  <plugin><name>Second only</name><prefix>second</prefix><artifactId>second-only-plugin</artifactId></plugin>
</plugins></metadata>"#;
        assert_eq!(
            send(
                &ctx.app,
                Method::PUT,
                &format!("/repository/second/{path}"),
                second_plugins,
            )
            .await
            .status(),
            StatusCode::CREATED
        );

        let hosted = body_bytes(
            send(
                &ctx.app,
                Method::GET,
                &format!("/repository/first/{path}"),
                "",
            )
            .await,
        )
        .await;
        let hosted = String::from_utf8_lossy(&hosted);
        assert!(hosted.contains("<version>1.0</version>"));
        assert!(hosted.contains("<latest>1.0</latest>"));
        assert!(hosted.contains("<prefix>collision</prefix>"));

        let grouped = body_bytes(
            send(
                &ctx.app,
                Method::GET,
                &format!("/repository/public/{path}"),
                "",
            )
            .await,
        )
        .await;
        let grouped_text = String::from_utf8_lossy(&grouped);
        assert!(grouped_text.contains("<version>1.0</version>"));
        assert!(grouped_text.contains("<latest>1.0</latest>"));
        assert_eq!(
            grouped_text.matches("<prefix>collision</prefix>").count(),
            1
        );
        assert!(grouped_text.contains("<artifactId>first-plugin</artifactId>"));
        assert!(!grouped_text.contains("<artifactId>second-plugin</artifactId>"));
        assert!(grouped_text.contains("<artifactId>second-only-plugin</artifactId>"));

        let checksum = body_bytes(
            send(
                &ctx.app,
                Method::GET,
                &format!("/repository/public/{path}.sha256"),
                "",
            )
            .await,
        )
        .await;
        assert_eq!(
            String::from_utf8_lossy(&checksum),
            checksum_hex("sha256", &grouped).unwrap()
        );
    }

    #[tokio::test]
    async fn test_retention_metadata_helper_preserves_plugins_and_deletes_empty_document() {
        let ctx = create_test_context();
        for version in ["1.0", "2.0"] {
            assert_eq!(
                send(
                    &ctx.app,
                    Method::PUT,
                    &format!("/maven2/org/example/retain/{version}/retain-{version}.jar"),
                    version,
                )
                .await
                .status(),
                StatusCode::CREATED
            );
        }
        let plugins = r#"<metadata><plugins><plugin><name>Retained</name><prefix>retained</prefix><artifactId>retained-plugin</artifactId></plugin></plugins></metadata>"#;
        assert_eq!(
            send(
                &ctx.app,
                Method::PUT,
                "/maven2/org/example/retain/maven-metadata.xml",
                plugins,
            )
            .await
            .status(),
            StatusCode::CREATED
        );

        assert_eq!(
            super::update_hosted_metadata_after_retention(
                &ctx.state.storage,
                "maven/",
                "org/example",
                "retain",
                "1.0",
            )
            .await
            .unwrap(),
            (0, 0)
        );
        let metadata_key = "maven/org/example/retain/maven-metadata.xml";
        let metadata = ctx.state.storage.get(metadata_key).await.unwrap();
        let metadata_text = String::from_utf8_lossy(&metadata);
        assert!(!metadata_text.contains("<version>1.0</version>"));
        assert!(metadata_text.contains("<version>2.0</version>"));
        assert!(metadata_text.contains("<prefix>retained</prefix>"));
        assert_eq!(
            String::from_utf8_lossy(
                &ctx.state
                    .storage
                    .get(&format!("{metadata_key}.sha256"))
                    .await
                    .unwrap()
            ),
            checksum_hex("sha256", &metadata).unwrap()
        );

        for key in ctx
            .state
            .storage
            .list("maven/org/example/retain/1.0/")
            .await
            .unwrap()
        {
            ctx.state.storage.delete(&key).await.unwrap();
        }
        assert_eq!(
            super::update_hosted_metadata_after_retention(
                &ctx.state.storage,
                "maven/",
                "org/example",
                "retain",
                "2.0",
            )
            .await
            .unwrap(),
            (0, 0)
        );
        let plugin_only = ctx.state.storage.get(metadata_key).await.unwrap();
        let plugin_only_text = String::from_utf8_lossy(&plugin_only);
        assert!(!plugin_only_text.contains("<versioning>"));
        assert!(plugin_only_text.contains("<prefix>retained</prefix>"));
        assert_eq!(
            String::from_utf8_lossy(
                &ctx.state
                    .storage
                    .get(&format!("{metadata_key}.sha1"))
                    .await
                    .unwrap()
            ),
            checksum_hex("sha1", &plugin_only).unwrap()
        );

        assert_eq!(
            send(
                &ctx.app,
                Method::PUT,
                "/maven2/org/example/empty/1.0/empty-1.0.jar",
                "empty",
            )
            .await
            .status(),
            StatusCode::CREATED
        );
        let removed = super::update_hosted_metadata_after_retention(
            &ctx.state.storage,
            "maven/",
            "org/example",
            "empty",
            "1.0",
        )
        .await
        .unwrap();
        assert_eq!(removed.0, 5);
        assert!(removed.1 > 0);
        let empty_metadata_key = "maven/org/example/empty/maven-metadata.xml";
        for suffix in ["", ".md5", ".sha1", ".sha256", ".sha512"] {
            assert!(ctx
                .state
                .storage
                .get(&format!("{empty_metadata_key}{suffix}"))
                .await
                .is_err());
        }
    }

    #[tokio::test]
    async fn test_group_keeps_proxy_first_match_and_merges_hosted_metadata() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let upstream = MockServer::start().await;
        let metadata = r#"<?xml version="1.0" encoding="UTF-8"?>
<metadata>
  <groupId>com.example</groupId>
  <artifactId>collision</artifactId>
  <versioning>
    <latest>1.0</latest>
    <release>1.0</release>
    <versions><version>1.0</version></versions>
    <lastUpdated>20260728010000</lastUpdated>
  </versioning>
</metadata>
"#;
        Mock::given(method("GET"))
            .and(path("/com/example/collision/1.0/collision-1.0.jar"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes("public"))
            .mount(&upstream)
            .await;
        Mock::given(method("GET"))
            .and(path("/com/example/collision/maven-metadata.xml"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(metadata, "application/xml"))
            .mount(&upstream)
            .await;

        let upstream_url = upstream.uri();
        let ctx = create_test_context_with_config(move |config| {
            config.maven.repositories = vec![
                MavenRepository::Proxy {
                    name: "central".to_string(),
                    url: upstream_url,
                    auth: None,
                    version_policy: MavenVersionPolicy::Release,
                    metadata_ttl: Some(0),
                    negative_ttl: 0,
                },
                MavenRepository::Hosted {
                    name: "releases".to_string(),
                    version_policy: MavenVersionPolicy::Release,
                    write_policy: MavenWritePolicy::Allow,
                },
                MavenRepository::Group {
                    name: "public".to_string(),
                    members: vec!["central".to_string(), "releases".to_string()],
                },
            ];
        });

        for (version, body) in [("1.0", "hosted-same-path"), ("9.0", "hosted-new")] {
            let response = send(
                &ctx.app,
                Method::PUT,
                &format!(
                    "/repository/releases/com/example/collision/{version}/collision-{version}.jar"
                ),
                body,
            )
            .await;
            assert_eq!(response.status(), StatusCode::CREATED);
        }

        let artifact = send(
            &ctx.app,
            Method::GET,
            "/repository/public/com/example/collision/1.0/collision-1.0.jar",
            "",
        )
        .await;
        assert_eq!(body_bytes(artifact).await, "public");

        let metadata = send(
            &ctx.app,
            Method::GET,
            "/repository/public/com/example/collision/maven-metadata.xml",
            "",
        )
        .await;
        let metadata = String::from_utf8(body_bytes(metadata).await.to_vec()).unwrap();
        assert!(metadata.contains("<version>1.0</version>"));
        assert!(metadata.contains("<version>9.0</version>"));
    }

    #[tokio::test]
    async fn test_group_continues_after_unavailable_proxy_member() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let upstream = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/com/example/failover/1.0/failover-1.0.jar"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&upstream)
            .await;
        let upstream_url = upstream.uri();
        let ctx = create_test_context_with_config(move |config| {
            config.maven.repositories = vec![
                MavenRepository::Proxy {
                    name: "unavailable".to_string(),
                    url: upstream_url,
                    auth: None,
                    version_policy: MavenVersionPolicy::Release,
                    metadata_ttl: Some(0),
                    negative_ttl: 0,
                },
                MavenRepository::Hosted {
                    name: "hosted".to_string(),
                    version_policy: MavenVersionPolicy::Release,
                    write_policy: MavenWritePolicy::Allow,
                },
                MavenRepository::Group {
                    name: "public".to_string(),
                    members: vec!["unavailable".to_string(), "hosted".to_string()],
                },
            ];
        });
        let path = "com/example/failover/1.0/failover-1.0.jar";
        assert_eq!(
            send(
                &ctx.app,
                Method::PUT,
                &format!("/repository/hosted/{path}"),
                "hosted",
            )
            .await
            .status(),
            StatusCode::CREATED
        );
        let response = send(
            &ctx.app,
            Method::GET,
            &format!("/repository/public/{path}"),
            "",
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(body_bytes(response).await, "hosted");
    }

    #[tokio::test]
    async fn test_group_stops_on_hosted_storage_error_before_proxy() {
        use crate::storage::{
            FileMeta, Result as StorageResult, Storage, StorageBackend, StorageError,
        };
        use axum::body::Bytes;
        use std::path::Path;
        use std::pin::Pin;
        use std::sync::Arc;
        use tokio::io::AsyncRead;
        use wiremock::MockServer;

        struct FailingStatBackend;

        #[async_trait::async_trait]
        impl StorageBackend for FailingStatBackend {
            async fn put(&self, _key: &str, _data: &[u8]) -> StorageResult<()> {
                Ok(())
            }

            async fn get(&self, _key: &str) -> StorageResult<Bytes> {
                Err(StorageError::Io(std::io::Error::other(
                    "injected hosted read failure",
                )))
            }

            async fn delete(&self, _key: &str) -> StorageResult<()> {
                Ok(())
            }

            async fn list(&self, _prefix: &str) -> StorageResult<Vec<String>> {
                Ok(Vec::new())
            }

            async fn stat(&self, _key: &str) -> StorageResult<Option<FileMeta>> {
                Err(StorageError::Network(
                    "injected hosted stat failure".to_string(),
                ))
            }

            async fn health_check(&self) -> bool {
                true
            }

            fn backend_name(&self) -> &'static str {
                "failing-maven-stat-test"
            }

            async fn put_from_path(&self, _key: &str, _src: &Path) -> StorageResult<()> {
                Ok(())
            }

            async fn get_reader(
                &self,
                _key: &str,
            ) -> StorageResult<(u64, Pin<Box<dyn AsyncRead + Send + Unpin>>)> {
                Err(StorageError::Io(std::io::Error::other(
                    "injected hosted read failure",
                )))
            }
        }

        let upstream = MockServer::start().await;
        let ctx = create_test_context();
        let mut config = (*ctx.state.config).clone();
        config.maven.repositories = vec![
            MavenRepository::Hosted {
                name: "hosted".to_string(),
                version_policy: MavenVersionPolicy::Release,
                write_policy: MavenWritePolicy::AllowOnce,
            },
            MavenRepository::Proxy {
                name: "proxy".to_string(),
                url: upstream.uri(),
                auth: None,
                version_policy: MavenVersionPolicy::Release,
                metadata_ttl: None,
                negative_ttl: 60,
            },
            MavenRepository::Group {
                name: "public".to_string(),
                members: vec!["hosted".to_string(), "proxy".to_string()],
            },
        ];
        let mut state = ctx.state.clone();
        state.config = Arc::new(config);
        state.storage = Storage::from_backend(Arc::new(FailingStatBackend));

        let members = ["hosted".to_string(), "proxy".to_string()];
        let response = super::download_group(
            state,
            axum::http::HeaderMap::new(),
            &members,
            "com/example/fail/1.0/fail-1.0.jar".to_string(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        assert!(
            upstream.received_requests().await.unwrap().is_empty(),
            "a hosted storage error must stop the group before proxy fallback"
        );
    }

    #[tokio::test]
    async fn test_proxy_metadata_list_failure_does_not_cache_partial_document() {
        use std::sync::Arc;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let upstream = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/com/example/fault/maven-metadata.xml"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(
                r#"<metadata><groupId>com.example</groupId><artifactId>fault</artifactId><versioning><versions><version>1.0</version></versions></versioning></metadata>"#,
                "application/xml",
            ))
            .mount(&upstream)
            .await;

        let ctx = create_test_context();
        let mut config = (*ctx.state.config).clone();
        config.maven.repositories = vec![
            MavenRepository::Proxy {
                name: "proxy".to_string(),
                url: upstream.uri(),
                auth: None,
                version_policy: MavenVersionPolicy::Release,
                metadata_ttl: Some(0),
                negative_ttl: 0,
            },
            MavenRepository::Group {
                name: "public".to_string(),
                members: vec!["proxy".to_string()],
            },
        ];
        let prefix = "maven/repositories/proxy/com/example/fault/";
        let backend = crate::test_helpers::FaultInjectBackend::new(ctx.state.storage.clone())
            .fail_list(prefix);
        let writes = backend.write_attempts();
        let mut state = ctx.state.clone();
        state.config = Arc::new(config);
        state.storage = crate::storage::Storage::from_backend(Arc::new(backend));

        let response = super::download_group(
            state,
            axum::http::HeaderMap::new(),
            &["proxy".to_string()],
            "com/example/fault/maven-metadata.xml".to_string(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        assert!(
            writes.lock().is_empty(),
            "metadata derived from an incomplete cached-version set must not be written"
        );
    }

    fn proxy_metadata_xml(versions: &[&str], last_updated: &str) -> String {
        let latest = versions.last().expect("at least one version");
        let version_elements = versions
            .iter()
            .map(|version| format!("      <version>{version}</version>"))
            .collect::<Vec<_>>()
            .join("\n");
        format!(
            r#"<metadata>
  <groupId>com.example</groupId>
  <artifactId>cached</artifactId>
  <versioning>
    <latest>{latest}</latest>
    <release>{latest}</release>
    <versions>
{version_elements}
    </versions>
    <lastUpdated>{last_updated}</lastUpdated>
  </versioning>
</metadata>"#
        )
    }

    fn direct_proxy_repository(metadata_ttl: i64) -> DirectRepository {
        DirectRepository {
            name: Some("proxy".to_string()),
            proxies: Vec::new(),
            metadata_ttl,
            negative_ttl: 0,
            version_policy: MavenVersionPolicy::Release,
            write_policy: MavenWritePolicy::Deny,
        }
    }

    #[tokio::test]
    async fn test_identical_proxy_metadata_revalidation_skips_all_writes() {
        use crate::storage::Storage;
        use crate::test_helpers::{create_test_context, FaultInjectBackend};
        use std::sync::Arc;

        let ctx = create_test_context();
        let backend = FaultInjectBackend::new(ctx.state.storage.clone());
        let writes = backend.write_attempts();
        let mut state = ctx.state.clone();
        state.storage = Storage::from_backend(Arc::new(backend));
        let upstream = proxy_metadata_xml(&["1.0"], "20260810070000");
        let repository = direct_proxy_repository(0);
        let key = "maven/repositories/proxy/com/example/cached/maven-metadata.xml";

        let first = merge_and_cache_proxy_metadata(
            &state,
            &repository,
            "com/example",
            "cached",
            "com/example/cached/maven-metadata.xml",
            upstream.as_bytes(),
        )
        .await
        .expect("first metadata merge");

        assert_eq!(
            writes.lock().as_slice(),
            [
                format!("put:{key}"),
                format!("put:{key}.md5"),
                format!("put:{key}.sha1"),
                format!("put:{key}.sha256"),
                format!("put:{key}.sha512"),
            ]
        );
        writes.lock().clear();

        let second = merge_and_cache_proxy_metadata(
            &state,
            &repository,
            "com/example",
            "cached",
            "com/example/cached/maven-metadata.xml",
            upstream.as_bytes(),
        )
        .await
        .expect("identical metadata revalidation");

        assert_eq!(second, first);
        assert!(
            writes.lock().is_empty(),
            "byte-identical metadata must not rewrite the document or sidecars"
        );
    }

    #[tokio::test]
    async fn test_changed_proxy_metadata_revalidation_rewrites_document_and_all_sidecars() {
        use crate::storage::Storage;
        use crate::test_helpers::{create_test_context, FaultInjectBackend};
        use std::sync::Arc;

        let ctx = create_test_context();
        let backend = FaultInjectBackend::new(ctx.state.storage.clone());
        let writes = backend.write_attempts();
        let mut state = ctx.state.clone();
        state.storage = Storage::from_backend(Arc::new(backend));
        let repository = direct_proxy_repository(0);
        let key = "maven/repositories/proxy/com/example/cached/maven-metadata.xml";
        let initial = proxy_metadata_xml(&["1.0"], "20260810070000");
        merge_and_cache_proxy_metadata(
            &state,
            &repository,
            "com/example",
            "cached",
            "com/example/cached/maven-metadata.xml",
            initial.as_bytes(),
        )
        .await
        .expect("initial metadata merge");
        writes.lock().clear();

        let changed = proxy_metadata_xml(&["1.0", "2.0"], "20260810070100");
        let merged = merge_and_cache_proxy_metadata(
            &state,
            &repository,
            "com/example",
            "cached",
            "com/example/cached/maven-metadata.xml",
            changed.as_bytes(),
        )
        .await
        .expect("changed metadata revalidation");

        assert_eq!(
            writes.lock().as_slice(),
            [
                format!("put:{key}"),
                format!("put:{key}.md5"),
                format!("put:{key}.sha1"),
                format!("put:{key}.sha256"),
                format!("put:{key}.sha512"),
            ]
        );
        assert_eq!(state.storage.get(key).await.unwrap(), merged);
        for suffix in ["md5", "sha1", "sha256", "sha512"] {
            assert_eq!(
                state.storage.get(&format!("{key}.{suffix}")).await.unwrap(),
                Bytes::from(checksum_hex(suffix, &merged).unwrap())
            );
        }
    }

    #[tokio::test]
    async fn test_proxy_metadata_cached_read_failure_is_fail_closed_without_writes() {
        use crate::storage::Storage;
        use crate::test_helpers::{create_test_context, FaultInjectBackend};
        use std::sync::Arc;

        let ctx = create_test_context();
        let key = "maven/repositories/proxy/com/example/cached/maven-metadata.xml";
        let original = proxy_metadata_xml(&["1.0"], "20260810070000");
        ctx.state
            .storage
            .put(key, original.as_bytes())
            .await
            .unwrap();
        let backend = FaultInjectBackend::new(ctx.state.storage.clone()).fail_get_times(key, 1);
        let writes = backend.write_attempts();
        let mut state = ctx.state.clone();
        state.storage = Storage::from_backend(Arc::new(backend));
        let repository = direct_proxy_repository(0);

        let result = merge_and_cache_proxy_metadata(
            &state,
            &repository,
            "com/example",
            "cached",
            "com/example/cached/maven-metadata.xml",
            original.as_bytes(),
        )
        .await;

        assert!(matches!(result, Err(StorageError::Network(_))));
        assert!(
            writes.lock().is_empty(),
            "an unavailable cached read must not be treated as absence"
        );
        assert_eq!(ctx.state.storage.get(key).await.unwrap(), original);
    }

    #[tokio::test]
    async fn test_identical_proxy_metadata_revalidation_renews_positive_ttl_without_writes() {
        use crate::test_helpers::FaultInjectBackend;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let upstream = MockServer::start().await;
        let metadata_path = "/com/example/cached/maven-metadata.xml";
        let upstream_metadata = proxy_metadata_xml(&["1.0"], "20260810070000");
        Mock::given(method("GET"))
            .and(path(metadata_path))
            .respond_with(
                ResponseTemplate::new(200).set_body_raw(upstream_metadata, "application/xml"),
            )
            .mount(&upstream)
            .await;
        let upstream_url = upstream.uri();
        let ctx = create_test_context_with_config(move |config| {
            config.maven.repositories = vec![MavenRepository::Proxy {
                name: "central".to_string(),
                url: upstream_url,
                auth: None,
                version_policy: MavenVersionPolicy::Release,
                metadata_ttl: Some(1),
                negative_ttl: 0,
            }];
            config.maven.default_repository = Some("central".to_string());
        });
        let backend = FaultInjectBackend::new(ctx.state.storage.clone());
        let writes = backend.write_attempts();
        let mut state = ctx.state.clone();
        state.storage = Storage::from_backend(Arc::new(backend));
        let app = super::routes().with_state(state.clone());
        let request_path = "/maven2/com/example/cached/maven-metadata.xml";
        let key = "maven/repositories/central/com/example/cached/maven-metadata.xml";

        let first = send(&app, Method::GET, request_path, "").await;
        assert_eq!(first.status(), StatusCode::OK);
        let first_body = body_bytes(first).await;
        assert_eq!(upstream.received_requests().await.unwrap().len(), 1);
        assert_eq!(writes.lock().len(), 5);
        writes.lock().clear();

        tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
        let revalidated = send(&app, Method::GET, request_path, "").await;
        assert_eq!(revalidated.status(), StatusCode::OK);
        assert_eq!(body_bytes(revalidated).await, first_body);
        assert_eq!(upstream.received_requests().await.unwrap().len(), 2);
        assert!(
            writes.lock().is_empty(),
            "successful byte-identical revalidation must not touch the S3 bundle"
        );
        assert!(state.maven_revalidation_cache.lock().contains_key(key));

        let fresh = send(&app, Method::GET, request_path, "").await;
        assert_eq!(fresh.status(), StatusCode::OK);
        assert_eq!(body_bytes(fresh).await, first_body);
        assert_eq!(
            upstream.received_requests().await.unwrap().len(),
            2,
            "a successful validation must start a new positive TTL window"
        );
        assert!(writes.lock().is_empty());
    }

    #[tokio::test]
    async fn test_proxy_and_group_artifact_metadata_respect_version_policy() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let upstream = MockServer::start().await;
        let metadata_path = "/com/example/policy/maven-metadata.xml";
        let upstream_metadata = r#"<metadata>
  <groupId>com.example</groupId>
  <artifactId>policy</artifactId>
  <versioning>
    <latest>2.0-SNAPSHOT</latest>
    <release>1.0</release>
    <versions>
      <version>1.0</version>
      <version>2.0-SNAPSHOT</version>
    </versions>
    <lastUpdated>20260730020202</lastUpdated>
  </versioning>
</metadata>"#;
        Mock::given(method("GET"))
            .and(path(metadata_path))
            .respond_with(
                ResponseTemplate::new(200).set_body_raw(upstream_metadata, "application/xml"),
            )
            .mount(&upstream)
            .await;
        let upstream_url = upstream.uri();
        let ctx = create_test_context_with_config(move |config| {
            config.maven.repositories = vec![
                MavenRepository::Proxy {
                    name: "releases".to_string(),
                    url: upstream_url.clone(),
                    auth: None,
                    version_policy: MavenVersionPolicy::Release,
                    metadata_ttl: Some(0),
                    negative_ttl: 0,
                },
                MavenRepository::Proxy {
                    name: "snapshots".to_string(),
                    url: upstream_url,
                    auth: None,
                    version_policy: MavenVersionPolicy::Snapshot,
                    metadata_ttl: Some(0),
                    negative_ttl: 0,
                },
                MavenRepository::Group {
                    name: "public".to_string(),
                    members: vec!["releases".to_string(), "snapshots".to_string()],
                },
            ];
        });

        let release = body_bytes(
            send(
                &ctx.app,
                Method::GET,
                "/repository/releases/com/example/policy/maven-metadata.xml",
                "",
            )
            .await,
        )
        .await;
        let release_text = String::from_utf8_lossy(&release);
        assert!(release_text.contains("<version>1.0</version>"));
        assert!(!release_text.contains("<version>2.0-SNAPSHOT</version>"));
        assert!(release_text.contains("<latest>1.0</latest>"));
        assert!(release_text.contains("<release>1.0</release>"));

        let snapshot = body_bytes(
            send(
                &ctx.app,
                Method::GET,
                "/repository/snapshots/com/example/policy/maven-metadata.xml",
                "",
            )
            .await,
        )
        .await;
        let snapshot_text = String::from_utf8_lossy(&snapshot);
        assert!(!snapshot_text.contains("<version>1.0</version>"));
        assert!(snapshot_text.contains("<version>2.0-SNAPSHOT</version>"));
        assert!(snapshot_text.contains("<latest>2.0-SNAPSHOT</latest>"));
        assert!(!snapshot_text.contains("<release>"));

        let grouped = body_bytes(
            send(
                &ctx.app,
                Method::GET,
                "/repository/public/com/example/policy/maven-metadata.xml",
                "",
            )
            .await,
        )
        .await;
        let grouped_text = String::from_utf8_lossy(&grouped);
        assert!(grouped_text.contains("<version>1.0</version>"));
        assert!(grouped_text.contains("<version>2.0-SNAPSHOT</version>"));
        assert!(grouped_text.contains("<latest>2.0-SNAPSHOT</latest>"));
        assert!(grouped_text.contains("<release>1.0</release>"));

        let grouped_checksum = body_bytes(
            send(
                &ctx.app,
                Method::GET,
                "/repository/public/com/example/policy/maven-metadata.xml.sha1",
                "",
            )
            .await,
        )
        .await;
        assert_eq!(
            String::from_utf8_lossy(&grouped_checksum),
            checksum_hex("sha1", &grouped).unwrap()
        );
    }

    #[tokio::test]
    async fn test_maven_namespace_scope_enforced() {
        use crate::auth::NamespaceAuthority;
        use crate::config::ScopeEnforcement;
        use axum::body::Bytes;
        use axum::extract::{Path, State};
        use axum::http::StatusCode;
        use axum::Extension;

        let ctx = create_test_context();
        let scoped = NamespaceAuthority::from_oidc_scope(
            "ci",
            &["com/myorg/**".to_string()],
            ScopeEnforcement::Enforce,
        );

        // Out of scope (different group) -> 403.
        let resp = super::upload_legacy(
            State(ctx.state.clone()),
            Path("com/other/lib/1.0/lib-1.0.jar".to_string()),
            Extension(scoped.clone()),
            Bytes::from_static(b"x"),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);

        // Opaque (unrecognized) path under a real scope -> fail-closed 403.
        let resp = super::upload_legacy(
            State(ctx.state.clone()),
            Path("foo".to_string()),
            Extension(scoped.clone()),
            Bytes::from_static(b"x"),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);

        // In scope -> enforcement passes (not 403).
        let resp = super::upload_legacy(
            State(ctx.state.clone()),
            Path("com/myorg/lib/1.0/lib-1.0.jar".to_string()),
            Extension(scoped),
            Bytes::from_static(b"x"),
        )
        .await;
        assert_ne!(resp.status(), StatusCode::FORBIDDEN);
    }
    use axum::http::header;
    use sha2::Digest;

    #[tokio::test]
    async fn test_maven_put_get_roundtrip() {
        let ctx = create_test_context();
        let jar_data = b"fake-jar-content";

        let put = send(
            &ctx.app,
            Method::PUT,
            "/maven2/com/example/mylib/1.0/mylib-1.0.jar",
            Body::from(&jar_data[..]),
        )
        .await;
        assert_eq!(put.status(), StatusCode::CREATED);

        let get = send(
            &ctx.app,
            Method::GET,
            "/maven2/com/example/mylib/1.0/mylib-1.0.jar",
            "",
        )
        .await;
        assert_eq!(get.status(), StatusCode::OK);
        let body = body_bytes(get).await;
        assert_eq!(&body[..], jar_data);
    }

    #[tokio::test]
    async fn test_maven_not_found_no_proxy() {
        let ctx = create_test_context();
        let resp = send(
            &ctx.app,
            Method::GET,
            "/maven2/missing/artifact/1.0/artifact-1.0.jar",
            "",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_maven_content_type_pom() {
        let ctx = create_test_context();
        send(
            &ctx.app,
            Method::PUT,
            "/maven2/com/ex/1.0/ex-1.0.pom",
            Body::from("<project/>"),
        )
        .await;

        let get = send(&ctx.app, Method::GET, "/maven2/com/ex/1.0/ex-1.0.pom", "").await;
        assert_eq!(get.status(), StatusCode::OK);
        assert_eq!(
            get.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/xml"
        );
    }

    #[tokio::test]
    async fn test_maven_content_type_jar() {
        let ctx = create_test_context();
        send(
            &ctx.app,
            Method::PUT,
            "/maven2/org/test/app/2.0/app-2.0.jar",
            Body::from("jar-data"),
        )
        .await;

        let get = send(
            &ctx.app,
            Method::GET,
            "/maven2/org/test/app/2.0/app-2.0.jar",
            "",
        )
        .await;
        assert_eq!(get.status(), StatusCode::OK);
        assert_eq!(
            get.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/java-archive"
        );
    }

    // ── Checksums ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_maven_auto_checksums() {
        let ctx = create_test_context();
        let data = b"test-jar-for-checksum";

        send(
            &ctx.app,
            Method::PUT,
            "/maven2/com/example/ck/1.0/ck-1.0.jar",
            Body::from(&data[..]),
        )
        .await;

        for suffix in ["md5", "sha1", "sha256", "sha512"] {
            let resp = send(
                &ctx.app,
                Method::GET,
                &format!("/maven2/com/example/ck/1.0/ck-1.0.jar.{suffix}"),
                "",
            )
            .await;
            assert_eq!(resp.status(), StatusCode::OK);
            let hash = body_bytes(resp).await;
            assert_eq!(
                String::from_utf8_lossy(&hash),
                checksum_hex(suffix, data).unwrap()
            );
        }
    }

    #[tokio::test]
    async fn test_maven_checksum_upload_accepts_all_algorithms() {
        let ctx = create_test_context();
        let data = b"checksum-test-jar";

        send(
            &ctx.app,
            Method::PUT,
            "/maven2/com/example/cv/1.0/cv-1.0.jar",
            Body::from(&data[..]),
        )
        .await;

        for suffix in ["md5", "sha1", "sha256", "sha512"] {
            let resp = send(
                &ctx.app,
                Method::PUT,
                &format!("/maven2/com/example/cv/1.0/cv-1.0.jar.{suffix}"),
                Body::from(checksum_hex(suffix, data).unwrap()),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::CREATED);
        }
    }

    #[tokio::test]
    async fn test_maven_checksum_upload_rejects_mismatch() {
        let ctx = create_test_context();
        let data = b"checksum-mismatch-test";

        send(
            &ctx.app,
            Method::PUT,
            "/maven2/com/example/cm/1.0/cm-1.0.jar",
            Body::from(&data[..]),
        )
        .await;

        let resp = send(
            &ctx.app,
            Method::PUT,
            "/maven2/com/example/cm/1.0/cm-1.0.jar.sha1",
            Body::from("0000000000000000000000000000000000000000"),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_maven_checksum_requires_base_object() {
        let ctx = create_test_context();
        for method in [Method::GET, Method::PUT] {
            let resp = send(
                &ctx.app,
                method,
                "/maven2/com/example/missing/1.0/missing-1.0.jar.sha256",
                Body::from("00"),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        }
    }

    #[tokio::test]
    async fn test_maven_checksum_get_repairs_stale_sidecar() {
        let ctx = create_test_context();
        let path = "com/example/repair/1.0/repair-1.0.jar";
        let data = b"authoritative";
        assert_eq!(
            send(
                &ctx.app,
                Method::PUT,
                &format!("/maven2/{path}"),
                Body::from(&data[..]),
            )
            .await
            .status(),
            StatusCode::CREATED
        );
        let sidecar_key = format!("maven/{path}.sha256");
        ctx.state.storage.put(&sidecar_key, b"stale").await.unwrap();

        let response = send(&ctx.app, Method::GET, &format!("/maven2/{path}.sha256"), "").await;
        assert_eq!(response.status(), StatusCode::OK);
        let expected = checksum_hex("sha256", data).unwrap();
        assert_eq!(
            String::from_utf8_lossy(&body_bytes(response).await),
            expected
        );
        assert_eq!(
            String::from_utf8_lossy(&ctx.state.storage.get(&sidecar_key).await.unwrap()),
            expected
        );
    }

    #[tokio::test]
    async fn checksum_and_managed_metadata_routes_cover_physical_events_without_root_reconcile() {
        let index_dir = tempfile::tempdir().unwrap();
        let ctx = create_test_context_with_config(|config| {
            config.index.path = index_dir
                .path()
                .join("index.redb")
                .to_string_lossy()
                .into_owned();
            config.maven.repositories.clear();
        });
        let backend = Arc::new(crate::test_helpers::FaultInjectBackend::new(
            ctx.state.storage.clone(),
        ));
        let list_attempts = backend.list_attempts();
        let storage = Storage::from_backend(backend);
        let index = crate::repo_index::RepoIndex::open_persistent_for_test(
            &ctx.state.config,
            ctx.state.enabled_registries.as_ref(),
            storage.clone(),
        )
        .await
        .unwrap();
        index.reconcile_persistent_for_test().await.unwrap();
        list_attempts.lock().clear();

        let mut state = ctx.state.clone();
        state.storage = storage;
        state.repo_index = Arc::clone(&index);
        state.storage.set_mutation_observer(index.clone());
        let repository = DirectRepository::legacy(&state);
        let path = "com/example/indexed/1.0/indexed-1.0.jar";
        let response = upload_direct(
            state.clone(),
            repository.clone(),
            path.to_string(),
            Bytes::from_static(b"jar"),
            |_| true,
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);

        let checksum_path = format!("{path}.sha256");
        let checksum = checksum_hex("sha256", b"jar").unwrap();
        let response = upload_direct(
            state.clone(),
            repository.clone(),
            checksum_path.clone(),
            Bytes::from(checksum),
            |_| true,
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);

        let sidecar_key = format!("maven/{checksum_path}");
        state.storage.put(&sidecar_key, b"stale").await.unwrap();
        let response = download_direct(
            state.clone(),
            HeaderMap::new(),
            repository.clone(),
            checksum_path,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);

        let metadata_path = "com/example/indexed/maven-metadata.xml";
        let metadata = state
            .storage
            .get(&format!("maven/{metadata_path}"))
            .await
            .unwrap();
        let response = upload_direct(
            state.clone(),
            repository,
            metadata_path.to_string(),
            metadata,
            |_| true,
        )
        .await;
        assert_eq!(response.status(), StatusCode::CREATED);

        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let caught_up = tokio::time::timeout(std::time::Duration::from_secs(3), async {
            while !index.persistent_protocol_ready() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await;
        if caught_up.is_err() {
            let meta = index.persistent_meta_for_test().await.unwrap();
            panic!(
                "typed Maven route hooks did not cover physical writes: meta={meta:?}, lists={:?}",
                list_attempts.lock().as_slice()
            );
        }
        assert!(
            !list_attempts.lock().iter().any(|prefix| prefix == "maven/"),
            "route-local repair may list one GA/directory but must not schedule a full Maven scan"
        );
        state.storage.clear_mutation_observer();
        index.shutdown_persistent().await;
    }

    #[tokio::test]
    async fn test_maven_checksum_reread_error_fails_instead_of_hashing_prelock_bytes() {
        let ctx = create_test_context();
        let backend = Arc::new(FaultInjectingBackend::default());
        let mut state = ctx.state.clone();
        state.storage = Storage::from_backend(backend.clone());
        let path = "com/example/locked/1.0/locked-1.0.jar";
        let key = format!("maven/{path}");
        let data = b"authoritative-under-lock";
        state.storage.put(&key, data).await.unwrap();

        // The recursive base GET consumes the first read. Fail the authoritative
        // re-read after the mutation lock is acquired.
        backend.fail_after(InjectedOperation::Get, &key, 1);
        let response = super::download_direct(
            state.clone(),
            axum::http::HeaderMap::new(),
            super::DirectRepository::legacy(&state),
            format!("{path}.sha256"),
        )
        .await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let retry = super::download_direct(
            state.clone(),
            axum::http::HeaderMap::new(),
            super::DirectRepository::legacy(&state),
            format!("{path}.sha256"),
        )
        .await;
        assert_eq!(retry.status(), StatusCode::OK);
        assert_eq!(
            String::from_utf8_lossy(&body_bytes(retry).await),
            checksum_hex("sha256", data).unwrap()
        );
    }

    // ── Immutability ────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_maven_release_immutability() {
        let ctx = create_test_context();

        let r1 = send(
            &ctx.app,
            Method::PUT,
            "/maven2/com/example/imm/1.0.0/imm-1.0.0.jar",
            Body::from("v1"),
        )
        .await;
        assert_eq!(r1.status(), StatusCode::CREATED);

        let r2 = send(
            &ctx.app,
            Method::PUT,
            "/maven2/com/example/imm/1.0.0/imm-1.0.0.jar",
            Body::from("v2"),
        )
        .await;
        assert_eq!(r2.status(), StatusCode::CONFLICT);

        let get = send(
            &ctx.app,
            Method::GET,
            "/maven2/com/example/imm/1.0.0/imm-1.0.0.jar",
            "",
        )
        .await;
        let body = body_bytes(get).await;
        assert_eq!(&body[..], b"v1");
    }

    #[tokio::test]
    async fn test_maven_exact_release_retry_repairs_derived_state() {
        let ctx = create_test_context();
        let path = "com/example/retry/1.0.0/retry-1.0.0.jar";
        let key = format!("maven/{path}");
        let data = b"immutable-release";

        // Simulate interruption immediately after the atomic create: the base
        // object exists, but none of its derived state was written.
        ctx.state.storage.put_if_absent(&key, data).await.unwrap();
        for suffix in ["md5", "sha1", "sha256", "sha512"] {
            assert!(ctx
                .state
                .storage
                .get(&format!("{key}.{suffix}"))
                .await
                .is_err());
        }
        let metadata_key = "maven/com/example/retry/maven-metadata.xml";
        assert!(ctx.state.storage.get(metadata_key).await.is_err());

        let retry = send(
            &ctx.app,
            Method::PUT,
            &format!("/maven2/{path}"),
            Body::from(&data[..]),
        )
        .await;
        assert_eq!(retry.status(), StatusCode::CREATED);

        for suffix in ["md5", "sha1", "sha256", "sha512"] {
            assert_eq!(
                String::from_utf8_lossy(
                    &ctx.state
                        .storage
                        .get(&format!("{key}.{suffix}"))
                        .await
                        .unwrap()
                ),
                checksum_hex(suffix, data).unwrap()
            );
        }
        let metadata = ctx.state.storage.get(metadata_key).await.unwrap();
        let metadata = String::from_utf8_lossy(&metadata);
        assert!(metadata.contains("<version>1.0.0</version>"));
        assert!(metadata.contains("<release>1.0.0</release>"));
        for suffix in ["md5", "sha1", "sha256", "sha512"] {
            assert_eq!(
                String::from_utf8_lossy(
                    &ctx.state
                        .storage
                        .get(&format!("{metadata_key}.{suffix}"))
                        .await
                        .unwrap()
                ),
                checksum_hex(suffix, metadata.as_bytes()).unwrap()
            );
        }
    }

    #[tokio::test]
    async fn test_maven_completed_exact_release_retry_keeps_metadata_byte_stable() {
        let ctx = create_test_context();
        let older_path = "com/example/stable-retry/1.0.0/stable-retry-1.0.0.jar";
        let current_path = "com/example/stable-retry/2.0.0/stable-retry-2.0.0.jar";
        let older_key = format!("maven/{older_path}");
        let metadata_key = "maven/com/example/stable-retry/maven-metadata.xml";
        let older_data = b"immutable-release-1";
        let current_data = b"immutable-release-2";

        for (path, data) in [
            (older_path, older_data.as_slice()),
            (current_path, current_data.as_slice()),
        ] {
            assert_eq!(
                send(
                    &ctx.app,
                    Method::PUT,
                    &format!("/maven2/{path}"),
                    Body::from(data.to_vec()),
                )
                .await
                .status(),
                StatusCode::CREATED
            );
        }

        let original =
            String::from_utf8(ctx.state.storage.get(metadata_key).await.unwrap().to_vec()).unwrap();
        let generated_timestamp = parse_artifact_metadata(original.as_bytes())
            .and_then(|metadata| metadata.last_updated)
            .unwrap();
        let original = original.replace(
            &format!("<lastUpdated>{generated_timestamp}</lastUpdated>"),
            "<lastUpdated>20000101000000</lastUpdated>",
        );
        assert!(parse_artifact_metadata(original.as_bytes()).is_some());
        ctx.state
            .storage
            .put(metadata_key, original.as_bytes())
            .await
            .unwrap();
        compute_and_store_checksums(&ctx.state.storage, metadata_key, original.as_bytes())
            .await
            .unwrap();
        ctx.state
            .storage
            .put(&format!("{metadata_key}.sha512"), b"corrupt")
            .await
            .unwrap();

        assert_eq!(
            send(
                &ctx.app,
                Method::PUT,
                &format!("/maven2/{older_path}"),
                Body::from(&older_data[..]),
            )
            .await
            .status(),
            StatusCode::CREATED
        );

        assert_eq!(
            ctx.state.storage.get(&older_key).await.unwrap(),
            older_data.as_slice()
        );
        assert_eq!(
            ctx.state.storage.get(metadata_key).await.unwrap(),
            original.as_bytes(),
            "a completed exact retry must not create a new metadata generation"
        );
        let metadata = parse_artifact_metadata(original.as_bytes()).unwrap();
        assert_eq!(metadata.latest.as_deref(), Some("2.0.0"));
        assert_eq!(metadata.release.as_deref(), Some("2.0.0"));
        assert_eq!(metadata.last_updated.as_deref(), Some("20000101000000"));
        for suffix in ["md5", "sha1", "sha256", "sha512"] {
            assert_eq!(
                String::from_utf8_lossy(
                    &ctx.state
                        .storage
                        .get(&format!("{metadata_key}.{suffix}"))
                        .await
                        .unwrap()
                ),
                checksum_hex(suffix, original.as_bytes()).unwrap()
            );
        }
    }

    #[tokio::test]
    async fn test_maven_derived_failures_return_500_and_exact_retry_repairs() {
        let ctx = create_test_context();
        let backend = Arc::new(FaultInjectingBackend::default());
        let mut state = ctx.state.clone();
        state.storage = Storage::from_backend(backend.clone());

        let checksum_path = "com/example/checksum-failure/1.0.0/checksum-failure-1.0.0.jar";
        let checksum_key = format!("maven/{checksum_path}");
        let checksum_data = b"checksum-body";
        backend.fail_once(InjectedOperation::Put, format!("{checksum_key}.md5"));
        assert_eq!(
            upload_with_state(&state, checksum_path, checksum_data)
                .await
                .status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            state.storage.get(&checksum_key).await.unwrap(),
            checksum_data.as_slice()
        );
        assert_eq!(
            upload_with_state(&state, checksum_path, checksum_data)
                .await
                .status(),
            StatusCode::CREATED
        );
        for suffix in ["md5", "sha1", "sha256", "sha512"] {
            assert_eq!(
                String::from_utf8_lossy(
                    &state
                        .storage
                        .get(&format!("{checksum_key}.{suffix}"))
                        .await
                        .unwrap()
                ),
                checksum_hex(suffix, checksum_data).unwrap()
            );
        }
        assert!(state
            .storage
            .get("maven/com/example/checksum-failure/maven-metadata.xml")
            .await
            .is_ok());

        let list_path = "com/example/list-failure/1.0.0/list-failure-1.0.0.jar";
        let list_key = format!("maven/{list_path}");
        let list_data = b"list-body";
        backend.fail_once(InjectedOperation::List, "maven/com/example/list-failure/");
        assert_eq!(
            upload_with_state(&state, list_path, list_data)
                .await
                .status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            state.storage.get(&list_key).await.unwrap(),
            list_data.as_slice()
        );
        assert_eq!(
            upload_with_state(&state, list_path, list_data)
                .await
                .status(),
            StatusCode::CREATED
        );
        assert!(state
            .storage
            .get("maven/com/example/list-failure/maven-metadata.xml")
            .await
            .is_ok());

        let metadata_write_path =
            "com/example/metadata-write-failure/1.0.0/metadata-write-failure-1.0.0.jar";
        let metadata_write_key = "maven/com/example/metadata-write-failure/maven-metadata.xml";
        let metadata_write_data = b"metadata-write-body";
        backend.fail_once(InjectedOperation::Put, metadata_write_key);
        assert_eq!(
            upload_with_state(&state, metadata_write_path, metadata_write_data)
                .await
                .status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            state
                .storage
                .get(&format!("maven/{metadata_write_path}"))
                .await
                .unwrap(),
            metadata_write_data.as_slice()
        );
        assert!(state.storage.get(metadata_write_key).await.is_err());
        assert_eq!(
            upload_with_state(&state, metadata_write_path, metadata_write_data)
                .await
                .status(),
            StatusCode::CREATED
        );

        let metadata_path = "com/example/metadata-failure/1.0.0/metadata-failure-1.0.0.jar";
        let metadata_key = "maven/com/example/metadata-failure/maven-metadata.xml";
        let metadata_data = b"metadata-body";
        backend.fail_once(InjectedOperation::Get, metadata_key);
        assert_eq!(
            upload_with_state(&state, metadata_path, metadata_data)
                .await
                .status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            state
                .storage
                .get(&format!("maven/{metadata_path}"))
                .await
                .unwrap(),
            metadata_data.as_slice()
        );
        assert!(state.storage.get(metadata_key).await.is_err());
        assert_eq!(
            upload_with_state(&state, metadata_path, metadata_data)
                .await
                .status(),
            StatusCode::CREATED
        );

        let metadata_before = state.storage.get(metadata_key).await.unwrap();
        backend.fail_once(InjectedOperation::Get, metadata_key);
        assert_eq!(
            upload_with_state(&state, metadata_path, metadata_data)
                .await
                .status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            state.storage.get(metadata_key).await.unwrap(),
            metadata_before,
            "an exact-retry metadata read failure must not regenerate or overwrite metadata"
        );
        assert_eq!(
            upload_with_state(&state, metadata_path, metadata_data)
                .await
                .status(),
            StatusCode::CREATED
        );
    }

    #[tokio::test]
    async fn test_maven_snapshot_overwrite() {
        let ctx = create_test_context();

        let r1 = send(
            &ctx.app,
            Method::PUT,
            "/maven2/com/example/snap/1.0-SNAPSHOT/snap-1.0-SNAPSHOT.jar",
            Body::from("snapshot-v1"),
        )
        .await;
        assert_eq!(r1.status(), StatusCode::CREATED);

        let r2 = send(
            &ctx.app,
            Method::PUT,
            "/maven2/com/example/snap/1.0-SNAPSHOT/snap-1.0-SNAPSHOT.jar",
            Body::from("snapshot-v2"),
        )
        .await;
        assert_eq!(r2.status(), StatusCode::CREATED);

        let get = send(
            &ctx.app,
            Method::GET,
            "/maven2/com/example/snap/1.0-SNAPSHOT/snap-1.0-SNAPSHOT.jar",
            "",
        )
        .await;
        let body = body_bytes(get).await;
        assert_eq!(&body[..], b"snapshot-v2");
    }

    // ── Metadata generation ─────────────────────────────────────────────

    #[tokio::test]
    async fn test_maven_metadata_generated() {
        let ctx = create_test_context();

        send(
            &ctx.app,
            Method::PUT,
            "/maven2/com/example/meta/1.0.0/meta-1.0.0.jar",
            Body::from("v1"),
        )
        .await;

        send(
            &ctx.app,
            Method::PUT,
            "/maven2/com/example/meta/2.0.0/meta-2.0.0.jar",
            Body::from("v2"),
        )
        .await;

        let resp = send(
            &ctx.app,
            Method::GET,
            "/maven2/com/example/meta/maven-metadata.xml",
            "",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_bytes(resp).await;
        let xml = String::from_utf8_lossy(&body);
        assert!(xml.contains("<groupId>com.example</groupId>"));
        assert!(xml.contains("<artifactId>meta</artifactId>"));
        assert!(xml.contains("<version>1.0.0</version>"));
        assert!(xml.contains("<version>2.0.0</version>"));
        assert!(xml.contains("<latest>2.0.0</latest>"));
        assert!(xml.contains("<release>2.0.0</release>"));
    }

    #[tokio::test]
    async fn test_maven_metadata_release_tracks_lower_version_deployed_last() {
        let ctx = create_test_context();
        for version in ["2.0.0", "1.0.0"] {
            let response = send(
                &ctx.app,
                Method::PUT,
                &format!("/maven2/com/example/order/{version}/order-{version}.jar"),
                Body::from(version.to_string()),
            )
            .await;
            assert_eq!(response.status(), StatusCode::CREATED);
        }

        let metadata = body_bytes(
            send(
                &ctx.app,
                Method::GET,
                "/maven2/com/example/order/maven-metadata.xml",
                "",
            )
            .await,
        )
        .await;
        let metadata = String::from_utf8_lossy(&metadata);
        assert!(metadata.contains("<release>1.0.0</release>"));
        assert!(metadata.contains("<latest>1.0.0</latest>"));
        assert!(metadata.contains("<version>2.0.0</version>"));
        assert!(metadata.contains("<version>1.0.0</version>"));
    }

    #[tokio::test]
    async fn test_ordinary_metadata_latest_and_client_checksum_deploy_sequence() {
        let ctx = create_test_context();
        let client_metadata = r#"<?xml version="1.0" encoding="UTF-8"?>
<metadata>
  <groupId>com.example</groupId>
  <artifactId>ordinary</artifactId>
  <versioning>
    <latest>1.0.0</latest>
    <release>1.0.0</release>
    <versions><version>1.0.0</version></versions>
    <lastUpdated>20260729010101</lastUpdated>
  </versioning>
</metadata>
"#;
        assert_eq!(
            send(
                &ctx.app,
                Method::PUT,
                "/maven2/com/example/ordinary/1.0.0/ordinary-1.0.0.pom",
                Body::from("<project/>"),
            )
            .await
            .status(),
            StatusCode::CREATED
        );
        assert_eq!(
            send(
                &ctx.app,
                Method::PUT,
                "/maven2/com/example/ordinary/maven-metadata.xml",
                Body::from(client_metadata),
            )
            .await
            .status(),
            StatusCode::CREATED
        );

        let stored = body_bytes(
            send(
                &ctx.app,
                Method::GET,
                "/maven2/com/example/ordinary/maven-metadata.xml",
                "",
            )
            .await,
        )
        .await;
        assert!(String::from_utf8_lossy(&stored).contains("<latest>1.0.0</latest>"));
        assert_ne!(stored.as_ref(), client_metadata.as_bytes());

        let client_sha1 = checksum_hex("sha1", client_metadata.as_bytes()).unwrap();
        assert_eq!(
            send(
                &ctx.app,
                Method::PUT,
                "/maven2/com/example/ordinary/maven-metadata.xml.sha1",
                Body::from(client_sha1.clone()),
            )
            .await
            .status(),
            StatusCode::CREATED
        );
        let served_sha1 = body_bytes(
            send(
                &ctx.app,
                Method::GET,
                "/maven2/com/example/ordinary/maven-metadata.xml.sha1",
                "",
            )
            .await,
        )
        .await;
        assert_eq!(
            String::from_utf8_lossy(&served_sha1),
            checksum_hex("sha1", &stored).unwrap()
        );
        assert_ne!(String::from_utf8_lossy(&served_sha1), client_sha1);
    }

    #[tokio::test]
    async fn test_maven_metadata_checksums() {
        let ctx = create_test_context();

        send(
            &ctx.app,
            Method::PUT,
            "/maven2/com/example/mck/1.0.0/mck-1.0.0.jar",
            Body::from("data"),
        )
        .await;

        let resp = send(
            &ctx.app,
            Method::GET,
            "/maven2/com/example/mck/maven-metadata.xml.sha256",
            "",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let hash = body_bytes(resp).await;
        assert_eq!(hash.len(), 64);
    }

    #[tokio::test]
    async fn test_maven_hosted_proxy_metadata_collision_in_both_orders() {
        use crate::config::MavenProxyEntry;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let upstream = MockServer::start().await;
        let upstream_metadata = r#"<?xml version="1.0" encoding="UTF-8"?>
<metadata>
  <groupId>com.example</groupId>
  <artifactId>collision</artifactId>
  <versioning>
    <latest>2.0.0</latest>
    <release>2.0.0</release>
    <versions>
      <version>1.0.0</version>
      <version>2.0.0</version>
    </versions>
    <lastUpdated>20260728010000</lastUpdated>
  </versioning>
</metadata>
"#;
        Mock::given(method("GET"))
            .and(path("/com/example/collision/maven-metadata.xml"))
            .respond_with(
                ResponseTemplate::new(200).set_body_raw(upstream_metadata, "application/xml"),
            )
            .mount(&upstream)
            .await;

        let new_context = || {
            let upstream_url = upstream.uri();
            create_test_context_with_config(move |config| {
                config.maven.proxies = vec![MavenProxyEntry::Simple(upstream_url)];
                config.maven.metadata_ttl = 0;
            })
        };

        let hosted_first = new_context();
        let upload = send(
            &hosted_first.app,
            Method::PUT,
            "/maven2/com/example/collision/9.0.0-internal/collision-9.0.0-internal.jar",
            Body::from("hosted-first"),
        )
        .await;
        assert_eq!(upload.status(), StatusCode::CREATED);

        let response = send(
            &hosted_first.app,
            Method::GET,
            "/maven2/com/example/collision/maven-metadata.xml",
            "",
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let metadata = body_bytes(response).await;
        let metadata_text = String::from_utf8_lossy(&metadata);
        assert!(metadata_text.contains("<version>1.0.0</version>"));
        assert!(metadata_text.contains("<version>2.0.0</version>"));
        assert!(metadata_text.contains("<version>9.0.0-internal</version>"));

        let checksum = body_bytes(
            send(
                &hosted_first.app,
                Method::GET,
                "/maven2/com/example/collision/maven-metadata.xml.sha1",
                "",
            )
            .await,
        )
        .await;
        assert_eq!(
            String::from_utf8_lossy(&checksum),
            hex::encode(sha1::Sha1::digest(&metadata))
        );

        let proxy_first = new_context();
        let response = send(
            &proxy_first.app,
            Method::GET,
            "/maven2/com/example/collision/maven-metadata.xml",
            "",
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);

        let upload = send(
            &proxy_first.app,
            Method::PUT,
            "/maven2/com/example/collision/9.1.0-internal/collision-9.1.0-internal.jar",
            Body::from("proxy-first"),
        )
        .await;
        assert_eq!(upload.status(), StatusCode::CREATED);

        let response = send(
            &proxy_first.app,
            Method::GET,
            "/maven2/com/example/collision/maven-metadata.xml",
            "",
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let metadata = String::from_utf8_lossy(&body_bytes(response).await).into_owned();
        assert!(metadata.contains("<version>1.0.0</version>"));
        assert!(metadata.contains("<version>2.0.0</version>"));
        assert!(metadata.contains("<version>9.1.0-internal</version>"));
    }

    #[tokio::test]
    async fn test_proxy_stale_metadata_only_on_error_not_upstream_404() {
        use crate::config::MavenProxyEntry;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let upstream = MockServer::start().await;
        let metadata = r#"<metadata><groupId>com.example</groupId><artifactId>stale</artifactId><versioning><release>1.0</release><versions><version>1.0</version></versions></versioning></metadata>"#;
        let metadata_path = "/com/example/stale/maven-metadata.xml";
        Mock::given(method("GET"))
            .and(path(metadata_path))
            .respond_with(ResponseTemplate::new(200).set_body_raw(metadata, "application/xml"))
            .mount(&upstream)
            .await;
        let upstream_url = upstream.uri();
        let ctx = create_test_context_with_config(move |config| {
            config.maven.proxies = vec![MavenProxyEntry::Simple(upstream_url)];
            config.maven.metadata_ttl = 0;
        });
        let request_path = "/maven2/com/example/stale/maven-metadata.xml";
        assert_eq!(
            send(&ctx.app, Method::GET, request_path, "").await.status(),
            StatusCode::OK
        );

        upstream.reset().await;
        Mock::given(method("GET"))
            .and(path(metadata_path))
            .respond_with(ResponseTemplate::new(404))
            .mount(&upstream)
            .await;
        let not_found = send(&ctx.app, Method::GET, request_path, "").await;
        assert_eq!(not_found.status(), StatusCode::NOT_FOUND);
        assert!(not_found.headers().get("x-nora-stale").is_none());

        upstream.reset().await;
        Mock::given(method("GET"))
            .and(path(metadata_path))
            .respond_with(ResponseTemplate::new(500))
            .mount(&upstream)
            .await;
        let stale = send(&ctx.app, Method::GET, request_path, "").await;
        assert_eq!(stale.status(), StatusCode::OK);
        assert_eq!(stale.headers().get("x-nora-stale").unwrap(), "true");
        assert!(
            String::from_utf8_lossy(&body_bytes(stale).await).contains("<version>1.0</version>")
        );
    }

    #[tokio::test]
    async fn test_client_artifact_metadata_controls_release_without_losing_versions() {
        let ctx = create_test_context();

        send(
            &ctx.app,
            Method::PUT,
            "/maven2/com/example/race/1.0.0/race-1.0.0.jar",
            Body::from("v1"),
        )
        .await;

        let stale = body_bytes(
            send(
                &ctx.app,
                Method::GET,
                "/maven2/com/example/race/maven-metadata.xml",
                "",
            )
            .await,
        )
        .await;

        send(
            &ctx.app,
            Method::PUT,
            "/maven2/com/example/race/2.0.0/race-2.0.0.jar",
            Body::from("v2"),
        )
        .await;

        let upload = send(
            &ctx.app,
            Method::PUT,
            "/maven2/com/example/race/maven-metadata.xml",
            Body::from(stale),
        )
        .await;
        assert_eq!(upload.status(), StatusCode::CREATED);

        let metadata = body_bytes(
            send(
                &ctx.app,
                Method::GET,
                "/maven2/com/example/race/maven-metadata.xml",
                "",
            )
            .await,
        )
        .await;
        let metadata = String::from_utf8_lossy(&metadata);
        assert!(metadata.contains("<version>1.0.0</version>"));
        assert!(metadata.contains("<version>2.0.0</version>"));
        assert!(metadata.contains("<latest>1.0.0</latest>"));
        assert!(metadata.contains("<release>1.0.0</release>"));
    }

    #[tokio::test]
    async fn test_stale_client_artifact_metadata_checksum_is_acknowledged_but_not_stored() {
        let ctx = create_test_context();

        send(
            &ctx.app,
            Method::PUT,
            "/maven2/com/example/race-checksum/1.0.0/race-checksum-1.0.0.jar",
            Body::from("v1"),
        )
        .await;
        let stale = body_bytes(
            send(
                &ctx.app,
                Method::GET,
                "/maven2/com/example/race-checksum/maven-metadata.xml.sha256",
                "",
            )
            .await,
        )
        .await;

        send(
            &ctx.app,
            Method::PUT,
            "/maven2/com/example/race-checksum/2.0.0/race-checksum-2.0.0.jar",
            Body::from("v2"),
        )
        .await;
        let expected = body_bytes(
            send(
                &ctx.app,
                Method::GET,
                "/maven2/com/example/race-checksum/maven-metadata.xml.sha256",
                "",
            )
            .await,
        )
        .await;
        assert_ne!(stale, expected);

        let upload = send(
            &ctx.app,
            Method::PUT,
            "/maven2/com/example/race-checksum/maven-metadata.xml.sha256",
            Body::from(stale),
        )
        .await;
        assert_eq!(upload.status(), StatusCode::CREATED);

        let actual = body_bytes(
            send(
                &ctx.app,
                Method::GET,
                "/maven2/com/example/race-checksum/maven-metadata.xml.sha256",
                "",
            )
            .await,
        )
        .await;
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn test_snapshot_version_metadata_and_checksum_are_preserved() {
        let ctx = create_test_context();
        let metadata = r#"<?xml version="1.0" encoding="UTF-8"?>
<metadata>
  <groupId>com.example</groupId>
  <artifactId>snapshot</artifactId>
  <version>1.0-SNAPSHOT</version>
  <versioning>
    <snapshot><timestamp>20260728.120000</timestamp><buildNumber>1</buildNumber></snapshot>
    <lastUpdated>20260728120000</lastUpdated>
  </versioning>
</metadata>
"#;

        let upload = send(
            &ctx.app,
            Method::PUT,
            "/maven2/com/example/snapshot/1.0-SNAPSHOT/maven-metadata.xml",
            Body::from(metadata),
        )
        .await;
        assert_eq!(upload.status(), StatusCode::CREATED);

        let checksum = hex::encode(sha1::Sha1::digest(metadata.as_bytes()));
        let checksum_upload = send(
            &ctx.app,
            Method::PUT,
            "/maven2/com/example/snapshot/1.0-SNAPSHOT/maven-metadata.xml.sha1",
            Body::from(checksum.clone()),
        )
        .await;
        assert_eq!(checksum_upload.status(), StatusCode::CREATED);

        let stored_metadata = body_bytes(
            send(
                &ctx.app,
                Method::GET,
                "/maven2/com/example/snapshot/1.0-SNAPSHOT/maven-metadata.xml",
                "",
            )
            .await,
        )
        .await;
        assert_eq!(&stored_metadata[..], metadata.as_bytes());

        let stored_checksum = body_bytes(
            send(
                &ctx.app,
                Method::GET,
                "/maven2/com/example/snapshot/1.0-SNAPSHOT/maven-metadata.xml.sha1",
                "",
            )
            .await,
        )
        .await;
        assert_eq!(String::from_utf8_lossy(&stored_checksum), checksum);
    }

    #[tokio::test]
    async fn test_group_plugin_metadata_is_preserved() {
        let ctx = create_test_context();
        let metadata = r#"<?xml version="1.0" encoding="UTF-8"?>
<metadata>
  <plugins>
    <plugin>
      <name>Example Maven Plugin</name>
      <prefix>example</prefix>
      <artifactId>example-maven-plugin</artifactId>
    </plugin>
  </plugins>
</metadata>
"#;

        let upload = send(
            &ctx.app,
            Method::PUT,
            "/maven2/org/example/plugins/maven-metadata.xml",
            Body::from(metadata),
        )
        .await;
        assert_eq!(upload.status(), StatusCode::CREATED);

        let stored = body_bytes(
            send(
                &ctx.app,
                Method::GET,
                "/maven2/org/example/plugins/maven-metadata.xml",
                "",
            )
            .await,
        )
        .await;
        assert_eq!(&stored[..], metadata.as_bytes());
    }

    #[tokio::test]
    async fn test_maven_different_versions_different_artifacts() {
        let ctx = create_test_context();

        send(
            &ctx.app,
            Method::PUT,
            "/maven2/com/example/multi/1.0.0/multi-1.0.0.jar",
            Body::from("v1-jar"),
        )
        .await;
        send(
            &ctx.app,
            Method::PUT,
            "/maven2/com/example/multi/1.0.0/multi-1.0.0.pom",
            Body::from("<pom/>"),
        )
        .await;
        send(
            &ctx.app,
            Method::PUT,
            "/maven2/com/example/multi/2.0.0/multi-2.0.0.jar",
            Body::from("v2-jar"),
        )
        .await;

        let r1 = send(
            &ctx.app,
            Method::GET,
            "/maven2/com/example/multi/1.0.0/multi-1.0.0.jar",
            "",
        )
        .await;
        assert_eq!(r1.status(), StatusCode::OK);

        let r2 = send(
            &ctx.app,
            Method::GET,
            "/maven2/com/example/multi/1.0.0/multi-1.0.0.pom",
            "",
        )
        .await;
        assert_eq!(r2.status(), StatusCode::OK);

        let r3 = send(
            &ctx.app,
            Method::GET,
            "/maven2/com/example/multi/2.0.0/multi-2.0.0.jar",
            "",
        )
        .await;
        assert_eq!(r3.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_maven_range_request() {
        let ctx = create_test_context();
        let jar = b"0123456789abcdef";
        ctx.state
            .storage
            .put("maven/com/example/rng/1.0/rng-1.0.jar", jar)
            .await
            .unwrap();
        let url = "/maven2/com/example/rng/1.0/rng-1.0.jar";

        let resp =
            send_with_headers(&ctx.app, Method::GET, url, vec![("range", "bytes=2-5")], "").await;
        assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            resp.headers()
                .get(header::CONTENT_RANGE)
                .unwrap()
                .to_str()
                .unwrap(),
            format!("bytes 2-5/{}", jar.len())
        );
        assert_eq!(
            resp.headers()
                .get(header::ACCEPT_RANGES)
                .unwrap()
                .to_str()
                .unwrap(),
            "bytes"
        );
        assert_eq!(
            resp.headers()
                .get(header::CONTENT_TYPE)
                .unwrap()
                .to_str()
                .unwrap(),
            "application/java-archive"
        );
        assert_eq!(body_bytes(resp).await.as_ref(), &jar[2..=5]);

        // A client that already holds the whole artifact resumes with `bytes=<size>-`.
        let resp = send_with_headers(
            &ctx.app,
            Method::GET,
            url,
            vec![("range", &format!("bytes={}-", jar.len())[..])],
            "",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(
            resp.headers()
                .get(header::CONTENT_RANGE)
                .unwrap()
                .to_str()
                .unwrap(),
            format!("bytes */{}", jar.len())
        );

        let resp = send(&ctx.app, Method::GET, url, "").await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get(header::ACCEPT_RANGES)
                .unwrap()
                .to_str()
                .unwrap(),
            "bytes"
        );
        assert_eq!(body_bytes(resp).await.as_ref(), &jar[..]);
    }

    #[tokio::test]
    async fn test_maven_range_ignored_on_mutable_path() {
        let ctx = create_test_context();
        let xml = b"<metadata>0123456789</metadata>";
        ctx.state
            .storage
            .put("maven/com/example/rng/maven-metadata.xml", xml)
            .await
            .unwrap();

        let resp = send_with_headers(
            &ctx.app,
            Method::GET,
            "/maven2/com/example/rng/maven-metadata.xml",
            vec![("range", "bytes=2-5")],
            "",
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp.headers().get(header::ACCEPT_RANGES).is_none());
        assert_eq!(body_bytes(resp).await.as_ref(), &xml[..]);
    }
}
