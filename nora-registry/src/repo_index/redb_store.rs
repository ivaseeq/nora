// Copyright (c) 2026 The NORA Authors
// SPDX-License-Identifier: MIT

//! Persistent, rebuildable Maven/npm index storage.
//!
//! This module intentionally contains no artifact authority. S3 commits first;
//! redb stores only a versioned projection that can be discarded and rebuilt.

use super::{NpmSearchDocument, RepoInfo, RepoQuery};
use crate::storage::FileMeta;
use redb::{
    Database, Durability, ReadTransaction, ReadableDatabase, ReadableTable, TableDefinition,
    WriteTransaction,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, Semaphore};

const SCHEMA_VERSION: u32 = 6;
/// Application-level identity of the redb engine contract. This MUST change
/// together with `SCHEMA_VERSION` whenever the pinned development revision or
/// the approved stable redb line changes, forcing a fresh derived DB rather
/// than opening it under unreviewed recovery semantics.
const ENGINE_REVISION: &str = "redb-c419f099-dev";
const CACHE_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_TX_ROWS: usize = 1_000;
pub const MAX_TX_BYTES: usize = 8 * 1024 * 1024;
const WRITER_QUEUE_CAPACITY: usize = 256;
const READ_CONCURRENCY: usize = 8;
pub const MAX_QUERY_EXAMINED: usize = 10_000;
const WRITER_COMMAND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta_v1");
const OBJECTS_A: TableDefinition<&[u8], &[u8]> = TableDefinition::new("objects_a_v1");
const OBJECTS_B: TableDefinition<&[u8], &[u8]> = TableDefinition::new("objects_b_v1");
const REPOS_A: TableDefinition<&[u8], &[u8]> = TableDefinition::new("repos_a_v1");
const REPOS_B: TableDefinition<&[u8], &[u8]> = TableDefinition::new("repos_b_v1");
const MAVEN_PREFIX_STATS_A: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("maven_prefix_stats_a_v1");
const MAVEN_PREFIX_STATS_B: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("maven_prefix_stats_b_v1");
const NPM_PACKAGES_A: TableDefinition<&[u8], &[u8]> = TableDefinition::new("npm_packages_a_v1");
const NPM_PACKAGES_B: TableDefinition<&[u8], &[u8]> = TableDefinition::new("npm_packages_b_v1");
const NPM_VERSIONS_A: TableDefinition<&[u8], &[u8]> = TableDefinition::new("npm_versions_a_v1");
const NPM_VERSIONS_B: TableDefinition<&[u8], &[u8]> = TableDefinition::new("npm_versions_b_v1");
const NPM_OBSERVATIONS_A: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("npm_observations_a_v1");
const NPM_OBSERVATIONS_B: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("npm_observations_b_v1");
const CHANGES: TableDefinition<u64, &[u8]> = TableDefinition::new("changes_v1");
const ENTITY_SEQUENCES: TableDefinition<&[u8], u64> = TableDefinition::new("entity_sequences_v1");

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Slot {
    A,
    B,
}

impl Slot {
    pub fn inactive(self) -> Self {
        match self {
            Self::A => Self::B,
            Self::B => Self::A,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegistryCompleteness {
    pub maven: bool,
    pub npm: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegistryTotals {
    pub maven_artifacts: u64,
    pub maven_bytes: u64,
    pub npm_versions: u64,
    pub npm_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaState {
    pub schema: u32,
    pub engine_revision: String,
    pub db_uuid: String,
    pub active_slot: Option<Slot>,
    pub generation: u64,
    pub completeness: RegistryCompleteness,
    pub config_digest: String,
    pub accepted_change_seq: u64,
    pub slot_watermarks: [u64; 2],
    pub global_dirty: bool,
    /// True only after the writer has drained every accepted command and
    /// durably closed the database. This is a structural-integrity hint, not a
    /// projection-freshness bit: `global_dirty` and the watermarks remain the
    /// authority for deciding whether S3 reconciliation is required.
    #[serde(default)]
    pub clean_shutdown: bool,
    #[serde(default)]
    pub totals: RegistryTotals,
}

impl MetaState {
    fn new(config_digest: String) -> Self {
        Self {
            schema: SCHEMA_VERSION,
            engine_revision: ENGINE_REVISION.to_string(),
            db_uuid: uuid::Uuid::new_v4().to_string(),
            active_slot: None,
            generation: 0,
            completeness: RegistryCompleteness::default(),
            config_digest,
            accepted_change_seq: 0,
            slot_watermarks: [0, 0],
            global_dirty: true,
            clean_shutdown: false,
            totals: RegistryTotals::default(),
        }
    }

    fn watermark(&self, slot: Slot) -> u64 {
        self.slot_watermarks[match slot {
            Slot::A => 0,
            Slot::B => 1,
        }]
    }

    fn set_watermark(&mut self, slot: Slot, value: u64) {
        self.slot_watermarks[match slot {
            Slot::A => 0,
            Slot::B => 1,
        }] = value;
    }

    pub fn active_watermark(&self) -> u64 {
        self.active_slot.map_or(0, |slot| self.watermark(slot))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Envelope<T> {
    schema: u8,
    value: T,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredObject {
    pub meta: FileMeta,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredRepo {
    pub name: String,
    /// Fixed-width on-disk count; never persist target-dependent `usize`.
    pub artifact_count: u64,
    pub logical_size: Option<u64>,
    pub modified: u64,
    pub is_file: bool,
}

/// Credential-free logical Maven view used by both the shadow builder and
/// incremental first-wins repair. `members` contains physical direct
/// repositories in lookup order. The empty member denotes the legacy
/// `maven/` namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MavenIndexView {
    pub repository: String,
    pub members: Vec<String>,
}

impl MavenIndexView {
    pub fn legacy() -> Self {
        Self {
            repository: String::new(),
            members: vec![String::new()],
        }
    }
}

/// Exact, rebuildable aggregate for one logical Maven directory. Timestamps
/// are intentionally absent: max-mtime cannot be decremented exactly on
/// delete without another index or a subtree scan.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MavenPrefixStats {
    pub direct_files: u64,
    pub direct_bytes: u64,
    pub subtree_files: u64,
    pub subtree_bytes: u64,
}

impl StoredRepo {
    fn into_repo_info(self) -> RepoInfo {
        RepoInfo {
            name: self.name,
            versions: usize::try_from(self.artifact_count).unwrap_or(usize::MAX),
            size: self.logical_size.unwrap_or(0),
            size_available: self.logical_size.is_some(),
            updated: if self.modified == 0 {
                "N/A".to_string()
            } else {
                crate::ui::components::format_timestamp(self.modified)
            },
            is_file: self.is_file,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredNpmPackage {
    pub repository: String,
    pub package: String,
    pub pointer_sha256: String,
    pub modified: u64,
    pub versions: u64,
    #[serde(default)]
    pub stable_versions: u64,
    #[serde(default)]
    pub prerelease_versions: u64,
    pub logical_size: u64,
    /// Exact presence map for every object that can affect this projection.
    /// `None` is load-bearing: it detects an optional split manifest or proxy
    /// tarball appearing without a packument identity change.
    pub dependencies: std::collections::BTreeMap<String, Option<FileMeta>>,
    pub search: Option<NpmSearchDocument>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredNpmVersion {
    pub repository: String,
    pub package: String,
    pub version: String,
    /// Zero-based position in the package's deterministic newest-first order.
    /// It is part of the table key so keyset pages preserve global order.
    pub sort_rank: u64,
    pub manifest: serde_json::Value,
    pub published: String,
    pub payload_key: String,
    pub payload_meta: Option<FileMeta>,
    pub declared_size: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NpmAuthorityObservation {
    pub repository: String,
    pub package: String,
    pub current_key: Option<String>,
    pub retired_key: Option<String>,
    pub proxy_packument_key: Option<String>,
    pub transitional: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ChangeEvent {
    PhysicalDirty { key: String },
    MavenPathChanged { repository: String, path: String },
    MavenGaChanged { repository: String, ga_path: String },
    NpmHostedChanged { repository: String, package: String },
    NpmProxyChanged { repository: String, package: String },
    GlobalDirty,
}

impl ChangeEvent {
    fn entity_key(&self) -> Option<Vec<u8>> {
        match self {
            Self::MavenPathChanged { repository, path } => {
                Some(format!("maven\0{repository}\0{path}").into_bytes())
            }
            Self::MavenGaChanged {
                repository,
                ga_path,
            } => Some(format!("maven\0{repository}\0{ga_path}").into_bytes()),
            Self::NpmHostedChanged {
                repository,
                package,
            }
            | Self::NpmProxyChanged {
                repository,
                package,
            } => Some(format!("npm\0{repository}\0{package}").into_bytes()),
            Self::PhysicalDirty { .. } | Self::GlobalDirty => None,
        }
    }

    fn coalesce_key(&self) -> Vec<u8> {
        match self {
            Self::PhysicalDirty { key } => format!("physical\0{key}").into_bytes(),
            Self::GlobalDirty => b"global".to_vec(),
            _ => self.entity_key().unwrap_or_default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IncrementalUpdate {
    Maven {
        entity_key: Vec<u8>,
        repository: String,
        object_prefix: Vec<u8>,
        repo_prefix: Vec<u8>,
        repo_recursive: bool,
        objects: Vec<(Vec<u8>, StoredObject)>,
        repos: Vec<(Vec<u8>, StoredRepo)>,
        views: Vec<MavenIndexView>,
    },
    Npm {
        entity_key: Vec<u8>,
        package_key: Vec<u8>,
        version_prefix: Vec<u8>,
        repo_key: Vec<u8>,
        package: Option<Box<StoredNpmPackage>>,
        versions: Vec<(Vec<u8>, StoredNpmVersion)>,
        repo: Option<StoredRepo>,
    },
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("index database is already open")]
    AlreadyOpen,
    #[error("index schema mismatch: {0}")]
    Schema(String),
    #[error("index writer is unavailable")]
    WriterUnavailable,
    #[error("index reconciliation epoch was superseded")]
    Superseded,
    #[error("index transaction exceeds {MAX_TX_ROWS} rows or {MAX_TX_BYTES} serialized bytes")]
    TransactionTooLarge,
    #[error("index projection arithmetic overflow")]
    ProjectionOverflow,
    #[error("index projection invariant failed: {0}")]
    ProjectionInvariant(String),
    #[error("index shadow build rejected: {0}")]
    DiskAdmission(String),
    #[error("index serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("index database error: {0}")]
    Database(String),
    #[error("index I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("index preflight failed without proving corruption: {0}")]
    PreflightUnavailable(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChildPreflight {
    Healthy,
    HealthyAfterIntegrity,
    Missing,
    IntegrityFailed,
    SchemaMismatch,
    AlreadyOpen,
    Failed,
}

impl ChildPreflight {
    pub fn exit_code(self) -> i32 {
        match self {
            Self::Healthy => 0,
            Self::HealthyAfterIntegrity => 25,
            Self::Missing => 20,
            Self::IntegrityFailed => 21,
            Self::SchemaMismatch => 22,
            Self::AlreadyOpen => 23,
            Self::Failed => 24,
        }
    }
}

impl From<redb::DatabaseError> for StoreError {
    fn from(value: redb::DatabaseError) -> Self {
        if matches!(value, redb::DatabaseError::DatabaseAlreadyOpen) {
            Self::AlreadyOpen
        } else {
            Self::Database(value.to_string())
        }
    }
}

impl StoreError {
    fn poisons_writer(&self) -> bool {
        matches!(self, Self::Database(_) | Self::Io(_) | Self::Schema(_))
    }
}

fn encode<T: Serialize>(value: T) -> Result<Vec<u8>, StoreError> {
    Ok(serde_json::to_vec(&Envelope { schema: 1, value })?)
}

/// Exact bytes admitted by the transaction budget for one key/value row.
///
/// Keep this shared with the S2 batch builder. Estimating from only the npm
/// manifest undercounts the duplicated repository/package/payload metadata and
/// can create a batch that `check_batch` must reject at real packument scale.
pub(super) fn encoded_row_bytes<T: Serialize>(key: &[u8], value: &T) -> Result<usize, StoreError> {
    let value = serde_json::to_vec(&Envelope { schema: 1, value })?;
    Ok(key.len().saturating_add(value.len()))
}

fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, StoreError> {
    let envelope: Envelope<T> = serde_json::from_slice(bytes)?;
    if envelope.schema != 1 {
        return Err(StoreError::Schema(format!(
            "unsupported row schema {}",
            envelope.schema
        )));
    }
    Ok(envelope.value)
}

fn object_table(slot: Slot) -> TableDefinition<'static, &'static [u8], &'static [u8]> {
    match slot {
        Slot::A => OBJECTS_A,
        Slot::B => OBJECTS_B,
    }
}

fn repo_table(slot: Slot) -> TableDefinition<'static, &'static [u8], &'static [u8]> {
    match slot {
        Slot::A => REPOS_A,
        Slot::B => REPOS_B,
    }
}

fn maven_prefix_stats_table(slot: Slot) -> TableDefinition<'static, &'static [u8], &'static [u8]> {
    match slot {
        Slot::A => MAVEN_PREFIX_STATS_A,
        Slot::B => MAVEN_PREFIX_STATS_B,
    }
}

pub fn maven_member_prefix(repository: &str) -> String {
    if repository.is_empty() {
        "maven/".to_string()
    } else {
        format!("maven/repositories/{repository}/")
    }
}

pub fn maven_prefix_stats_key(repository: &str, logical_path: &str) -> Vec<u8> {
    let mut key = repository.as_bytes().to_vec();
    key.push(0);
    key.extend_from_slice(logical_path.trim_matches('/').as_bytes());
    key
}

fn npm_package_table(slot: Slot) -> TableDefinition<'static, &'static [u8], &'static [u8]> {
    match slot {
        Slot::A => NPM_PACKAGES_A,
        Slot::B => NPM_PACKAGES_B,
    }
}

fn npm_version_table(slot: Slot) -> TableDefinition<'static, &'static [u8], &'static [u8]> {
    match slot {
        Slot::A => NPM_VERSIONS_A,
        Slot::B => NPM_VERSIONS_B,
    }
}

fn npm_observation_table(slot: Slot) -> TableDefinition<'static, &'static [u8], &'static [u8]> {
    match slot {
        Slot::A => NPM_OBSERVATIONS_A,
        Slot::B => NPM_OBSERVATIONS_B,
    }
}

fn read_meta_from(transaction: &ReadTransaction) -> Result<MetaState, StoreError> {
    let table = transaction
        .open_table(META)
        .map_err(|error| StoreError::Database(error.to_string()))?;
    let value = table
        .get("state")
        .map_err(|error| StoreError::Database(error.to_string()))?
        .ok_or_else(|| StoreError::Schema("meta_v1/state is missing".to_string()))?;
    let state: MetaState = decode(value.value())?;
    if state.schema != SCHEMA_VERSION {
        return Err(StoreError::Schema(format!(
            "expected application schema {SCHEMA_VERSION}, found {}",
            state.schema
        )));
    }
    if state.engine_revision != ENGINE_REVISION {
        return Err(StoreError::Schema(format!(
            "expected engine revision {ENGINE_REVISION}, found {}",
            state.engine_revision
        )));
    }
    Ok(state)
}

fn read_meta(db: &Database) -> Result<MetaState, StoreError> {
    let transaction = db
        .begin_read()
        .map_err(|error| StoreError::Database(error.to_string()))?;
    read_meta_from(&transaction)
}

/// Run only in the resource-limited preflight child. The caller deliberately
/// observes the process exit status as well, because damaged files may abort
/// inside redb before a Rust error can be returned.
fn classify_database_preflight_error(error: &redb::DatabaseError) -> ChildPreflight {
    match error {
        redb::DatabaseError::DatabaseAlreadyOpen => ChildPreflight::AlreadyOpen,
        redb::DatabaseError::UpgradeRequired(_) => ChildPreflight::SchemaMismatch,
        redb::DatabaseError::Storage(redb::StorageError::Corrupted(_)) => {
            ChildPreflight::IntegrityFailed
        }
        // I/O, repair cancellation and transaction state do not prove that the
        // file is disposable. Preserve it and fail closed in the parent.
        _ => ChildPreflight::Failed,
    }
}

fn classify_meta_preflight(
    result: Result<MetaState, StoreError>,
) -> Result<MetaState, ChildPreflight> {
    match result {
        Ok(state) => Ok(state),
        Err(StoreError::Schema(_)) | Err(StoreError::Serialization(_)) => {
            Err(ChildPreflight::SchemaMismatch)
        }
        Err(StoreError::AlreadyOpen) => Err(ChildPreflight::AlreadyOpen),
        Err(_) => Err(ChildPreflight::Failed),
    }
}

fn preflight_database_with(
    path: &Path,
    integrity_check: impl FnOnce(&mut Database) -> Result<bool, redb::DatabaseError>,
) -> ChildPreflight {
    if !path.exists() || std::fs::metadata(path).is_ok_and(|metadata| metadata.len() == 0) {
        return ChildPreflight::Missing;
    }
    let mut builder = redb::Builder::new();
    builder.set_cache_size(CACHE_BYTES);
    let mut database = match builder.open(path) {
        Ok(database) => database,
        Err(error) => return classify_database_preflight_error(&error),
    };

    // redb performs its own crash recovery while opening the file and documents
    // `check_integrity()` as unnecessary and slow during normal operation. A
    // durable clean close therefore needs only open + application-meta
    // validation. After an unclean process exit we retain the isolated full
    // scan so damaged pages can never abort the long-lived server process.
    match classify_meta_preflight(read_meta(&database)) {
        Ok(state) if state.clean_shutdown => return ChildPreflight::Healthy,
        Ok(_) | Err(ChildPreflight::Failed) => {}
        Err(outcome) => return outcome,
    }

    match integrity_check(&mut database) {
        Ok(true) => {}
        Ok(false) => return ChildPreflight::IntegrityFailed,
        Err(error) => return classify_database_preflight_error(&error),
    }
    match classify_meta_preflight(read_meta(&database)) {
        Ok(_) => ChildPreflight::HealthyAfterIntegrity,
        Err(outcome) => outcome,
    }
}

pub fn preflight_database(path: &Path) -> ChildPreflight {
    preflight_database_with(path, Database::check_integrity)
}

pub fn admit_reseed(path: &Path) -> Result<(), StoreError> {
    let current = std::fs::metadata(path).map_or(0, |metadata| metadata.len());
    let available = available_bytes(path.parent().unwrap_or_else(|| Path::new(".")))?;
    ensure_reseed_capacity(current, available, 0)
}

fn reseed_required_available(current: u64) -> u64 {
    // The old file is retained in-place. Reserve enough free space for a fresh
    // active generation, one later A/B shadow generation, and explicit
    // filesystem headroom. Existing retained files are already reflected in
    // `available_bytes`, so repeated recovery evidence cannot bypass this gate.
    let headroom = (current / 4).max(64 * 1024 * 1024);
    current.saturating_mul(2).saturating_add(headroom)
}

fn ensure_reseed_capacity(
    current: u64,
    available: u64,
    reclaimable_timeout_evidence: u64,
) -> Result<(), StoreError> {
    let required = reseed_required_available(current);
    let effective_available = available.saturating_add(reclaimable_timeout_evidence);
    if effective_available < required {
        return Err(StoreError::DiskAdmission(format!(
            "available_bytes={available} reclaimable_timeout_evidence_bytes={reclaimable_timeout_evidence} required_bytes={required} current_bytes={current}"
        )));
    }
    Ok(())
}

fn commit<T>(
    db: &Database,
    operation: impl FnOnce(&WriteTransaction) -> Result<T, StoreError>,
) -> Result<T, StoreError> {
    let mut transaction = db
        .begin_write()
        .map_err(|error| StoreError::Database(error.to_string()))?;
    transaction
        .set_durability(Durability::Immediate)
        .map_err(|error| StoreError::Database(error.to_string()))?;
    transaction.set_two_phase_commit(true);
    let value = operation(&transaction)?;
    transaction
        .commit()
        .map_err(|error| StoreError::Database(error.to_string()))?;
    Ok(value)
}

fn write_meta(transaction: &WriteTransaction, state: &MetaState) -> Result<(), StoreError> {
    let value = encode(state)?;
    let mut table = transaction
        .open_table(META)
        .map_err(|error| StoreError::Database(error.to_string()))?;
    table
        .insert("state", value.as_slice())
        .map_err(|error| StoreError::Database(error.to_string()))?;
    Ok(())
}

fn set_clean_shutdown(db: &Database, clean_shutdown: bool) -> Result<(), StoreError> {
    commit(db, |transaction| {
        let table = transaction
            .open_table(META)
            .map_err(|error| StoreError::Database(error.to_string()))?;
        let value = table
            .get("state")
            .map_err(|error| StoreError::Database(error.to_string()))?
            .ok_or_else(|| StoreError::Schema("meta_v1/state is missing".to_string()))?;
        let mut state: MetaState = decode(value.value())?;
        drop(value);
        drop(table);
        state.clean_shutdown = clean_shutdown;
        write_meta(transaction, &state)
    })
}

fn initialise(db: &Database, config_digest: String) -> Result<(), StoreError> {
    commit(db, |transaction| {
        for table in [object_table(Slot::A), object_table(Slot::B)] {
            drop(
                transaction
                    .open_table(table)
                    .map_err(|error| StoreError::Database(error.to_string()))?,
            );
        }
        for table in [repo_table(Slot::A), repo_table(Slot::B)] {
            drop(
                transaction
                    .open_table(table)
                    .map_err(|error| StoreError::Database(error.to_string()))?,
            );
        }
        for table in [
            maven_prefix_stats_table(Slot::A),
            maven_prefix_stats_table(Slot::B),
        ] {
            drop(
                transaction
                    .open_table(table)
                    .map_err(|error| StoreError::Database(error.to_string()))?,
            );
        }
        for table in [npm_package_table(Slot::A), npm_package_table(Slot::B)] {
            drop(
                transaction
                    .open_table(table)
                    .map_err(|error| StoreError::Database(error.to_string()))?,
            );
        }
        for table in [npm_version_table(Slot::A), npm_version_table(Slot::B)] {
            drop(
                transaction
                    .open_table(table)
                    .map_err(|error| StoreError::Database(error.to_string()))?,
            );
        }
        for table in [
            npm_observation_table(Slot::A),
            npm_observation_table(Slot::B),
        ] {
            drop(
                transaction
                    .open_table(table)
                    .map_err(|error| StoreError::Database(error.to_string()))?,
            );
        }
        drop(
            transaction
                .open_table(CHANGES)
                .map_err(|error| StoreError::Database(error.to_string()))?,
        );
        drop(
            transaction
                .open_table(ENTITY_SEQUENCES)
                .map_err(|error| StoreError::Database(error.to_string()))?,
        );
        write_meta(transaction, &MetaState::new(config_digest))
    })
}

enum WriterCommand {
    PutMavenObjects {
        slot: Slot,
        rows: Vec<(Vec<u8>, StoredObject)>,
        encoded_bytes: usize,
        reply: oneshot::Sender<Result<(), StoreError>>,
    },
    PutNpmObjects {
        slot: Slot,
        rows: Vec<(Vec<u8>, StoredObject)>,
        encoded_bytes: usize,
        reply: oneshot::Sender<Result<(), StoreError>>,
    },
    PutRepos {
        slot: Slot,
        rows: Vec<(Vec<u8>, StoredRepo)>,
        encoded_bytes: usize,
        reply: oneshot::Sender<Result<(), StoreError>>,
    },
    PutMavenPrefixStats {
        slot: Slot,
        rows: Vec<(Vec<u8>, MavenPrefixStats)>,
        encoded_bytes: usize,
        reply: oneshot::Sender<Result<(), StoreError>>,
    },
    PutNpmPackages {
        slot: Slot,
        rows: Vec<(Vec<u8>, StoredNpmPackage)>,
        encoded_bytes: usize,
        reply: oneshot::Sender<Result<(), StoreError>>,
    },
    PutNpmVersions {
        slot: Slot,
        rows: Vec<(Vec<u8>, StoredNpmVersion)>,
        encoded_bytes: usize,
        reply: oneshot::Sender<Result<(), StoreError>>,
    },
    ClearSlotChunk {
        slot: Slot,
        reply: oneshot::Sender<Result<bool, StoreError>>,
    },
    RegisterChange {
        event: ChangeEvent,
        reply: oneshot::Sender<Result<u64, StoreError>>,
    },
    Flip {
        slot: Slot,
        fence: u64,
        completeness: RegistryCompleteness,
        totals: RegistryTotals,
        config_digest: String,
        reply: oneshot::Sender<Result<MetaState, StoreError>>,
    },
    ApplyIncremental {
        sequence: u64,
        update: IncrementalUpdate,
        reply: oneshot::Sender<Result<MetaState, StoreError>>,
    },
    ApplyShadow {
        slot: Slot,
        update: IncrementalUpdate,
        reply: oneshot::Sender<Result<TotalsDelta, StoreError>>,
    },
    Shutdown {
        mark_clean: bool,
        reply: oneshot::Sender<Result<(), StoreError>>,
    },
}

pub struct PersistentIndex {
    path: PathBuf,
    db: Arc<Database>,
    /// True only when the isolated child proved that this exact database was
    /// durably closed cleanly. `open()` immediately arms the next unclean
    /// fence, so this startup fact must live outside MetaState.
    startup_clean: bool,
    writer: mpsc::Sender<WriterCommand>,
    writer_healthy: Arc<AtomicBool>,
    writer_join: parking_lot::Mutex<Option<std::thread::JoinHandle<()>>>,
    reads: Arc<Semaphore>,
}

impl PersistentIndex {
    /// Run redb's open/schema path and any required integrity scan outside the
    /// long-lived server process.
    ///
    /// A damaged file can currently abort inside redb. The parent therefore
    /// classifies the child exit and only quarantines outcomes that prove a
    /// corrupt/incompatible derived database. A timed-out child is explicitly
    /// reaped and its derived file is preserved before a fresh S3 reseed;
    /// generic I/O failures and writer overlap remain fail-closed in place.
    pub async fn open_after_child_preflight(
        path: impl AsRef<Path>,
        config_digest: String,
    ) -> Result<Arc<Self>, StoreError> {
        let path = path.as_ref().to_path_buf();
        let outcome = run_preflight_child(&path).await?;
        let startup_clean = matches!(
            outcome,
            ParentPreflight::Healthy {
                full_integrity: false
            }
        );
        prepare_database_after_preflight(&path, outcome)?;
        Self::open_with_startup_state(path, config_digest, startup_clean)
    }

    /// Open a preflighted database or initialise a missing/empty file.
    #[cfg(test)]
    pub fn open(path: impl AsRef<Path>, config_digest: String) -> Result<Arc<Self>, StoreError> {
        Self::open_with_startup_state(path, config_digest, false)
    }

    fn open_with_startup_state(
        path: impl AsRef<Path>,
        config_digest: String,
        startup_clean: bool,
    ) -> Result<Arc<Self>, StoreError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let is_new = std::fs::metadata(&path).map_or(true, |metadata| metadata.len() == 0);
        let mut builder = redb::Builder::new();
        builder
            .set_cache_size(CACHE_BYTES)
            .set_repair_callback(|session| {
                tracing::warn!(
                    progress = session.progress(),
                    "redb crash recovery in progress"
                );
            });
        let db = if is_new {
            builder.create(&path)?
        } else {
            builder.open(&path)?
        };
        let current_marker_clean = if is_new {
            initialise(&db, config_digest)?;
            false
        } else {
            let state = read_meta(&db)?;
            let current_marker_clean = state.clean_shutdown;
            if state.config_digest != config_digest {
                tracing::warn!(
                    stored_digest = %state.config_digest,
                    current_digest = %config_digest,
                    "index topology/config changed; reconciliation required"
                );
            }
            // Arm the unclean-start fence before the writer can acknowledge a
            // mutation. A crash after this durable commit takes the isolated
            // full-integrity preflight on the next boot.
            set_clean_shutdown(&db, false)?;
            current_marker_clean
        };

        let db = Arc::new(db);
        let (writer, receiver) = mpsc::channel(WRITER_QUEUE_CAPACITY);
        let writer_healthy = Arc::new(AtomicBool::new(true));
        let actor_db = Arc::clone(&db);
        let actor_health = Arc::clone(&writer_healthy);
        let writer_join = std::thread::Builder::new()
            .name("nora-redb-writer".to_string())
            .spawn(move || writer_loop(actor_db, receiver, actor_health))?;

        Ok(Arc::new(Self {
            path,
            db,
            // The child result alone is insufficient: another opener can
            // re-arm the file between child exit and this parent open. Trust a
            // clean start only when the marker we just read from the currently
            // opened database agrees with the child proof.
            startup_clean: startup_clean && current_marker_clean,
            writer,
            writer_healthy,
            writer_join: parking_lot::Mutex::new(Some(writer_join)),
            reads: Arc::new(Semaphore::new(READ_CONCURRENCY)),
        }))
    }

    /// Whether the isolated startup preflight observed a durable clean close
    /// for this exact file before the long-lived writer armed it again.
    pub fn startup_clean(&self) -> bool {
        self.startup_clean
    }

    #[cfg(test)]
    pub(crate) fn open_with_startup_state_for_test(
        path: impl AsRef<Path>,
        config_digest: String,
        startup_clean: bool,
    ) -> Result<Arc<Self>, StoreError> {
        Self::open_with_startup_state(path, config_digest, startup_clean)
    }

    pub fn writer_healthy(&self) -> bool {
        self.writer_healthy.load(Ordering::Acquire)
    }

    /// Require enough free space for a second full metadata generation before
    /// touching the inactive slot. The PVC production sizing gate remains
    /// stricter (4x measured working set); this is the runtime fail-closed
    /// guard against ENOSPC during a shadow build.
    pub fn admit_shadow_build(&self) -> Result<(), StoreError> {
        let current = std::fs::metadata(&self.path).map_or(0, |metadata| metadata.len());
        let required = current.max(64 * 1024 * 1024);
        let available = available_bytes(self.path.parent().unwrap_or_else(|| Path::new(".")))?;
        if available < required {
            return Err(StoreError::DiskAdmission(format!(
                "available_bytes={available} required_bytes={required}"
            )));
        }
        Ok(())
    }

    pub async fn meta(&self) -> Result<MetaState, StoreError> {
        let permit = self
            .reads
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| StoreError::WriterUnavailable)?;
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            read_meta(&db)
        })
        .await
        .map_err(|error| StoreError::Database(error.to_string()))?
    }

    fn check_batch<T: Serialize>(rows: &[(Vec<u8>, T)]) -> Result<usize, StoreError> {
        if rows.len() > MAX_TX_ROWS {
            return Err(StoreError::TransactionTooLarge);
        }
        let mut bytes = 0usize;
        for (key, value) in rows {
            bytes = bytes.saturating_add(encoded_row_bytes(key, value)?);
            if bytes > MAX_TX_BYTES {
                return Err(StoreError::TransactionTooLarge);
            }
        }
        Ok(bytes)
    }

    async fn send<T>(
        &self,
        make: impl FnOnce(oneshot::Sender<Result<T, StoreError>>) -> WriterCommand,
    ) -> Result<T, StoreError> {
        if !self.writer_healthy() {
            return Err(StoreError::WriterUnavailable);
        }
        let (reply, receive) = oneshot::channel();
        tokio::time::timeout(WRITER_COMMAND_TIMEOUT, async {
            self.writer
                .send(make(reply))
                .await
                .map_err(|_| StoreError::WriterUnavailable)?;
            receive.await.map_err(|_| StoreError::WriterUnavailable)?
        })
        .await
        .map_err(|_| StoreError::WriterUnavailable)?
    }

    /// Enqueue a physical invalidation without coupling an acknowledged S3
    /// mutation to redb/PVC latency. The bounded actor queue is the admission
    /// control; callers retain an in-memory global-dirty flag when it is full.
    pub fn try_register_change(
        &self,
        event: ChangeEvent,
    ) -> Result<oneshot::Receiver<Result<u64, StoreError>>, StoreError> {
        if !self.writer_healthy() {
            return Err(StoreError::WriterUnavailable);
        }
        let (reply, receive) = oneshot::channel();
        self.writer
            .try_send(WriterCommand::RegisterChange { event, reply })
            .map_err(|_| StoreError::WriterUnavailable)?;
        Ok(receive)
    }

    pub async fn put_maven_objects(
        &self,
        slot: Slot,
        rows: Vec<(Vec<u8>, StoredObject)>,
    ) -> Result<(), StoreError> {
        let encoded_bytes = Self::check_batch(&rows)?;
        self.send(|reply| WriterCommand::PutMavenObjects {
            slot,
            rows,
            encoded_bytes,
            reply,
        })
        .await
    }

    pub async fn put_npm_objects(
        &self,
        slot: Slot,
        rows: Vec<(Vec<u8>, StoredObject)>,
    ) -> Result<(), StoreError> {
        let encoded_bytes = Self::check_batch(&rows)?;
        self.send(|reply| WriterCommand::PutNpmObjects {
            slot,
            rows,
            encoded_bytes,
            reply,
        })
        .await
    }

    pub async fn put_repos(
        &self,
        slot: Slot,
        rows: Vec<(Vec<u8>, StoredRepo)>,
    ) -> Result<(), StoreError> {
        let encoded_bytes = Self::check_batch(&rows)?;
        self.send(|reply| WriterCommand::PutRepos {
            slot,
            rows,
            encoded_bytes,
            reply,
        })
        .await
    }

    pub async fn put_npm_packages(
        &self,
        slot: Slot,
        rows: Vec<(Vec<u8>, StoredNpmPackage)>,
    ) -> Result<(), StoreError> {
        let encoded_bytes = Self::check_batch(&rows)?;
        self.send(|reply| WriterCommand::PutNpmPackages {
            slot,
            rows,
            encoded_bytes,
            reply,
        })
        .await
    }

    pub async fn put_npm_versions(
        &self,
        slot: Slot,
        rows: Vec<(Vec<u8>, StoredNpmVersion)>,
    ) -> Result<(), StoreError> {
        let encoded_bytes = Self::check_batch(&rows)?;
        self.send(|reply| WriterCommand::PutNpmVersions {
            slot,
            rows,
            encoded_bytes,
            reply,
        })
        .await
    }

    pub async fn put_maven_prefix_stats(
        &self,
        slot: Slot,
        rows: Vec<(Vec<u8>, MavenPrefixStats)>,
    ) -> Result<(), StoreError> {
        let encoded_bytes = Self::check_batch(&rows)?;
        self.send(|reply| WriterCommand::PutMavenPrefixStats {
            slot,
            rows,
            encoded_bytes,
            reply,
        })
        .await
    }

    pub async fn clear_slot(&self, slot: Slot) -> Result<(), StoreError> {
        loop {
            let more = self
                .send(|reply| WriterCommand::ClearSlotChunk { slot, reply })
                .await?;
            if !more {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
    }

    pub async fn register_change(&self, event: ChangeEvent) -> Result<u64, StoreError> {
        self.send(|reply| WriterCommand::RegisterChange { event, reply })
            .await
    }

    pub async fn apply_incremental(
        &self,
        sequence: u64,
        update: IncrementalUpdate,
    ) -> Result<MetaState, StoreError> {
        let encoded = serde_json::to_vec(&update)?;
        if encoded.len() > MAX_TX_BYTES {
            return Err(StoreError::TransactionTooLarge);
        }
        self.send(|reply| WriterCommand::ApplyIncremental {
            sequence,
            update,
            reply,
        })
        .await
    }

    pub async fn apply_shadow(
        &self,
        slot: Slot,
        update: IncrementalUpdate,
    ) -> Result<TotalsDelta, StoreError> {
        let encoded = serde_json::to_vec(&update)?;
        if encoded.len() > MAX_TX_BYTES {
            return Err(StoreError::TransactionTooLarge);
        }
        self.send(|reply| WriterCommand::ApplyShadow {
            slot,
            update,
            reply,
        })
        .await
    }

    pub async fn pending_changes(
        &self,
        after: u64,
        through: u64,
    ) -> Result<Vec<(u64, ChangeEvent)>, StoreError> {
        self.read(move |transaction, _| {
            let table = transaction
                .open_table(CHANGES)
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let rows = table
                .range((
                    std::ops::Bound::Excluded(after),
                    std::ops::Bound::Included(through),
                ))
                .map_err(|error| StoreError::Database(error.to_string()))?
                .take(MAX_TX_ROWS + 1)
                .map(|entry| {
                    let (sequence, value) =
                        entry.map_err(|error| StoreError::Database(error.to_string()))?;
                    Ok((sequence.value(), decode::<ChangeEvent>(value.value())?))
                })
                .collect::<Result<Vec<_>, StoreError>>()?;
            if rows.len() > MAX_TX_ROWS {
                return Err(StoreError::TransactionTooLarge);
            }
            Ok(rows)
        })
        .await
    }

    pub async fn flip(
        &self,
        slot: Slot,
        fence: u64,
        completeness: RegistryCompleteness,
        totals: RegistryTotals,
        config_digest: String,
    ) -> Result<MetaState, StoreError> {
        let state = self
            .send(|reply| WriterCommand::Flip {
                slot,
                fence,
                completeness,
                totals,
                config_digest,
                reply,
            })
            .await?;

        // A timed-out predecessor is diagnostic evidence only until S3 has
        // produced and atomically published a complete replacement. Retire it
        // after that durability boundary so repeated timeouts cannot consume
        // the PVC without bound. Cleanup failure cannot roll back the already
        // published derived generation; the next disk-admission check remains
        // fail-closed and accounts for the retained bytes.
        if prune_timeout_evidence(&self.path).is_err() {
            tracing::warn!(
                "could not retire preserved preflight-timeout evidence after reconciliation"
            );
        }
        Ok(state)
    }

    pub async fn shutdown(&self) {
        self.shutdown_until_mode(tokio::time::Instant::now() + WRITER_COMMAND_TIMEOUT, true)
            .await;
    }

    pub async fn shutdown_until(&self, deadline: tokio::time::Instant) {
        self.shutdown_until_mode(deadline, true).await;
    }

    pub async fn shutdown_unclean_until(&self, deadline: tokio::time::Instant) {
        self.shutdown_until_mode(deadline, false).await;
    }

    async fn shutdown_until_mode(&self, deadline: tokio::time::Instant, mark_clean: bool) {
        self.writer_healthy.store(false, Ordering::Release);
        let (reply, receive) = oneshot::channel();
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if tokio::time::timeout(
            remaining,
            self.writer
                .send(WriterCommand::Shutdown { mark_clean, reply }),
        )
        .await
        .is_ok_and(|result| result.is_ok())
        {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if !tokio::time::timeout(remaining, receive)
                .await
                .is_ok_and(|result| result.is_ok_and(|result| result.is_ok()))
            {
                tracing::error!("redb clean-shutdown marker was not durably committed");
            }
        }
        let join = self.writer_join.lock().take();
        if let Some(join) = join {
            while !join.is_finished() && tokio::time::Instant::now() < deadline {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            if join.is_finished() {
                if join.join().is_err() {
                    tracing::error!("redb writer thread panicked during shutdown");
                }
            } else {
                // Dropping a std JoinHandle detaches it. The receiver exits as
                // soon as the shutdown command or channel close is observed;
                // unlike spawn_blocking(join), no unabortable Tokio task remains.
                tracing::error!("redb writer thread did not stop within the shared 30s deadline");
            }
        }
    }

    #[cfg(test)]
    async fn shutdown_unclean_for_test(&self) {
        self.shutdown_until_mode(tokio::time::Instant::now() + WRITER_COMMAND_TIMEOUT, false)
            .await;
    }

    pub async fn get_object_in_slot(
        &self,
        slot: Slot,
        key: &str,
    ) -> Result<Option<FileMeta>, StoreError> {
        let key = key.as_bytes().to_vec();
        self.read(move |transaction, _| {
            let table = transaction
                .open_table(object_table(slot))
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let value = table
                .get(key.as_slice())
                .map_err(|error| StoreError::Database(error.to_string()))?;
            value
                .map(|value| decode::<StoredObject>(value.value()).map(|stored| stored.meta))
                .transpose()
        })
        .await
    }

    /// Read one bounded ordered page from a slot's raw object inventory.
    pub async fn scan_objects(
        &self,
        slot: Slot,
        prefix: Vec<u8>,
        after: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<(Vec<(String, FileMeta)>, Option<Vec<u8>>), StoreError> {
        if limit == 0 || limit > MAX_TX_ROWS {
            return Err(StoreError::TransactionTooLarge);
        }
        self.read(move |transaction, _| {
            let end = prefix_successor(&prefix);
            let start = after.as_deref().map_or(
                std::ops::Bound::Included(prefix.as_slice()),
                std::ops::Bound::Excluded,
            );
            let table = transaction
                .open_table(object_table(slot))
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let range = table
                .range::<&[u8]>((start, std::ops::Bound::Excluded(end.as_slice())))
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let mut rows = Vec::with_capacity(limit);
            let mut last = None;
            let mut has_more = false;
            for entry in range {
                if rows.len() == limit {
                    has_more = true;
                    break;
                }
                let (key, value) =
                    entry.map_err(|error| StoreError::Database(error.to_string()))?;
                let key = key.value().to_vec();
                let key_text = String::from_utf8(key.clone())
                    .map_err(|_| StoreError::Schema("non-UTF-8 storage key".to_string()))?;
                let stored: StoredObject = decode(value.value())?;
                rows.push((key_text, stored.meta));
                last = Some(key);
            }
            Ok((rows, has_more.then_some(last).flatten()))
        })
        .await
    }

    /// Read exact root aggregates for configured logical repositories. A
    /// missing row remains unavailable; an explicit zero row is an exact empty
    /// repository and must not be conflated with warming/incomplete state.
    pub async fn maven_repository_rows(
        &self,
        repositories: Vec<String>,
    ) -> Result<(Vec<RepoInfo>, u64), StoreError> {
        self.read(move |transaction, state| {
            let Some(slot) = state.active_slot else {
                return Ok((
                    repositories
                        .into_iter()
                        .map(|name| RepoInfo {
                            name,
                            versions: 0,
                            size: 0,
                            size_available: false,
                            updated: "N/A".to_string(),
                            is_file: false,
                        })
                        .collect(),
                    state.generation,
                ));
            };
            let table = transaction
                .open_table(maven_prefix_stats_table(slot))
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let mut rows = Vec::with_capacity(repositories.len());
            for name in repositories {
                let stats = table
                    .get(maven_prefix_stats_key(&name, "").as_slice())
                    .map_err(|error| StoreError::Database(error.to_string()))?
                    .map(|value| decode::<MavenPrefixStats>(value.value()))
                    .transpose()?;
                rows.push(RepoInfo {
                    name,
                    versions: stats
                        .map(|stats| usize::try_from(stats.subtree_files).unwrap_or(usize::MAX))
                        .unwrap_or(0),
                    size: stats.map_or(0, |stats| stats.subtree_bytes),
                    size_available: stats.is_some() && state.completeness.maven,
                    updated: "N/A".to_string(),
                    is_file: false,
                });
            }
            Ok((rows, state.generation))
        })
        .await
    }

    /// Read immediate Maven child directories with prefix seeks. Each member
    /// contributes at most `limit + 1` child names; group members are merged in
    /// configured order and exact subtree aggregates come from the same active
    /// A/B generation.
    pub async fn maven_children_page(
        &self,
        repository: String,
        prefixes: Vec<String>,
        logical_path: String,
        after: Option<String>,
        limit: usize,
    ) -> Result<(Vec<RepoInfo>, Option<String>, u64, bool), StoreError> {
        if limit == 0 || limit > 100 {
            return Err(StoreError::TransactionTooLarge);
        }
        self.read(move |transaction, state| {
            let Some(slot) = state.active_slot else {
                return Ok((Vec::new(), None, state.generation, false));
            };
            let table = transaction
                .open_table(object_table(slot))
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let stats_table = transaction
                .open_table(maven_prefix_stats_table(slot))
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let mut children = std::collections::BTreeMap::<String, usize>::new();
            let mut has_direct_files = false;
            for (member_order, base) in prefixes.iter().enumerate() {
                let full_prefix = if logical_path.is_empty() {
                    base.clone()
                } else {
                    format!("{}{}/", base, logical_path.trim_matches('/'))
                };
                let end = prefix_successor(full_prefix.as_bytes());
                let mut start = after.as_ref().map_or_else(
                    || (full_prefix.as_bytes().to_vec(), true),
                    |child| {
                        (
                            prefix_successor(format!("{full_prefix}{child}/").as_bytes()),
                            true,
                        )
                    },
                );
                let mut member_children = 0usize;
                let mut examined = 0usize;
                while member_children <= limit {
                    if examined == MAX_TX_ROWS {
                        return Err(StoreError::TransactionTooLarge);
                    }
                    let lower = if start.1 {
                        std::ops::Bound::Included(start.0.as_slice())
                    } else {
                        std::ops::Bound::Excluded(start.0.as_slice())
                    };
                    let mut range = table
                        .range::<&[u8]>((lower, std::ops::Bound::Excluded(end.as_slice())))
                        .map_err(|error| StoreError::Database(error.to_string()))?;
                    let Some(entry) = range.next() else { break };
                    let (key, _) =
                        entry.map_err(|error| StoreError::Database(error.to_string()))?;
                    let key_bytes = key.value().to_vec();
                    let key_text = std::str::from_utf8(&key_bytes)
                        .map_err(|_| StoreError::Schema("non-UTF-8 storage key".to_string()))?;
                    let Some(rest) = key_text.strip_prefix(&full_prefix) else {
                        return Err(StoreError::Schema(
                            "Maven prefix seek escaped its range".to_string(),
                        ));
                    };
                    examined = examined.saturating_add(1);
                    if let Some((child, _)) = rest.split_once('/') {
                        children.entry(child.to_string()).or_insert(member_order);
                        member_children = member_children.saturating_add(1);
                        start = (
                            prefix_successor(format!("{full_prefix}{child}/").as_bytes()),
                            true,
                        );
                    } else {
                        has_direct_files = true;
                        start = (key_bytes, false);
                    }
                }
            }
            let has_more = children.len() > limit;
            let names = children.into_keys().take(limit).collect::<Vec<_>>();
            let next = has_more.then(|| names.last().cloned()).flatten();
            let rows = names
                .into_iter()
                .map(|name| {
                    let child_path = if logical_path.is_empty() {
                        name.clone()
                    } else {
                        format!("{}/{name}", logical_path.trim_matches('/'))
                    };
                    let stats = stats_table
                        .get(maven_prefix_stats_key(&repository, &child_path).as_slice())
                        .map_err(|error| StoreError::Database(error.to_string()))?
                        .map(|value| decode::<MavenPrefixStats>(value.value()))
                        .transpose()?;
                    Ok(RepoInfo {
                        name,
                        versions: stats
                            .map(|stats| usize::try_from(stats.subtree_files).unwrap_or(usize::MAX))
                            .unwrap_or(0),
                        size: stats.map_or(0, |stats| stats.subtree_bytes),
                        size_available: stats.is_some() && state.completeness.maven,
                        updated: "N/A".to_string(),
                        is_file: false,
                    })
                })
                .collect::<Result<Vec<_>, StoreError>>()?;
            Ok((rows, next, state.generation, has_direct_files))
        })
        .await
    }

    /// Read direct files below one Maven logical directory. Group members are
    /// merged in configured order, so an earlier member wins when two members
    /// contain the same filename. Descendant directories are skipped with a
    /// prefix jump instead of being materialized.
    pub async fn maven_files_page(
        &self,
        prefixes: Vec<String>,
        logical_path: String,
        after: Option<String>,
        limit: usize,
    ) -> Result<(Vec<(String, FileMeta)>, Option<String>, u64), StoreError> {
        if limit == 0 || limit > 100 {
            return Err(StoreError::TransactionTooLarge);
        }
        self.read(move |transaction, state| {
            let Some(slot) = state.active_slot else {
                return Ok((Vec::new(), None, state.generation));
            };
            let table = transaction
                .open_table(object_table(slot))
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let mut files = std::collections::BTreeMap::<String, (usize, FileMeta)>::new();
            for (member_order, base) in prefixes.iter().enumerate() {
                let full_prefix = if logical_path.is_empty() {
                    base.clone()
                } else {
                    format!("{}{}/", base, logical_path.trim_matches('/'))
                };
                let end = prefix_successor(full_prefix.as_bytes());
                let mut start = after.as_ref().map_or_else(
                    || (full_prefix.as_bytes().to_vec(), true),
                    |filename| (format!("{full_prefix}{filename}").into_bytes(), false),
                );
                let mut member_files = 0usize;
                let mut examined = 0usize;
                while member_files <= limit {
                    if examined == MAX_TX_ROWS {
                        return Err(StoreError::TransactionTooLarge);
                    }
                    let lower = if start.1 {
                        std::ops::Bound::Included(start.0.as_slice())
                    } else {
                        std::ops::Bound::Excluded(start.0.as_slice())
                    };
                    let mut range = table
                        .range::<&[u8]>((lower, std::ops::Bound::Excluded(end.as_slice())))
                        .map_err(|error| StoreError::Database(error.to_string()))?;
                    let Some(entry) = range.next() else { break };
                    let (key, value) =
                        entry.map_err(|error| StoreError::Database(error.to_string()))?;
                    let key_bytes = key.value().to_vec();
                    let key_text = std::str::from_utf8(&key_bytes)
                        .map_err(|_| StoreError::Schema("non-UTF-8 storage key".to_string()))?;
                    let Some(rest) = key_text.strip_prefix(&full_prefix) else {
                        return Err(StoreError::Schema(
                            "Maven file seek escaped its range".to_string(),
                        ));
                    };
                    examined = examined.saturating_add(1);
                    if let Some((child, _)) = rest.split_once('/') {
                        start = (
                            prefix_successor(format!("{full_prefix}{child}/").as_bytes()),
                            true,
                        );
                        continue;
                    }
                    let stored: StoredObject = decode(value.value())?;
                    files
                        .entry(rest.to_string())
                        .or_insert((member_order, stored.meta));
                    member_files = member_files.saturating_add(1);
                    start = (key_bytes, false);
                }
            }
            let has_more = files.len() > limit;
            let rows = files
                .into_iter()
                .take(limit)
                .map(|(name, (_, meta))| (name, meta))
                .collect::<Vec<_>>();
            let next = has_more
                .then(|| rows.last().map(|(name, _)| name.clone()))
                .flatten();
            Ok((rows, next, state.generation))
        })
        .await
    }

    pub async fn scan_npm_observations(
        &self,
        slot: Slot,
        after: Option<Vec<u8>>,
        limit: usize,
    ) -> Result<(Vec<NpmAuthorityObservation>, Option<Vec<u8>>), StoreError> {
        if limit == 0 || limit > MAX_TX_ROWS {
            return Err(StoreError::TransactionTooLarge);
        }
        self.read(move |transaction, _| {
            let table = transaction
                .open_table(npm_observation_table(slot))
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let start = after
                .as_deref()
                .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded);
            let range = table
                .range::<&[u8]>((start, std::ops::Bound::Unbounded))
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let mut rows = Vec::with_capacity(limit);
            let mut last = None;
            let mut has_more = false;
            for entry in range {
                if rows.len() == limit {
                    has_more = true;
                    break;
                }
                let (key, value) =
                    entry.map_err(|error| StoreError::Database(error.to_string()))?;
                last = Some(key.value().to_vec());
                rows.push(decode(value.value())?);
            }
            Ok((rows, has_more.then_some(last).flatten()))
        })
        .await
    }

    #[cfg(test)]
    pub async fn list_repos(
        &self,
        registry: &str,
        after: Option<Vec<u8>>,
        limit: usize,
        max_examined: usize,
        deadline: std::time::Duration,
    ) -> Result<(Vec<RepoInfo>, Option<Vec<u8>>, u64), StoreError> {
        let registry = registry.to_string();
        self.read(move |transaction, state| {
            let Some(slot) = state.active_slot else {
                return Ok((Vec::new(), None, state.generation));
            };
            let prefix = format!("{registry}\0").into_bytes();
            let end = prefix_successor(&prefix);
            let start = after.as_deref().map_or(
                std::ops::Bound::Included(prefix.as_slice()),
                std::ops::Bound::Excluded,
            );
            let table = transaction
                .open_table(repo_table(slot))
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let mut range = table
                .range::<&[u8]>((start, std::ops::Bound::Excluded(end.as_slice())))
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let started = std::time::Instant::now();
            let mut rows = Vec::with_capacity(limit.min(100));
            let mut last = None;
            for examined in 0..max_examined {
                if examined % 64 == 0 && started.elapsed() >= deadline {
                    break;
                }
                let Some(entry) = range.next() else { break };
                let (key, value) =
                    entry.map_err(|error| StoreError::Database(error.to_string()))?;
                let key = key.value().to_vec();
                rows.push(decode::<StoredRepo>(value.value())?.into_repo_info());
                last = Some(key);
                if rows.len() == limit {
                    break;
                }
            }
            Ok((rows, last, state.generation))
        })
        .await
    }

    pub async fn query_repos(
        &self,
        query: RepoQuery,
    ) -> Result<(Vec<RepoInfo>, Option<Vec<u8>>, u64), StoreError> {
        let RepoQuery {
            registry,
            after,
            filter,
            limit,
            max_examined,
            deadline,
            name_prefix,
            before_name,
            allowed_repositories,
        } = query;
        if limit == 0 || limit > 100 || max_examined == 0 || max_examined > MAX_QUERY_EXAMINED {
            return Err(StoreError::TransactionTooLarge);
        }
        let filter = filter.map(|value| value.to_lowercase());
        self.read(move |transaction, state| {
            let Some(slot) = state.active_slot else {
                return Ok((Vec::new(), None, state.generation));
            };
            let registry_prefix = format!("{registry}\0").into_bytes();
            let prefix = name_prefix.as_ref().map_or_else(
                || registry_prefix.clone(),
                |name| {
                    let mut key = registry_prefix.clone();
                    key.extend_from_slice(name.as_bytes());
                    key
                },
            );
            let end = before_name.as_ref().map_or_else(
                || prefix_successor(&prefix),
                |name| {
                    let mut key = registry_prefix.clone();
                    key.extend_from_slice(name.as_bytes());
                    key
                },
            );
            let start = after.as_deref().map_or(
                std::ops::Bound::Included(prefix.as_slice()),
                std::ops::Bound::Excluded,
            );
            let table = transaction
                .open_table(repo_table(slot))
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let range = table
                .range::<&[u8]>((start, std::ops::Bound::Excluded(end.as_slice())))
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let started = std::time::Instant::now();
            let mut rows = Vec::with_capacity(limit);
            let mut last_examined = None;
            let mut has_more = false;
            for (examined, entry) in range.enumerate() {
                if examined >= max_examined || (examined % 64 == 0 && started.elapsed() >= deadline)
                {
                    has_more = true;
                    break;
                }
                let (key, value) =
                    entry.map_err(|error| StoreError::Database(error.to_string()))?;
                if rows.len() == limit {
                    has_more = true;
                    break;
                }
                last_examined = Some(key.value().to_vec());
                let stored = decode::<StoredRepo>(value.value())?;
                if allowed_repositories.as_ref().is_some_and(|allowed| {
                    let repository = stored
                        .name
                        .strip_prefix("repositories/")
                        .and_then(|name| name.split('/').next());
                    repository.is_none_or(|repository| {
                        !allowed.iter().any(|allowed| allowed == repository)
                    })
                }) {
                    continue;
                }
                if filter
                    .as_deref()
                    .is_some_and(|needle| !stored.name.to_lowercase().contains(needle))
                {
                    continue;
                }
                rows.push(stored.into_repo_info());
            }
            Ok((
                rows,
                has_more.then_some(last_examined).flatten(),
                state.generation,
            ))
        })
        .await
    }

    #[cfg(test)]
    pub async fn get_npm_package(
        &self,
        repository: &str,
        package: &str,
    ) -> Result<(Option<StoredNpmPackage>, u64), StoreError> {
        let key = npm_package_key(repository, package);
        self.read(move |transaction, state| {
            let Some(slot) = state.active_slot else {
                return Ok((None, state.generation));
            };
            let table = transaction
                .open_table(npm_package_table(slot))
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let value = table
                .get(key.as_slice())
                .map_err(|error| StoreError::Database(error.to_string()))?;
            Ok((
                value
                    .map(|value| decode::<StoredNpmPackage>(value.value()))
                    .transpose()?,
                state.generation,
            ))
        })
        .await
    }

    /// Return a package and one keyset page of versions from the same redb
    /// read transaction. The generation and both tables therefore describe
    /// one MVCC snapshot and cannot be mixed across an incremental update.
    pub async fn npm_package_page(
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
        ),
        StoreError,
    > {
        if limit == 0 || limit > 100 {
            return Err(StoreError::TransactionTooLarge);
        }
        let package_key = npm_package_key(repository, package);
        let mut version_prefix = package_key.clone();
        version_prefix.push(0);
        if after
            .as_ref()
            .is_some_and(|key| !key.starts_with(&version_prefix))
        {
            return Err(StoreError::Schema(
                "npm version cursor does not match package".to_string(),
            ));
        }
        self.read(move |transaction, state| {
            let Some(slot) = state.active_slot else {
                return Ok((None, Vec::new(), None, state.generation));
            };
            let packages = transaction
                .open_table(npm_package_table(slot))
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let package = packages
                .get(package_key.as_slice())
                .map_err(|error| StoreError::Database(error.to_string()))?
                .map(|value| decode::<StoredNpmPackage>(value.value()))
                .transpose()?;
            let versions = transaction
                .open_table(npm_version_table(slot))
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let end = prefix_successor(&version_prefix);
            let start = after.as_deref().map_or(
                std::ops::Bound::Included(version_prefix.as_slice()),
                std::ops::Bound::Excluded,
            );
            let range = versions
                .range::<&[u8]>((start, std::ops::Bound::Excluded(end.as_slice())))
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let mut rows = Vec::with_capacity(limit);
            let mut last = None;
            let mut has_more = false;
            for entry in range {
                if rows.len() == limit {
                    has_more = true;
                    break;
                }
                let (key, value) =
                    entry.map_err(|error| StoreError::Database(error.to_string()))?;
                last = Some(key.value().to_vec());
                rows.push(decode(value.value())?);
            }
            Ok((
                package,
                rows,
                has_more.then_some(last).flatten(),
                state.generation,
            ))
        })
        .await
    }

    /// Load an existing npm projection only when every recorded authoritative
    /// dependency has identical metadata in the newly staged object inventory.
    /// This turns periodic reconciliation into metadata comparisons for
    /// unchanged packages instead of repeated packument GET/parse fan-out.
    pub async fn reusable_npm_projection(
        &self,
        active: Slot,
        shadow: Slot,
        repository: &str,
        package: &str,
    ) -> Result<Option<(StoredNpmPackage, Vec<StoredNpmVersion>)>, StoreError> {
        let package_key = npm_package_key(repository, package);
        let mut version_prefix = package_key.clone();
        version_prefix.push(0);
        self.read(move |transaction, _| {
            let packages = transaction
                .open_table(npm_package_table(active))
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let Some(package) = packages
                .get(package_key.as_slice())
                .map_err(|error| StoreError::Database(error.to_string()))?
                .map(|value| decode::<StoredNpmPackage>(value.value()))
                .transpose()?
            else {
                return Ok(None);
            };
            let shadow_objects = transaction
                .open_table(object_table(shadow))
                .map_err(|error| StoreError::Database(error.to_string()))?;
            for (key, expected) in &package.dependencies {
                let observed = shadow_objects
                    .get(key.as_bytes())
                    .map_err(|error| StoreError::Database(error.to_string()))?
                    .map(|value| decode::<StoredObject>(value.value()).map(|row| row.meta))
                    .transpose()?;
                match expected {
                    Some(expected) => {
                        let has_strong_identity =
                            expected.version_id.is_some() || expected.etag.is_some();
                        if !has_strong_identity || observed.as_ref() != Some(expected) {
                            return Ok(None);
                        }
                    }
                    None if observed.is_some() => return Ok(None),
                    None => {}
                }
            }
            let versions = transaction
                .open_table(npm_version_table(active))
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let end = prefix_successor(&version_prefix);
            let mut rows = versions
                .range::<&[u8]>((
                    std::ops::Bound::Included(version_prefix.as_slice()),
                    std::ops::Bound::Excluded(end.as_slice()),
                ))
                .map_err(|error| StoreError::Database(error.to_string()))?
                .take(MAX_QUERY_EXAMINED + 1)
                .map(|entry| {
                    let (key, value) =
                        entry.map_err(|error| StoreError::Database(error.to_string()))?;
                    Ok((
                        key.value().to_vec(),
                        decode::<StoredNpmVersion>(value.value())?,
                    ))
                })
                .collect::<Result<Vec<_>, StoreError>>()?;
            if rows.len() > MAX_QUERY_EXAMINED
                || rows.len() != usize::try_from(package.versions).unwrap_or(usize::MAX)
            {
                return Ok(None);
            }
            rows.sort_by(|left, right| left.0.cmp(&right.0));
            Ok(Some((
                package,
                rows.into_iter().map(|(_, row)| row).collect(),
            )))
        })
        .await
    }

    #[cfg(test)]
    pub async fn list_npm_versions(
        &self,
        repository: &str,
        package: &str,
        limit: usize,
    ) -> Result<(Vec<StoredNpmVersion>, u64, bool), StoreError> {
        if limit == 0 || limit > MAX_QUERY_EXAMINED {
            return Err(StoreError::TransactionTooLarge);
        }
        let mut prefix = npm_package_key(repository, package);
        prefix.push(0);
        self.read(move |transaction, state| {
            let Some(slot) = state.active_slot else {
                return Ok((Vec::new(), state.generation, false));
            };
            let end = prefix_successor(&prefix);
            let table = transaction
                .open_table(npm_version_table(slot))
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let range = table
                .range::<&[u8]>((
                    std::ops::Bound::Included(prefix.as_slice()),
                    std::ops::Bound::Excluded(end.as_slice()),
                ))
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let mut rows = Vec::with_capacity(limit.min(256));
            let mut truncated = false;
            for entry in range {
                if rows.len() == limit {
                    truncated = true;
                    break;
                }
                let (_, value) = entry.map_err(|error| StoreError::Database(error.to_string()))?;
                rows.push(decode(value.value())?);
            }
            Ok((rows, state.generation, truncated))
        })
        .await
    }

    pub async fn list_npm_search_documents(
        &self,
        max_examined: usize,
    ) -> Result<(Vec<NpmSearchDocument>, u64, bool), StoreError> {
        if max_examined == 0 || max_examined > MAX_QUERY_EXAMINED {
            return Err(StoreError::TransactionTooLarge);
        }
        self.read(move |transaction, state| {
            let Some(slot) = state.active_slot else {
                return Ok((Vec::new(), state.generation, false));
            };
            let table = transaction
                .open_table(npm_package_table(slot))
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let mut documents = Vec::new();
            let mut truncated = false;
            for (examined, entry) in table
                .iter()
                .map_err(|error| StoreError::Database(error.to_string()))?
                .enumerate()
            {
                if examined == max_examined {
                    truncated = true;
                    break;
                }
                let (_, value) = entry.map_err(|error| StoreError::Database(error.to_string()))?;
                if let Some(search) = decode::<StoredNpmPackage>(value.value())?.search {
                    documents.push(search);
                }
            }
            Ok((documents, state.generation, truncated))
        })
        .await
    }

    async fn read<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&ReadTransaction, &MetaState) -> Result<T, StoreError> + Send + 'static,
    ) -> Result<T, StoreError> {
        if !self.writer_healthy() {
            return Err(StoreError::WriterUnavailable);
        }
        let permit = self
            .reads
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| StoreError::WriterUnavailable)?;
        let db = Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let transaction = db
                .begin_read()
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let state = read_meta_from(&transaction)?;
            operation(&transaction, &state)
        })
        .await
        .map_err(|error| StoreError::Database(error.to_string()))?
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ParentPreflight {
    Healthy { full_integrity: bool },
    Missing,
    TimedOut,
    CorruptOrIncompatible { reason: String },
    AlreadyOpen,
    Unavailable { reason: String },
}

fn prepare_database_after_preflight(
    path: &Path,
    outcome: ParentPreflight,
) -> Result<(), StoreError> {
    match outcome {
        ParentPreflight::Healthy { .. } | ParentPreflight::Missing => Ok(()),
        ParentPreflight::TimedOut => {
            if path.exists() {
                let preserved = preserve_timed_out_database(path)?;
                crate::metrics::INDEX_PREFLIGHT_TIMEOUT_RESEED_TOTAL.inc();
                tracing::warn!(
                    path = %path.display(),
                    preserved = %preserved.display(),
                    "preserved timed-out derived index and starting a fresh S3 reseed"
                );
            }
            Ok(())
        }
        ParentPreflight::CorruptOrIncompatible { reason } => {
            admit_reseed(path)?;
            if path.exists() {
                let quarantine = quarantine_path(path);
                std::fs::rename(path, &quarantine)?;
                tracing::warn!(
                    path = %path.display(),
                    quarantine = %quarantine.display(),
                    %reason,
                    "quarantined unusable derived index; a fresh S3 reconciliation is required"
                );
            }
            Ok(())
        }
        ParentPreflight::AlreadyOpen => Err(StoreError::AlreadyOpen),
        ParentPreflight::Unavailable { reason } => Err(StoreError::PreflightUnavailable(reason)),
    }
}

async fn run_preflight_child(path: &Path) -> Result<ParentPreflight, StoreError> {
    let started = std::time::Instant::now();
    let executable = std::env::current_exe()?;
    let mut child = tokio::process::Command::new(executable)
        .arg("index-preflight")
        .arg(path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()?;
    let outcome = wait_preflight_child(&mut child, std::time::Duration::from_secs(120)).await?;
    match &outcome {
        ParentPreflight::Healthy { full_integrity } => tracing::info!(
            elapsed_ms = started.elapsed().as_millis(),
            full_integrity,
            "redb child preflight completed"
        ),
        ParentPreflight::Missing => tracing::info!(
            elapsed_ms = started.elapsed().as_millis(),
            "redb child preflight found no existing database"
        ),
        ParentPreflight::TimedOut => tracing::warn!(
            elapsed_ms = started.elapsed().as_millis(),
            "redb child preflight exceeded the startup budget and was reaped"
        ),
        _ => {}
    }
    Ok(outcome)
}

async fn wait_preflight_child(
    child: &mut tokio::process::Child,
    timeout: std::time::Duration,
) -> Result<ParentPreflight, StoreError> {
    // CANCEL-SAFETY: Tokio's Child::wait may be polled again after this future
    // is dropped. On timeout this function retains ownership of the child and
    // immediately enters the explicit kill-and-reap path below.
    match tokio::time::timeout(timeout, child.wait()).await {
        Ok(status) => Ok(classify_preflight_status(status?)),
        Err(_) => {
            // `kill_on_drop` is only a final safety net. Reap explicitly before
            // the parent renames the file, otherwise the child could continue
            // writing through the same inode under its evidence name.
            if child.start_kill().is_err() {
                // The process may have completed in the narrow interval after
                // the timeout future was dropped. Preserve its real outcome
                // when it can be reaped; otherwise fail closed.
                return match child.try_wait()? {
                    Some(status) => Ok(classify_preflight_status(status)),
                    None => Err(StoreError::PreflightUnavailable(
                        "timed-out child could not be killed safely".to_string(),
                    )),
                };
            }
            // CANCEL-SAFETY: a reap timeout returns PreflightUnavailable and
            // therefore leaves the primary DB untouched; kill_on_drop remains
            // the final process-lifecycle safety net.
            let status = tokio::time::timeout(std::time::Duration::from_secs(5), child.wait())
                .await
                .map_err(|_| {
                    StoreError::PreflightUnavailable(
                        "timed-out child could not be reaped safely".to_string(),
                    )
                })??;
            // If the child completed on its own in the timeout/kill race,
            // preserve the precise typed result. A process terminated by our
            // kill has no recognised protocol exit code and remains TimedOut.
            if matches!(status.code(), Some(0 | 20 | 21 | 22 | 23 | 24 | 25)) {
                Ok(classify_preflight_status(status))
            } else {
                Ok(ParentPreflight::TimedOut)
            }
        }
    }
}

fn classify_preflight_status(status: std::process::ExitStatus) -> ParentPreflight {
    match status.code() {
        Some(0) => ParentPreflight::Healthy {
            full_integrity: false,
        },
        Some(20) => ParentPreflight::Missing,
        Some(21) => ParentPreflight::CorruptOrIncompatible {
            reason: "integrity check failed".to_string(),
        },
        Some(22) => ParentPreflight::CorruptOrIncompatible {
            reason: "application or redb schema is incompatible".to_string(),
        },
        Some(23) => ParentPreflight::AlreadyOpen,
        Some(25) => ParentPreflight::Healthy {
            full_integrity: true,
        },
        Some(code) => ParentPreflight::Unavailable {
            reason: format!("child exited with unclassified status {code}"),
        },
        None => classify_preflight_signal(status),
    }
}

#[cfg(unix)]
fn classify_preflight_signal(status: std::process::ExitStatus) -> ParentPreflight {
    use std::os::unix::process::ExitStatusExt as _;
    match status.signal() {
        Some(signal) => ParentPreflight::Unavailable {
            reason: format!("preflight child terminated by signal {signal}"),
        },
        None => ParentPreflight::Unavailable {
            reason: "preflight child ended without an exit code".to_string(),
        },
    }
}

#[cfg(not(unix))]
fn classify_preflight_signal(_status: std::process::ExitStatus) -> ParentPreflight {
    ParentPreflight::Unavailable {
        reason: "preflight child ended without an exit code".to_string(),
    }
}

fn quarantine_path(path: &Path) -> PathBuf {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let filename = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("nora.redb");
    path.with_file_name(format!(
        "{filename}.quarantine.{timestamp}.{}",
        uuid::Uuid::new_v4()
    ))
}

fn timeout_evidence_prefix(path: &Path) -> String {
    let filename = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("nora.redb");
    format!("{filename}.preflight-timeout.")
}

fn timeout_preservation_path(path: &Path) -> PathBuf {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    path.with_file_name(format!(
        "{}{timestamp}.{}",
        timeout_evidence_prefix(path),
        uuid::Uuid::new_v4()
    ))
}

fn timeout_evidence_paths(path: &Path) -> Result<Vec<PathBuf>, StoreError> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let prefix = timeout_evidence_prefix(path);
    let mut paths = Vec::new();
    for entry in std::fs::read_dir(parent)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !name.starts_with(&prefix) {
            continue;
        }
        if !entry.file_type()?.is_file() {
            return Err(StoreError::PreflightUnavailable(
                "preflight-timeout evidence is not a regular file".to_string(),
            ));
        }
        paths.push(entry.path());
    }
    paths.sort();
    Ok(paths)
}

fn prune_timeout_evidence(path: &Path) -> Result<usize, StoreError> {
    let paths = timeout_evidence_paths(path)?;
    for evidence in &paths {
        std::fs::remove_file(evidence)?;
    }
    Ok(paths.len())
}

fn preserve_timed_out_database(path: &Path) -> Result<PathBuf, StoreError> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    preserve_timed_out_database_with_available(path, available_bytes(parent)?)
}

fn preserve_timed_out_database_with_available(
    path: &Path,
    available: u64,
) -> Result<PathBuf, StoreError> {
    let current = std::fs::metadata(path)?.len();
    let old_evidence = timeout_evidence_paths(path)?;
    let reclaimable = old_evidence.iter().try_fold(0u64, |total, evidence| {
        Ok::<_, StoreError>(total.saturating_add(std::fs::metadata(evidence)?.len()))
    })?;

    // Do not remove prior evidence unless its space plus current free space is
    // sufficient for the complete replacement lifecycle. The primary remains
    // untouched on every admission or cleanup error.
    ensure_reseed_capacity(current, available, reclaimable)?;
    for evidence in old_evidence {
        std::fs::remove_file(evidence)?;
    }
    // Re-read the filesystem after reclamation to cover concurrent consumers
    // of the same volume. The singleton contract prevents another redb owner,
    // but not kubelet or filesystem overhead.
    admit_reseed(path)?;

    let preserved = timeout_preservation_path(path);
    std::fs::rename(path, &preserved)?;
    Ok(preserved)
}

fn put_batch<T: Serialize>(
    db: &Database,
    table: TableDefinition<'static, &'static [u8], &'static [u8]>,
    rows: Vec<(Vec<u8>, T)>,
) -> Result<(), StoreError> {
    commit(db, |transaction| {
        let mut table = transaction
            .open_table(table)
            .map_err(|error| StoreError::Database(error.to_string()))?;
        for (key, value) in rows {
            let value = encode(value)?;
            table
                .insert(key.as_slice(), value.as_slice())
                .map_err(|error| StoreError::Database(error.to_string()))?;
        }
        Ok(())
    })
}

fn put_maven_batch(
    db: &Database,
    slot: Slot,
    rows: Vec<(Vec<u8>, StoredObject)>,
) -> Result<(), StoreError> {
    commit(db, |transaction| {
        let mut objects = transaction
            .open_table(object_table(slot))
            .map_err(|error| StoreError::Database(error.to_string()))?;
        let mut repos = transaction
            .open_table(repo_table(slot))
            .map_err(|error| StoreError::Database(error.to_string()))?;
        for (key, stored) in rows {
            let value = encode(&stored)?;
            objects
                .insert(key.as_slice(), value.as_slice())
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let key_text = std::str::from_utf8(&key)
                .map_err(|_| StoreError::Schema("non-UTF-8 storage key".to_string()))?;
            let Some(rest) = key_text.strip_prefix("maven/") else {
                continue;
            };
            let Some((path, _)) = rest.rsplit_once('/') else {
                continue;
            };
            let aggregate_key = repo_key("maven", path);
            let existing = repos
                .get(aggregate_key.as_slice())
                .map_err(|error| StoreError::Database(error.to_string()))?
                .map(|value| decode::<StoredRepo>(value.value()))
                .transpose()?
                .unwrap_or_else(|| StoredRepo {
                    name: path.to_string(),
                    artifact_count: 0,
                    logical_size: Some(0),
                    modified: 0,
                    is_file: false,
                });
            let mut aggregate = existing;
            let primary = !crate::gc::is_checksum_sidecar(key_text)
                && !key_text.ends_with("maven-metadata.xml");
            if primary {
                aggregate.artifact_count = aggregate.artifact_count.saturating_add(1);
            }
            aggregate.logical_size = aggregate
                .logical_size
                .map(|size| size.saturating_add(stored.meta.size));
            aggregate.modified = aggregate.modified.max(stored.meta.modified);
            let value = encode(aggregate)?;
            repos
                .insert(aggregate_key.as_slice(), value.as_slice())
                .map_err(|error| StoreError::Database(error.to_string()))?;
        }
        Ok(())
    })
}

fn put_npm_batch(
    db: &Database,
    slot: Slot,
    rows: Vec<(Vec<u8>, StoredObject)>,
) -> Result<(), StoreError> {
    commit(db, |transaction| {
        let mut objects = transaction
            .open_table(object_table(slot))
            .map_err(|error| StoreError::Database(error.to_string()))?;
        let mut repos = transaction
            .open_table(repo_table(slot))
            .map_err(|error| StoreError::Database(error.to_string()))?;
        let mut observations = transaction
            .open_table(npm_observation_table(slot))
            .map_err(|error| StoreError::Database(error.to_string()))?;
        for (key, stored) in rows {
            let value = encode(&stored)?;
            objects
                .insert(key.as_slice(), value.as_slice())
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let key_text = std::str::from_utf8(&key)
                .map_err(|_| StoreError::Schema("non-UTF-8 storage key".to_string()))?;
            let Some(parsed) = crate::npm_layout::parse_npm_object_key(key_text) else {
                continue;
            };
            let package_key = npm_package_key(&parsed.repository, &parsed.package);
            let mut observation = observations
                .get(package_key.as_slice())
                .map_err(|error| StoreError::Database(error.to_string()))?
                .map(|value| decode::<NpmAuthorityObservation>(value.value()))
                .transpose()?
                .unwrap_or_else(|| NpmAuthorityObservation {
                    repository: parsed.repository.clone(),
                    package: parsed.package.clone(),
                    ..NpmAuthorityObservation::default()
                });
            match parsed.kind {
                crate::npm_layout::NpmObjectKind::HostedPackumentCurrent => {
                    observation.current_key = Some(key_text.to_string());
                }
                crate::npm_layout::NpmObjectKind::HostedPackumentRetired => {
                    observation.retired_key = Some(key_text.to_string());
                }
                crate::npm_layout::NpmObjectKind::ProxyPackument => {
                    observation.proxy_packument_key = Some(key_text.to_string());
                }
                crate::npm_layout::NpmObjectKind::ProxyTarball(_) => {
                    let name = format!("repositories/{}/{}", parsed.repository, parsed.package);
                    let aggregate_key = repo_key("npm", &name);
                    let mut aggregate = repos
                        .get(aggregate_key.as_slice())
                        .map_err(|error| StoreError::Database(error.to_string()))?
                        .map(|value| decode::<StoredRepo>(value.value()))
                        .transpose()?
                        .unwrap_or_else(|| StoredRepo {
                            name: name.clone(),
                            artifact_count: 0,
                            logical_size: Some(0),
                            modified: 0,
                            is_file: false,
                        });
                    aggregate.artifact_count = aggregate.artifact_count.saturating_add(1);
                    aggregate.logical_size = aggregate
                        .logical_size
                        .map(|size| size.saturating_add(stored.meta.size));
                    aggregate.modified = aggregate.modified.max(stored.meta.modified);
                    let value = encode(aggregate)?;
                    repos
                        .insert(aggregate_key.as_slice(), value.as_slice())
                        .map_err(|error| StoreError::Database(error.to_string()))?;
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
                    observation.transitional = true;
                }
                _ => {}
            }
            let value = encode(observation)?;
            observations
                .insert(package_key.as_slice(), value.as_slice())
                .map_err(|error| StoreError::Database(error.to_string()))?;
        }
        Ok(())
    })
}

fn clear_table_chunk(
    transaction: &WriteTransaction,
    definition: TableDefinition<'static, &'static [u8], &'static [u8]>,
    limit: usize,
) -> Result<(bool, usize), StoreError> {
    let mut table = transaction
        .open_table(definition)
        .map_err(|error| StoreError::Database(error.to_string()))?;
    let keys: Vec<Vec<u8>> = table
        .iter()
        .map_err(|error| StoreError::Database(error.to_string()))?
        .take(limit.saturating_add(1))
        .map(|entry| {
            entry
                .map(|(key, _)| key.value().to_vec())
                .map_err(|error| StoreError::Database(error.to_string()))
        })
        .collect::<Result<_, _>>()?;
    let more = keys.len() > limit;
    let removed = keys.len().min(limit);
    for key in keys.into_iter().take(limit) {
        table
            .remove(key.as_slice())
            .map_err(|error| StoreError::Database(error.to_string()))?;
    }
    Ok((more, removed))
}

fn clear_slot_chunk(db: &Database, slot: Slot) -> Result<bool, StoreError> {
    commit(db, |transaction| {
        let mut more = false;
        let mut remaining = MAX_TX_ROWS;
        for table in [
            object_table(slot),
            repo_table(slot),
            maven_prefix_stats_table(slot),
            npm_package_table(slot),
            npm_version_table(slot),
            npm_observation_table(slot),
        ] {
            if remaining == 0 {
                // Later tables were not inspected in this transaction.
                return Ok(true);
            }
            let (table_more, removed) = clear_table_chunk(transaction, table, remaining)?;
            more |= table_more;
            remaining = remaining.saturating_sub(removed);
        }
        Ok(more)
    })
}

fn register_change(db: &Database, event: ChangeEvent) -> Result<u64, StoreError> {
    commit(db, |transaction| {
        let current = {
            let table = transaction
                .open_table(META)
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let value = table
                .get("state")
                .map_err(|error| StoreError::Database(error.to_string()))?
                .ok_or_else(|| StoreError::Schema("meta_v1/state is missing".to_string()))?;
            decode::<MetaState>(value.value())?
        };
        let mut state = current;
        let sequence = state.accepted_change_seq.saturating_add(1);
        {
            let mut changes = transaction
                .open_table(CHANGES)
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let mut existing = changes
                .iter()
                .map_err(|error| StoreError::Database(error.to_string()))?
                .take(MAX_TX_ROWS + 1)
                .map(|entry| {
                    let (key, value) =
                        entry.map_err(|error| StoreError::Database(error.to_string()))?;
                    Ok((key.value(), decode::<ChangeEvent>(value.value())?))
                })
                .collect::<Result<Vec<_>, StoreError>>()?;

            // A bounded durable queue is more important than preserving every
            // intermediate mutation. Once the cap is reached, one global-dirty
            // record safely asks S2 to reread S3. The caller may still apply
            // the current typed semantic update to the active slot.
            if existing.len() >= MAX_TX_ROWS {
                let keys = existing.drain(..).map(|(key, _)| key).collect::<Vec<_>>();
                for key in keys {
                    changes
                        .remove(key)
                        .map_err(|error| StoreError::Database(error.to_string()))?;
                }
                let payload = encode(ChangeEvent::GlobalDirty)?;
                changes
                    .insert(sequence, payload.as_slice())
                    .map_err(|error| StoreError::Database(error.to_string()))?;
                state.accepted_change_seq = sequence;
                state.global_dirty = true;
                write_meta(transaction, &state)?;
                return Ok(sequence);
            }

            let coalesce_key = event.coalesce_key();
            for (key, pending) in existing {
                if !matches!(pending, ChangeEvent::GlobalDirty)
                    && pending.coalesce_key() == coalesce_key
                {
                    changes
                        .remove(key)
                        .map_err(|error| StoreError::Database(error.to_string()))?;
                }
            }
            let payload = encode(&event)?;
            changes
                .insert(sequence, payload.as_slice())
                .map_err(|error| StoreError::Database(error.to_string()))?;
        }
        // The accepted sequence is published only after its durable change
        // record is inserted in the same transaction.
        state.accepted_change_seq = sequence;
        state.global_dirty |= matches!(
            event,
            ChangeEvent::PhysicalDirty { .. } | ChangeEvent::GlobalDirty
        );
        write_meta(transaction, &state)?;
        Ok(sequence)
    })
}

fn flip_slot(
    db: &Database,
    slot: Slot,
    fence: u64,
    completeness: RegistryCompleteness,
    totals: RegistryTotals,
    config_digest: String,
) -> Result<MetaState, StoreError> {
    commit(db, |transaction| {
        let current = {
            let table = transaction
                .open_table(META)
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let value = table
                .get("state")
                .map_err(|error| StoreError::Database(error.to_string()))?
                .ok_or_else(|| StoreError::Schema("meta_v1/state is missing".to_string()))?;
            decode::<MetaState>(value.value())?
        };
        if current.accepted_change_seq != fence {
            tracing::debug!(
                expected_fence = fence,
                current_fence = current.accepted_change_seq,
                "S2 epoch superseded by a newer accepted change"
            );
            return Err(StoreError::Superseded);
        }
        let mut state = current;
        state.active_slot = Some(slot);
        state.generation = state.generation.saturating_add(1);
        state.completeness = completeness;
        state.totals = totals;
        state.config_digest = config_digest;
        state.set_watermark(slot, fence);
        state.global_dirty = false;
        write_meta(transaction, &state)?;

        // A change record is retained until the candidate slot includes its
        // sequence. Removing <= fence is part of the same atomic publish.
        let mut changes = transaction
            .open_table(CHANGES)
            .map_err(|error| StoreError::Database(error.to_string()))?;
        let keys: Vec<u64> = changes
            .range(..=fence)
            .map_err(|error| StoreError::Database(error.to_string()))?
            .map(|entry| {
                entry
                    .map(|(key, _)| key.value())
                    .map_err(|error| StoreError::Database(error.to_string()))
            })
            .collect::<Result<_, _>>()?;
        // Finalisation is rejected when replay would make the transaction
        // unbounded. The caller must rebuild/retry with a later fence.
        if keys.len() > MAX_TX_ROWS {
            return Err(StoreError::TransactionTooLarge);
        }
        for key in keys {
            changes
                .remove(key)
                .map_err(|error| StoreError::Database(error.to_string()))?;
        }
        Ok(state)
    })
}

fn table_prefix_keys(
    table: &redb::Table<&[u8], &[u8]>,
    prefix: &[u8],
) -> Result<Vec<Vec<u8>>, StoreError> {
    let end = prefix_successor(prefix);
    let keys = table
        .range::<&[u8]>((
            std::ops::Bound::Included(prefix),
            std::ops::Bound::Excluded(end.as_slice()),
        ))
        .map_err(|error| StoreError::Database(error.to_string()))?
        .take(MAX_TX_ROWS + 1)
        .map(|entry| {
            entry
                .map(|(key, _)| key.value().to_vec())
                .map_err(|error| StoreError::Database(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if keys.len() > MAX_TX_ROWS {
        return Err(StoreError::TransactionTooLarge);
    }
    Ok(keys)
}

fn update_entity_key(update: &IncrementalUpdate) -> &[u8] {
    match update {
        IncrementalUpdate::Maven { entity_key, .. } | IncrementalUpdate::Npm { entity_key, .. } => {
            entity_key
        }
    }
}

fn update_covers_physical(update: &IncrementalUpdate, key: &str) -> bool {
    match update {
        IncrementalUpdate::Maven {
            object_prefix,
            repo_recursive,
            ..
        } => {
            (*repo_recursive && key.as_bytes().starts_with(object_prefix))
                || (!*repo_recursive && is_exact_maven_bundle_key(key.as_bytes(), object_prefix))
        }
        IncrementalUpdate::Npm { package_key, .. } => {
            let Some(separator) = package_key.iter().position(|byte| *byte == 0) else {
                return false;
            };
            let Ok(repository) = std::str::from_utf8(&package_key[..separator]) else {
                return false;
            };
            let Ok(package) = std::str::from_utf8(&package_key[separator + 1..]) else {
                return false;
            };
            crate::npm_layout::parse_npm_object_key(key)
                .is_some_and(|parsed| parsed.repository == repository && parsed.package == package)
                || key
                    == format!("npm/repositories/{repository}/proxy/packuments/{package}.json.meta")
        }
    }
}

fn is_exact_maven_bundle_key(key: &[u8], base: &[u8]) -> bool {
    key == base
        || [b".md5".as_slice(), b".sha1", b".sha256", b".sha512"]
            .iter()
            .any(|suffix| key == [base, suffix].concat())
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct TotalsDelta {
    npm: bool,
    old_artifacts: u64,
    old_bytes: u64,
    new_artifacts: u64,
    new_bytes: u64,
}

pub(crate) fn apply_totals_delta(totals: &mut RegistryTotals, delta: TotalsDelta) {
    let (artifacts, bytes) = if delta.npm {
        (&mut totals.npm_versions, &mut totals.npm_bytes)
    } else {
        (&mut totals.maven_artifacts, &mut totals.maven_bytes)
    };
    *artifacts = artifacts
        .saturating_sub(delta.old_artifacts)
        .saturating_add(delta.new_artifacts);
    *bytes = bytes
        .saturating_sub(delta.old_bytes)
        .saturating_add(delta.new_bytes);
}

#[derive(Debug, Default, Serialize)]
struct MavenStatsReplacement {
    old_direct_files: u64,
    old_direct_bytes: u64,
    new_direct_files: u64,
    new_direct_bytes: u64,
    old_subtree_files: u64,
    old_subtree_bytes: u64,
    new_subtree_files: u64,
    new_subtree_bytes: u64,
    requires_existing: bool,
}

fn checked_accumulate(value: &mut u64, delta: u64) -> Result<(), StoreError> {
    *value = value
        .checked_add(delta)
        .ok_or(StoreError::ProjectionOverflow)?;
    Ok(())
}

fn replace_counter(value: u64, old: u64, new: u64) -> Result<u64, StoreError> {
    value
        .checked_sub(old)
        .and_then(|value| value.checked_add(new))
        .ok_or(StoreError::ProjectionOverflow)
}

fn maven_directory_ancestors(logical_path: &str) -> Result<(String, Vec<String>), StoreError> {
    if logical_path
        .split('/')
        .any(|component| component.is_empty() || matches!(component, "." | ".."))
    {
        return Err(StoreError::ProjectionInvariant(
            "Maven object path contains an invalid component".to_string(),
        ));
    }
    let parent = logical_path
        .rsplit_once('/')
        .map_or("", |(parent, _)| parent)
        .to_string();
    let mut ancestors = vec![String::new()];
    if !parent.is_empty() {
        let mut current = String::new();
        for component in parent.split('/') {
            if !current.is_empty() {
                current.push('/');
            }
            current.push_str(component);
            ancestors.push(current.clone());
        }
    }
    Ok((parent, ancestors))
}

fn add_maven_winner_replacement(
    replacements: &mut BTreeMap<Vec<u8>, (String, MavenStatsReplacement)>,
    repository: &str,
    logical_path: &str,
    old: Option<&FileMeta>,
    new: Option<&FileMeta>,
) -> Result<(), StoreError> {
    if old.map(|meta| meta.size) == new.map(|meta| meta.size) {
        return Ok(());
    }
    let (parent, ancestors) = maven_directory_ancestors(logical_path)?;
    let direct_key = maven_prefix_stats_key(repository, &parent);
    let direct = &mut replacements
        .entry(direct_key)
        .or_insert_with(|| (parent, MavenStatsReplacement::default()))
        .1;
    if let Some(meta) = old {
        checked_accumulate(&mut direct.old_direct_files, 1)?;
        checked_accumulate(&mut direct.old_direct_bytes, meta.size)?;
        direct.requires_existing = true;
    }
    if let Some(meta) = new {
        checked_accumulate(&mut direct.new_direct_files, 1)?;
        checked_accumulate(&mut direct.new_direct_bytes, meta.size)?;
    }
    for ancestor in ancestors {
        let key = maven_prefix_stats_key(repository, &ancestor);
        let replacement = &mut replacements
            .entry(key)
            .or_insert_with(|| (ancestor, MavenStatsReplacement::default()))
            .1;
        if let Some(meta) = old {
            checked_accumulate(&mut replacement.old_subtree_files, 1)?;
            checked_accumulate(&mut replacement.old_subtree_bytes, meta.size)?;
            replacement.requires_existing = true;
        }
        if let Some(meta) = new {
            checked_accumulate(&mut replacement.new_subtree_files, 1)?;
            checked_accumulate(&mut replacement.new_subtree_bytes, meta.size)?;
        }
    }
    Ok(())
}

fn read_maven_object_meta(
    objects: &redb::Table<&[u8], &[u8]>,
    key: &[u8],
) -> Result<Option<FileMeta>, StoreError> {
    objects
        .get(key)
        .map_err(|error| StoreError::Database(error.to_string()))?
        .map(|value| decode::<StoredObject>(value.value()).map(|stored| stored.meta))
        .transpose()
}

fn maven_winner(
    objects: &redb::Table<&[u8], &[u8]>,
    view: &MavenIndexView,
    logical_path: &str,
    changed_repository: &str,
    changed_after: Option<&BTreeMap<String, FileMeta>>,
) -> Result<Option<FileMeta>, StoreError> {
    for member in &view.members {
        let meta = if changed_after.is_some() && member == changed_repository {
            changed_after.and_then(|objects| objects.get(logical_path).cloned())
        } else {
            let key = format!("{}{logical_path}", maven_member_prefix(member));
            read_maven_object_meta(objects, key.as_bytes())?
        };
        if meta.is_some() {
            return Ok(meta);
        }
    }
    Ok(None)
}

fn apply_maven_prefix_replacements(
    transaction: &WriteTransaction,
    slot: Slot,
    replacements: BTreeMap<Vec<u8>, (String, MavenStatsReplacement)>,
) -> Result<(), StoreError> {
    let mut table = transaction
        .open_table(maven_prefix_stats_table(slot))
        .map_err(|error| StoreError::Database(error.to_string()))?;
    for (key, (path, replacement)) in replacements {
        let existing = table
            .get(key.as_slice())
            .map_err(|error| StoreError::Database(error.to_string()))?
            .map(|value| decode::<MavenPrefixStats>(value.value()))
            .transpose()?;
        if existing.is_none() && replacement.requires_existing {
            return Err(StoreError::ProjectionInvariant(format!(
                "missing Maven prefix aggregate for {path:?}"
            )));
        }
        let mut stats = existing.unwrap_or_default();
        stats.direct_files = replace_counter(
            stats.direct_files,
            replacement.old_direct_files,
            replacement.new_direct_files,
        )?;
        stats.direct_bytes = replace_counter(
            stats.direct_bytes,
            replacement.old_direct_bytes,
            replacement.new_direct_bytes,
        )?;
        stats.subtree_files = replace_counter(
            stats.subtree_files,
            replacement.old_subtree_files,
            replacement.new_subtree_files,
        )?;
        stats.subtree_bytes = replace_counter(
            stats.subtree_bytes,
            replacement.old_subtree_bytes,
            replacement.new_subtree_bytes,
        )?;
        let empty = stats == MavenPrefixStats::default();
        if empty && !path.is_empty() {
            table
                .remove(key.as_slice())
                .map_err(|error| StoreError::Database(error.to_string()))?;
        } else {
            let value = encode(stats)?;
            table
                .insert(key.as_slice(), value.as_slice())
                .map_err(|error| StoreError::Database(error.to_string()))?;
        }
    }
    Ok(())
}

fn prepare_maven_prefix_replacements(
    objects: &redb::Table<&[u8], &[u8]>,
    repository: &str,
    old_keys: &[Vec<u8>],
    new_rows: &[(Vec<u8>, StoredObject)],
    views: &[MavenIndexView],
) -> Result<BTreeMap<Vec<u8>, (String, MavenStatsReplacement)>, StoreError> {
    if views.is_empty() {
        return Err(StoreError::ProjectionInvariant(format!(
            "Maven repository {repository:?} has no logical view"
        )));
    }
    let base = maven_member_prefix(repository);
    let mut logical_paths = BTreeSet::new();
    for key in old_keys.iter().chain(new_rows.iter().map(|(key, _)| key)) {
        let key = std::str::from_utf8(key)
            .map_err(|_| StoreError::ProjectionInvariant("non-UTF-8 Maven key".to_string()))?;
        let logical = key.strip_prefix(&base).ok_or_else(|| {
            StoreError::ProjectionInvariant(
                "Maven incremental row escaped its repository prefix".to_string(),
            )
        })?;
        if repository.is_empty() && logical.starts_with("repositories/") {
            continue;
        }
        logical_paths.insert(logical.to_string());
    }
    let new_by_logical = new_rows
        .iter()
        .filter_map(|(key, row)| {
            let key = std::str::from_utf8(key).ok()?;
            let logical = key.strip_prefix(&base)?;
            Some((logical.to_string(), row.meta.clone()))
        })
        .collect::<BTreeMap<_, _>>();

    let mut work = 0usize;
    let mut replacements = BTreeMap::new();
    for view in views {
        if !view.members.iter().any(|member| member == repository) {
            return Err(StoreError::ProjectionInvariant(format!(
                "Maven view {:?} does not contain changed repository {repository:?}",
                view.repository
            )));
        }
        work = work
            .checked_add(
                logical_paths
                    .len()
                    .checked_mul(view.members.len())
                    .and_then(|lookups| lookups.checked_mul(2))
                    .ok_or(StoreError::ProjectionOverflow)?,
            )
            .ok_or(StoreError::ProjectionOverflow)?;
        if work > MAX_TX_ROWS {
            return Err(StoreError::TransactionTooLarge);
        }
        for logical in &logical_paths {
            let before = maven_winner(objects, view, logical, repository, None)?;
            let after = maven_winner(objects, view, logical, repository, Some(&new_by_logical))?;
            add_maven_winner_replacement(
                &mut replacements,
                &view.repository,
                logical,
                before.as_ref(),
                after.as_ref(),
            )?;
        }
    }
    if replacements.len() > MAX_TX_ROWS {
        return Err(StoreError::TransactionTooLarge);
    }
    let encoded_bytes = maven_replacements_encoded_bytes(&replacements)?;
    if encoded_bytes > MAX_TX_BYTES {
        return Err(StoreError::TransactionTooLarge);
    }
    Ok(replacements)
}

fn maven_replacements_encoded_bytes(
    replacements: &BTreeMap<Vec<u8>, (String, MavenStatsReplacement)>,
) -> Result<usize, StoreError> {
    replacements
        .iter()
        .try_fold(0usize, |total, (key, (_, row))| {
            total
                .checked_add(encoded_row_bytes(key, row)?)
                .ok_or(StoreError::ProjectionOverflow)
        })
}

fn apply_update_to_slot(
    transaction: &WriteTransaction,
    slot: Slot,
    update: &IncrementalUpdate,
) -> Result<TotalsDelta, StoreError> {
    match update {
        IncrementalUpdate::Maven {
            repository,
            object_prefix,
            repo_prefix,
            repo_recursive,
            objects: rows,
            repos,
            views,
            ..
        } => {
            let mut objects = transaction
                .open_table(object_table(slot))
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let mut object_keys = table_prefix_keys(&objects, object_prefix)?;
            if !*repo_recursive {
                object_keys.retain(|key| is_exact_maven_bundle_key(key, object_prefix));
            }
            let mut repos_table = transaction
                .open_table(repo_table(slot))
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let mut repo_keys = Vec::new();
            if repos_table
                .get(repo_prefix.as_slice())
                .map_err(|error| StoreError::Database(error.to_string()))?
                .is_some()
            {
                repo_keys.push(repo_prefix.clone());
            }
            if *repo_recursive {
                let mut child_prefix = repo_prefix.clone();
                child_prefix.push(b'/');
                repo_keys.extend(table_prefix_keys(&repos_table, &child_prefix)?);
            }
            let prefix_replacements =
                prepare_maven_prefix_replacements(&objects, repository, &object_keys, rows, views)?;
            let delete_key_bytes =
                object_keys
                    .iter()
                    .chain(&repo_keys)
                    .try_fold(0usize, |total, key| {
                        total
                            .checked_add(key.len())
                            .ok_or(StoreError::ProjectionOverflow)
                    })?;
            let projected_bytes = serde_json::to_vec(update)?
                .len()
                .checked_add(maven_replacements_encoded_bytes(&prefix_replacements)?)
                .and_then(|bytes| bytes.checked_add(delete_key_bytes))
                .ok_or(StoreError::ProjectionOverflow)?;
            if projected_bytes > MAX_TX_BYTES {
                return Err(StoreError::TransactionTooLarge);
            }
            let mut old_artifacts = 0u64;
            let mut old_bytes = 0u64;
            for key in &repo_keys {
                if let Some(value) = repos_table
                    .get(key.as_slice())
                    .map_err(|error| StoreError::Database(error.to_string()))?
                {
                    let row = decode::<StoredRepo>(value.value())?;
                    old_artifacts = old_artifacts.saturating_add(row.artifact_count);
                    old_bytes = old_bytes.saturating_add(row.logical_size.unwrap_or(0));
                }
            }
            let new_artifacts = repos.iter().map(|(_, row)| row.artifact_count).sum();
            let new_bytes = repos
                .iter()
                .map(|(_, row)| row.logical_size.unwrap_or(0))
                .sum();
            if object_keys
                .len()
                .saturating_add(repo_keys.len())
                .saturating_add(rows.len())
                .saturating_add(repos.len())
                .saturating_add(prefix_replacements.len())
                > MAX_TX_ROWS
            {
                return Err(StoreError::TransactionTooLarge);
            }
            for key in object_keys {
                objects
                    .remove(key.as_slice())
                    .map_err(|error| StoreError::Database(error.to_string()))?;
            }
            for (key, value) in rows {
                let value = encode(value)?;
                objects
                    .insert(key.as_slice(), value.as_slice())
                    .map_err(|error| StoreError::Database(error.to_string()))?;
            }
            for key in repo_keys {
                repos_table
                    .remove(key.as_slice())
                    .map_err(|error| StoreError::Database(error.to_string()))?;
            }
            for (key, value) in repos {
                let value = encode(value)?;
                repos_table
                    .insert(key.as_slice(), value.as_slice())
                    .map_err(|error| StoreError::Database(error.to_string()))?;
            }
            apply_maven_prefix_replacements(transaction, slot, prefix_replacements)?;
            Ok(TotalsDelta {
                npm: false,
                old_artifacts,
                old_bytes,
                new_artifacts,
                new_bytes,
            })
        }
        IncrementalUpdate::Npm {
            package_key,
            version_prefix,
            repo_key,
            package,
            versions,
            repo,
            ..
        } => {
            let mut packages = transaction
                .open_table(npm_package_table(slot))
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let mut versions_table = transaction
                .open_table(npm_version_table(slot))
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let version_keys = table_prefix_keys(&versions_table, version_prefix)?;
            if version_keys.len().saturating_add(versions.len()) > MAX_TX_ROWS {
                return Err(StoreError::TransactionTooLarge);
            }
            packages
                .remove(package_key.as_slice())
                .map_err(|error| StoreError::Database(error.to_string()))?;
            for key in version_keys {
                versions_table
                    .remove(key.as_slice())
                    .map_err(|error| StoreError::Database(error.to_string()))?;
            }
            if let Some(package) = package {
                let value = encode(package)?;
                packages
                    .insert(package_key.as_slice(), value.as_slice())
                    .map_err(|error| StoreError::Database(error.to_string()))?;
            }
            for (key, version) in versions {
                let value = encode(version)?;
                versions_table
                    .insert(key.as_slice(), value.as_slice())
                    .map_err(|error| StoreError::Database(error.to_string()))?;
            }
            let mut repos = transaction
                .open_table(repo_table(slot))
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let previous = repos
                .get(repo_key.as_slice())
                .map_err(|error| StoreError::Database(error.to_string()))?
                .map(|value| decode::<StoredRepo>(value.value()))
                .transpose()?;
            repos
                .remove(repo_key.as_slice())
                .map_err(|error| StoreError::Database(error.to_string()))?;
            if let Some(repo) = repo {
                let value = encode(repo)?;
                repos
                    .insert(repo_key.as_slice(), value.as_slice())
                    .map_err(|error| StoreError::Database(error.to_string()))?;
            }
            Ok(TotalsDelta {
                npm: true,
                old_artifacts: previous.as_ref().map_or(0, |row| row.artifact_count),
                old_bytes: previous
                    .as_ref()
                    .and_then(|row| row.logical_size)
                    .unwrap_or(0),
                new_artifacts: repo.as_ref().map_or(0, |row| row.artifact_count),
                new_bytes: repo.as_ref().and_then(|row| row.logical_size).unwrap_or(0),
            })
        }
    }
}

fn apply_shadow_update(
    db: &Database,
    slot: Slot,
    update: IncrementalUpdate,
) -> Result<TotalsDelta, StoreError> {
    commit(db, |transaction| {
        apply_update_to_slot(transaction, slot, &update)
    })
}

fn apply_incremental_update(
    db: &Database,
    sequence: u64,
    update: IncrementalUpdate,
) -> Result<MetaState, StoreError> {
    commit(db, |transaction| {
        let mut state = {
            let table = transaction
                .open_table(META)
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let value = table
                .get("state")
                .map_err(|error| StoreError::Database(error.to_string()))?
                .ok_or_else(|| StoreError::Schema("meta_v1/state is missing".to_string()))?;
            decode::<MetaState>(value.value())?
        };
        if state.accepted_change_seq < sequence {
            return Err(StoreError::Superseded);
        }
        let slot = state.active_slot.ok_or(StoreError::Superseded)?;
        let entity_key = update_entity_key(&update).to_vec();
        let previous_sequence = {
            let table = transaction
                .open_table(ENTITY_SEQUENCES)
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let previous = table
                .get(entity_key.as_slice())
                .map_err(|error| StoreError::Database(error.to_string()))?
                .map(|value| value.value())
                .unwrap_or(0);
            previous
        };

        // Concurrent S3 preparation may complete out of order. The newest
        // update for one logical entity wins, while unrelated entities may be
        // prepared concurrently and are serialized by this writer actor.
        let changed = previous_sequence < sequence;
        if changed {
            let delta = apply_update_to_slot(transaction, slot, &update)?;
            apply_totals_delta(&mut state.totals, delta);
            let mut sequences = transaction
                .open_table(ENTITY_SEQUENCES)
                .map_err(|error| StoreError::Database(error.to_string()))?;
            sequences
                .insert(entity_key.as_slice(), sequence)
                .map_err(|error| StoreError::Database(error.to_string()))?;
        }

        // Keep the newest semantic record until a shadow slot has replayed or
        // scanned it. Older same-entity records and their lower-level physical
        // writes are covered by this authoritative S3 reread and can be safely
        // coalesced away.
        let pending = {
            let mut changes = transaction
                .open_table(CHANGES)
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let records = changes
                .iter()
                .map_err(|error| StoreError::Database(error.to_string()))?
                .map(|entry| {
                    let (key, value) =
                        entry.map_err(|error| StoreError::Database(error.to_string()))?;
                    Ok((key.value(), decode::<ChangeEvent>(value.value())?))
                })
                .collect::<Result<Vec<_>, StoreError>>()?;
            for (key, event) in &records {
                let covered_semantic =
                    *key < sequence && event.entity_key().as_deref() == Some(entity_key.as_slice());
                let covered_physical = *key <= sequence
                    && matches!(event, ChangeEvent::PhysicalDirty { key } if update_covers_physical(&update, key));
                if covered_semantic || covered_physical {
                    changes
                        .remove(*key)
                        .map_err(|error| StoreError::Database(error.to_string()))?;
                }
            }
            changes
                .iter()
                .map_err(|error| StoreError::Database(error.to_string()))?
                .map(|entry| {
                    let (key, value) =
                        entry.map_err(|error| StoreError::Database(error.to_string()))?;
                    Ok((key.value(), decode::<ChangeEvent>(value.value())?))
                })
                .collect::<Result<Vec<_>, StoreError>>()?
        };

        let sequences = transaction
            .open_table(ENTITY_SEQUENCES)
            .map_err(|error| StoreError::Database(error.to_string()))?;
        let mut global_dirty = false;
        let mut all_applied = true;
        for (pending_sequence, event) in &pending {
            let Some(pending_entity) = event.entity_key() else {
                global_dirty = true;
                all_applied = false;
                continue;
            };
            let applied = sequences
                .get(pending_entity.as_slice())
                .map_err(|error| StoreError::Database(error.to_string()))?
                .map(|value| value.value())
                .unwrap_or(0);
            if applied < *pending_sequence {
                all_applied = false;
            }
        }

        if changed {
            state.generation = state.generation.saturating_add(1);
        }
        if all_applied {
            state.set_watermark(slot, state.accepted_change_seq);
        }
        state.global_dirty = global_dirty;
        write_meta(transaction, &state)?;
        Ok(state)
    })
}

fn observe_writer_command<T>(
    command: &'static str,
    rows: usize,
    encoded_bytes: usize,
    started: std::time::Instant,
    result: &Result<T, StoreError>,
) {
    let outcome = if result.is_ok() { "success" } else { "error" };
    crate::metrics::INDEX_REDB_COMMAND_TOTAL
        .with_label_values(&[command, outcome])
        .inc();
    crate::metrics::INDEX_REDB_COMMAND_DURATION_SECONDS
        .with_label_values(&[command, outcome])
        .observe(started.elapsed().as_secs_f64());
    crate::metrics::INDEX_REDB_COMMAND_ROWS
        .with_label_values(&[command])
        .observe(rows as f64);
    crate::metrics::INDEX_REDB_COMMAND_BYTES
        .with_label_values(&[command])
        .observe(encoded_bytes as f64);
}

fn writer_loop(
    db: Arc<Database>,
    mut receiver: mpsc::Receiver<WriterCommand>,
    healthy: Arc<AtomicBool>,
) {
    while let Some(command) = receiver.blocking_recv() {
        let result = match command {
            WriterCommand::PutMavenObjects {
                slot,
                rows,
                encoded_bytes,
                reply,
            } => {
                let row_count = rows.len();
                let started = std::time::Instant::now();
                let result = put_maven_batch(&db, slot, rows);
                observe_writer_command("maven_objects", row_count, encoded_bytes, started, &result);
                let failed = result
                    .as_ref()
                    .err()
                    .is_some_and(StoreError::poisons_writer);
                let _ = reply.send(result);
                failed
            }
            WriterCommand::PutNpmObjects {
                slot,
                rows,
                encoded_bytes,
                reply,
            } => {
                let row_count = rows.len();
                let started = std::time::Instant::now();
                let result = put_npm_batch(&db, slot, rows);
                observe_writer_command("npm_objects", row_count, encoded_bytes, started, &result);
                let failed = result
                    .as_ref()
                    .err()
                    .is_some_and(StoreError::poisons_writer);
                let _ = reply.send(result);
                failed
            }
            WriterCommand::PutRepos {
                slot,
                rows,
                encoded_bytes,
                reply,
            } => {
                let row_count = rows.len();
                let started = std::time::Instant::now();
                let result = put_batch(&db, repo_table(slot), rows);
                observe_writer_command("repos", row_count, encoded_bytes, started, &result);
                let failed = result
                    .as_ref()
                    .err()
                    .is_some_and(StoreError::poisons_writer);
                let _ = reply.send(result);
                failed
            }
            WriterCommand::PutMavenPrefixStats {
                slot,
                rows,
                encoded_bytes,
                reply,
            } => {
                let row_count = rows.len();
                let started = std::time::Instant::now();
                let result = put_batch(&db, maven_prefix_stats_table(slot), rows);
                observe_writer_command(
                    "maven_prefix_stats",
                    row_count,
                    encoded_bytes,
                    started,
                    &result,
                );
                let failed = result
                    .as_ref()
                    .err()
                    .is_some_and(StoreError::poisons_writer);
                let _ = reply.send(result);
                failed
            }
            WriterCommand::PutNpmPackages {
                slot,
                rows,
                encoded_bytes,
                reply,
            } => {
                let row_count = rows.len();
                let started = std::time::Instant::now();
                let result = put_batch(&db, npm_package_table(slot), rows);
                observe_writer_command("npm_packages", row_count, encoded_bytes, started, &result);
                let failed = result
                    .as_ref()
                    .err()
                    .is_some_and(StoreError::poisons_writer);
                let _ = reply.send(result);
                failed
            }
            WriterCommand::PutNpmVersions {
                slot,
                rows,
                encoded_bytes,
                reply,
            } => {
                let row_count = rows.len();
                let started = std::time::Instant::now();
                let result = put_batch(&db, npm_version_table(slot), rows);
                observe_writer_command("npm_versions", row_count, encoded_bytes, started, &result);
                let failed = result
                    .as_ref()
                    .err()
                    .is_some_and(StoreError::poisons_writer);
                let _ = reply.send(result);
                failed
            }
            WriterCommand::ClearSlotChunk { slot, reply } => {
                let result = clear_slot_chunk(&db, slot);
                let failed = result
                    .as_ref()
                    .err()
                    .is_some_and(StoreError::poisons_writer);
                let _ = reply.send(result);
                failed
            }
            WriterCommand::RegisterChange { event, reply } => {
                let result = register_change(&db, event);
                let failed = result
                    .as_ref()
                    .err()
                    .is_some_and(StoreError::poisons_writer);
                let _ = reply.send(result);
                failed
            }
            WriterCommand::Flip {
                slot,
                fence,
                completeness,
                totals,
                config_digest,
                reply,
            } => {
                let result = flip_slot(&db, slot, fence, completeness, totals, config_digest);
                let failed = result
                    .as_ref()
                    .err()
                    .is_some_and(StoreError::poisons_writer);
                let _ = reply.send(result);
                failed
            }
            WriterCommand::ApplyIncremental {
                sequence,
                update,
                reply,
            } => {
                let result = apply_incremental_update(&db, sequence, update);
                let failed = result
                    .as_ref()
                    .err()
                    .is_some_and(StoreError::poisons_writer);
                let _ = reply.send(result);
                failed
            }
            WriterCommand::ApplyShadow {
                slot,
                update,
                reply,
            } => {
                let result = apply_shadow_update(&db, slot, update);
                let failed = result
                    .as_ref()
                    .err()
                    .is_some_and(StoreError::poisons_writer);
                let _ = reply.send(result);
                failed
            }
            WriterCommand::Shutdown { mark_clean, reply } => {
                let result = if mark_clean {
                    set_clean_shutdown(&db, true)
                } else {
                    Ok(())
                };
                let _ = reply.send(result);
                healthy.store(false, Ordering::Release);
                return;
            }
        };
        if result {
            // Any transaction/commit error poisons the writer. Continuing with
            // the same handle after an I/O failure is explicitly forbidden by
            // the accepted redb dependency contract.
            healthy.store(false, Ordering::Release);
            tracing::error!("redb writer poisoned; refusing further index mutations");
            return;
        }
    }
    healthy.store(false, Ordering::Release);
}

pub fn prefix_successor(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last != u8::MAX {
            end.push(last + 1);
            return end;
        }
    }
    vec![u8::MAX]
}

pub fn repo_key(registry: &str, name: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(registry.len() + name.len() + 1);
    key.extend_from_slice(registry.as_bytes());
    key.push(0);
    key.extend_from_slice(name.as_bytes());
    key
}

pub fn npm_package_key(repository: &str, package: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(repository.len() + package.len() + 1);
    key.extend_from_slice(repository.as_bytes());
    key.push(0);
    key.extend_from_slice(package.as_bytes());
    key
}

pub fn npm_version_key(repository: &str, package: &str, sort_rank: u64, version: &str) -> Vec<u8> {
    let mut key = npm_package_key(repository, package);
    key.push(0);
    key.extend_from_slice(&sort_rank.to_be_bytes());
    key.push(0);
    key.extend_from_slice(version.as_bytes());
    key
}

fn available_bytes(path: &Path) -> Result<u64, StoreError> {
    let stats = rustix::fs::statvfs(path)
        .map_err(|error| StoreError::Io(std::io::Error::from_raw_os_error(error.raw_os_error())))?;
    Ok(stats.f_bavail.saturating_mul(stats.f_frsize))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn s2_flip_is_atomic_and_warm_reopen_preserves_generation() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("index.redb");
        let index = PersistentIndex::open(&path, "cfg-a".to_string()).unwrap();
        let state = index.meta().await.unwrap();
        assert_eq!(state.active_slot, None);
        index
            .put_repos(
                Slot::A,
                vec![(
                    repo_key("maven", "com/acme"),
                    StoredRepo {
                        name: "com/acme".to_string(),
                        artifact_count: 1,
                        logical_size: Some(7),
                        modified: 1,
                        is_file: false,
                    },
                )],
            )
            .await
            .unwrap();
        let state = index
            .flip(
                Slot::A,
                0,
                RegistryCompleteness {
                    maven: true,
                    npm: false,
                },
                RegistryTotals::default(),
                "cfg-a".to_string(),
            )
            .await
            .unwrap();
        assert_eq!(state.generation, 1);
        let (rows, _, generation) = index
            .list_repos(
                "maven",
                None,
                50,
                10_000,
                std::time::Duration::from_millis(250),
            )
            .await
            .unwrap();
        assert_eq!(generation, 1);
        assert_eq!(rows.len(), 1);
        index.shutdown().await;
        drop(index);

        let reopened = PersistentIndex::open(&path, "cfg-a".to_string()).unwrap();
        assert_eq!(reopened.meta().await.unwrap().generation, 1);
        reopened.shutdown().await;
    }

    #[tokio::test]
    async fn change_sequence_and_flip_share_a_fence() {
        let temp = tempfile::tempdir().unwrap();
        let index =
            PersistentIndex::open(temp.path().join("index.redb"), "cfg".to_string()).unwrap();
        let sequence = index
            .register_change(ChangeEvent::PhysicalDirty {
                key: "maven/a.jar".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(sequence, 1);
        assert!(index
            .flip(
                Slot::A,
                0,
                RegistryCompleteness::default(),
                RegistryTotals::default(),
                "cfg".to_string(),
            )
            .await
            .is_err());
        // A fence mismatch is a normal superseded S2 epoch, not a poisoned
        // database handle. The caller retries with a later candidate.
        assert!(index.writer_healthy());
        index.shutdown().await;
    }

    #[tokio::test]
    async fn writer_metrics_record_one_bounded_command_without_entity_labels() {
        let command_name = "repos";
        let command =
            crate::metrics::INDEX_REDB_COMMAND_TOTAL.with_label_values(&[command_name, "success"]);
        let duration = crate::metrics::INDEX_REDB_COMMAND_DURATION_SECONDS
            .with_label_values(&[command_name, "success"]);
        let rows = crate::metrics::INDEX_REDB_COMMAND_ROWS.with_label_values(&[command_name]);
        let bytes = crate::metrics::INDEX_REDB_COMMAND_BYTES.with_label_values(&[command_name]);
        let before_command = command.get();
        let before_duration = duration.get_sample_count();
        let before_rows_count = rows.get_sample_count();
        let before_rows_sum = rows.get_sample_sum();
        let before_bytes_count = bytes.get_sample_count();
        let before_bytes_sum = bytes.get_sample_sum();

        let batch = vec![(
            repo_key("npm", "repositories/npm-hosted/telemetry"),
            StoredRepo {
                name: "repositories/npm-hosted/telemetry".to_string(),
                artifact_count: 3,
                logical_size: Some(42),
                modified: 1,
                is_file: false,
            },
        )];
        let expected_bytes = PersistentIndex::check_batch(&batch).unwrap() as f64;
        let temp = tempfile::tempdir().unwrap();
        let index =
            PersistentIndex::open(temp.path().join("index.redb"), "cfg".to_string()).unwrap();
        index.put_repos(Slot::A, batch).await.unwrap();

        // Other writer tests share the process-wide Prometheus registry and
        // may add samples concurrently. Monotonic lower bounds prove that this
        // real writer command propagated its exact preflighted row/byte budget
        // without relying on globally exclusive test execution.
        assert!(command.get() >= before_command + 1);
        assert!(duration.get_sample_count() >= before_duration + 1);
        assert!(rows.get_sample_count() >= before_rows_count + 1);
        assert!(rows.get_sample_sum() >= before_rows_sum + 1.0);
        assert!(bytes.get_sample_count() >= before_bytes_count + 1);
        assert!(bytes.get_sample_sum() >= before_bytes_sum + expected_bytes);
        index.shutdown().await;
    }

    #[tokio::test]
    async fn repo_query_cursor_progresses_across_filtered_rows_and_namespace_bounds() {
        let temp = tempfile::tempdir().unwrap();
        let index =
            PersistentIndex::open(temp.path().join("index.redb"), "cfg".to_string()).unwrap();
        index
            .put_repos(
                Slot::A,
                [
                    "com/legacy",
                    "repositories/allowed/a",
                    "repositories/allowed/b",
                    "repositories/other/a",
                ]
                .into_iter()
                .map(|name| {
                    (
                        repo_key("maven", name),
                        StoredRepo {
                            name: name.to_string(),
                            artifact_count: 1,
                            logical_size: Some(1),
                            modified: 1,
                            is_file: false,
                        },
                    )
                })
                .collect(),
            )
            .await
            .unwrap();
        index
            .flip(
                Slot::A,
                0,
                RegistryCompleteness {
                    maven: true,
                    npm: false,
                },
                RegistryTotals::default(),
                "cfg".to_string(),
            )
            .await
            .unwrap();

        let (first, cursor, _) = index
            .query_repos(RepoQuery {
                registry: "maven".to_string(),
                after: None,
                filter: None,
                limit: 1,
                max_examined: 10,
                deadline: std::time::Duration::from_secs(1),
                name_prefix: Some("repositories/".to_string()),
                before_name: None,
                allowed_repositories: Some(vec!["allowed".to_string()]),
            })
            .await
            .unwrap();
        assert_eq!(first[0].name, "repositories/allowed/a");
        let (second, _, _) = index
            .query_repos(RepoQuery {
                registry: "maven".to_string(),
                after: cursor,
                filter: None,
                limit: 1,
                max_examined: 10,
                deadline: std::time::Duration::from_secs(1),
                name_prefix: Some("repositories/".to_string()),
                before_name: None,
                allowed_repositories: Some(vec!["allowed".to_string()]),
            })
            .await
            .unwrap();
        assert_eq!(second[0].name, "repositories/allowed/b");

        let (empty, progress, _) = index
            .query_repos(RepoQuery {
                registry: "maven".to_string(),
                after: None,
                filter: Some("does-not-match".to_string()),
                limit: 10,
                max_examined: 1,
                deadline: std::time::Duration::from_secs(1),
                name_prefix: Some("repositories/".to_string()),
                before_name: None,
                allowed_repositories: None,
            })
            .await
            .unwrap();
        assert!(empty.is_empty());
        assert!(
            progress.is_some(),
            "bounded filtered scans must make cursor progress"
        );

        let (legacy, _, _) = index
            .query_repos(RepoQuery {
                registry: "maven".to_string(),
                after: None,
                filter: None,
                limit: 10,
                max_examined: 10,
                deadline: std::time::Duration::from_secs(1),
                name_prefix: None,
                before_name: Some("repositories/".to_string()),
                allowed_repositories: None,
            })
            .await
            .unwrap();
        assert_eq!(legacy.len(), 1);
        assert_eq!(legacy[0].name, "com/legacy");
        index.shutdown().await;
    }

    #[test]
    fn prefix_successor_handles_rollover() {
        assert_eq!(prefix_successor(b"repo"), b"repp");
        assert_eq!(prefix_successor(&[1, 255]), vec![2]);
        assert_eq!(prefix_successor(&[255]), vec![255]);
    }

    #[tokio::test]
    async fn preflight_distinguishes_missing_healthy_and_writer_overlap() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("index.redb");
        assert_eq!(preflight_database(&path), ChildPreflight::Missing);

        let index = PersistentIndex::open(&path, "cfg".to_string()).unwrap();
        assert_eq!(preflight_database(&path), ChildPreflight::AlreadyOpen);
        assert!(matches!(
            PersistentIndex::open(&path, "cfg".to_string()),
            Err(StoreError::AlreadyOpen)
        ));
        index.shutdown().await;
        drop(index);
        assert_eq!(preflight_database(&path), ChildPreflight::Healthy);
    }

    #[tokio::test]
    async fn clean_shutdown_skips_full_integrity_but_unclean_shutdown_runs_it() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("index.redb");
        let index = PersistentIndex::open(&path, "cfg".to_string()).unwrap();
        index.shutdown().await;
        drop(index);

        assert_eq!(
            preflight_database_with(&path, |_| {
                panic!("a cleanly closed database must not run the full integrity scan")
            }),
            ChildPreflight::Healthy
        );

        let index = PersistentIndex::open(&path, "cfg".to_string()).unwrap();
        index.shutdown_unclean_for_test().await;
        drop(index);
        let called = AtomicBool::new(false);
        assert_eq!(
            preflight_database_with(&path, |database| {
                called.store(true, Ordering::Release);
                database.check_integrity()
            }),
            ChildPreflight::HealthyAfterIntegrity
        );
        assert!(called.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn stale_child_clean_proof_cannot_override_current_unclean_marker() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("index.redb");
        let first = PersistentIndex::open(&path, "cfg".to_string()).unwrap();
        first.shutdown_unclean_for_test().await;
        drop(first);

        let reopened =
            PersistentIndex::open_with_startup_state_for_test(&path, "cfg".to_string(), true)
                .unwrap();
        assert!(
            !reopened.startup_clean(),
            "a stale clean child result must be ANDed with the marker read by the current parent opener"
        );
        reopened.shutdown_unclean_for_test().await;
    }

    #[tokio::test]
    async fn incompatible_application_schema_is_rejected_without_full_scan() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("index.redb");
        let index = PersistentIndex::open(&path, "cfg".to_string()).unwrap();
        index.shutdown().await;
        drop(index);

        let mut builder = redb::Builder::new();
        builder.set_cache_size(CACHE_BYTES);
        let database = builder.open(&path).unwrap();
        commit(&database, |transaction| {
            let table = transaction
                .open_table(META)
                .map_err(|error| StoreError::Database(error.to_string()))?;
            let value = table
                .get("state")
                .map_err(|error| StoreError::Database(error.to_string()))?
                .unwrap();
            let mut state: MetaState = decode(value.value())?;
            drop(value);
            drop(table);
            state.schema = SCHEMA_VERSION - 1;
            write_meta(transaction, &state)
        })
        .unwrap();
        drop(database);

        assert_eq!(
            preflight_database_with(&path, |_| {
                panic!("an incompatible application schema must not scan the old database")
            }),
            ChildPreflight::SchemaMismatch
        );
    }

    #[tokio::test]
    async fn preflight_classifies_redb_corruption_without_masking_it_as_io() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("index.redb");
        let marker = "nora-redb-corruption-marker-".repeat(96);
        let index = PersistentIndex::open(&path, "cfg".to_string()).unwrap();
        index
            .put_maven_objects(
                Slot::A,
                vec![(
                    b"maven/corruption-probe".to_vec(),
                    StoredObject {
                        meta: FileMeta {
                            size: 1,
                            modified: 1,
                            etag: Some(marker.clone()),
                            version_id: None,
                        },
                    },
                )],
            )
            .await
            .unwrap();
        index.shutdown_unclean_for_test().await;
        drop(index);

        let mut bytes = std::fs::read(&path).unwrap();
        let offset = bytes
            .windows(marker.len())
            .position(|window| window == marker.as_bytes())
            .expect("marker must be stored verbatim in a redb data page");
        bytes[offset + marker.len() / 2] ^= 0xff;
        std::fs::write(&path, bytes).unwrap();

        assert_eq!(
            preflight_database(&path),
            ChildPreflight::IntegrityFailed,
            "redb's explicit Corrupted error is safe to quarantine; generic I/O remains Failed"
        );
    }

    #[test]
    fn timeout_reseed_capacity_reserves_fresh_active_shadow_and_headroom() {
        let current = 725_848_064u64;
        let required = current.saturating_mul(2).saturating_add(current / 4);
        assert_eq!(reseed_required_available(current), required);
        assert!(matches!(
            ensure_reseed_capacity(current, required - 1, 0),
            Err(StoreError::DiskAdmission(_))
        ));
        assert!(ensure_reseed_capacity(current, required - 1, 1).is_ok());

        let small = 1024u64;
        assert_eq!(
            reseed_required_available(small),
            small * 2 + 64 * 1024 * 1024
        );
    }

    #[test]
    fn timeout_reseed_low_space_refusal_keeps_primary_and_prior_evidence() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("index.redb");
        let primary = b"current derived database";
        std::fs::write(&path, primary).unwrap();
        let old_evidence = path.with_file_name(format!("{}old", timeout_evidence_prefix(&path)));
        std::fs::write(&old_evidence, b"old").unwrap();

        assert!(matches!(
            preserve_timed_out_database_with_available(&path, 0),
            Err(StoreError::DiskAdmission(_))
        ));
        assert_eq!(std::fs::read(&path).unwrap(), primary);
        assert!(old_evidence.exists());
    }

    #[tokio::test]
    async fn timed_out_preflight_preserves_one_exact_file_then_retires_it_after_s2() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("index.redb");
        let index = PersistentIndex::open(&path, "cfg".to_string()).unwrap();
        let old_uuid = index.meta().await.unwrap().db_uuid;
        index.shutdown().await;
        drop(index);
        let original = std::fs::read(&path).unwrap();

        let old_evidence = path.with_file_name(format!("{}old", timeout_evidence_prefix(&path)));
        std::fs::write(&old_evidence, b"older derived evidence").unwrap();
        let quarantine = path.with_file_name("index.redb.quarantine.keep");
        std::fs::write(&quarantine, b"typed corruption evidence").unwrap();

        prepare_database_after_preflight(&path, ParentPreflight::TimedOut).unwrap();
        assert!(!path.exists());
        assert!(!old_evidence.exists(), "timeout evidence must stay bounded");
        assert!(quarantine.exists(), "typed-corruption evidence is separate");
        let evidence = timeout_evidence_paths(&path).unwrap();
        assert_eq!(evidence.len(), 1);
        let preserved = evidence[0].clone();
        assert_eq!(std::fs::read(&preserved).unwrap(), original);

        let fresh = PersistentIndex::open(&path, "cfg".to_string()).unwrap();
        assert_ne!(fresh.meta().await.unwrap().db_uuid, old_uuid);
        fresh
            .flip(
                Slot::A,
                0,
                RegistryCompleteness::default(),
                RegistryTotals::default(),
                "cfg".to_string(),
            )
            .await
            .unwrap();
        assert!(
            !preserved.exists(),
            "successful S2 publication retires timeout evidence"
        );
        assert!(quarantine.exists());
        fresh.shutdown().await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn timed_out_preflight_child_is_killed_and_reaped_before_fallback() {
        let mut child = tokio::process::Command::new("/bin/sh")
            .arg("-c")
            .arg("sleep 60")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();

        assert_eq!(
            wait_preflight_child(&mut child, std::time::Duration::from_millis(10))
                .await
                .unwrap(),
            ParentPreflight::TimedOut
        );
        assert!(
            child.try_wait().unwrap().is_some(),
            "the timed-out child must be reaped before the DB file can be renamed"
        );
    }

    #[test]
    fn non_timeout_preflight_failure_preserves_primary_in_place() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("index.redb");
        let bytes = b"derived index evidence";
        std::fs::write(&path, bytes).unwrap();

        assert!(matches!(
            prepare_database_after_preflight(
                &path,
                ParentPreflight::Unavailable {
                    reason: "permission or signal".to_string(),
                },
            ),
            Err(StoreError::PreflightUnavailable(_))
        ));
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        assert!(timeout_evidence_paths(&path).unwrap().is_empty());
    }

    #[test]
    fn preflight_quarantines_only_typed_corruption_not_generic_io() {
        let corrupt = redb::DatabaseError::Storage(redb::StorageError::Corrupted(
            "checksum mismatch".to_string(),
        ));
        assert_eq!(
            classify_database_preflight_error(&corrupt),
            ChildPreflight::IntegrityFailed
        );

        let io = redb::DatabaseError::Storage(redb::StorageError::Io(std::io::Error::from(
            std::io::ErrorKind::PermissionDenied,
        )));
        assert_eq!(
            classify_database_preflight_error(&io),
            ChildPreflight::Failed,
            "an access failure must preserve the original database"
        );
    }

    #[cfg(unix)]
    #[test]
    fn parent_preflight_quarantines_only_typed_corruption_exit_codes() {
        use std::os::unix::process::ExitStatusExt as _;

        assert!(matches!(
            classify_preflight_status(std::process::ExitStatus::from_raw(21 << 8)),
            ParentPreflight::CorruptOrIncompatible { .. }
        ));
        assert!(matches!(
            classify_preflight_status(std::process::ExitStatus::from_raw(6)),
            ParentPreflight::Unavailable { .. }
        ));
        assert!(matches!(
            classify_preflight_status(std::process::ExitStatus::from_raw(9)),
            ParentPreflight::Unavailable { .. }
        ));
        assert_eq!(
            classify_preflight_status(std::process::ExitStatus::from_raw(23 << 8)),
            ParentPreflight::AlreadyOpen
        );
        assert_eq!(
            classify_preflight_status(std::process::ExitStatus::from_raw(25 << 8)),
            ParentPreflight::Healthy {
                full_integrity: true
            }
        );
    }
}
