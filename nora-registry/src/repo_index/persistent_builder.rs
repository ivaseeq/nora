// Copyright (c) 2026 The NORA Authors
// SPDX-License-Identifier: MIT

use super::redb_store::{
    apply_totals_delta, encoded_row_bytes, maven_member_prefix, maven_prefix_stats_key,
    npm_package_key, npm_version_key, repo_key, ChangeEvent, IncrementalUpdate, MavenIndexView,
    MavenPrefixStats, MetaState, PersistentIndex, RegistryCompleteness, RegistryTotals, Slot,
    StoreError, StoredNpmPackage, StoredNpmVersion, StoredObject, StoredRepo, MAX_QUERY_EXAMINED,
    MAX_TX_BYTES, MAX_TX_ROWS,
};
use super::{npm_search_projection, valid_index_sha256};
use crate::storage::{FileMeta, Storage, StorageError};
use futures::StreamExt;
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use thiserror::Error;

/// Observable phase of the current authoritative Maven/npm reconciliation.
///
/// S3 listings are streamed and do not expose a total before EOF, so this is
/// deliberately phase-and-count progress rather than a misleading percent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum PersistentIndexPhase {
    Idle = 0,
    Preparing = 1,
    Recovering = 2,
    MavenInventory = 3,
    NpmInventory = 4,
    NpmAuthority = 5,
    Publishing = 6,
    RetryWaiting = 7,
}

impl PersistentIndexPhase {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::Preparing,
            2 => Self::Recovering,
            3 => Self::MavenInventory,
            4 => Self::NpmInventory,
            5 => Self::NpmAuthority,
            6 => Self::Publishing,
            7 => Self::RetryWaiting,
            _ => Self::Idle,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PersistentIndexProgress {
    pub phase: PersistentIndexPhase,
    pub maven_objects: u64,
    pub npm_objects: u64,
    pub npm_packages: u64,
}

pub(super) struct ReconcileProgress {
    phase: AtomicU8,
    maven_objects: AtomicU64,
    npm_objects: AtomicU64,
    npm_packages: AtomicU64,
}

impl ReconcileProgress {
    pub(super) fn new(phase: PersistentIndexPhase) -> Self {
        Self {
            phase: AtomicU8::new(phase as u8),
            maven_objects: AtomicU64::new(0),
            npm_objects: AtomicU64::new(0),
            npm_packages: AtomicU64::new(0),
        }
    }

    pub(super) fn begin(&self) {
        self.reset(PersistentIndexPhase::Preparing);
    }

    pub(super) fn reset(&self, phase: PersistentIndexPhase) {
        self.maven_objects.store(0, Ordering::Release);
        self.npm_objects.store(0, Ordering::Release);
        self.npm_packages.store(0, Ordering::Release);
        self.set_phase(phase);
    }

    pub(super) fn set_phase(&self, phase: PersistentIndexPhase) {
        self.phase.store(phase as u8, Ordering::Release);
    }

    fn set_objects(&self, maven: bool, count: u64) {
        if maven {
            self.maven_objects.store(count, Ordering::Release);
        } else {
            self.npm_objects.store(count, Ordering::Release);
        }
    }

    fn set_npm_packages(&self, count: u64) {
        self.npm_packages.store(count, Ordering::Release);
    }

    pub(super) fn snapshot(&self) -> PersistentIndexProgress {
        PersistentIndexProgress {
            phase: PersistentIndexPhase::from_u8(self.phase.load(Ordering::Acquire)),
            maven_objects: self.maven_objects.load(Ordering::Acquire),
            npm_objects: self.npm_objects.load(Ordering::Acquire),
            npm_packages: self.npm_packages.load(Ordering::Acquire),
        }
    }
}

#[derive(Clone, Copy)]
struct InventoryProgress<'a> {
    reconcile: Option<&'a ReconcileProgress>,
    maven: bool,
}

impl<'a> InventoryProgress<'a> {
    fn new(reconcile: Option<&'a ReconcileProgress>, maven: bool) -> Self {
        Self { reconcile, maven }
    }

    fn set_objects(self, count: u64) {
        if let Some(progress) = self.reconcile {
            progress.set_objects(self.maven, count);
        }
    }
}

#[derive(Debug, Error)]
pub enum ReconcileError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("storage reconciliation failed: {0}")]
    Storage(#[from] StorageError),
    #[error("index projection serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("npm authority is incomplete for {repository}/{package}: {reason}")]
    NpmAuthority {
        repository: String,
        package: String,
        reason: &'static str,
    },
}

fn npm_error(repository: &str, package: &str, reason: &'static str) -> ReconcileError {
    ReconcileError::NpmAuthority {
        repository: repository.to_string(),
        package: package.to_string(),
        reason,
    }
}

fn npm_version_counts(versions: &serde_json::Map<String, serde_json::Value>) -> (u64, u64) {
    versions
        .keys()
        .fold((0, 0), |(stable, prerelease), version| {
            if version.contains('-') {
                (stable, prerelease.saturating_add(1))
            } else {
                (stable.saturating_add(1), prerelease)
            }
        })
}

fn npm_version_ranks(
    versions: &serde_json::Map<String, serde_json::Value>,
) -> BTreeMap<String, u64> {
    let mut ordered = versions.keys().cloned().collect::<Vec<_>>();
    ordered.sort_by(|left, right| {
        let parsed_left = semver::Version::parse(left.trim_start_matches('v')).ok();
        let parsed_right = semver::Version::parse(right.trim_start_matches('v')).ok();
        match (parsed_left, parsed_right) {
            (Some(left_version), Some(right_version)) => right_version
                .cmp(&left_version)
                .then_with(|| right.cmp(left)),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => right.cmp(left),
        }
    });
    ordered
        .into_iter()
        .enumerate()
        .map(|(rank, version)| (version, rank as u64))
        .collect()
}

/// Page npm version writes by their exact redb key/envelope serialization.
/// One large packument may contain thousands of moderately sized manifests;
/// manifest-only estimates are not a safe transaction admission boundary.
struct NpmVersionBatch<'a> {
    index: &'a PersistentIndex,
    slot: Slot,
    rows: Vec<(Vec<u8>, StoredNpmVersion)>,
    bytes: usize,
}

impl<'a> NpmVersionBatch<'a> {
    fn new(index: &'a PersistentIndex, slot: Slot) -> Self {
        Self {
            index,
            slot,
            rows: Vec::with_capacity(MAX_TX_ROWS),
            bytes: 0,
        }
    }

    async fn push(&mut self, key: Vec<u8>, row: StoredNpmVersion) -> Result<(), ReconcileError> {
        let row_bytes = encoded_row_bytes(&key, &row)?;
        if row_bytes > MAX_TX_BYTES {
            return Err(StoreError::TransactionTooLarge.into());
        }
        if !self.rows.is_empty()
            && (self.rows.len() == MAX_TX_ROWS
                || self.bytes.saturating_add(row_bytes) > MAX_TX_BYTES)
        {
            self.flush().await?;
        }
        self.bytes = self.bytes.saturating_add(row_bytes);
        self.rows.push((key, row));
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), ReconcileError> {
        if self.rows.is_empty() {
            return Ok(());
        }
        self.index
            .put_npm_versions(self.slot, std::mem::take(&mut self.rows))
            .await?;
        self.bytes = 0;
        Ok(())
    }

    async fn finish(mut self) -> Result<(), ReconcileError> {
        self.flush().await
    }
}

fn checked_add_counter(value: &mut u64, delta: u64) -> Result<(), StoreError> {
    *value = value
        .checked_add(delta)
        .ok_or(StoreError::ProjectionOverflow)?;
    Ok(())
}

const MAVEN_STATS_SCAN_PAGE_ROWS: usize = 64;

struct MavenMemberCursor {
    base: String,
    after: Option<Vec<u8>>,
    buffered: VecDeque<(String, FileMeta)>,
    complete: bool,
    exclude_named_layout: bool,
}

impl MavenMemberCursor {
    fn new(member: &str, exclude_named_layout: bool) -> Self {
        Self {
            base: maven_member_prefix(member),
            after: None,
            buffered: VecDeque::new(),
            complete: false,
            exclude_named_layout,
        }
    }

    async fn next(
        &mut self,
        index: &PersistentIndex,
        slot: Slot,
    ) -> Result<Option<(String, FileMeta)>, ReconcileError> {
        loop {
            if let Some(row) = self.buffered.pop_front() {
                return Ok(Some(row));
            }
            if self.complete {
                return Ok(None);
            }
            let (rows, next) = index
                .scan_objects(
                    slot,
                    self.base.as_bytes().to_vec(),
                    self.after.take(),
                    MAVEN_STATS_SCAN_PAGE_ROWS,
                )
                .await?;
            self.after = next;
            self.complete = self.after.is_none();
            for (key, meta) in rows {
                let logical = key.strip_prefix(&self.base).ok_or_else(|| {
                    StoreError::ProjectionInvariant(
                        "Maven inventory scan escaped its member prefix".to_string(),
                    )
                })?;
                if self.exclude_named_layout && logical.starts_with("repositories/") {
                    continue;
                }
                self.buffered.push_back((logical.to_string(), meta));
            }
        }
    }
}

struct MavenPrefixBatch<'a> {
    index: &'a PersistentIndex,
    slot: Slot,
    repository: &'a str,
    rows: Vec<(Vec<u8>, MavenPrefixStats)>,
    bytes: usize,
}

impl<'a> MavenPrefixBatch<'a> {
    fn new(index: &'a PersistentIndex, slot: Slot, repository: &'a str) -> Self {
        Self {
            index,
            slot,
            repository,
            rows: Vec::with_capacity(MAX_TX_ROWS),
            bytes: 0,
        }
    }

    async fn push(&mut self, path: String, row: MavenPrefixStats) -> Result<(), ReconcileError> {
        let key = maven_prefix_stats_key(self.repository, &path);
        let row_bytes = encoded_row_bytes(&key, &row)?;
        if row_bytes > MAX_TX_BYTES {
            return Err(StoreError::TransactionTooLarge.into());
        }
        if !self.rows.is_empty()
            && (self.rows.len() == MAX_TX_ROWS
                || self
                    .bytes
                    .checked_add(row_bytes)
                    .ok_or(StoreError::ProjectionOverflow)?
                    > MAX_TX_BYTES)
        {
            self.flush().await?;
        }
        self.bytes = self
            .bytes
            .checked_add(row_bytes)
            .ok_or(StoreError::ProjectionOverflow)?;
        self.rows.push((key, row));
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), ReconcileError> {
        if self.rows.is_empty() {
            return Ok(());
        }
        self.index
            .put_maven_prefix_stats(self.slot, std::mem::take(&mut self.rows))
            .await?;
        self.bytes = 0;
        Ok(())
    }

    async fn finish(mut self) -> Result<(), ReconcileError> {
        self.flush().await
    }
}

#[derive(Debug)]
struct MavenDirectoryFrame {
    path: String,
    stats: MavenPrefixStats,
}

struct MavenStatsAccumulator {
    /// Lexicographic traversal keeps only the current ancestor chain open.
    frames: Vec<MavenDirectoryFrame>,
}

impl MavenStatsAccumulator {
    fn new() -> Self {
        Self {
            frames: vec![MavenDirectoryFrame {
                path: String::new(),
                stats: MavenPrefixStats::default(),
            }],
        }
    }

    fn parent_paths(logical_path: &str) -> Result<Vec<String>, StoreError> {
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
            .map_or("", |(parent, _)| parent);
        let mut paths = vec![String::new()];
        if !parent.is_empty() {
            let mut current = String::new();
            for component in parent.split('/') {
                if !current.is_empty() {
                    current.push('/');
                }
                current.push_str(component);
                paths.push(current.clone());
            }
        }
        Ok(paths)
    }

    fn add(
        &mut self,
        logical_path: &str,
        size: u64,
    ) -> Result<Vec<MavenDirectoryFrame>, StoreError> {
        let paths = Self::parent_paths(logical_path)?;
        let common = self
            .frames
            .iter()
            .zip(&paths)
            .take_while(|(frame, path)| frame.path == **path)
            .count();
        let mut finalized = Vec::with_capacity(self.frames.len().saturating_sub(common));
        while self.frames.len() > common {
            finalized.push(self.frames.pop().ok_or_else(|| {
                StoreError::ProjectionInvariant("Maven prefix stack underflow".to_string())
            })?);
        }
        for path in paths.into_iter().skip(common) {
            self.frames.push(MavenDirectoryFrame {
                path,
                stats: MavenPrefixStats::default(),
            });
        }
        for frame in &mut self.frames {
            checked_add_counter(&mut frame.stats.subtree_files, 1)?;
            checked_add_counter(&mut frame.stats.subtree_bytes, size)?;
        }
        let direct = self.frames.last_mut().ok_or_else(|| {
            StoreError::ProjectionInvariant("Maven prefix stack is empty".to_string())
        })?;
        checked_add_counter(&mut direct.stats.direct_files, 1)?;
        checked_add_counter(&mut direct.stats.direct_bytes, size)?;
        Ok(finalized)
    }

    fn finish(mut self) -> Vec<MavenDirectoryFrame> {
        let mut finalized = Vec::with_capacity(self.frames.len());
        while let Some(frame) = self.frames.pop() {
            finalized.push(frame);
        }
        finalized
    }
}

/// Build exact logical Maven directory aggregates after the raw object
/// inventory is complete. Ordered member cursors are merged by logical path;
/// ties consume every member while the earliest member supplies the winner.
/// The directory stack finalizes prefixes as lexical traversal leaves them,
/// bounding memory by member pages, path depth and one writer batch.
async fn build_maven_prefix_stats(
    index: &PersistentIndex,
    slot: Slot,
    views: &[MavenIndexView],
) -> Result<(), ReconcileError> {
    for view in views {
        if view.members.is_empty() {
            return Err(StoreError::ProjectionInvariant(format!(
                "Maven view {:?} has no members",
                view.repository
            ))
            .into());
        }
        let mut cursors = view
            .members
            .iter()
            .map(|member| MavenMemberCursor::new(member, view.repository.is_empty()))
            .collect::<Vec<_>>();
        let mut heads = Vec::with_capacity(cursors.len());
        for cursor in &mut cursors {
            heads.push(cursor.next(index, slot).await?);
        }
        let mut accumulator = MavenStatsAccumulator::new();
        let mut batch = MavenPrefixBatch::new(index, slot, &view.repository);
        while let Some(winner_index) = heads
            .iter()
            .enumerate()
            .filter_map(|(index, row)| row.as_ref().map(|(path, _)| (index, path)))
            .min_by(|(left_index, left), (right_index, right)| {
                left.cmp(right).then_with(|| left_index.cmp(right_index))
            })
            .map(|(index, _)| index)
        {
            let (logical, meta) = heads[winner_index].as_ref().cloned().ok_or_else(|| {
                StoreError::ProjectionInvariant("Maven merge winner disappeared".to_string())
            })?;
            for finalized in accumulator.add(&logical, meta.size)? {
                batch.push(finalized.path, finalized.stats).await?;
            }
            for cursor_index in 0..heads.len() {
                if heads[cursor_index]
                    .as_ref()
                    .is_some_and(|(path, _)| path == &logical)
                {
                    heads[cursor_index] = cursors[cursor_index].next(index, slot).await?;
                }
            }
        }
        for finalized in accumulator.finish() {
            batch.push(finalized.path, finalized.stats).await?;
        }
        batch.finish().await?;
    }
    Ok(())
}

async fn stage_prefix(
    index: &PersistentIndex,
    storage: &Storage,
    slot: Slot,
    prefix: &str,
    progress: InventoryProgress<'_>,
) -> Result<(u64, u64, u64), ReconcileError> {
    let mut stream = storage.list_with_meta_stream(prefix).await?;
    let mut batch = Vec::<(Vec<u8>, StoredObject)>::with_capacity(MAX_TX_ROWS);
    let mut bytes = 0usize;
    let mut count = 0u64;
    let mut artifacts = 0u64;
    let mut logical_bytes = 0u64;
    while let Some(entry) = stream.next().await {
        let (key, meta) = entry?;
        if progress.maven {
            logical_bytes = logical_bytes.saturating_add(meta.size);
            if !crate::gc::is_checksum_sidecar(&key) && !key.ends_with("maven-metadata.xml") {
                artifacts = artifacts.saturating_add(1);
            }
        }
        let key = key.into_bytes();
        let row = StoredObject { meta };
        let row_bytes = encoded_row_bytes(&key, &row)?;
        if !batch.is_empty()
            && (batch.len() == MAX_TX_ROWS || bytes.saturating_add(row_bytes) > MAX_TX_BYTES)
        {
            let rows = std::mem::take(&mut batch);
            if progress.maven {
                index.put_maven_objects(slot, rows).await?;
            } else {
                index.put_npm_objects(slot, rows).await?;
            }
            progress.set_objects(count);
            bytes = 0;
        }
        bytes = bytes.saturating_add(row_bytes);
        batch.push((key, row));
        count = count.saturating_add(1);
    }
    if !batch.is_empty() {
        if progress.maven {
            index.put_maven_objects(slot, batch).await?;
        } else {
            index.put_npm_objects(slot, batch).await?;
        }
    }
    progress.set_objects(count);
    Ok((count, artifacts, logical_bytes))
}

/// Defend an S2 publication against a provider LIST that returned success but
/// silently omitted keys from the compatible last-good generation. Only keys
/// absent from the new shadow inventory incur an exact HEAD. A real NotFound
/// proves deletion; any uncertain HEAD aborts the generation.
async fn restore_unseen_active(
    index: &PersistentIndex,
    storage: &Storage,
    active: Slot,
    shadow: Slot,
    prefix: &str,
    base_count: u64,
    progress: InventoryProgress<'_>,
) -> Result<(u64, u64, u64), ReconcileError> {
    let mut after = None;
    let mut restored = 0u64;
    let mut artifacts = 0u64;
    let mut logical_bytes = 0u64;
    let mut batch = Vec::<(Vec<u8>, StoredObject)>::with_capacity(MAX_TX_ROWS);
    let mut batch_bytes = 0usize;

    loop {
        let (rows, next) = index
            .scan_objects(active, prefix.as_bytes().to_vec(), after, MAX_TX_ROWS)
            .await?;
        for (key, _) in rows {
            if index.get_object_in_slot(shadow, &key).await?.is_some() {
                continue;
            }
            let Some(meta) = storage.stat(&key).await? else {
                continue;
            };
            if progress.maven {
                logical_bytes = logical_bytes.saturating_add(meta.size);
                if !crate::gc::is_checksum_sidecar(&key) && !key.ends_with("maven-metadata.xml") {
                    artifacts = artifacts.saturating_add(1);
                }
            }
            let key = key.into_bytes();
            let row = StoredObject { meta };
            let row_bytes = encoded_row_bytes(&key, &row)?;
            if !batch.is_empty()
                && (batch.len() == MAX_TX_ROWS
                    || batch_bytes.saturating_add(row_bytes) > MAX_TX_BYTES)
            {
                let rows = std::mem::take(&mut batch);
                if progress.maven {
                    index.put_maven_objects(shadow, rows).await?;
                } else {
                    index.put_npm_objects(shadow, rows).await?;
                }
                progress.set_objects(base_count.saturating_add(restored));
                batch_bytes = 0;
            }
            batch_bytes = batch_bytes.saturating_add(row_bytes);
            batch.push((key, row));
            restored = restored.saturating_add(1);
        }
        let Some(next) = next else { break };
        after = Some(next);
    }
    if !batch.is_empty() {
        if progress.maven {
            index.put_maven_objects(shadow, batch).await?;
        } else {
            index.put_npm_objects(shadow, batch).await?;
        }
    }
    progress.set_objects(base_count.saturating_add(restored));
    Ok((restored, artifacts, logical_bytes))
}

fn hosted_version_key(repository: &str, package: &str, version: &str) -> String {
    format!("npm/repositories/{repository}/{package}/versions/{version}.json")
}

async fn read_slot_meta(
    index: &PersistentIndex,
    slot: Slot,
    key: &str,
    repository: &str,
    package: &str,
    reason: &'static str,
) -> Result<FileMeta, ReconcileError> {
    index
        .get_object_in_slot(slot, key)
        .await?
        .ok_or_else(|| npm_error(repository, package, reason))
}

async fn build_hosted_package(
    index: &PersistentIndex,
    storage: &Storage,
    slot: Slot,
    repository: &str,
    package: &str,
    current_key: &str,
) -> Result<(u64, u64), ReconcileError> {
    let current_meta = read_slot_meta(
        index,
        slot,
        current_key,
        repository,
        package,
        "current_missing_from_listing",
    )
    .await?;
    let pointer_bytes = storage.get(current_key).await.map_err(|_| {
        npm_error(
            repository,
            package,
            "current_get_failed_or_integrity_unknown",
        )
    })?;
    let pointer: crate::npm_layout::HostedPackumentPointer = serde_json::from_slice(&pointer_bytes)
        .map_err(|_| npm_error(repository, package, "current_invalid"))?;
    if !valid_index_sha256(&pointer.generation)
        || !valid_index_sha256(&pointer.full_sha256)
        || !valid_index_sha256(&pointer.install_v1_sha256)
    {
        return Err(npm_error(repository, package, "current_invalid"));
    }
    let pointer_sha256 = crate::npm_layout::hosted_manifest_digest(&pointer_bytes);
    let full_key =
        crate::npm_layout::hosted_packument_full_key(repository, package, &pointer.generation);
    let install_key = crate::npm_layout::hosted_packument_install_v1_key(
        repository,
        package,
        &pointer.generation,
    );
    let full_meta = read_slot_meta(
        index,
        slot,
        &full_key,
        repository,
        package,
        "full_missing_from_listing",
    )
    .await?;
    let install_meta = read_slot_meta(
        index,
        slot,
        &install_key,
        repository,
        package,
        "install_v1_missing_from_listing",
    )
    .await?;
    let full = storage
        .get(&full_key)
        .await
        .map_err(|_| npm_error(repository, package, "full_get_failed_or_integrity_unknown"))?;
    let packument = crate::registry::validate_hosted_packument_generation(&full, package, &pointer)
        .ok_or_else(|| npm_error(repository, package, "generation_invalid"))?;
    let versions = packument
        .get("versions")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| npm_error(repository, package, "versions_invalid"))?;

    let mut modified = current_meta
        .modified
        .max(full_meta.modified)
        .max(install_meta.modified);
    let mut logical_size = 0u64;
    let mut dependencies = BTreeMap::from([
        (current_key.to_string(), Some(current_meta)),
        (full_key.clone(), Some(full_meta)),
        (install_key.clone(), Some(install_meta)),
    ]);
    let mut unique_blobs = BTreeMap::<String, FileMeta>::new();
    let mut version_batch = NpmVersionBatch::new(index, slot);
    let version_ranks = npm_version_ranks(versions);

    for (version, manifest) in versions {
        let sort_rank = version_ranks[version];
        if manifest.get("version").and_then(serde_json::Value::as_str) != Some(version.as_str()) {
            return Err(npm_error(repository, package, "version_manifest_mismatch"));
        }
        let manifest_bytes = serde_json::to_vec(manifest)?;
        let blob_key =
            crate::npm_layout::hosted_blob_key_from_manifest(repository, package, &manifest_bytes)
                .ok_or_else(|| npm_error(repository, package, "blob_reference_invalid"))?;
        let blob_meta = read_slot_meta(
            index,
            slot,
            &blob_key,
            repository,
            package,
            "blob_missing_from_listing",
        )
        .await?;
        modified = modified.max(blob_meta.modified);
        dependencies
            .entry(blob_key.clone())
            .or_insert_with(|| Some(blob_meta.clone()));
        unique_blobs
            .entry(blob_key.clone())
            .or_insert_with(|| blob_meta.clone());

        let split_key = hosted_version_key(repository, package, version);
        let split_meta = index.get_object_in_slot(slot, &split_key).await?;
        if let Some(split_meta) = &split_meta {
            logical_size = logical_size.saturating_add(split_meta.size);
            modified = modified.max(split_meta.modified);
            dependencies.insert(split_key, Some(split_meta.clone()));
        } else {
            dependencies.insert(split_key, None);
            tracing::warn!(
                repository,
                package,
                version,
                error_class = "active_split_version_missing",
                "npm index: active generation version has no split manifest; visibility retained"
            );
        }

        let row = StoredNpmVersion {
            repository: repository.to_string(),
            package: package.to_string(),
            version: version.to_string(),
            sort_rank,
            manifest: manifest.clone(),
            published: if split_meta.as_ref().map_or(0, |meta| meta.modified) == 0 {
                "N/A".to_string()
            } else {
                crate::ui::components::format_timestamp(
                    split_meta.as_ref().map_or(0, |meta| meta.modified),
                )
            },
            payload_key: blob_key.clone(),
            payload_meta: Some(blob_meta.clone()),
            declared_size: manifest
                .get("dist")
                .and_then(|dist| dist.get("unpackedSize"))
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0),
        };
        version_batch
            .push(
                npm_version_key(repository, package, sort_rank, version),
                row,
            )
            .await?;
    }
    version_batch.finish().await?;
    for meta in unique_blobs.values() {
        logical_size = logical_size.saturating_add(meta.size);
    }

    let search = npm_search_projection(repository, package, &packument);
    let (stable_versions, prerelease_versions) = npm_version_counts(versions);
    let package_row = StoredNpmPackage {
        repository: repository.to_string(),
        package: package.to_string(),
        pointer_sha256,
        modified,
        versions: versions.len() as u64,
        stable_versions,
        prerelease_versions,
        logical_size,
        dependencies,
        search,
    };
    index
        .put_npm_packages(
            slot,
            vec![(npm_package_key(repository, package), package_row)],
        )
        .await?;
    let name = format!("repositories/{repository}/{package}");
    index
        .put_repos(
            slot,
            vec![(
                repo_key("npm", &name),
                StoredRepo {
                    name,
                    artifact_count: versions.len() as u64,
                    logical_size: Some(logical_size),
                    modified,
                    is_file: false,
                },
            )],
        )
        .await?;
    Ok((versions.len() as u64, logical_size))
}

async fn build_proxy_package(
    index: &PersistentIndex,
    storage: &Storage,
    slot: Slot,
    repository: &str,
    package: &str,
    packument_key: &str,
) -> Result<(u64, u64), ReconcileError> {
    let packument_meta = read_slot_meta(
        index,
        slot,
        packument_key,
        repository,
        package,
        "proxy_packument_missing_from_listing",
    )
    .await?;
    let bytes = storage
        .get(packument_key)
        .await
        .map_err(|_| npm_error(repository, package, "proxy_packument_unavailable"))?;
    let packument: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|_| npm_error(repository, package, "proxy_packument_invalid"))?;
    if packument.get("name").and_then(serde_json::Value::as_str) != Some(package) {
        return Err(npm_error(
            repository,
            package,
            "proxy_packument_name_mismatch",
        ));
    }
    let versions = packument
        .get("versions")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| npm_error(repository, package, "proxy_versions_invalid"))?;
    let times = packument.get("time").and_then(serde_json::Value::as_object);
    let mut modified = packument_meta.modified;
    let mut logical_size = 0u64;
    let mut dependencies = BTreeMap::from([(packument_key.to_string(), Some(packument_meta))]);
    let mut unique_payloads = BTreeMap::<String, FileMeta>::new();
    let mut version_batch = NpmVersionBatch::new(index, slot);
    let version_ranks = npm_version_ranks(versions);

    for (version, manifest) in versions {
        let sort_rank = version_ranks[version];
        if manifest.get("version").and_then(serde_json::Value::as_str) != Some(version.as_str()) {
            return Err(npm_error(
                repository,
                package,
                "proxy_version_manifest_mismatch",
            ));
        }
        let filename = crate::registry::canonical_tarball_filename(package, version);
        let payload_key = crate::registry::proxy_tarball_key(repository, package, &filename);
        let payload_meta = index.get_object_in_slot(slot, &payload_key).await?;
        if let Some(meta) = &payload_meta {
            modified = modified.max(meta.modified);
            dependencies.insert(payload_key.clone(), Some(meta.clone()));
            unique_payloads
                .entry(payload_key.clone())
                .or_insert_with(|| meta.clone());
        } else {
            dependencies.insert(payload_key.clone(), None);
        }
        let row = StoredNpmVersion {
            repository: repository.to_string(),
            package: package.to_string(),
            version: version.to_string(),
            sort_rank,
            manifest: manifest.clone(),
            published: times
                .and_then(|values| values.get(version))
                .and_then(serde_json::Value::as_str)
                .map(|value| value.get(..10).unwrap_or(value).to_string())
                .unwrap_or_else(|| "N/A".to_string()),
            payload_key,
            payload_meta,
            declared_size: manifest
                .get("dist")
                .and_then(|dist| dist.get("unpackedSize"))
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0),
        };
        version_batch
            .push(
                npm_version_key(repository, package, sort_rank, version),
                row,
            )
            .await?;
    }
    version_batch.finish().await?;
    for meta in unique_payloads.values() {
        logical_size = logical_size.saturating_add(meta.size);
    }

    let (stable_versions, prerelease_versions) = npm_version_counts(versions);
    index
        .put_npm_packages(
            slot,
            vec![(
                npm_package_key(repository, package),
                StoredNpmPackage {
                    repository: repository.to_string(),
                    package: package.to_string(),
                    pointer_sha256: crate::npm_layout::hosted_manifest_digest(&bytes),
                    modified,
                    versions: versions.len() as u64,
                    stable_versions,
                    prerelease_versions,
                    logical_size,
                    dependencies,
                    search: npm_search_projection(repository, package, &packument),
                },
            )],
        )
        .await?;
    let name = format!("repositories/{repository}/{package}");
    index
        .put_repos(
            slot,
            vec![(
                repo_key("npm", &name),
                StoredRepo {
                    name,
                    artifact_count: versions.len() as u64,
                    logical_size: Some(logical_size),
                    modified,
                    is_file: false,
                },
            )],
        )
        .await?;
    Ok((versions.len() as u64, logical_size))
}

async fn reuse_npm_package(
    index: &PersistentIndex,
    active: Slot,
    shadow: Slot,
    repository: &str,
    package: &str,
) -> Result<Option<(u64, u64)>, ReconcileError> {
    let Some((package_row, versions)) = index
        .reusable_npm_projection(active, shadow, repository, package)
        .await?
    else {
        return Ok(None);
    };
    let version_count = package_row.versions;
    let logical_size = package_row.logical_size;
    index
        .put_npm_packages(
            shadow,
            vec![(npm_package_key(repository, package), package_row.clone())],
        )
        .await?;
    let mut version_batch = NpmVersionBatch::new(index, shadow);
    for row in versions {
        let key = npm_version_key(repository, package, row.sort_rank, &row.version);
        version_batch.push(key, row).await?;
    }
    version_batch.finish().await?;
    let name = format!("repositories/{repository}/{package}");
    index
        .put_repos(
            shadow,
            vec![(
                repo_key("npm", &name),
                StoredRepo {
                    name,
                    artifact_count: version_count,
                    logical_size: Some(logical_size),
                    modified: package_row.modified,
                    is_file: false,
                },
            )],
        )
        .await?;
    Ok(Some((version_count, logical_size)))
}

async fn list_bounded(
    storage: &Storage,
    prefix: &str,
) -> Result<BTreeMap<String, FileMeta>, ReconcileError> {
    let mut stream = storage.list_with_meta_stream(prefix).await?;
    let mut rows = BTreeMap::new();
    while let Some(entry) = stream.next().await {
        let (key, meta) = entry?;
        if rows.len() == MAX_QUERY_EXAMINED {
            return Err(StoreError::TransactionTooLarge.into());
        }
        rows.insert(key, meta);
    }
    Ok(rows)
}

async fn prepare_maven_incremental(
    storage: &Storage,
    repository: &str,
    path: &str,
    recursive: bool,
    views: &[MavenIndexView],
) -> Result<IncrementalUpdate, ReconcileError> {
    if path.is_empty() || path == "*" {
        return Err(StoreError::TransactionTooLarge.into());
    }
    let base = if repository.is_empty() {
        "maven/".to_string()
    } else {
        format!("maven/repositories/{repository}/")
    };
    let logical = path.trim_matches('/');
    let object_prefix = if recursive {
        format!("{base}{logical}/")
    } else {
        format!("{base}{logical}")
    };
    let list_prefix = if recursive {
        object_prefix.clone()
    } else {
        object_prefix
            .rsplit_once('/')
            .map_or_else(|| base.clone(), |(parent, _)| format!("{parent}/"))
    };
    let listed = list_bounded(storage, &list_prefix).await?;
    if listed.len() > MAX_QUERY_EXAMINED {
        return Err(StoreError::TransactionTooLarge.into());
    }
    let mut aggregates = BTreeMap::<String, (usize, u64, u64)>::new();
    let mut objects = Vec::with_capacity(listed.len());
    for (key, meta) in listed {
        if !recursive
            && key
                .strip_prefix(&list_prefix)
                .is_some_and(|relative| relative.contains('/'))
        {
            // A non-recursive bundle refresh replaces exactly one parent row.
            // S3 prefix LIST is recursive, so including descendants here would
            // add their totals without removing their previous rows.
            continue;
        }
        let Some(rest) = key.strip_prefix("maven/") else {
            continue;
        };
        if let Some((parent, _)) = rest.rsplit_once('/') {
            let aggregate = aggregates.entry(parent.to_string()).or_default();
            if !crate::gc::is_checksum_sidecar(&key) && !key.ends_with("maven-metadata.xml") {
                aggregate.0 = aggregate.0.saturating_add(1);
            }
            aggregate.1 = aggregate.1.saturating_add(meta.size);
            aggregate.2 = aggregate.2.max(meta.modified);
        }
        let belongs_to_bundle = recursive
            || key == object_prefix
            || ["md5", "sha1", "sha256", "sha512"]
                .iter()
                .any(|suffix| key == format!("{object_prefix}.{suffix}"));
        if belongs_to_bundle {
            objects.push((key.into_bytes(), StoredObject { meta }));
        }
    }
    let repos = aggregates
        .into_iter()
        .map(|(name, (count, size, modified))| {
            (
                repo_key("maven", &name),
                StoredRepo {
                    name,
                    artifact_count: count as u64,
                    logical_size: Some(size),
                    modified,
                    is_file: false,
                },
            )
        })
        .collect();
    let repo_name = if recursive {
        format!("{}{}", base.trim_start_matches("maven/"), logical)
    } else {
        let parent = logical.rsplit_once('/').map_or("", |(parent, _)| parent);
        format!("{}{}", base.trim_start_matches("maven/"), parent)
            .trim_end_matches('/')
            .to_string()
    };
    Ok(IncrementalUpdate::Maven {
        entity_key: format!("maven\0{repository}\0{logical}").into_bytes(),
        repository: repository.to_string(),
        object_prefix: object_prefix.into_bytes(),
        repo_prefix: repo_key("maven", &repo_name),
        repo_recursive: recursive,
        objects,
        repos,
        views: views
            .iter()
            .filter(|view| view.members.iter().any(|member| member == repository))
            .cloned()
            .collect(),
    })
}

fn npm_removal(repository: &str, package: &str) -> IncrementalUpdate {
    let package_key = npm_package_key(repository, package);
    let mut version_prefix = package_key.clone();
    version_prefix.push(0);
    let name = format!("repositories/{repository}/{package}");
    IncrementalUpdate::Npm {
        entity_key: format!("npm\0{repository}\0{package}").into_bytes(),
        package_key,
        version_prefix,
        repo_key: repo_key("npm", &name),
        package: None,
        versions: Vec::new(),
        repo: None,
    }
}

async fn prepare_hosted_npm_incremental(
    storage: &Storage,
    repository: &str,
    package: &str,
) -> Result<IncrementalUpdate, ReconcileError> {
    let package_prefix = format!("npm/repositories/{repository}/{package}/");
    let listed = list_bounded(storage, &package_prefix).await?;
    let current_key = crate::npm_layout::hosted_packument_current_key(repository, package);
    let Some(current_meta) = listed.get(&current_key).cloned() else {
        let retired_key = crate::npm_layout::hosted_packument_retired_key(repository, package);
        if listed.contains_key(&retired_key) {
            let retired = storage.get(&retired_key).await?;
            if retired.as_ref() == crate::npm_layout::HOSTED_PACKUMENT_RETIRED_V1 {
                return Ok(npm_removal(repository, package));
            }
            return Err(npm_error(repository, package, "retired_invalid"));
        }
        if listed.keys().any(|key| {
            crate::npm_layout::parse_npm_object_key(key).is_some_and(|parsed| {
                matches!(
                    parsed.kind,
                    crate::npm_layout::NpmObjectKind::HostedPackage
                        | crate::npm_layout::NpmObjectKind::HostedMaintenanceActive
                        | crate::npm_layout::NpmObjectKind::HostedImportPending
                        | crate::npm_layout::NpmObjectKind::HostedImportEvidence { .. }
                        | crate::npm_layout::NpmObjectKind::HostedImportReceipt(_)
                        | crate::npm_layout::NpmObjectKind::HostedPublishPending(_)
                        | crate::npm_layout::NpmObjectKind::HostedPublishPendingIndex
                        | crate::npm_layout::NpmObjectKind::HostedPublishComplete(_)
                        | crate::npm_layout::NpmObjectKind::HostedDistTag(_)
                        | crate::npm_layout::NpmObjectKind::HostedDeprecation(_)
                )
            })
        }) {
            return Err(npm_error(
                repository,
                package,
                "current_absent_with_live_state",
            ));
        }
        return Ok(npm_removal(repository, package));
    };
    let pointer_bytes = storage.get(&current_key).await?;
    let pointer: crate::npm_layout::HostedPackumentPointer = serde_json::from_slice(&pointer_bytes)
        .map_err(|_| npm_error(repository, package, "current_invalid"))?;
    if !valid_index_sha256(&pointer.generation)
        || !valid_index_sha256(&pointer.full_sha256)
        || !valid_index_sha256(&pointer.install_v1_sha256)
    {
        return Err(npm_error(repository, package, "current_invalid"));
    }
    let full_key =
        crate::npm_layout::hosted_packument_full_key(repository, package, &pointer.generation);
    let install_key = crate::npm_layout::hosted_packument_install_v1_key(
        repository,
        package,
        &pointer.generation,
    );
    let full_meta = listed
        .get(&full_key)
        .cloned()
        .ok_or_else(|| npm_error(repository, package, "full_missing_from_listing"))?;
    let install_meta = listed
        .get(&install_key)
        .cloned()
        .ok_or_else(|| npm_error(repository, package, "install_v1_missing_from_listing"))?;
    let full = storage.get(&full_key).await?;
    let packument = crate::registry::validate_hosted_packument_generation(&full, package, &pointer)
        .ok_or_else(|| npm_error(repository, package, "generation_invalid"))?;
    let version_map = packument
        .get("versions")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| npm_error(repository, package, "versions_invalid"))?;
    if version_map.len() > MAX_TX_ROWS {
        return Err(StoreError::TransactionTooLarge.into());
    }
    let mut modified = current_meta
        .modified
        .max(full_meta.modified)
        .max(install_meta.modified);
    let mut logical_size = 0u64;
    let mut dependencies = BTreeMap::from([
        (current_key.clone(), Some(current_meta)),
        (full_key.clone(), Some(full_meta)),
        (install_key, Some(install_meta)),
    ]);
    let mut unique_blobs = BTreeMap::<String, FileMeta>::new();
    let mut versions = Vec::with_capacity(version_map.len());
    let version_ranks = npm_version_ranks(version_map);
    for (version, manifest) in version_map {
        let sort_rank = version_ranks[version];
        let manifest_bytes = serde_json::to_vec(manifest)?;
        let blob_key =
            crate::npm_layout::hosted_blob_key_from_manifest(repository, package, &manifest_bytes)
                .ok_or_else(|| npm_error(repository, package, "blob_reference_invalid"))?;
        let blob_meta = listed
            .get(&blob_key)
            .cloned()
            .ok_or_else(|| npm_error(repository, package, "blob_missing_from_listing"))?;
        unique_blobs
            .entry(blob_key.clone())
            .or_insert_with(|| blob_meta.clone());
        dependencies.insert(blob_key.clone(), Some(blob_meta.clone()));
        modified = modified.max(blob_meta.modified);
        let split_key = hosted_version_key(repository, package, version);
        let split_meta = listed.get(&split_key).cloned();
        if let Some(meta) = &split_meta {
            logical_size = logical_size.saturating_add(meta.size);
            dependencies.insert(split_key, Some(meta.clone()));
            modified = modified.max(meta.modified);
        } else {
            dependencies.insert(split_key, None);
        }
        versions.push((
            npm_version_key(repository, package, sort_rank, version),
            StoredNpmVersion {
                repository: repository.to_string(),
                package: package.to_string(),
                version: version.to_string(),
                sort_rank,
                manifest: manifest.clone(),
                published: split_meta.as_ref().map_or_else(
                    || "N/A".to_string(),
                    |meta| crate::ui::components::format_timestamp(meta.modified),
                ),
                payload_key: blob_key,
                payload_meta: Some(blob_meta),
                declared_size: manifest
                    .get("dist")
                    .and_then(|dist| dist.get("unpackedSize"))
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0),
            },
        ));
    }
    logical_size =
        logical_size.saturating_add(unique_blobs.values().map(|meta| meta.size).sum::<u64>());
    let (stable_versions, prerelease_versions) = npm_version_counts(version_map);
    let name = format!("repositories/{repository}/{package}");
    let package_key = npm_package_key(repository, package);
    let mut version_prefix = package_key.clone();
    version_prefix.push(0);
    Ok(IncrementalUpdate::Npm {
        entity_key: format!("npm\0{repository}\0{package}").into_bytes(),
        package_key,
        version_prefix,
        repo_key: repo_key("npm", &name),
        package: Some(Box::new(StoredNpmPackage {
            repository: repository.to_string(),
            package: package.to_string(),
            pointer_sha256: crate::npm_layout::hosted_manifest_digest(&pointer_bytes),
            modified,
            versions: version_map.len() as u64,
            stable_versions,
            prerelease_versions,
            logical_size,
            dependencies,
            search: npm_search_projection(repository, package, &packument),
        })),
        versions,
        repo: Some(StoredRepo {
            name,
            artifact_count: version_map.len() as u64,
            logical_size: Some(logical_size),
            modified,
            is_file: false,
        }),
    })
}

async fn prepare_proxy_npm_incremental(
    storage: &Storage,
    repository: &str,
    package: &str,
) -> Result<IncrementalUpdate, ReconcileError> {
    let packument_key = format!("npm/repositories/{repository}/proxy/packuments/{package}.json");
    let packument_meta = match storage.stat(&packument_key).await? {
        Some(meta) => meta,
        None => return Ok(npm_removal(repository, package)),
    };
    let bytes = storage.get(&packument_key).await?;
    let packument: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|_| npm_error(repository, package, "proxy_packument_invalid"))?;
    let version_map = packument
        .get("versions")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| npm_error(repository, package, "proxy_versions_invalid"))?;
    if version_map.len() > MAX_TX_ROWS {
        return Err(StoreError::TransactionTooLarge.into());
    }
    let tarball_prefix = format!("npm/repositories/{repository}/proxy/tarballs/{package}/");
    let tarballs = list_bounded(storage, &tarball_prefix).await?;
    let times = packument.get("time").and_then(serde_json::Value::as_object);
    let mut logical_size = 0u64;
    let mut modified = packument_meta.modified;
    let mut dependencies = BTreeMap::from([(packument_key.clone(), Some(packument_meta))]);
    let mut versions = Vec::with_capacity(version_map.len());
    let version_ranks = npm_version_ranks(version_map);
    for (version, manifest) in version_map {
        let sort_rank = version_ranks[version];
        let filename = crate::registry::canonical_tarball_filename(package, version);
        let payload_key = crate::registry::proxy_tarball_key(repository, package, &filename);
        let payload_meta = tarballs.get(&payload_key).cloned();
        if let Some(meta) = &payload_meta {
            logical_size = logical_size.saturating_add(meta.size);
            modified = modified.max(meta.modified);
            dependencies.insert(payload_key.clone(), Some(meta.clone()));
        } else {
            dependencies.insert(payload_key.clone(), None);
        }
        versions.push((
            npm_version_key(repository, package, sort_rank, version),
            StoredNpmVersion {
                repository: repository.to_string(),
                package: package.to_string(),
                version: version.to_string(),
                sort_rank,
                manifest: manifest.clone(),
                published: times
                    .and_then(|values| values.get(version))
                    .and_then(serde_json::Value::as_str)
                    .map(|value| value.get(..10).unwrap_or(value).to_string())
                    .unwrap_or_else(|| "N/A".to_string()),
                payload_key,
                payload_meta,
                declared_size: manifest
                    .get("dist")
                    .and_then(|dist| dist.get("unpackedSize"))
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0),
            },
        ));
    }
    let name = format!("repositories/{repository}/{package}");
    let (stable_versions, prerelease_versions) = npm_version_counts(version_map);
    let package_key = npm_package_key(repository, package);
    let mut version_prefix = package_key.clone();
    version_prefix.push(0);
    Ok(IncrementalUpdate::Npm {
        entity_key: format!("npm\0{repository}\0{package}").into_bytes(),
        package_key,
        version_prefix,
        repo_key: repo_key("npm", &name),
        package: Some(Box::new(StoredNpmPackage {
            repository: repository.to_string(),
            package: package.to_string(),
            pointer_sha256: crate::npm_layout::hosted_manifest_digest(&bytes),
            modified,
            versions: version_map.len() as u64,
            stable_versions,
            prerelease_versions,
            logical_size,
            dependencies,
            search: npm_search_projection(repository, package, &packument),
        })),
        versions,
        repo: Some(StoredRepo {
            name,
            artifact_count: version_map.len() as u64,
            logical_size: Some(logical_size),
            modified,
            is_file: false,
        }),
    })
}

async fn prepare_change(
    storage: &Storage,
    event: ChangeEvent,
    maven_views: &[MavenIndexView],
) -> Result<IncrementalUpdate, ReconcileError> {
    match event {
        ChangeEvent::MavenPathChanged { repository, path } => {
            prepare_maven_incremental(storage, &repository, &path, false, maven_views).await
        }
        ChangeEvent::MavenGaChanged {
            repository,
            ga_path,
        } => prepare_maven_incremental(storage, &repository, &ga_path, true, maven_views).await,
        ChangeEvent::NpmHostedChanged {
            repository,
            package,
        } => prepare_hosted_npm_incremental(storage, &repository, &package).await,
        ChangeEvent::NpmProxyChanged {
            repository,
            package,
        } => prepare_proxy_npm_incremental(storage, &repository, &package).await,
        ChangeEvent::PhysicalDirty { .. } | ChangeEvent::GlobalDirty => {
            Err(StoreError::Superseded.into())
        }
    }
}

pub async fn apply_change(
    index: Arc<PersistentIndex>,
    storage: Storage,
    sequence: u64,
    event: ChangeEvent,
    maven_views: &[MavenIndexView],
) -> Result<MetaState, ReconcileError> {
    let update = prepare_change(&storage, event, maven_views).await?;
    Ok(index.apply_incremental(sequence, update).await?)
}

fn record_npm_package_error(error: ReconcileError) -> ReconcileError {
    crate::metrics::INDEX_NPM_RECONCILE_PACKAGES_TOTAL
        .with_label_values(&["error"])
        .inc();
    error
}

async fn build_npm_authority(
    index: &PersistentIndex,
    storage: &Storage,
    slot: Slot,
    reusable_active: Option<Slot>,
    progress: Option<&ReconcileProgress>,
) -> Result<(u64, u64), ReconcileError> {
    let mut after = None;
    let mut versions = 0u64;
    let mut bytes = 0u64;
    let mut observations_total = 0u64;
    let mut reused_total = 0u64;
    let mut rebuilt_total = 0u64;
    let mut retired_total = 0u64;
    loop {
        let (observations, next) = index
            .scan_npm_observations(slot, after, MAX_TX_ROWS)
            .await?;
        if observations.is_empty() {
            tracing::info!(
                observations = observations_total,
                reused = reused_total,
                rebuilt = rebuilt_total,
                retired = retired_total,
                versions,
                logical_bytes = bytes,
                "persistent npm authority staged"
            );
            return Ok((versions, bytes));
        }
        for observation in observations {
            observations_total = observations_total.saturating_add(1);
            if let Some(current) = observation.current_key {
                // A listed current pointer is authoritative even if a stale
                // retired marker remains beside it.
                let reused = match reusable_active {
                    Some(active) => reuse_npm_package(
                        index,
                        active,
                        slot,
                        &observation.repository,
                        &observation.package,
                    )
                    .await
                    .map_err(record_npm_package_error)?,
                    None => None,
                };
                let (package_versions, package_bytes) = match reused {
                    Some(reused) => {
                        reused_total = reused_total.saturating_add(1);
                        crate::metrics::INDEX_NPM_RECONCILE_PACKAGES_TOTAL
                            .with_label_values(&["reused"])
                            .inc();
                        reused
                    }
                    None => {
                        let rebuilt = build_hosted_package(
                            index,
                            storage,
                            slot,
                            &observation.repository,
                            &observation.package,
                            &current,
                        )
                        .await
                        .map_err(record_npm_package_error)?;
                        rebuilt_total = rebuilt_total.saturating_add(1);
                        crate::metrics::INDEX_NPM_RECONCILE_PACKAGES_TOTAL
                            .with_label_values(&["rebuilt"])
                            .inc();
                        rebuilt
                    }
                };
                versions = versions.saturating_add(package_versions);
                bytes = bytes.saturating_add(package_bytes);
            } else if let Some(retired) = observation.retired_key {
                let bytes = storage.get(&retired).await.map_err(|_| {
                    record_npm_package_error(npm_error(
                        &observation.repository,
                        &observation.package,
                        "retired_unavailable",
                    ))
                })?;
                if bytes.as_ref() != crate::npm_layout::HOSTED_PACKUMENT_RETIRED_V1 {
                    return Err(record_npm_package_error(npm_error(
                        &observation.repository,
                        &observation.package,
                        "retired_invalid",
                    )));
                }
                retired_total = retired_total.saturating_add(1);
                crate::metrics::INDEX_NPM_RECONCILE_PACKAGES_TOTAL
                    .with_label_values(&["retired"])
                    .inc();
            } else if let Some(packument) = observation.proxy_packument_key {
                let reused = match reusable_active {
                    Some(active) => reuse_npm_package(
                        index,
                        active,
                        slot,
                        &observation.repository,
                        &observation.package,
                    )
                    .await
                    .map_err(record_npm_package_error)?,
                    None => None,
                };
                let (package_versions, package_bytes) = match reused {
                    Some(reused) => {
                        reused_total = reused_total.saturating_add(1);
                        crate::metrics::INDEX_NPM_RECONCILE_PACKAGES_TOTAL
                            .with_label_values(&["reused"])
                            .inc();
                        reused
                    }
                    None => {
                        let rebuilt = build_proxy_package(
                            index,
                            storage,
                            slot,
                            &observation.repository,
                            &observation.package,
                            &packument,
                        )
                        .await
                        .map_err(record_npm_package_error)?;
                        rebuilt_total = rebuilt_total.saturating_add(1);
                        crate::metrics::INDEX_NPM_RECONCILE_PACKAGES_TOTAL
                            .with_label_values(&["rebuilt"])
                            .inc();
                        rebuilt
                    }
                };
                versions = versions.saturating_add(package_versions);
                bytes = bytes.saturating_add(package_bytes);
            } else if observation.transitional {
                return Err(record_npm_package_error(npm_error(
                    &observation.repository,
                    &observation.package,
                    "current_absent_with_live_state",
                )));
            }
            if let Some(progress) = progress {
                progress.set_npm_packages(observations_total);
            }
        }
        let Some(next) = next else {
            tracing::info!(
                observations = observations_total,
                reused = reused_total,
                rebuilt = rebuilt_total,
                retired = retired_total,
                versions,
                logical_bytes = bytes,
                "persistent npm authority staged"
            );
            return Ok((versions, bytes));
        };
        after = Some(next);
    }
}

/// Build a fully invisible S2 slot, then publish it with one metadata flip.
/// `complete` means an error-free traversal plus the local sequence fence; it
/// is not a provider-wide point-in-time snapshot.
#[cfg(test)]
pub async fn reconcile(
    index: Arc<PersistentIndex>,
    storage: Storage,
    config_digest: String,
    maven_enabled: bool,
    npm_enabled: bool,
) -> Result<MetaState, ReconcileError> {
    let maven_views = [MavenIndexView::legacy()];
    reconcile_inner(
        index,
        storage,
        config_digest,
        maven_enabled,
        npm_enabled,
        &maven_views,
        None,
    )
    .await
}

#[cfg(test)]
async fn reconcile_with_maven_views(
    index: Arc<PersistentIndex>,
    storage: Storage,
    config_digest: String,
    maven_enabled: bool,
    npm_enabled: bool,
    maven_views: &[MavenIndexView],
) -> Result<MetaState, ReconcileError> {
    reconcile_inner(
        index,
        storage,
        config_digest,
        maven_enabled,
        npm_enabled,
        maven_views,
        None,
    )
    .await
}

pub(super) async fn reconcile_with_progress(
    index: Arc<PersistentIndex>,
    storage: Storage,
    config_digest: String,
    maven_enabled: bool,
    npm_enabled: bool,
    maven_views: &[MavenIndexView],
    progress: &ReconcileProgress,
) -> Result<MetaState, ReconcileError> {
    progress.begin();
    let result = reconcile_inner(
        index,
        storage,
        config_digest,
        maven_enabled,
        npm_enabled,
        maven_views,
        Some(progress),
    )
    .await;
    if result.is_err() {
        progress.set_phase(PersistentIndexPhase::RetryWaiting);
    }
    result
}

async fn reconcile_inner(
    index: Arc<PersistentIndex>,
    storage: Storage,
    config_digest: String,
    maven_enabled: bool,
    npm_enabled: bool,
    maven_views: &[MavenIndexView],
    progress: Option<&ReconcileProgress>,
) -> Result<MetaState, ReconcileError> {
    index.admit_shadow_build()?;
    let before = index.meta().await?;
    // Fence the epoch before the first provider page. Until exact change
    // replay is applied to the shadow slot, any accepted local mutation during
    // the scan must supersede the candidate. Taking the fence after the scan
    // would incorrectly claim the mutation was included.
    let fence = before.accepted_change_seq;
    let slot = before.active_slot.map_or(Slot::A, Slot::inactive);
    let compatible_active = before
        .active_slot
        .filter(|_| before.config_digest == config_digest);
    index.clear_slot(slot).await?;

    let mut completeness = RegistryCompleteness::default();
    let mut totals = RegistryTotals::default();
    if maven_enabled {
        let inventory_progress = InventoryProgress::new(progress, true);
        if let Some(progress) = progress {
            progress.set_phase(PersistentIndexPhase::MavenInventory);
        }
        let started = std::time::Instant::now();
        let staged = async {
            let (mut count, mut artifacts, mut bytes) =
                stage_prefix(&index, &storage, slot, "maven/", inventory_progress).await?;
            if before.completeness.maven {
                if let Some(active) = compatible_active {
                    let (restored, restored_artifacts, restored_bytes) = restore_unseen_active(
                        &index,
                        &storage,
                        active,
                        slot,
                        "maven/",
                        count,
                        inventory_progress,
                    )
                    .await?;
                    count = count.saturating_add(restored);
                    artifacts = artifacts.saturating_add(restored_artifacts);
                    bytes = bytes.saturating_add(restored_bytes);
                }
            }
            Ok::<_, ReconcileError>((count, artifacts, bytes))
        }
        .await;
        let result = if staged.is_ok() { "success" } else { "error" };
        crate::metrics::INDEX_RECONCILE_STAGE_DURATION_SECONDS
            .with_label_values(&["maven_inventory", result])
            .observe(started.elapsed().as_secs_f64());
        let (count, artifacts, bytes) = staged?;
        let prefix_started = std::time::Instant::now();
        let prefix_result = build_maven_prefix_stats(&index, slot, maven_views).await;
        crate::metrics::INDEX_RECONCILE_STAGE_DURATION_SECONDS
            .with_label_values(&[
                "maven_prefix_stats",
                if prefix_result.is_ok() {
                    "success"
                } else {
                    "error"
                },
            ])
            .observe(prefix_started.elapsed().as_secs_f64());
        prefix_result?;
        totals.maven_artifacts = artifacts;
        totals.maven_bytes = bytes;
        completeness.maven = true;
        tracing::info!(objects = count, ?slot, "persistent Maven index staged");
    }
    if npm_enabled {
        let inventory_progress = InventoryProgress::new(progress, false);
        if let Some(progress) = progress {
            progress.set_phase(PersistentIndexPhase::NpmInventory);
        }
        let inventory_started = std::time::Instant::now();
        let staged = async {
            let (mut count, _, _) =
                stage_prefix(&index, &storage, slot, "npm/", inventory_progress).await?;
            if before.completeness.npm {
                if let Some(active) = compatible_active {
                    let (restored, _, _) = restore_unseen_active(
                        &index,
                        &storage,
                        active,
                        slot,
                        "npm/",
                        count,
                        inventory_progress,
                    )
                    .await?;
                    count = count.saturating_add(restored);
                }
            }
            Ok::<_, ReconcileError>(count)
        }
        .await;
        let result = if staged.is_ok() { "success" } else { "error" };
        crate::metrics::INDEX_RECONCILE_STAGE_DURATION_SECONDS
            .with_label_values(&["npm_inventory", result])
            .observe(inventory_started.elapsed().as_secs_f64());
        let count = staged?;
        if let Some(progress) = progress {
            progress.set_phase(PersistentIndexPhase::NpmAuthority);
        }
        let authority_started = std::time::Instant::now();
        let authority = build_npm_authority(
            &index,
            &storage,
            slot,
            compatible_active.filter(|_| before.completeness.npm),
            progress,
        )
        .await;
        let result = if authority.is_ok() {
            "success"
        } else {
            "error"
        };
        crate::metrics::INDEX_RECONCILE_STAGE_DURATION_SECONDS
            .with_label_values(&["npm_authority", result])
            .observe(authority_started.elapsed().as_secs_f64());
        let (versions, bytes) = authority?;
        totals.npm_versions = versions;
        totals.npm_bytes = bytes;
        completeness.npm = true;
        tracing::info!(objects = count, ?slot, "persistent npm index staged");
    }

    if let Some(progress) = progress {
        progress.set_phase(PersistentIndexPhase::Publishing);
    }

    // Replay typed mutations that committed while the provider traversal was
    // running. Physical/unknown/global events cannot be proven complete from
    // an entity-level reread, so they supersede this epoch and the next S2
    // starts after their sequence. This loop is bounded by the durable queue;
    // every replay transaction still obeys the per-entity row/byte caps.
    let mut replayed_through = fence;
    loop {
        let latest = index.meta().await?.accepted_change_seq;
        if latest == replayed_through {
            break;
        }
        let pending = index.pending_changes(replayed_through, latest).await?;
        let mut latest_by_entity = BTreeMap::<Vec<u8>, (u64, ChangeEvent)>::new();
        for (sequence, event) in pending {
            let entity = match &event {
                ChangeEvent::MavenPathChanged { repository, path } => {
                    format!("maven\0{repository}\0{path}").into_bytes()
                }
                ChangeEvent::MavenGaChanged {
                    repository,
                    ga_path,
                } => format!("maven\0{repository}\0{ga_path}").into_bytes(),
                ChangeEvent::NpmHostedChanged {
                    repository,
                    package,
                }
                | ChangeEvent::NpmProxyChanged {
                    repository,
                    package,
                } => format!("npm\0{repository}\0{package}").into_bytes(),
                ChangeEvent::PhysicalDirty { .. } | ChangeEvent::GlobalDirty => {
                    return Err(StoreError::Superseded.into());
                }
            };
            latest_by_entity.insert(entity, (sequence, event));
        }
        for (_, event) in latest_by_entity.into_values() {
            let update = prepare_change(&storage, event, maven_views).await?;
            let delta = index.apply_shadow(slot, update).await?;
            apply_totals_delta(&mut totals, delta);
        }
        replayed_through = latest;
    }

    index
        .flip(slot, replayed_through, completeness, totals, config_digest)
        .await
        .map_err(ReconcileError::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn publish_hosted_generation(
        storage: &Storage,
        repository: &str,
        package: &str,
        versions: &[(&str, u8)],
    ) {
        use base64::Engine as _;

        let mut manifests = serde_json::Map::new();
        for (version, fill) in versions {
            let digest = vec![*fill; 64];
            let integrity = format!(
                "sha512-{}",
                base64::engine::general_purpose::STANDARD.encode(&digest)
            );
            let manifest = serde_json::json!({
                "name": package,
                "version": version,
                "dist": { "integrity": integrity }
            });
            let blob_key = crate::npm_layout::hosted_blob_key_for_digest(
                repository,
                package,
                &hex::encode(&digest),
            );
            storage.put(&blob_key, &[*fill]).await.unwrap();
            storage
                .put(
                    &hosted_version_key(repository, package, version),
                    &serde_json::to_vec(&manifest).unwrap(),
                )
                .await
                .unwrap();
            manifests.insert((*version).to_string(), manifest);
        }
        let latest = versions.last().unwrap().0;
        let packument = serde_json::json!({
            "name": package,
            "versions": manifests,
            "dist-tags": { "latest": latest }
        });
        let full = serde_json::to_vec(&packument).unwrap();
        let generation = crate::registry::write_hosted_packument_generation_documents(
            storage, repository, package, &packument, &full,
        )
        .await
        .unwrap();
        crate::registry::commit_hosted_packument_pointer(storage, repository, package, &generation)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn cold_reconcile_streams_maven_and_publishes_one_generation() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage = Storage::new_local(storage_dir.path().to_str().unwrap());
        storage
            .put(
                "maven/repositories/releases/com/acme/app/1.0/app-1.0.jar",
                b"jar",
            )
            .await
            .unwrap();
        storage
            .put(
                "maven/repositories/releases/com/acme/app/1.0/app-1.0.jar.sha256",
                b"sum",
            )
            .await
            .unwrap();
        let db_dir = tempfile::tempdir().unwrap();
        let index =
            PersistentIndex::open(db_dir.path().join("index.redb"), "cfg".to_string()).unwrap();
        let progress = ReconcileProgress::new(PersistentIndexPhase::Recovering);
        let state = reconcile_with_progress(
            Arc::clone(&index),
            storage,
            "cfg".to_string(),
            true,
            false,
            &[MavenIndexView::legacy()],
            &progress,
        )
        .await
        .unwrap();
        assert_eq!(state.generation, 1);
        assert!(state.completeness.maven);
        assert_eq!(
            progress.snapshot(),
            PersistentIndexProgress {
                phase: PersistentIndexPhase::Publishing,
                maven_objects: 2,
                npm_objects: 0,
                npm_packages: 0,
            }
        );
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
        assert_eq!(rows[0].versions, 1);
        assert_eq!(rows[0].size, 6);
        index.shutdown().await;
    }

    #[tokio::test]
    async fn reconcile_progress_is_observable_before_generation_publish() {
        let storage_dir = tempfile::tempdir().unwrap();
        let authoritative = Storage::new_local(storage_dir.path().to_str().unwrap());
        authoritative
            .put(
                "maven/repositories/releases/com/acme/app/1.0/app-1.0.jar",
                b"jar",
            )
            .await
            .unwrap();
        publish_hosted_generation(&authoritative, "npm-private", "pkg", &[("1.0.0", 1)]).await;

        let current = crate::npm_layout::hosted_packument_current_key("npm-private", "pkg");
        let captured = Arc::new(tokio::sync::Barrier::new(2));
        let release = Arc::new(tokio::sync::Barrier::new(2));
        let storage = Storage::from_backend(Arc::new(
            crate::test_helpers::FaultInjectBackend::new(authoritative).barrier_get(
                current,
                Arc::clone(&captured),
                Arc::clone(&release),
            ),
        ));
        let db_dir = tempfile::tempdir().unwrap();
        let index =
            PersistentIndex::open(db_dir.path().join("index.redb"), "cfg".to_string()).unwrap();
        let progress = Arc::new(ReconcileProgress::new(PersistentIndexPhase::Preparing));
        let reconcile_index = Arc::clone(&index);
        let reconcile_progress = Arc::clone(&progress);
        let task = tokio::spawn(async move {
            reconcile_with_progress(
                reconcile_index,
                storage,
                "cfg".to_string(),
                true,
                true,
                &[MavenIndexView::legacy()],
                reconcile_progress.as_ref(),
            )
            .await
        });

        tokio::time::timeout(std::time::Duration::from_secs(5), captured.wait())
            .await
            .expect("npm authority GET must be reached");
        let in_progress = progress.snapshot();
        assert_eq!(in_progress.phase, PersistentIndexPhase::NpmAuthority);
        assert_eq!(in_progress.maven_objects, 1);
        assert!(in_progress.npm_objects > 0);
        assert_eq!(in_progress.npm_packages, 0);

        release.wait().await;
        task.await.unwrap().unwrap();
        let published = progress.snapshot();
        assert_eq!(published.phase, PersistentIndexPhase::Publishing);
        assert_eq!(published.maven_objects, 1);
        assert_eq!(published.npm_packages, 1);
        index.shutdown().await;
    }

    #[tokio::test]
    async fn npm_version_batch_uses_exact_serialized_row_budget() {
        let db_dir = tempfile::tempdir().unwrap();
        let index =
            PersistentIndex::open(db_dir.path().join("index.redb"), "cfg".to_string()).unwrap();
        let repository = "npm-private";
        let package = "large-packument";
        let padding = "x".repeat(13_000);
        let payload_key = format!(
            "npm/repositories/{repository}/{package}/blobs/sha512/{}.tgz",
            "a".repeat(300)
        );
        let rows = (0..630u64)
            .map(|rank| {
                let version = format!("1.0.{rank}");
                let manifest = serde_json::json!({
                    "name": package,
                    "version": version,
                    "padding": padding,
                });
                let key = npm_version_key(repository, package, rank, &version);
                let row = StoredNpmVersion {
                    repository: repository.to_string(),
                    package: package.to_string(),
                    version,
                    sort_rank: rank,
                    manifest,
                    published: "2026-08-11".to_string(),
                    payload_key: payload_key.clone(),
                    payload_meta: Some(FileMeta {
                        size: 1,
                        modified: 1,
                        etag: Some("b".repeat(64)),
                        version_id: None,
                    }),
                    declared_size: 1,
                };
                (key, row)
            })
            .collect::<Vec<_>>();

        let old_manifest_only_estimate = rows
            .iter()
            .map(|(_, row)| {
                serde_json::to_vec(&row.manifest).unwrap().len()
                    + repository.len()
                    + package.len()
                    + row.version.len()
                    + 64
            })
            .sum::<usize>();
        let exact_bytes = rows
            .iter()
            .map(|(key, row)| encoded_row_bytes(key, row).unwrap())
            .sum::<usize>();
        assert!(old_manifest_only_estimate <= MAX_TX_BYTES);
        assert!(exact_bytes > MAX_TX_BYTES);
        assert!(matches!(
            index.put_npm_versions(Slot::A, rows.clone()).await,
            Err(StoreError::TransactionTooLarge)
        ));

        let mut batch = NpmVersionBatch::new(&index, Slot::A);
        for (key, row) in rows {
            batch.push(key, row).await.unwrap();
        }
        batch.finish().await.unwrap();
        index
            .flip(
                Slot::A,
                0,
                RegistryCompleteness {
                    maven: false,
                    npm: true,
                },
                RegistryTotals::default(),
                "cfg".to_string(),
            )
            .await
            .unwrap();
        let (stored, generation, truncated) = index
            .list_npm_versions(repository, package, 1_000)
            .await
            .unwrap();
        assert_eq!(stored.len(), 630);
        assert_eq!(generation, 1);
        assert!(!truncated);
        index.shutdown().await;
    }

    #[tokio::test]
    async fn successful_list_omission_is_repaired_by_exact_head_before_publish() {
        let storage_dir = tempfile::tempdir().unwrap();
        let authoritative = Storage::new_local(storage_dir.path().to_str().unwrap());
        let artifact = "maven/repositories/releases/com/acme/app/1.0/app-1.0.jar";
        authoritative.put(artifact, b"jar").await.unwrap();
        let db_dir = tempfile::tempdir().unwrap();
        let index =
            PersistentIndex::open(db_dir.path().join("index.redb"), "cfg".to_string()).unwrap();
        reconcile(
            Arc::clone(&index),
            authoritative.clone(),
            "cfg".to_string(),
            true,
            false,
        )
        .await
        .unwrap();

        let omitted = Storage::from_backend(Arc::new(
            crate::test_helpers::FaultInjectBackend::new(authoritative.clone())
                .omit_from_list(artifact),
        ));
        let state = reconcile(Arc::clone(&index), omitted, "cfg".to_string(), true, false)
            .await
            .unwrap();
        assert_eq!(state.generation, 2);
        assert_eq!(state.totals.maven_artifacts, 1);
        assert_eq!(state.totals.maven_bytes, 3);
        assert!(index
            .get_object_in_slot(state.active_slot.unwrap(), artifact)
            .await
            .unwrap()
            .is_some());

        let uncertain = Storage::from_backend(Arc::new(
            crate::test_helpers::FaultInjectBackend::new(authoritative)
                .omit_from_list(artifact)
                .fail_stat(artifact),
        ));
        let inventory_errors = crate::metrics::INDEX_RECONCILE_STAGE_DURATION_SECONDS
            .with_label_values(&["maven_inventory", "error"]);
        let before_inventory_errors = inventory_errors.get_sample_count();
        assert!(matches!(
            reconcile(
                Arc::clone(&index),
                uncertain,
                "cfg".to_string(),
                true,
                false,
            )
            .await,
            Err(ReconcileError::Storage(StorageError::Network(_)))
        ));
        assert!(
            inventory_errors.get_sample_count() >= before_inventory_errors + 1,
            "the inventory stage must include exact-HEAD omission repair in its error timing"
        );
        let retained = index.meta().await.unwrap();
        assert_eq!(retained.generation, 2);
        assert!(index
            .get_object_in_slot(retained.active_slot.unwrap(), artifact)
            .await
            .unwrap()
            .is_some());
        index.shutdown().await;
    }

    #[tokio::test]
    async fn unchanged_npm_authority_reuses_projection_without_package_gets() {
        let storage_dir = tempfile::tempdir().unwrap();
        let authoritative = Storage::new_local(storage_dir.path().to_str().unwrap());
        publish_hosted_generation(
            &authoritative,
            "npm-private",
            "pkg",
            &[("1.0.0", 1), ("2.0.0", 2)],
        )
        .await;
        let backend = Arc::new(
            crate::test_helpers::FaultInjectBackend::new(authoritative.clone())
                .with_content_etags(),
        );
        let get_attempts = backend.get_attempts();
        let indexed_storage = Storage::from_backend(backend);
        let db_dir = tempfile::tempdir().unwrap();
        let index =
            PersistentIndex::open(db_dir.path().join("index.redb"), "cfg".to_string()).unwrap();
        let progress = ReconcileProgress::new(PersistentIndexPhase::Preparing);
        reconcile_with_progress(
            Arc::clone(&index),
            indexed_storage.clone(),
            "cfg".to_string(),
            false,
            true,
            &[MavenIndexView::legacy()],
            &progress,
        )
        .await
        .unwrap();
        let staged = progress.snapshot();
        assert_eq!(staged.phase, PersistentIndexPhase::Publishing);
        assert_eq!(staged.maven_objects, 0);
        assert!(staged.npm_objects > 0);
        assert_eq!(staged.npm_packages, 1);
        get_attempts.lock().clear();
        let reused = reconcile(
            Arc::clone(&index),
            indexed_storage.clone(),
            "cfg".to_string(),
            false,
            true,
        )
        .await
        .unwrap();
        assert_eq!(reused.generation, 2);
        assert!(
            get_attempts.lock().is_empty(),
            "an unchanged complete dependency set must reuse its redb projection"
        );

        publish_hosted_generation(
            &authoritative,
            "npm-private",
            "pkg",
            &[("1.0.0", 1), ("2.0.0", 2), ("3.0.0", 3)],
        )
        .await;
        get_attempts.lock().clear();
        let changed = reconcile(
            Arc::clone(&index),
            indexed_storage,
            "cfg".to_string(),
            false,
            true,
        )
        .await
        .unwrap();
        assert_eq!(changed.generation, 3);
        assert!(
            !get_attempts.lock().is_empty(),
            "changed authority metadata must force a bounded rebuild"
        );
        let (versions, _, truncated) = index
            .list_npm_versions("npm-private", "pkg", 10)
            .await
            .unwrap();
        assert!(!truncated);
        assert_eq!(versions.len(), 3);
        index.shutdown().await;
    }

    #[tokio::test]
    async fn npm_authority_get_failure_records_one_package_error() {
        let storage_dir = tempfile::tempdir().unwrap();
        let authoritative = Storage::new_local(storage_dir.path().to_str().unwrap());
        publish_hosted_generation(&authoritative, "npm-private", "pkg", &[("1.0.0", 1)]).await;
        let current = crate::npm_layout::hosted_packument_current_key("npm-private", "pkg");
        let failing = Storage::from_backend(Arc::new(
            crate::test_helpers::FaultInjectBackend::new(authoritative)
                .with_content_etags()
                .fail_get(&current),
        ));
        let db_dir = tempfile::tempdir().unwrap();
        let index =
            PersistentIndex::open(db_dir.path().join("index.redb"), "cfg".to_string()).unwrap();
        let errors =
            crate::metrics::INDEX_NPM_RECONCILE_PACKAGES_TOTAL.with_label_values(&["error"]);
        let before_errors = errors.get();

        assert!(matches!(
            reconcile(Arc::clone(&index), failing, "cfg".to_string(), false, true,).await,
            Err(ReconcileError::NpmAuthority { .. })
        ));
        assert!(
            errors.get() >= before_errors + 1,
            "a package-local authority GET failure must be represented in the bounded outcome counter"
        );
        index.shutdown().await;
    }

    #[tokio::test]
    async fn hosted_optional_split_appearing_invalidates_strong_identity_reuse() {
        let storage_dir = tempfile::tempdir().unwrap();
        let authoritative = Storage::new_local(storage_dir.path().to_str().unwrap());
        publish_hosted_generation(&authoritative, "npm-private", "pkg", &[("1.0.0", 1)]).await;
        let split_key = hosted_version_key("npm-private", "pkg", "1.0.0");
        authoritative.delete(&split_key).await.unwrap();

        let backend = Arc::new(
            crate::test_helpers::FaultInjectBackend::new(authoritative.clone())
                .with_content_etags(),
        );
        let get_attempts = backend.get_attempts();
        let indexed_storage = Storage::from_backend(backend);
        let db_dir = tempfile::tempdir().unwrap();
        let index =
            PersistentIndex::open(db_dir.path().join("index.redb"), "cfg".to_string()).unwrap();
        reconcile(
            Arc::clone(&index),
            indexed_storage.clone(),
            "cfg".to_string(),
            false,
            true,
        )
        .await
        .unwrap();

        get_attempts.lock().clear();
        authoritative
            .put(&split_key, br#"{"version":"1.0.0"}"#)
            .await
            .unwrap();
        let state = reconcile(
            Arc::clone(&index),
            indexed_storage,
            "cfg".to_string(),
            false,
            true,
        )
        .await
        .unwrap();
        assert!(
            !get_attempts.lock().is_empty(),
            "an optional split changing absent→present must rebuild the package"
        );
        let (package, versions, _, _) = index
            .npm_package_page("npm-private", "pkg", None, 10)
            .await
            .unwrap();
        assert!(package.unwrap().logical_size > 1);
        assert_ne!(versions[0].published, "N/A");
        assert_eq!(state.totals.npm_versions, 1);
        index.shutdown().await;
    }

    #[tokio::test]
    async fn proxy_optional_tarball_appearing_invalidates_strong_identity_reuse() {
        let storage_dir = tempfile::tempdir().unwrap();
        let authoritative = Storage::new_local(storage_dir.path().to_str().unwrap());
        let packument_key = "npm/repositories/npm-proxy/proxy/packuments/pkg.json";
        authoritative
            .put(
                packument_key,
                br#"{"name":"pkg","versions":{"1.0.0":{"name":"pkg","version":"1.0.0"}},"dist-tags":{"latest":"1.0.0"}}"#,
            )
            .await
            .unwrap();
        let backend = Arc::new(
            crate::test_helpers::FaultInjectBackend::new(authoritative.clone())
                .with_content_etags(),
        );
        let get_attempts = backend.get_attempts();
        let indexed_storage = Storage::from_backend(backend);
        let db_dir = tempfile::tempdir().unwrap();
        let index =
            PersistentIndex::open(db_dir.path().join("index.redb"), "cfg".to_string()).unwrap();
        reconcile(
            Arc::clone(&index),
            indexed_storage.clone(),
            "cfg".to_string(),
            false,
            true,
        )
        .await
        .unwrap();

        get_attempts.lock().clear();
        let tarball_key = crate::registry::proxy_tarball_key(
            "npm-proxy",
            "pkg",
            &crate::registry::canonical_tarball_filename("pkg", "1.0.0"),
        );
        authoritative.put(&tarball_key, b"tarball").await.unwrap();
        let state = reconcile(
            Arc::clone(&index),
            indexed_storage,
            "cfg".to_string(),
            false,
            true,
        )
        .await
        .unwrap();
        assert!(
            !get_attempts.lock().is_empty(),
            "an optional proxy tarball changing absent→present must rebuild the package"
        );
        let (_, versions, _, _) = index
            .npm_package_page("npm-proxy", "pkg", None, 10)
            .await
            .unwrap();
        assert_eq!(versions[0].payload_meta.as_ref().unwrap().size, 7);
        assert_eq!(state.totals.npm_bytes, 7);
        index.shutdown().await;
    }

    #[tokio::test]
    async fn nonrecursive_maven_metadata_update_preserves_global_totals() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage = Storage::new_local(storage_dir.path().to_str().unwrap());
        let prefix = "maven/repositories/releases/com/acme/app";
        storage
            .put(&format!("{prefix}/1.0/app-1.0.jar"), b"one")
            .await
            .unwrap();
        storage
            .put(&format!("{prefix}/2.0/app-2.0.jar"), b"two")
            .await
            .unwrap();
        let metadata_path = "com/acme/app/maven-metadata.xml";
        let metadata_key = format!("maven/repositories/releases/{metadata_path}");
        storage.put(&metadata_key, b"old").await.unwrap();
        let db_dir = tempfile::tempdir().unwrap();
        let index =
            PersistentIndex::open(db_dir.path().join("index.redb"), "cfg".to_string()).unwrap();
        let views = [MavenIndexView {
            repository: "releases".to_string(),
            members: vec!["releases".to_string()],
        }];
        let before = reconcile_with_maven_views(
            Arc::clone(&index),
            storage.clone(),
            "cfg".to_string(),
            true,
            false,
            &views,
        )
        .await
        .unwrap();
        assert_eq!(before.totals.maven_artifacts, 2);

        storage.put(&metadata_key, b"new-metadata").await.unwrap();
        let sequence = index
            .register_change(ChangeEvent::MavenPathChanged {
                repository: "releases".to_string(),
                path: metadata_path.to_string(),
            })
            .await
            .unwrap();
        let after = apply_change(
            Arc::clone(&index),
            storage,
            sequence,
            ChangeEvent::MavenPathChanged {
                repository: "releases".to_string(),
                path: metadata_path.to_string(),
            },
            &views,
        )
        .await
        .unwrap();
        assert_eq!(after.totals.maven_artifacts, 2);
        assert_eq!(
            after.totals.maven_bytes,
            before.totals.maven_bytes - 3 + b"new-metadata".len() as u64
        );
        index.shutdown().await;
    }

    #[tokio::test]
    async fn maven_prefix_stats_are_exact_incremental_grouped_and_warm_reusable() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage = Storage::new_local(storage_dir.path().to_str().unwrap());
        let logical = "com/acme/app/1.0/app.jar";
        let releases_key = format!("maven/repositories/releases/{logical}");
        let public_key = format!("maven/repositories/public/{logical}");
        storage.put(&releases_key, b"one").await.unwrap();
        storage.put(&public_key, b"shadowed").await.unwrap();
        storage
            .put(
                "maven/repositories/public/com/acme/other/1.0/other.jar",
                b"xy",
            )
            .await
            .unwrap();
        let views = vec![
            MavenIndexView {
                repository: "releases".to_string(),
                members: vec!["releases".to_string()],
            },
            MavenIndexView {
                repository: "public".to_string(),
                members: vec!["public".to_string()],
            },
            MavenIndexView {
                repository: "all".to_string(),
                members: vec!["releases".to_string(), "public".to_string()],
            },
            MavenIndexView {
                repository: "empty".to_string(),
                members: vec!["empty".to_string()],
            },
        ];
        let db_dir = tempfile::tempdir().unwrap();
        let db_path = db_dir.path().join("index.redb");
        let index = PersistentIndex::open(&db_path, "cfg".to_string()).unwrap();
        let first = reconcile_with_maven_views(
            Arc::clone(&index),
            storage.clone(),
            "cfg".to_string(),
            true,
            false,
            &views,
        )
        .await
        .unwrap();
        assert_eq!(first.generation, 1);
        let (roots, generation) = index
            .maven_repository_rows(vec![
                "releases".to_string(),
                "public".to_string(),
                "all".to_string(),
                "empty".to_string(),
                "missing".to_string(),
            ])
            .await
            .unwrap();
        assert_eq!(generation, 1);
        assert_eq!((roots[0].versions, roots[0].size), (1, 3));
        assert_eq!((roots[1].versions, roots[1].size), (2, 10));
        assert_eq!((roots[2].versions, roots[2].size), (2, 5));
        assert_eq!((roots[3].versions, roots[3].size), (0, 0));
        assert!(roots[..4].iter().all(|row| row.size_available));
        assert!(!roots[4].size_available);

        storage.put(&releases_key, b"12345").await.unwrap();
        let replace_sequence = index
            .register_change(ChangeEvent::MavenPathChanged {
                repository: "releases".to_string(),
                path: logical.to_string(),
            })
            .await
            .unwrap();
        let replaced = apply_change(
            Arc::clone(&index),
            storage.clone(),
            replace_sequence,
            ChangeEvent::MavenPathChanged {
                repository: "releases".to_string(),
                path: logical.to_string(),
            },
            &views,
        )
        .await
        .unwrap();
        assert_eq!(replaced.generation, 2);
        let (roots, _) = index
            .maven_repository_rows(vec!["releases".to_string(), "all".to_string()])
            .await
            .unwrap();
        assert_eq!((roots[0].versions, roots[0].size), (1, 5));
        assert_eq!((roots[1].versions, roots[1].size), (2, 7));

        storage.delete(&releases_key).await.unwrap();
        let delete_sequence = index
            .register_change(ChangeEvent::MavenPathChanged {
                repository: "releases".to_string(),
                path: logical.to_string(),
            })
            .await
            .unwrap();
        let deleted = apply_change(
            Arc::clone(&index),
            storage.clone(),
            delete_sequence,
            ChangeEvent::MavenPathChanged {
                repository: "releases".to_string(),
                path: logical.to_string(),
            },
            &views,
        )
        .await
        .unwrap();
        assert_eq!(deleted.generation, 3);
        let (roots, _) = index
            .maven_repository_rows(vec!["releases".to_string(), "all".to_string()])
            .await
            .unwrap();
        assert_eq!((roots[0].versions, roots[0].size), (0, 0));
        assert!(roots[0].size_available);
        assert_eq!((roots[1].versions, roots[1].size), (2, 10));
        let (children, _, _, _) = index
            .maven_children_page(
                "all".to_string(),
                vec![
                    "maven/repositories/releases/".to_string(),
                    "maven/repositories/public/".to_string(),
                ],
                String::new(),
                None,
                10,
            )
            .await
            .unwrap();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].name, "com");
        assert_eq!((children[0].versions, children[0].size), (2, 10));
        assert!(children[0].size_available);
        let (files, _, _) = index
            .maven_files_page(
                vec![
                    "maven/repositories/releases/".to_string(),
                    "maven/repositories/public/".to_string(),
                ],
                "com/acme/app/1.0".to_string(),
                None,
                10,
            )
            .await
            .unwrap();
        assert_eq!(files[0].1.size, 8, "lower member becomes the winner");

        let flipped = reconcile_with_maven_views(
            Arc::clone(&index),
            storage,
            "cfg".to_string(),
            true,
            false,
            &views,
        )
        .await
        .unwrap();
        assert_ne!(flipped.active_slot, first.active_slot);
        let expected_generation = flipped.generation;
        index.shutdown().await;
        drop(index);

        let reopened =
            PersistentIndex::open_with_startup_state_for_test(&db_path, "cfg".to_string(), true)
                .unwrap();
        assert!(reopened.startup_clean());
        let (roots, generation) = reopened
            .maven_repository_rows(vec!["all".to_string(), "empty".to_string()])
            .await
            .unwrap();
        assert_eq!(generation, expected_generation);
        assert_eq!((roots[0].versions, roots[0].size), (2, 10));
        assert_eq!((roots[1].versions, roots[1].size), (0, 0));
        assert!(roots.iter().all(|row| row.size_available));
        reopened.shutdown().await;
    }

    #[test]
    fn maven_prefix_stats_reject_counter_overflow() {
        let mut stats = MavenStatsAccumulator::new();
        stats.frames[0].stats.subtree_files = u64::MAX;
        assert!(matches!(
            stats.add("artifact.jar", 0),
            Err(StoreError::ProjectionOverflow)
        ));
    }

    #[tokio::test]
    async fn oversized_group_incremental_fails_closed_without_partial_publication() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage = Storage::new_local(storage_dir.path().to_str().unwrap());
        let logical = "com/acme/app/1.0/app.jar";
        let key = format!("maven/repositories/member000/{logical}");
        storage.put(&key, b"old").await.unwrap();
        let members = (0..=500)
            .map(|index| format!("member{index:03}"))
            .collect::<Vec<_>>();
        let views = [MavenIndexView {
            repository: "all".to_string(),
            members,
        }];
        let db_dir = tempfile::tempdir().unwrap();
        let index =
            PersistentIndex::open(db_dir.path().join("index.redb"), "cfg".to_string()).unwrap();
        let before = reconcile_with_maven_views(
            Arc::clone(&index),
            storage.clone(),
            "cfg".to_string(),
            true,
            false,
            &views,
        )
        .await
        .unwrap();
        storage.put(&key, b"replacement").await.unwrap();
        let sequence = index
            .register_change(ChangeEvent::MavenPathChanged {
                repository: "member000".to_string(),
                path: logical.to_string(),
            })
            .await
            .unwrap();
        assert!(matches!(
            apply_change(
                Arc::clone(&index),
                storage,
                sequence,
                ChangeEvent::MavenPathChanged {
                    repository: "member000".to_string(),
                    path: logical.to_string(),
                },
                &views,
            )
            .await,
            Err(ReconcileError::Store(StoreError::TransactionTooLarge))
        ));
        let meta = index.meta().await.unwrap();
        assert_eq!(meta.generation, before.generation);
        let active = meta.active_slot.unwrap();
        assert_eq!(
            index
                .get_object_in_slot(active, &key)
                .await
                .unwrap()
                .unwrap()
                .size,
            3
        );
        let (roots, generation) = index
            .maven_repository_rows(vec!["all".to_string()])
            .await
            .unwrap();
        assert_eq!(generation, before.generation);
        assert_eq!((roots[0].versions, roots[0].size), (1, 3));
        index.shutdown().await;
    }

    #[tokio::test]
    async fn npm_keyset_pages_preserve_one_global_newest_first_order() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage = Storage::new_local(storage_dir.path().to_str().unwrap());
        let version_names = (0..105)
            .map(|patch| format!("1.0.{patch}"))
            .collect::<Vec<_>>();
        let versions = version_names
            .iter()
            .enumerate()
            .map(|(index, version)| (version.as_str(), (index % 251) as u8))
            .collect::<Vec<_>>();
        publish_hosted_generation(&storage, "npm-private", "many", &versions).await;
        let db_dir = tempfile::tempdir().unwrap();
        let index =
            PersistentIndex::open(db_dir.path().join("index.redb"), "cfg".to_string()).unwrap();
        reconcile(Arc::clone(&index), storage, "cfg".to_string(), false, true)
            .await
            .unwrap();

        let (package, first, cursor, generation) = index
            .npm_package_page("npm-private", "many", None, 100)
            .await
            .unwrap();
        assert_eq!(package.unwrap().versions, 105);
        assert_eq!(first.len(), 100);
        assert!(cursor.is_some());
        let (_, second, end, second_generation) = index
            .npm_package_page("npm-private", "many", cursor, 100)
            .await
            .unwrap();
        assert_eq!(generation, second_generation);
        assert_eq!(second.len(), 5);
        assert!(end.is_none());
        let observed = first
            .into_iter()
            .chain(second)
            .map(|row| row.version)
            .collect::<Vec<_>>();
        let expected = (0..105)
            .rev()
            .map(|patch| format!("1.0.{patch}"))
            .collect::<Vec<_>>();
        assert_eq!(observed, expected);
        index.shutdown().await;
    }

    #[tokio::test]
    async fn semantic_maven_update_covers_physical_change_and_preserves_prefix_neighbor() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage = Storage::new_local(storage_dir.path().to_str().unwrap());
        let artifact = "maven/repositories/releases/com/acme/app/1.0/app-1.0.jar";
        let neighbor = "maven/repositories/releases/com/acme/app/1.0/app-1.0.jar.unrelated";
        storage.put(artifact, b"old").await.unwrap();
        storage.put(neighbor, b"keep").await.unwrap();

        let db_dir = tempfile::tempdir().unwrap();
        let index =
            PersistentIndex::open(db_dir.path().join("index.redb"), "cfg".to_string()).unwrap();
        let views = [MavenIndexView {
            repository: "releases".to_string(),
            members: vec!["releases".to_string()],
        }];
        reconcile_with_maven_views(
            Arc::clone(&index),
            storage.clone(),
            "cfg".to_string(),
            true,
            false,
            &views,
        )
        .await
        .unwrap();

        storage.put(artifact, b"replacement").await.unwrap();
        let physical = index
            .register_change(ChangeEvent::PhysicalDirty {
                key: artifact.to_string(),
            })
            .await
            .unwrap();
        let semantic = index
            .register_change(ChangeEvent::MavenPathChanged {
                repository: "releases".to_string(),
                path: "com/acme/app/1.0/app-1.0.jar".to_string(),
            })
            .await
            .unwrap();
        assert_eq!((physical, semantic), (1, 2));
        let state = apply_change(
            Arc::clone(&index),
            storage.clone(),
            semantic,
            ChangeEvent::MavenPathChanged {
                repository: "releases".to_string(),
                path: "com/acme/app/1.0/app-1.0.jar".to_string(),
            },
            &views,
        )
        .await
        .unwrap();
        assert!(!state.global_dirty);
        assert_eq!(state.active_watermark(), semantic);
        let active = state.active_slot.unwrap();
        assert_eq!(
            index
                .get_object_in_slot(active, artifact)
                .await
                .unwrap()
                .unwrap()
                .size,
            b"replacement".len() as u64
        );
        assert_eq!(
            index
                .get_object_in_slot(active, neighbor)
                .await
                .unwrap()
                .unwrap()
                .size,
            b"keep".len() as u64
        );
        let pending = index.pending_changes(0, semantic).await.unwrap();
        assert_eq!(pending.len(), 1);
        assert!(matches!(pending[0].1, ChangeEvent::MavenPathChanged { .. }));

        let published = reconcile(index.clone(), storage, "cfg".to_string(), true, false)
            .await
            .unwrap();
        assert_eq!(published.active_watermark(), semantic);
        assert!(index.pending_changes(0, semantic).await.unwrap().is_empty());
        index.shutdown().await;
    }

    #[tokio::test]
    async fn shadow_replays_semantic_changes_and_rejects_unresolved_physical_changes() {
        let storage_dir = tempfile::tempdir().unwrap();
        let authoritative = Storage::new_local(storage_dir.path().to_str().unwrap());
        let artifact = "maven/repositories/releases/com/acme/app/1.0/app-1.0.jar";
        authoritative.put(artifact, b"old").await.unwrap();

        let db_dir = tempfile::tempdir().unwrap();
        let index =
            PersistentIndex::open(db_dir.path().join("index.redb"), "cfg".to_string()).unwrap();
        let views = vec![MavenIndexView {
            repository: "releases".to_string(),
            members: vec!["releases".to_string()],
        }];
        reconcile_with_maven_views(
            index.clone(),
            authoritative.clone(),
            "cfg".to_string(),
            true,
            false,
            &views,
        )
        .await
        .unwrap();

        // Capture the provider listing, then commit a newer authoritative
        // mutation while the S2 scan is paused. The typed event must be replayed
        // into the invisible slot before its atomic publication.
        let captured = Arc::new(tokio::sync::Barrier::new(2));
        let release = Arc::new(tokio::sync::Barrier::new(2));
        let scan_storage = Storage::from_backend(Arc::new(
            crate::test_helpers::FaultInjectBackend::new(authoritative.clone()).barrier_lists(
                "maven/",
                captured.clone(),
                release.clone(),
            ),
        ));
        let scan_index = index.clone();
        let scan_views = views.clone();
        let scan = tokio::spawn(async move {
            reconcile_with_maven_views(
                scan_index,
                scan_storage,
                "cfg".to_string(),
                true,
                false,
                &scan_views,
            )
            .await
        });
        captured.wait().await;
        authoritative.put(artifact, b"replacement").await.unwrap();
        index
            .register_change(ChangeEvent::MavenPathChanged {
                repository: "releases".to_string(),
                path: "com/acme/app/1.0/app-1.0.jar".to_string(),
            })
            .await
            .unwrap();
        release.wait().await;
        let published = scan.await.unwrap().unwrap();
        assert_eq!(published.generation, 2);
        assert_eq!(
            index
                .get_object_in_slot(published.active_slot.unwrap(), artifact)
                .await
                .unwrap()
                .unwrap()
                .size,
            b"replacement".len() as u64
        );

        // A low-level event with no semantic completion proof must never flip a
        // shadow slot. The current last-good generation remains readable and a
        // later full reconciliation repairs it from S3.
        let captured = Arc::new(tokio::sync::Barrier::new(2));
        let release = Arc::new(tokio::sync::Barrier::new(2));
        let scan_storage = Storage::from_backend(Arc::new(
            crate::test_helpers::FaultInjectBackend::new(authoritative.clone()).barrier_lists(
                "maven/",
                captured.clone(),
                release.clone(),
            ),
        ));
        let scan_index = index.clone();
        let scan_views = views.clone();
        let scan = tokio::spawn(async move {
            reconcile_with_maven_views(
                scan_index,
                scan_storage,
                "cfg".to_string(),
                true,
                false,
                &scan_views,
            )
            .await
        });
        captured.wait().await;
        authoritative.put(artifact, b"unresolved").await.unwrap();
        index
            .register_change(ChangeEvent::PhysicalDirty {
                key: artifact.to_string(),
            })
            .await
            .unwrap();
        release.wait().await;
        assert!(matches!(
            scan.await.unwrap(),
            Err(ReconcileError::Store(StoreError::Superseded))
        ));
        let retained = index.meta().await.unwrap();
        assert_eq!(retained.generation, 2);
        assert_eq!(
            index
                .get_object_in_slot(retained.active_slot.unwrap(), artifact)
                .await
                .unwrap()
                .unwrap()
                .size,
            b"replacement".len() as u64
        );
        index.shutdown().await;
    }

    #[tokio::test]
    async fn hosted_npm_incremental_uses_current_generation_and_retirement_authority() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage = Storage::new_local(storage_dir.path().to_str().unwrap());
        publish_hosted_generation(
            &storage,
            "npm-private",
            "pkg",
            &[("1.0.0", 1), ("2.0.0", 2)],
        )
        .await;
        let db_dir = tempfile::tempdir().unwrap();
        let index =
            PersistentIndex::open(db_dir.path().join("index.redb"), "cfg".to_string()).unwrap();
        reconcile(
            index.clone(),
            storage.clone(),
            "cfg".to_string(),
            false,
            true,
        )
        .await
        .unwrap();
        let (versions, _, truncated) = index
            .list_npm_versions("npm-private", "pkg", 100)
            .await
            .unwrap();
        assert!(!truncated);
        assert_eq!(
            versions
                .iter()
                .map(|row| row.version.as_str())
                .collect::<Vec<_>>(),
            vec!["2.0.0", "1.0.0"]
        );

        // The split v1 manifest deliberately remains. Only current.json plus
        // its active full generation may decide hosted visibility.
        publish_hosted_generation(&storage, "npm-private", "pkg", &[("2.0.0", 2)]).await;
        let current = crate::npm_layout::hosted_packument_current_key("npm-private", "pkg");
        let physical = index
            .register_change(ChangeEvent::PhysicalDirty {
                key: current.clone(),
            })
            .await
            .unwrap();
        let semantic = index
            .register_change(ChangeEvent::NpmHostedChanged {
                repository: "npm-private".to_string(),
                package: "pkg".to_string(),
            })
            .await
            .unwrap();
        assert!(semantic > physical);
        let state = apply_change(
            index.clone(),
            storage.clone(),
            semantic,
            ChangeEvent::NpmHostedChanged {
                repository: "npm-private".to_string(),
                package: "pkg".to_string(),
            },
            &[MavenIndexView::legacy()],
        )
        .await
        .unwrap();
        assert!(!state.global_dirty);
        let (versions, _, truncated) = index
            .list_npm_versions("npm-private", "pkg", 100)
            .await
            .unwrap();
        assert!(!truncated);
        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].version, "2.0.0");

        storage.delete(&current).await.unwrap();
        let retired = crate::npm_layout::hosted_packument_retired_key("npm-private", "pkg");
        storage
            .put(&retired, crate::npm_layout::HOSTED_PACKUMENT_RETIRED_V1)
            .await
            .unwrap();
        let sequence = index
            .register_change(ChangeEvent::NpmHostedChanged {
                repository: "npm-private".to_string(),
                package: "pkg".to_string(),
            })
            .await
            .unwrap();
        apply_change(
            index.clone(),
            storage,
            sequence,
            ChangeEvent::NpmHostedChanged {
                repository: "npm-private".to_string(),
                package: "pkg".to_string(),
            },
            &[MavenIndexView::legacy()],
        )
        .await
        .unwrap();
        assert!(index
            .get_npm_package("npm-private", "pkg")
            .await
            .unwrap()
            .0
            .is_none());
        index.shutdown().await;
    }
}
