// Copyright (c) 2026 The NORA Authors
// SPDX-License-Identifier: MIT

use super::components::{format_available_size, format_timestamp, html_escape, sanitize_href};
use super::templates::encode_uri_component;
use crate::activity_log::ActivityEntry;
use crate::registry_type::RegistryType;
use crate::repo_index::{IndexStatus, RepoInfo};
use crate::validation::ends_with_ci;
use crate::AppState;
use crate::Storage;
use axum::{
    extract::{Path, Query, State},
    response::Json,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use utoipa::ToSchema;

async fn display_stat(storage: &Storage, key: &str) -> Option<crate::storage::FileMeta> {
    match storage.stat(key).await {
        Ok(meta) => meta,
        Err(error) => {
            tracing::warn!(%key, %error, "UI metadata lookup failed");
            None
        }
    }
}

fn snapshot_keys(state: &AppState, registry: &str, prefix: &str) -> Vec<String> {
    state
        .repo_index
        .objects(registry)
        .iter()
        .filter(|object| object.key.starts_with(prefix))
        .map(|object| object.key.clone())
        .collect()
}

fn snapshot_meta(state: &AppState, registry: &str, key: &str) -> Option<crate::storage::FileMeta> {
    state
        .repo_index
        .objects(registry)
        .iter()
        .find(|object| object.key == key)
        .map(|object| object.meta.clone())
}

#[derive(Serialize)]
pub struct RegistryStats {
    pub docker: usize,
    pub maven: usize,
    pub npm: usize,
    pub cargo: usize,
    pub pypi: usize,
    pub go: usize,
    pub raw: usize,
    pub nuget: usize,
    pub gems: usize,
    pub terraform: usize,
    pub ansible: usize,
    #[serde(rename = "pub")]
    pub pub_dart: usize,
    pub conan: usize,
    pub rpm: usize,
    pub deb: usize,
}

#[derive(Serialize)]
pub struct TagInfo {
    pub name: String,
    pub size: u64,
    pub created: String,
    pub downloads: u64,
    pub last_pulled: Option<String>,
    pub os: String,
    pub arch: String,
    pub layers_count: usize,
    pub pull_command: String,
}

#[derive(Serialize)]
pub struct DockerDetail {
    pub tags: Vec<TagInfo>,
}

#[derive(Serialize)]
pub struct VersionInfo {
    pub version: String,
    pub size: u64,
    pub published: String,
    pub cached: bool,
}

#[derive(Serialize, Default)]
pub struct PackageMetadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub license: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub homepage: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repository: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub keywords: Vec<String>,
}

impl PackageMetadata {
    pub fn has_any(&self) -> bool {
        self.description.is_some()
            || self.license.is_some()
            || self.author.is_some()
            || self.homepage.is_some()
            || self.repository.is_some()
            || !self.keywords.is_empty()
    }
}

#[derive(Serialize)]
pub struct PackageDetail {
    pub versions: Vec<VersionInfo>,
    pub prerelease_count: usize,
    /// Total stable versions available (may be > versions.len() if truncated)
    pub total_stable: usize,
    pub metadata: PackageMetadata,
}

#[derive(Serialize)]
pub struct MavenArtifact {
    pub filename: String,
    pub size: u64,
}

#[derive(Serialize)]
pub struct MavenDetail {
    pub artifacts: Vec<MavenArtifact>,
}

#[derive(Deserialize)]
pub struct SearchQuery {
    pub q: Option<String>,
}

#[derive(Serialize, ToSchema)]
pub struct DashboardResponse {
    pub global_stats: GlobalStats,
    pub registry_stats: Vec<RegistryCardStats>,
    pub mount_points: Vec<MountPoint>,
    pub activity: Vec<ActivityEntry>,
    pub uptime_seconds: u64,
    pub startup_duration_ms: u64,
}

#[derive(Serialize, ToSchema)]
pub struct GlobalStats {
    pub downloads: u64,
    pub uploads: u64,
    pub artifacts: u64,
    pub cache_hit_percent: f64,
    /// Retained for API compatibility; zero when `size_available` is false.
    pub storage_bytes: u64,
    /// Physical total storage is intentionally not scanned in the background.
    pub size_available: bool,
}

#[derive(Serialize, ToSchema)]
pub struct RegistryCardStats {
    pub name: String,
    pub artifact_count: usize,
    pub downloads: u64,
    pub uploads: u64,
    /// Retained for API compatibility; zero when `size_available` is false.
    pub size_bytes: u64,
    /// Whether `size_bytes` came from a published logical index snapshot.
    pub size_available: bool,
}

#[derive(Serialize, ToSchema)]
pub struct MountPoint {
    pub registry: String,
    pub mount_path: String,
    pub proxy_upstreams: Vec<String>,
}

// ============ API Handlers ============

pub async fn api_stats(State(state): State<AppState>) -> Json<RegistryStats> {
    // Trigger index rebuild if needed, then get counts
    for reg in state.enabled_registries.iter() {
        let _ = state.repo_index.get(reg.as_str(), &state.storage).await;
    }

    let counts = state.repo_index.counts();
    let get = |rt: RegistryType| counts.get(&rt).copied().unwrap_or(0);
    Json(RegistryStats {
        docker: get(RegistryType::Docker),
        maven: get(RegistryType::Maven),
        npm: get(RegistryType::Npm),
        cargo: get(RegistryType::Cargo),
        pypi: get(RegistryType::PyPI),
        go: get(RegistryType::Go),
        raw: get(RegistryType::Raw),
        nuget: get(RegistryType::Nuget),
        gems: get(RegistryType::Gems),
        terraform: get(RegistryType::Terraform),
        ansible: get(RegistryType::Ansible),
        pub_dart: get(RegistryType::PubDart),
        conan: get(RegistryType::Conan),
        rpm: get(RegistryType::Rpm),
        deb: get(RegistryType::Deb),
    })
}

pub async fn api_dashboard(State(state): State<AppState>) -> Json<DashboardResponse> {
    let mut total_artifacts: usize = 0;
    let mut registry_card_stats = Vec::new();
    let mut mount_points = Vec::new();

    for reg in RegistryType::all() {
        if !state.enabled_registries.contains(reg) {
            continue;
        }

        let name = reg.as_str();
        let repos = state.repo_index.get(name, &state.storage).await;
        let size_available = *reg != RegistryType::Npm
            && if repos.is_empty() {
                state.repo_index.status(name) == Some(IndexStatus::Ready)
            } else {
                repos.iter().all(|repo| repo.size_available)
            };
        let size: u64 = if size_available {
            repos.iter().map(|r| r.size).sum()
        } else {
            0
        };
        let versions: usize = repos.iter().map(|r| r.versions).sum();

        total_artifacts += versions;

        registry_card_stats.push(RegistryCardStats {
            name: name.to_string(),
            artifact_count: versions,
            downloads: state.metrics.get_registry_downloads(name),
            uploads: state.metrics.get_registry_uploads(name),
            size_bytes: size,
            size_available,
        });

        let proxy_upstreams: Vec<String> =
            match reg {
                RegistryType::Docker => state
                    .config
                    .docker
                    .upstreams
                    .iter()
                    .map(|u| u.url.clone())
                    .collect(),
                RegistryType::Maven => {
                    let mut upstreams: std::collections::BTreeSet<String> = state
                        .config
                        .maven
                        .proxies
                        .iter()
                        .map(|proxy| proxy.url().to_string())
                        .collect();
                    upstreams.extend(state.config.maven.repositories.iter().filter_map(
                        |repository| match repository {
                            crate::config::MavenRepository::Proxy { url, .. } => Some(url.clone()),
                            _ => None,
                        },
                    ));
                    upstreams.into_iter().collect()
                }
                RegistryType::Npm => {
                    let mut upstreams: std::collections::BTreeSet<String> =
                        state.config.npm.proxy.clone().into_iter().collect();
                    upstreams.extend(state.config.npm.repositories.iter().filter_map(
                        |repository| match repository {
                            crate::config::NpmRepository::Proxy { url, .. } => Some(url.clone()),
                            _ => None,
                        },
                    ));
                    upstreams.into_iter().collect()
                }
                RegistryType::Cargo => state.config.cargo.proxy.clone().into_iter().collect(),
                RegistryType::PyPI => state
                    .config
                    .pypi
                    .upstreams()
                    .iter()
                    .map(|u| u.url().to_string())
                    .collect(),
                RegistryType::Go => state.config.go.proxy.clone().into_iter().collect(),
                RegistryType::Raw => vec![],
                RegistryType::Gems => state.config.gems.proxy.clone().into_iter().collect(),
                RegistryType::Terraform => {
                    state.config.terraform.proxy.clone().into_iter().collect()
                }
                RegistryType::Ansible => state.config.ansible.proxy.clone().into_iter().collect(),
                RegistryType::Nuget => state.config.nuget.proxy.clone().into_iter().collect(),
                RegistryType::PubDart => state.config.pub_dart.proxy.clone().into_iter().collect(),
                RegistryType::Conan => state.config.conan.proxy.clone().into_iter().collect(),
                RegistryType::Rpm => vec![],
                RegistryType::Deb => vec![],
            };

        let mount_path = match reg {
            RegistryType::Maven if !state.config.maven.repositories.is_empty() => {
                "/repository/{repository}/".to_string()
            }
            RegistryType::Npm if !state.config.npm.repositories.is_empty() => {
                "/repository/{repository}/".to_string()
            }
            _ => reg.mount_point().to_string(),
        };
        mount_points.push(MountPoint {
            registry: reg.display_name().to_string(),
            mount_path,
            proxy_upstreams,
        });
    }

    let global_stats = GlobalStats {
        downloads: state.metrics.downloads(),
        uploads: state.metrics.uploads(),
        artifacts: total_artifacts as u64,
        cache_hit_percent: state.metrics.cache_hit_rate(),
        storage_bytes: 0,
        size_available: false,
    };

    let activity = state.activity.recent(20);
    let uptime_seconds = state.start_time.elapsed().as_secs();

    Json(DashboardResponse {
        global_stats,
        registry_stats: registry_card_stats,
        mount_points,
        activity,
        uptime_seconds,
        startup_duration_ms: state.startup_duration_ms,
    })
}

#[cfg(test)]
mod dashboard_size_tests {
    use super::*;

    #[tokio::test]
    async fn physical_and_npm_sizes_are_explicitly_unavailable() {
        let ctx = crate::test_helpers::create_test_context();
        let dashboard = api_dashboard(State(ctx.state.clone())).await.0;

        assert_eq!(dashboard.global_stats.storage_bytes, 0);
        assert!(!dashboard.global_stats.size_available);

        let npm = dashboard
            .registry_stats
            .iter()
            .find(|registry| registry.name == "npm")
            .expect("default config enables npm");
        assert_eq!(npm.size_bytes, 0);
        assert!(!npm.size_available);
    }
}

pub async fn api_list(
    State(state): State<AppState>,
    Path(registry_type): Path<String>,
) -> Json<Vec<RepoInfo>> {
    let repos = state.repo_index.get(&registry_type, &state.storage).await;
    Json((*repos).clone())
}

pub async fn api_detail(
    State(state): State<AppState>,
    Path((registry_type, name)): Path<(String, String)>,
) -> Json<serde_json::Value> {
    match registry_type.as_str() {
        "docker" => {
            let detail = get_docker_detail(&state, &name).await;
            Json(serde_json::to_value(detail).unwrap_or_default())
        }
        "npm" => {
            let detail = get_npm_detail(&state, &name, true, true).await;
            Json(serde_json::to_value(detail).unwrap_or_default())
        }
        "cargo" => {
            let detail = get_cargo_detail(&state, &name, true, true).await;
            Json(serde_json::to_value(detail).unwrap_or_default())
        }
        _ => Json(serde_json::json!({})),
    }
}

pub async fn api_search(
    State(state): State<AppState>,
    Path(registry_type): Path<String>,
    Query(params): Query<SearchQuery>,
) -> axum::response::Html<String> {
    let query = params.q.unwrap_or_default().to_lowercase();

    let repos = state.repo_index.get(&registry_type, &state.storage).await;

    let filtered: Vec<&RepoInfo> = if query.is_empty() {
        repos.iter().collect()
    } else {
        repos
            .iter()
            .filter(|r| r.name.to_lowercase().contains(&query))
            .collect()
    };

    // Return HTML fragment for HTMX
    let html = if filtered.is_empty() {
        r#"<tr><td colspan="4" class="px-6 py-12 text-center text-slate-500">
            <div class="text-4xl mb-2">🔍</div>
            <div>No matching repositories found</div>
        </td></tr>"#
            .to_string()
    } else {
        let folder_icon = r#"<svg class="w-4 h-4 flex-shrink-0 text-slate-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 7v10a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-6l-2-2H5a2 2 0 00-2 2z"/></svg>"#;
        filtered
            .iter()
            .map(|repo| {
                let detail_url =
                    format!("/ui/{}/{}", registry_type, encode_uri_component(&repo.name));
                format!(
                    r#"
                <tr class="hover:bg-slate-700 cursor-pointer" onclick="window.location='{}'">
                    <td class="px-3 md:px-6 py-3 md:py-4">
                        <div class="flex items-center gap-3">{}<a href="{}" class="text-blue-400 hover:text-blue-300 font-medium">{}</a></div>
                    </td>
                    <td class="px-3 md:px-6 py-3 md:py-4 text-slate-400">{}</td>
                    <td class="px-3 md:px-6 py-3 md:py-4 text-slate-400 hidden md:table-cell">{}</td>
                    <td class="px-3 md:px-6 py-3 md:py-4 text-slate-500 text-sm hidden md:table-cell">{}</td>
                </tr>
            "#,
                    detail_url,
                    folder_icon,
                    detail_url,
                    html_escape(&repo.name),
                    repo.versions,
                    format_available_size(repo.size, repo.size_available),
                    &repo.updated
                )
            })
            .collect::<Vec<_>>()
            .join("")
    };

    axum::response::Html(html)
}

pub async fn get_docker_detail(state: &AppState, name: &str) -> DockerDetail {
    // Search for manifests under both the bare name and all namespace-prefixed variants.
    // E.g. for "library/nginx", find keys in docker/library/nginx/manifests/
    // AND docker/docker.io/library/nginx/manifests/, docker/ghcr.io/library/nginx/manifests/, etc.
    let prefix = format!("docker/{}/manifests/", name);
    let mut keys = snapshot_keys(state, "docker", &prefix);

    // Also collect namespaced keys by scanning all docker/ keys for this image name
    let all_docker_keys = snapshot_keys(state, "docker", "docker/");
    for key in &all_docker_keys {
        if let Some(rest) = key.strip_prefix("docker/") {
            if let Some(idx) = rest.find("/manifests/") {
                let raw_name = &rest[..idx];
                if crate::registry::docker::strip_docker_namespace(raw_name) == name
                    && raw_name != name
                {
                    keys.push(key.clone());
                }
            }
        }
    }
    keys.sort();
    keys.dedup();

    // Scheme-less host authority for `docker pull` commands.
    let registry_host = state.config.server.public_host();

    let mut tags = Vec::new();
    for key in &keys {
        // Skip .meta.json files
        if ends_with_ci(key, ".meta.json") {
            continue;
        }

        // Extract tag name: find /manifests/{tag}.json regardless of namespace prefix
        let tag_name = key
            .strip_prefix(&prefix)
            .or_else(|| {
                // For namespaced keys: docker/{ns}/{name}/manifests/{tag}.json
                key.find("/manifests/")
                    .map(|idx| &key[idx + "/manifests/".len()..])
            })
            .and_then(|s| s.strip_suffix(".json"));

        if let Some(tag_name) = tag_name {
            // Load metadata from .meta.json file
            let meta_key = format!("{}.meta.json", key.trim_end_matches(".json"));
            let metadata = if let Ok(meta_data) = state.storage.get(&meta_key).await {
                serde_json::from_slice::<crate::registry::docker::ImageMetadata>(&meta_data)
                    .unwrap_or_default()
            } else {
                crate::registry::docker::ImageMetadata::default()
            };

            // Get file stats for created timestamp if metadata doesn't have push_timestamp
            let created = if metadata.push_timestamp > 0 {
                format_timestamp(metadata.push_timestamp)
            } else if let Some(file_meta) = display_stat(&state.storage, key).await {
                format_timestamp(file_meta.modified)
            } else {
                "N/A".to_string()
            };

            // Calculate size from manifest layers (config + layers)
            let size = if metadata.size_bytes > 0 {
                metadata.size_bytes
            } else {
                // Parse manifest to get actual image size
                if let Ok(manifest_data) = state.storage.get(key).await {
                    if let Ok(manifest) =
                        serde_json::from_slice::<serde_json::Value>(&manifest_data)
                    {
                        let config_size = manifest
                            .get("config")
                            .and_then(|c| c.get("size"))
                            .and_then(|s| s.as_u64())
                            .unwrap_or(0);
                        let layers_size: u64 = manifest
                            .get("layers")
                            .and_then(|l| l.as_array())
                            .map(|layers| {
                                layers
                                    .iter()
                                    .filter_map(|l| l.get("size").and_then(|s| s.as_u64()))
                                    .sum()
                            })
                            .unwrap_or(0);
                        config_size + layers_size
                    } else {
                        0
                    }
                } else {
                    0
                }
            };

            // Format last_pulled
            let last_pulled = if metadata.last_pulled > 0 {
                Some(format_timestamp(metadata.last_pulled))
            } else {
                None
            };

            // Build pull command
            let pull_command = format!("docker pull {}/{}:{}", registry_host, name, tag_name);

            tags.push(TagInfo {
                name: tag_name.to_string(),
                size,
                created,
                downloads: metadata.downloads,
                last_pulled,
                os: if metadata.os.is_empty() {
                    "unknown".to_string()
                } else {
                    metadata.os
                },
                arch: if metadata.arch.is_empty() {
                    "unknown".to_string()
                } else {
                    metadata.arch
                },
                layers_count: metadata.layers.len(),
                pull_command,
            });
        }
    }

    DockerDetail { tags }
}

fn direct_maven_objects(
    state: &AppState,
    repository: &str,
) -> Vec<(String, crate::storage::FileMeta)> {
    let prefix = format!("maven/repositories/{repository}/");
    state
        .repo_index
        .maven_objects()
        .iter()
        .filter_map(|object| {
            object
                .key
                .strip_prefix(&prefix)
                .map(|path| (path.to_string(), object.meta.clone()))
        })
        .collect()
}

fn logical_maven_objects(
    state: &AppState,
    repository: &str,
) -> Option<Vec<(String, crate::storage::FileMeta)>> {
    use crate::config::MavenRepository;

    match state.config.maven.repository(repository)? {
        MavenRepository::Hosted { .. } | MavenRepository::Proxy { .. } => {
            Some(direct_maven_objects(state, repository))
        }
        MavenRepository::Group { members, .. } => {
            let mut selected = BTreeMap::new();
            for member in members {
                for (path, meta) in direct_maven_objects(state, member) {
                    // Same first-member precedence as the protocol group GET.
                    selected.entry(path).or_insert(meta);
                }
            }
            Some(selected.into_iter().collect())
        }
    }
}

fn legacy_maven_objects(state: &AppState) -> Vec<(String, crate::storage::FileMeta)> {
    state
        .repo_index
        .maven_objects()
        .iter()
        .filter_map(|object| {
            object
                .key
                .strip_prefix("maven/")
                .filter(|path| !path.starts_with("repositories/"))
                .map(|path| (path.to_string(), object.meta.clone()))
        })
        .collect()
}

fn maven_repository_rows(state: &AppState) -> Vec<RepoInfo> {
    state
        .config
        .maven
        .repositories
        .iter()
        .map(|repository| {
            let objects = logical_maven_objects(state, repository.name()).unwrap_or_default();
            let versions = objects
                .iter()
                .filter(|(path, _)| {
                    !crate::gc::is_checksum_sidecar(path) && !path.ends_with("maven-metadata.xml")
                })
                .count();
            let size = objects.iter().map(|(_, meta)| meta.size).sum();
            let modified = objects
                .iter()
                .map(|(_, meta)| meta.modified)
                .max()
                .unwrap_or(0);
            RepoInfo {
                name: repository.name().to_string(),
                versions,
                size,
                size_available: true,
                updated: format_timestamp(modified),
                ..Default::default()
            }
        })
        .collect()
}

/// List immediate children of a logical Maven repository path from the last
/// background snapshot. Named groups merge member objects in configured order;
/// groups never acquire a physical storage prefix of their own.
pub async fn get_maven_dir_listing(state: &AppState, path: &str) -> (Vec<RepoInfo>, bool) {
    if !state.config.maven.repositories.is_empty() && path.is_empty() {
        return (maven_repository_rows(state), false);
    }

    let (objects, logical_path) = if state.config.maven.repositories.is_empty() {
        (legacy_maven_objects(state), path)
    } else {
        let (repository, logical_path) = path.split_once('/').unwrap_or((path, ""));
        let Some(objects) = logical_maven_objects(state, repository) else {
            return (Vec::new(), false);
        };
        (objects, logical_path)
    };
    let prefix = if logical_path.is_empty() {
        String::new()
    } else {
        format!("{logical_path}/")
    };
    let keys: Vec<_> = objects
        .iter()
        .filter_map(|(key, meta)| key.strip_prefix(&prefix).map(|rest| (rest, meta)))
        .collect();
    if keys.is_empty() {
        return (Vec::new(), false);
    }

    // Leaf = no subdirectories (only direct files like JARs, POMs, checksums).
    let has_subdirs = keys
        .iter()
        .any(|(rest, _)| !rest.is_empty() && rest.contains('/'));
    if !has_subdirs {
        return (vec![], true);
    }

    // Group by immediate child segment (skip direct files like maven-metadata.xml)
    let mut groups: HashMap<String, (usize, u64, u64)> = HashMap::new();
    for (rest, meta) in keys {
        if rest.is_empty() || !rest.contains('/') {
            continue;
        }
        let child_name = rest.split('/').next().unwrap_or(rest).to_string();
        let entry = groups.entry(child_name).or_insert((0, 0, 0));
        entry.0 += 1;
        entry.1 += meta.size;
        if meta.modified > entry.2 {
            entry.2 = meta.modified;
        }
    }

    let mut result: Vec<RepoInfo> = groups
        .into_iter()
        .map(|(name, (count, size, modified))| RepoInfo {
            name,
            versions: count,
            size,
            size_available: true,
            updated: format_timestamp(modified),
            ..Default::default()
        })
        .collect();
    result.sort_by(|a, b| a.name.cmp(&b.name));

    (result, false)
}

pub async fn get_maven_detail(state: &AppState, path: &str) -> MavenDetail {
    let (objects, logical_path) = if state.config.maven.repositories.is_empty() {
        (legacy_maven_objects(state), path)
    } else {
        let (repository, logical_path) = path.split_once('/').unwrap_or((path, ""));
        let Some(objects) = logical_maven_objects(state, repository) else {
            return MavenDetail { artifacts: vec![] };
        };
        (objects, logical_path)
    };
    let prefix = if logical_path.is_empty() {
        String::new()
    } else {
        format!("{logical_path}/")
    };

    let mut artifacts = Vec::new();
    for (key, meta) in objects {
        if let Some(filename) = key.strip_prefix(&prefix) {
            if filename.contains('/') {
                continue;
            }
            artifacts.push(MavenArtifact {
                filename: filename.to_string(),
                size: meta.size,
            });
        }
    }
    artifacts.sort_by(|left, right| left.filename.cmp(&right.filename));

    MavenDetail { artifacts }
}

#[cfg(test)]
mod named_maven_tests {
    use super::*;
    use crate::config::{MavenRepository, MavenVersionPolicy, MavenWritePolicy};

    #[tokio::test]
    async fn group_browser_deduplicates_with_first_member_precedence_without_storage_scans() {
        let ctx = crate::test_helpers::create_test_context_with_config(|config| {
            config.maven.repositories = vec![
                MavenRepository::Hosted {
                    name: "first".to_string(),
                    version_policy: MavenVersionPolicy::Mixed,
                    write_policy: MavenWritePolicy::AllowOnce,
                },
                MavenRepository::Hosted {
                    name: "second".to_string(),
                    version_policy: MavenVersionPolicy::Mixed,
                    write_policy: MavenWritePolicy::AllowOnce,
                },
                MavenRepository::Group {
                    name: "public".to_string(),
                    members: vec!["first".to_string(), "second".to_string()],
                },
            ];
        });
        let logical = "com/example/app/1.0/app-1.0.jar";
        ctx.state
            .storage
            .put(&format!("maven/repositories/first/{logical}"), b"first")
            .await
            .unwrap();
        ctx.state
            .storage
            .put(
                &format!("maven/repositories/second/{logical}"),
                b"second-copy",
            )
            .await
            .unwrap();
        ctx.state
            .storage
            .put(
                "maven/repositories/second/com/example/app/1.0/app-1.0.pom",
                b"pom",
            )
            .await
            .unwrap();
        ctx.state.repo_index.invalidate("maven");
        assert!(
            ctx.state
                .repo_index
                .rebuild_for_test(RegistryType::Maven, &ctx.state.storage)
                .await
        );

        let backend = crate::test_helpers::FaultInjectBackend::new(ctx.state.storage.clone());
        let list_attempts = backend.list_attempts();
        let mut snapshot_only = ctx.state.clone();
        snapshot_only.storage = Storage::from_backend(std::sync::Arc::new(backend));

        let objects = logical_maven_objects(&snapshot_only, "public").unwrap();
        let duplicate = objects.iter().find(|(path, _)| path == logical).unwrap();
        assert_eq!(duplicate.1.size, 5, "first group member must win");
        assert_eq!(
            objects.len(),
            2,
            "duplicate logical path must be counted once"
        );

        let detail = get_maven_detail(&snapshot_only, "public/com/example/app/1.0").await;
        assert_eq!(
            detail
                .artifacts
                .iter()
                .map(|artifact| artifact.filename.as_str())
                .collect::<Vec<_>>(),
            vec!["app-1.0.jar", "app-1.0.pom"]
        );
        assert!(
            list_attempts.lock().is_empty(),
            "Maven browser request path must use only the published snapshot"
        );
    }
}

pub async fn get_npm_detail(
    state: &AppState,
    name: &str,
    show_prerelease: bool,
    show_all: bool,
) -> PackageDetail {
    let storage = &state.storage;
    let Some((repository, package)) = name
        .strip_prefix("repositories/")
        .and_then(|rest| rest.split_once('/'))
    else {
        return PackageDetail {
            versions: vec![],
            prerelease_count: 0,
            total_stable: 0,
            metadata: PackageMetadata::default(),
        };
    };

    let package_leaf = package.split('/').next_back().unwrap_or(package);
    let hosted_versions_prefix = format!("npm/repositories/{repository}/{package}/versions/");
    let hosted_version_keys = snapshot_keys(state, "npm", &hosted_versions_prefix);
    let mut version_rows: Vec<(String, serde_json::Value, String, String)> = Vec::new();
    let metadata_json = if hosted_version_keys.is_empty() {
        let packument_key =
            format!("npm/repositories/{repository}/proxy/packuments/{package}.json");
        storage.get(&packument_key).await.ok().and_then(|data| {
            let metadata = serde_json::from_slice::<serde_json::Value>(&data).ok()?;
            let time = metadata.get("time").and_then(|value| value.as_object());
            if let Some(versions) = metadata.get("versions").and_then(|value| value.as_object()) {
                for (version, info) in versions {
                    let published = time
                        .and_then(|times| times.get(version))
                        .and_then(|value| value.as_str())
                        .map(|value| value.get(..10).unwrap_or(value).to_string())
                        .unwrap_or_else(|| "N/A".to_string());
                    version_rows.push((
                        version.clone(),
                        info.clone(),
                        published,
                        format!(
                            "npm/repositories/{repository}/proxy/tarballs/{package}/{package_leaf}-{version}.tgz"
                        ),
                    ));
                }
            }
            Some(metadata)
        })
    } else {
        for key in hosted_version_keys {
            let Some(version) = key
                .rsplit('/')
                .next()
                .and_then(|part| part.strip_suffix(".json"))
            else {
                continue;
            };
            let Ok(data) = storage.get(&key).await else {
                continue;
            };
            let Ok(info) = serde_json::from_slice::<serde_json::Value>(&data) else {
                continue;
            };
            let Some(blob_key) =
                crate::npm_layout::hosted_blob_key_from_manifest(repository, package, &data)
            else {
                continue;
            };
            let published = display_stat(storage, &key)
                .await
                .map(|meta| format_timestamp(meta.modified))
                .unwrap_or_else(|| "N/A".to_string());
            version_rows.push((version.to_string(), info, published, blob_key));
        }
        let package_key = format!("npm/repositories/{repository}/{package}/pkg.json");
        storage
            .get(&package_key)
            .await
            .ok()
            .and_then(|data| serde_json::from_slice::<serde_json::Value>(&data).ok())
    };

    let mut stable_versions = Vec::new();
    let mut prerelease_count: usize = 0;
    for (version, info, published, tarball_key) in version_rows {
        let is_prerelease = version.contains('-');
        if is_prerelease {
            prerelease_count += 1;
            if !show_prerelease {
                continue;
            }
        }
        let declared_size = info
            .get("dist")
            .and_then(|dist| dist.get("unpackedSize"))
            .and_then(|size| size.as_u64())
            .unwrap_or(0);
        let (size, cached) = display_stat(storage, &tarball_key)
            .await
            .map_or((declared_size, false), |meta| (meta.size, true));
        stable_versions.push(VersionInfo {
            version,
            size,
            published,
            cached,
        });
    }

    // Sort by version (semver-like, newest first)
    stable_versions.sort_by(|a, b| {
        let a_parts: Vec<u32> = a
            .version
            .split(|c: char| !c.is_ascii_digit())
            .filter_map(|s| s.parse().ok())
            .collect();
        let b_parts: Vec<u32> = b
            .version
            .split(|c: char| !c.is_ascii_digit())
            .filter_map(|s| s.parse().ok())
            .collect();
        b_parts.cmp(&a_parts)
    });

    let total_stable = stable_versions
        .iter()
        .filter(|v| !v.version.contains('-'))
        .count();

    // Limit default view to 20 versions when not showing all
    if !show_prerelease && !show_all && stable_versions.len() > 20 {
        stable_versions.truncate(20);
    }

    let mut metadata = PackageMetadata::default();
    if let Some(meta_json) = metadata_json {
        metadata.description = meta_json
            .get("description")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        metadata.license = meta_json
            .get("license")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        metadata.author = meta_json.get("author").and_then(|v| {
            v.as_str().map(|s| s.to_string()).or_else(|| {
                v.get("name")
                    .and_then(|n| n.as_str())
                    .map(|s| s.to_string())
            })
        });
        metadata.homepage = meta_json
            .get("homepage")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .filter(|s| sanitize_href(s).is_some())
            .map(|s| s.to_string());
        metadata.repository = meta_json
            .get("repository")
            .and_then(|v| {
                v.as_str()
                    .map(|s| s.to_string())
                    .or_else(|| v.get("url").and_then(|u| u.as_str()).map(|s| s.to_string()))
            })
            .map(|s| {
                s.trim_start_matches("git+")
                    .trim_end_matches(".git")
                    .to_string()
            })
            .filter(|s| sanitize_href(s).is_some());
        metadata.keywords = meta_json
            .get("keywords")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
    }

    PackageDetail {
        versions: stable_versions,
        prerelease_count,
        total_stable,
        metadata,
    }
}

pub async fn get_cargo_detail(
    state: &AppState,
    name: &str,
    show_prerelease: bool,
    show_all: bool,
) -> PackageDetail {
    let storage = &state.storage;
    let prefix = format!("cargo/{}/", name);
    let keys = snapshot_keys(state, "cargo", &prefix);

    let mut versions = Vec::new();
    for key in keys.iter().filter(|k| ends_with_ci(k, ".crate")) {
        if let Some(rest) = key.strip_prefix(&prefix) {
            let parts: Vec<_> = rest.split('/').collect();
            if !parts.is_empty() {
                let (size, published) = if let Some(meta) = display_stat(storage, key).await {
                    (meta.size, format_timestamp(meta.modified))
                } else {
                    (0, "N/A".to_string())
                };
                versions.push(VersionInfo {
                    version: parts[0].to_string(),
                    size,
                    published,
                    cached: true,
                });
            }
        }
    }

    versions.sort_by(|a, b| b.version.cmp(&a.version));
    let (versions, prerelease_count, total_stable) =
        apply_prerelease_filter(versions, show_prerelease, show_all);

    // Extract crate metadata from cached metadata.json
    let mut metadata = PackageMetadata::default();
    let cargo_meta_key = format!("cargo/{}/metadata.json", name);
    if let Ok(data) = storage.get(&cargo_meta_key).await {
        if let Ok(meta_json) = serde_json::from_slice::<serde_json::Value>(&data) {
            let crate_obj = meta_json.get("crate").unwrap_or(&meta_json);
            metadata.description = crate_obj
                .get("description")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            metadata.homepage = crate_obj
                .get("homepage")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .filter(|s| sanitize_href(s).is_some())
                .map(|s| s.to_string());
            metadata.repository = crate_obj
                .get("repository")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .filter(|s| sanitize_href(s).is_some())
                .map(|s| s.to_string());
            // Cargo uses documentation field as well; use homepage fallback
            if metadata.homepage.is_none() {
                metadata.homepage = crate_obj
                    .get("documentation")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .filter(|s| sanitize_href(s).is_some())
                    .map(|s| s.to_string());
            }
            metadata.keywords = crate_obj
                .get("keywords")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();
            // Merge categories into keywords for cargo
            if let Some(cats) = crate_obj.get("categories").and_then(|v| v.as_array()) {
                for cat in cats {
                    if let Some(s) = cat.as_str() {
                        if !metadata.keywords.contains(&s.to_string()) {
                            metadata.keywords.push(s.to_string());
                        }
                    }
                }
            }
        }
    }

    PackageDetail {
        versions,
        prerelease_count,
        total_stable,
        metadata,
    }
}

pub async fn get_pypi_detail(
    state: &AppState,
    name: &str,
    show_prerelease: bool,
    show_all: bool,
) -> PackageDetail {
    let storage = &state.storage;
    let prefix = format!("pypi/{}/", name);
    let keys = snapshot_keys(state, "pypi", &prefix);

    let mut versions = Vec::new();
    for key in &keys {
        if let Some(filename) = key.strip_prefix(&prefix) {
            if let Some(version) = extract_pypi_version(name, filename) {
                let (size, published) = if let Some(meta) = display_stat(storage, key).await {
                    (meta.size, format_timestamp(meta.modified))
                } else {
                    (0, "N/A".to_string())
                };
                versions.push(VersionInfo {
                    version,
                    size,
                    published,
                    cached: true,
                });
            }
        }
    }

    versions.sort_by(|a, b| b.version.cmp(&a.version));
    let (versions, prerelease_count, total_stable) =
        apply_prerelease_filter(versions, show_prerelease, show_all);
    PackageDetail {
        versions,
        prerelease_count,
        total_stable,
        metadata: PackageMetadata::default(),
    }
}

/// List immediate children of a Go namespace path for hierarchical browsing.
/// Returns (entries, is_leaf_module). A leaf module has an `@v/` subdirectory.
pub async fn get_go_dir_listing(state: &AppState, path: &str) -> (Vec<RepoInfo>, bool) {
    let storage = &state.storage;
    let prefix = if path.is_empty() {
        "go/".to_string()
    } else {
        format!("go/{}/", path)
    };
    let keys = snapshot_keys(state, "go", &prefix);

    if keys.is_empty() {
        return (vec![], false);
    }

    // Leaf detection: if any key under this prefix contains /@v/ → this is a module
    let is_module = keys.iter().any(|k| {
        k.strip_prefix(&prefix)
            .is_some_and(|r| r.starts_with("@v/"))
    });
    if is_module {
        return (vec![], true);
    }

    // Group by immediate child segment (skip @latest and other direct files)
    // (object count, bytes, latest mtime, complete metadata)
    let mut groups: HashMap<String, (usize, u64, u64, bool)> = HashMap::new();
    for key in &keys {
        if let Some(rest) = key.strip_prefix(&prefix) {
            if rest.is_empty() || !rest.contains('/') {
                continue;
            }
            let child_name = rest.split('/').next().unwrap_or(rest).to_string();
            // Skip @v and @latest markers — they are not namespace directories
            if child_name.starts_with('@') {
                continue;
            }
            let entry = groups.entry(child_name).or_insert((0, 0, 0, true));
            entry.0 += 1;
            if let Some(meta) = display_stat(storage, key).await {
                entry.1 += meta.size;
                if meta.modified > entry.2 {
                    entry.2 = meta.modified;
                }
            } else {
                entry.3 = false;
            }
        }
    }

    let mut result: Vec<RepoInfo> = groups
        .into_iter()
        .map(|(name, (count, size, modified, size_available))| RepoInfo {
            name,
            versions: count,
            size,
            size_available,
            updated: format_timestamp(modified),
            ..Default::default()
        })
        .collect();
    result.sort_by(|a, b| a.name.cmp(&b.name));

    (result, false)
}

/// List Ansible Galaxy namespaces or collections within a namespace.
///
/// - `path == ""` → group all `ansible/download/*.tar.gz` by namespace, return `Vec<RepoInfo>`
///   where `name` = namespace, `versions` = collection count.
/// - `path == "community"` → filter by namespace, group by collection name, return `Vec<RepoInfo>`
///   where `name` = collection name, `versions` = version count.
///
/// Filenames follow the Galaxy convention: `{namespace}-{name}-{version}.tar.gz`.
/// Namespace and name are `[a-z0-9_]+`, so the first `-` is an unambiguous separator.
/// Does NOT call `storage.stat` — avoids latency on large collection counts.
pub async fn get_ansible_namespace_listing(state: &AppState, path: &str) -> Vec<RepoInfo> {
    let keys = snapshot_keys(state, "ansible", "ansible/download/");

    if keys.is_empty() {
        return vec![];
    }

    // Parse filenames: {ns}-{name}-{version}.tar.gz
    // splitn(3, '-') → [ns, name, version_with_ext]
    struct Parsed {
        namespace: String,
        collection: String,
    }
    let mut parsed: Vec<Parsed> = Vec::new();
    for key in &keys {
        if let Some(filename) = key
            .strip_prefix("ansible/download/")
            .and_then(|f| f.strip_suffix(".tar.gz"))
        {
            let parts: Vec<&str> = filename.splitn(3, '-').collect();
            if parts.len() == 3 && !parts[0].is_empty() && !parts[1].is_empty() {
                parsed.push(Parsed {
                    namespace: parts[0].to_string(),
                    collection: parts[1].to_string(),
                });
            }
        }
    }

    if path.is_empty() {
        // Root: group by namespace, count distinct collections per namespace
        let mut ns_collections: HashMap<String, std::collections::HashSet<String>> = HashMap::new();
        for p in &parsed {
            ns_collections
                .entry(p.namespace.clone())
                .or_default()
                .insert(p.collection.clone());
        }
        let mut result: Vec<RepoInfo> = ns_collections
            .into_iter()
            .map(|(ns, cols)| RepoInfo {
                name: ns,
                versions: cols.len(),
                ..Default::default()
            })
            .collect();
        result.sort_by(|a, b| a.name.cmp(&b.name));
        result
    } else {
        // Namespace level: filter by namespace, group by collection name, count versions
        let mut col_counts: HashMap<String, usize> = HashMap::new();
        for p in &parsed {
            if p.namespace == path {
                *col_counts.entry(p.collection.clone()).or_insert(0) += 1;
            }
        }
        let mut result: Vec<RepoInfo> = col_counts
            .into_iter()
            .map(|(col, count)| RepoInfo {
                name: col,
                versions: count,
                ..Default::default()
            })
            .collect();
        result.sort_by(|a, b| a.name.cmp(&b.name));
        result
    }
}

pub async fn get_go_detail(
    state: &AppState,
    module: &str,
    show_prerelease: bool,
    show_all: bool,
) -> PackageDetail {
    let storage = &state.storage;
    let prefix = format!("go/{}/@v/", module);

    // Read version list file (populated by go proxy on list requests)
    let list_key = format!("{}list", prefix);
    let mut known_versions: Vec<String> = Vec::new();
    if let Ok(data) = storage.get(&list_key).await {
        if let Ok(text) = String::from_utf8(data.to_vec()) {
            for line in text.lines() {
                let v = line.trim();
                if !v.is_empty() {
                    known_versions.push(v.to_string());
                }
            }
        }
    }

    // Also scan for .zip files that might exist without being in the list
    let keys = snapshot_keys(state, "go", &prefix);
    for key in keys.iter().filter(|k| ends_with_ci(k, ".zip")) {
        if let Some(rest) = key.strip_prefix(&prefix) {
            if let Some(version) = rest.strip_suffix(".zip") {
                if !known_versions.iter().any(|v| v == version) {
                    known_versions.push(version.to_string());
                }
            }
        }
    }

    let list_ts = display_stat(storage, &list_key)
        .await
        .map(|m| format_timestamp(m.modified))
        .unwrap_or_else(|| "N/A".to_string());

    let mut versions = Vec::new();
    for v in &known_versions {
        let zip_key = format!("{}{}.zip", prefix, v);
        let (size, published, cached) = if let Some(meta) = display_stat(storage, &zip_key).await {
            (meta.size, format_timestamp(meta.modified), true)
        } else {
            (0, list_ts.clone(), false)
        };
        versions.push(VersionInfo {
            version: v.clone(),
            size,
            published,
            cached,
        });
    }

    versions.sort_by(|a, b| b.version.cmp(&a.version));
    let (versions, prerelease_count, total_stable) =
        apply_prerelease_filter(versions, show_prerelease, show_all);
    PackageDetail {
        versions,
        prerelease_count,
        total_stable,
        metadata: PackageMetadata::default(),
    }
}

/// Generic detail for new-format registries (NuGet, Gems, Terraform, Ansible, Pub, Conan).
/// Reads version info from storage using registry-specific paths.
pub async fn get_generic_detail(
    state: &AppState,
    registry: &str,
    name: &str,
    show_prerelease: bool,
    show_all: bool,
) -> PackageDetail {
    let name_lower = name.to_lowercase();
    let storage = &state.storage;

    match registry {
        "nuget" => get_nuget_detail(storage, &name_lower, show_prerelease, show_all).await,
        "conan" => get_conan_detail(state, &name_lower, show_prerelease, show_all).await,
        "rpm" => get_rpm_detail(state, name, show_all).await,
        "deb" => get_deb_detail(state, name, show_all).await,
        "gems" => get_gems_detail(storage, &name_lower, show_prerelease, show_all).await,
        "pub" => get_pub_detail(state, &name_lower, show_prerelease, show_all).await,
        "ansible" => get_ansible_detail(state, &name_lower, show_prerelease, show_all).await,
        _ => get_storage_scan_detail(state, registry, &name_lower, show_prerelease, show_all).await,
    }
}

async fn get_nuget_detail(
    storage: &Storage,
    name: &str,
    show_prerelease: bool,
    show_all: bool,
) -> PackageDetail {
    // Load registration index for real published dates from upstream metadata
    let reg_key = format!("nuget/registration/{}/index.json", name);
    let reg_meta = load_nuget_registration_meta(storage, &reg_key).await;

    // Extract package-level metadata from the registration index
    let metadata = load_nuget_package_metadata(storage, &reg_key).await;

    let key = format!("nuget/flatcontainer/{}/index.json", name);
    if let Ok(data) = storage.get(&key).await {
        if let Ok(index) = serde_json::from_slice::<serde_json::Value>(&data) {
            if let Some(versions) = index.get("versions").and_then(|v| v.as_array()) {
                let fallback_ts = display_stat(storage, &key)
                    .await
                    .map(|m| format_timestamp(m.modified))
                    .unwrap_or_else(|| "N/A".to_string());

                let mut stable_versions = Vec::new();
                let mut prerelease_count: usize = 0;

                for v in versions.iter().rev().filter_map(|v| v.as_str()) {
                    let is_prerelease = v.contains('-');

                    // Get published date from registration metadata
                    let (published, _upstream_size) = reg_meta
                        .get(v)
                        .map(|(p, s)| (p.clone(), *s))
                        .unwrap_or_else(|| (fallback_ts.clone(), 0));

                    // Skip unlisted (NuGet convention: published=1900-01-01)
                    if published.starts_with("1900") {
                        continue;
                    }

                    // Count pre-release, skip unless toggled
                    if is_prerelease {
                        prerelease_count += 1;
                        if !show_prerelease {
                            continue;
                        }
                    }

                    // Check if .nupkg is cached locally
                    let nupkg_key =
                        format!("nuget/flatcontainer/{}/{}/{}.{}.nupkg", name, v, name, v);
                    let (size, cached) = if let Some(meta) = display_stat(storage, &nupkg_key).await
                    {
                        (meta.size, true)
                    } else {
                        (0, false)
                    };

                    stable_versions.push(VersionInfo {
                        version: v.to_string(),
                        size,
                        published,
                        cached,
                    });
                }

                let total_stable = stable_versions
                    .iter()
                    .filter(|v| !v.version.contains('-'))
                    .count();
                // Limit default view to 20 versions when not showing all
                if !show_prerelease && !show_all && stable_versions.len() > 20 {
                    stable_versions.truncate(20);
                }

                return PackageDetail {
                    versions: stable_versions,
                    prerelease_count,
                    total_stable,
                    metadata,
                };
            }
        }
    }
    PackageDetail {
        versions: vec![],
        prerelease_count: 0,
        total_stable: 0,
        metadata,
    }
}

/// Extract per-version (published, packageSize) from cached NuGet registration index.
async fn load_nuget_registration_meta(
    storage: &Storage,
    key: &str,
) -> HashMap<String, (String, u64)> {
    let mut map = HashMap::new();
    let data = match storage.get(key).await {
        Ok(d) => d,
        Err(_) => return map,
    };
    let json: serde_json::Value = match serde_json::from_slice(&data) {
        Ok(v) => v,
        Err(_) => return map,
    };
    if let Some(pages) = json.get("items").and_then(|v| v.as_array()) {
        for page in pages {
            if let Some(items) = page.get("items").and_then(|v| v.as_array()) {
                for item in items {
                    if let Some(entry) = item.get("catalogEntry") {
                        let ver = entry
                            .get("version")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default();
                        let published = entry
                            .get("published")
                            .and_then(|v| v.as_str())
                            .map(|s| s.split('T').next().unwrap_or(s).to_string())
                            .unwrap_or_else(|| "N/A".to_string());
                        let size = entry
                            .get("packageSize")
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0);
                        if !ver.is_empty() {
                            map.insert(ver.to_string(), (published, size));
                        }
                    }
                }
            }
        }
    }
    map
}

/// Extract package-level metadata (description, authors, tags) from NuGet registration index.
/// Uses the latest catalogEntry in the index.
async fn load_nuget_package_metadata(storage: &Storage, key: &str) -> PackageMetadata {
    let mut metadata = PackageMetadata::default();
    let data = match storage.get(key).await {
        Ok(d) => d,
        Err(_) => return metadata,
    };
    let json: serde_json::Value = match serde_json::from_slice(&data) {
        Ok(v) => v,
        Err(_) => return metadata,
    };
    // Walk pages → items → catalogEntry, take the last (newest) entry with data
    if let Some(pages) = json.get("items").and_then(|v| v.as_array()) {
        for page in pages.iter().rev() {
            if let Some(items) = page.get("items").and_then(|v| v.as_array()) {
                for item in items.iter().rev() {
                    if let Some(entry) = item.get("catalogEntry") {
                        if metadata.description.is_none() {
                            metadata.description = entry
                                .get("description")
                                .and_then(|v| v.as_str())
                                .filter(|s| !s.is_empty())
                                .map(|s| s.to_string());
                        }
                        if metadata.author.is_none() {
                            metadata.author = entry
                                .get("authors")
                                .and_then(|v| v.as_str())
                                .filter(|s| !s.is_empty())
                                .map(|s| s.to_string());
                        }
                        if metadata.keywords.is_empty() {
                            if let Some(tags) = entry.get("tags").and_then(|v| v.as_array()) {
                                metadata.keywords = tags
                                    .iter()
                                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                    .collect();
                            } else if let Some(tags_str) =
                                entry.get("tags").and_then(|v| v.as_str())
                            {
                                // NuGet sometimes stores tags as space-separated string
                                metadata.keywords =
                                    tags_str.split_whitespace().map(|s| s.to_string()).collect();
                            }
                        }
                        // Once we have all fields, stop iterating
                        if metadata.description.is_some() && metadata.author.is_some() {
                            return metadata;
                        }
                    }
                }
            }
        }
    }
    metadata
}

async fn get_conan_detail(
    state: &AppState,
    name: &str,
    show_prerelease: bool,
    show_all: bool,
) -> PackageDetail {
    let storage = &state.storage;
    // Conan: conan/{name}/{version}/_/_/revisions.json (metadata)
    // Actual files: conan/{name}/{version}/_/_/{rrev}/export/* or /packages/*/
    let prefix = format!("conan/{}/", name);
    let keys = snapshot_keys(state, "conan", &prefix);
    let mut version_data: HashMap<String, (u64, u64, bool)> = HashMap::new(); // (size, mtime, has_content)

    for key in &keys {
        if let Some(rest) = key.strip_prefix(&prefix) {
            if let Some(version) = rest.split('/').next() {
                let entry = version_data
                    .entry(version.to_string())
                    .or_insert((0, 0, false));
                let is_content = !ends_with_ci(key, "/revisions.json");
                if let Some(meta) = display_stat(storage, key).await {
                    if is_content {
                        entry.0 += meta.size;
                        entry.2 = true;
                    }
                    if meta.modified > entry.1 {
                        entry.1 = meta.modified;
                    }
                }
            }
        }
    }

    let mut versions: Vec<VersionInfo> = version_data
        .into_iter()
        .map(|(version, (size, modified, has_content))| VersionInfo {
            version,
            size,
            published: format_timestamp(modified),
            cached: has_content,
        })
        .collect();
    versions.sort_by(|a, b| b.version.cmp(&a.version));
    let (versions, prerelease_count, total_stable) =
        apply_prerelease_filter(versions, show_prerelease, show_all);
    PackageDetail {
        versions,
        prerelease_count,
        total_stable,
        metadata: PackageMetadata::default(),
    }
}

/// RPM: the UI "package" is a repository; each row is one stored package
/// (NEVRA). Reads the per-package metadata sidecars written at upload —
/// never the .rpm payloads. No prerelease filter: NEVRA strings always
/// contain '-' and would all be misclassified as prerelease.
async fn get_rpm_detail(state: &AppState, repo: &str, show_all: bool) -> PackageDetail {
    let storage = &state.storage;
    let prefix = format!("rpm/{}/.nora-meta/", repo);
    let keys = snapshot_keys(state, "rpm", &prefix);
    let mut versions = Vec::new();

    for key in &keys {
        let Ok(data) = storage.get(key).await else {
            continue;
        };
        let Ok(rec) = serde_json::from_slice::<serde_json::Value>(&data) else {
            continue;
        };
        let s = |f: &str| {
            rec.get(f)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string()
        };
        versions.push(VersionInfo {
            version: format!(
                "{}-{}-{}.{}",
                s("name"),
                s("version"),
                s("release"),
                s("arch")
            ),
            size: rec
                .get("size_package")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            published: format_timestamp(rec.get("file_time").and_then(|v| v.as_u64()).unwrap_or(0)),
            cached: true,
        });
    }

    versions.sort_by(|a, b| b.version.cmp(&a.version));
    let total = versions.len();
    if !show_all && versions.len() > 20 {
        versions.truncate(20);
    }
    PackageDetail {
        versions,
        prerelease_count: 0,
        total_stable: total,
        metadata: PackageMetadata::default(),
    }
}

/// Debian: the UI "package" is a repository; each row is one stored package
/// (package_version_arch). Reads the per-package control sidecars written at
/// upload — never the .deb payloads. No prerelease filter: Debian versions
/// routinely contain '-' and would all be misclassified as prerelease.
async fn get_deb_detail(state: &AppState, repo: &str, show_all: bool) -> PackageDetail {
    let storage = &state.storage;
    let prefix = format!("deb/{}/.nora-meta/", repo);
    let keys = snapshot_keys(state, "deb", &prefix);
    let mut versions = Vec::new();

    for key in &keys {
        let Ok(data) = storage.get(key).await else {
            continue;
        };
        let Ok(rec) = serde_json::from_slice::<serde_json::Value>(&data) else {
            continue;
        };
        let s = |f: &str| {
            rec.get(f)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string()
        };
        let published = display_stat(storage, key)
            .await
            .map(|m| format_timestamp(m.modified))
            .unwrap_or_else(|| "N/A".to_string());
        versions.push(VersionInfo {
            version: format!("{}_{}_{}", s("package"), s("version"), s("arch")),
            size: rec.get("size").and_then(|v| v.as_u64()).unwrap_or(0),
            published,
            cached: true,
        });
    }

    versions.sort_by(|a, b| b.version.cmp(&a.version));
    let total = versions.len();
    if !show_all && versions.len() > 20 {
        versions.truncate(20);
    }
    PackageDetail {
        versions,
        prerelease_count: 0,
        total_stable: total,
        metadata: PackageMetadata::default(),
    }
}

async fn get_gems_detail(
    storage: &Storage,
    name: &str,
    show_prerelease: bool,
    show_all: bool,
) -> PackageDetail {
    // Read compact index: gems/info/{name}
    // Format: "VERSION DEPS|checksum:HEX" per line, first line is "---"
    let info_key = format!("gems/info/{}", name);
    let info_ts = display_stat(storage, &info_key)
        .await
        .map(|m| format_timestamp(m.modified))
        .unwrap_or_else(|| "N/A".to_string());

    let mut versions = Vec::new();

    if let Ok(data) = storage.get(&info_key).await {
        if let Ok(text) = String::from_utf8(data.to_vec()) {
            for line in text.lines() {
                if line.starts_with('-') || line.is_empty() {
                    continue;
                }
                // "1.3.0 deps...|checksum:..." — version is first token
                let version = match line.split_whitespace().next() {
                    Some(v) if !v.is_empty() => v.to_string(),
                    _ => continue,
                };

                // Check if .gem is cached
                let gem_key = format!("gems/gems/{}-{}.gem", name, version);
                let (size, published, cached) =
                    if let Some(meta) = display_stat(storage, &gem_key).await {
                        (meta.size, format_timestamp(meta.modified), true)
                    } else {
                        (0, info_ts.clone(), false)
                    };

                versions.push(VersionInfo {
                    version,
                    size,
                    published,
                    cached,
                });
            }
        }
    }

    // Reverse: newest first (compact index is chronological)
    versions.reverse();
    let (versions, prerelease_count, total_stable) =
        apply_prerelease_filter(versions, show_prerelease, show_all);
    PackageDetail {
        versions,
        prerelease_count,
        total_stable,
        metadata: PackageMetadata::default(),
    }
}

async fn get_pub_detail(
    state: &AppState,
    name: &str,
    show_prerelease: bool,
    show_all: bool,
) -> PackageDetail {
    let storage = &state.storage;
    // Pub: pub/packages/{name}/versions/{version}.tar.gz
    let prefix = format!("pub/packages/{}/versions/", name);
    let keys = snapshot_keys(state, "pub", &prefix);
    let mut versions = Vec::new();

    for key in &keys {
        if ends_with_ci(key, ".sha256") {
            continue;
        }
        if let Some(rest) = key.strip_prefix(&prefix) {
            let version = rest.trim_end_matches(".tar.gz").to_string();
            if !version.is_empty() {
                let (size, published) = if let Some(meta) = display_stat(storage, key).await {
                    (meta.size, format_timestamp(meta.modified))
                } else {
                    (0, "N/A".to_string())
                };
                versions.push(VersionInfo {
                    version,
                    size,
                    published,
                    cached: true,
                });
            }
        }
    }
    versions.sort_by(|a, b| b.version.cmp(&a.version));
    let (versions, prerelease_count, total_stable) =
        apply_prerelease_filter(versions, show_prerelease, show_all);
    PackageDetail {
        versions,
        prerelease_count,
        total_stable,
        metadata: PackageMetadata::default(),
    }
}

/// Ansible Galaxy collection detail: list versions of `{ns}.{name}`.
/// Files stored as `ansible/download/{ns}-{name}-{ver}.tar.gz`.
async fn get_ansible_detail(
    state: &AppState,
    name: &str,
    show_prerelease: bool,
    show_all: bool,
) -> PackageDetail {
    let storage = &state.storage;
    // name comes as "community.general" → split to (ns, collection)
    let (ns, col) = match name.split_once('.') {
        Some(pair) => pair,
        None => {
            return PackageDetail {
                versions: vec![],
                prerelease_count: 0,
                total_stable: 0,
                metadata: PackageMetadata::default(),
            };
        }
    };

    let keys = snapshot_keys(state, "ansible", "ansible/download/");
    let prefix = format!("{}-{}-", ns, col);
    let mut versions = Vec::new();

    for key in &keys {
        if let Some(filename) = key
            .strip_prefix("ansible/download/")
            .and_then(|f| f.strip_suffix(".tar.gz"))
        {
            if let Some(version) = filename.strip_prefix(&prefix) {
                if !version.is_empty() {
                    let (size, published) = if let Some(meta) = display_stat(storage, key).await {
                        (meta.size, format_timestamp(meta.modified))
                    } else {
                        (0, "N/A".to_string())
                    };
                    versions.push(VersionInfo {
                        version: version.to_string(),
                        size,
                        published,
                        cached: true,
                    });
                }
            }
        }
    }

    versions.sort_by(|a, b| b.version.cmp(&a.version));
    let (versions, prerelease_count, total_stable) =
        apply_prerelease_filter(versions, show_prerelease, show_all);
    PackageDetail {
        versions,
        prerelease_count,
        total_stable,
        metadata: PackageMetadata::default(),
    }
}

/// Fallback: scan storage for files matching {registry}/{name}/*
async fn get_storage_scan_detail(
    state: &AppState,
    registry: &str,
    name: &str,
    show_prerelease: bool,
    show_all: bool,
) -> PackageDetail {
    let storage = &state.storage;
    let prefix = format!("{}/{}/", registry, name);
    let keys = snapshot_keys(state, registry, &prefix);
    let mut versions = Vec::new();
    for key in &keys {
        if let Some(rest) = key.strip_prefix(&prefix) {
            let version = rest
                .trim_end_matches(".tar.gz")
                .trim_end_matches(".gem")
                .trim_end_matches(".zip")
                .trim_end_matches(".tgz")
                .to_string();
            if !version.is_empty() && !version.contains('/') {
                let (size, published) = if let Some(meta) = display_stat(storage, key).await {
                    (meta.size, format_timestamp(meta.modified))
                } else {
                    (0, "N/A".to_string())
                };
                versions.push(VersionInfo {
                    version,
                    size,
                    published,
                    cached: true,
                });
            }
        }
    }
    versions.sort_by(|a, b| b.version.cmp(&a.version));
    let (versions, prerelease_count, total_stable) =
        apply_prerelease_filter(versions, show_prerelease, show_all);
    PackageDetail {
        versions,
        prerelease_count,
        total_stable,
        metadata: PackageMetadata::default(),
    }
}

/// Detect pre-release versions across registry ecosystems.
/// Covers semver (`-alpha`), PyPI (`1.0a1`, `.dev`), and RubyGems (`.pre`).
fn is_prerelease_version(version: &str) -> bool {
    // Semver: 1.0.0-alpha.1 (cargo, npm, nuget, go)
    if version.contains('-') {
        return true;
    }
    let lower = version.to_lowercase();
    // RubyGems: 1.0.0.pre.1, 1.0.0.alpha, 1.0.0.beta, 1.0.0.rc1
    if lower.contains(".pre")
        || lower.contains(".alpha")
        || lower.contains(".beta")
        || lower.contains(".rc")
    {
        return true;
    }
    // PyPI PEP 440: 1.0a1, 1.0b2, 1.0rc1, 1.0.dev3
    if lower.contains(".dev") {
        return true;
    }
    // PyPI short: digit followed by 'a' or 'b' followed by digit (e.g., 1.0a1, 2.3b4)
    let bytes = lower.as_bytes();
    for window in bytes.windows(3) {
        if window[0].is_ascii_digit()
            && (window[1] == b'a' || window[1] == b'b')
            && window[2].is_ascii_digit()
        {
            return true;
        }
    }
    // PyPI: digit followed by 'rc' followed by digit (e.g., 1.0rc1)
    for window in bytes.windows(4) {
        if window[0].is_ascii_digit()
            && window[1] == b'r'
            && window[2] == b'c'
            && window[3].is_ascii_digit()
        {
            return true;
        }
    }
    false
}

/// Apply prerelease filtering, counting, and pagination to a version list.
///
/// - `show_prerelease`: include pre-release versions in output
/// - `show_all`: disable top-20 truncation (show all stable versions)
///
/// Returns `(filtered_versions, prerelease_count, total_stable)`.
fn apply_prerelease_filter(
    mut versions: Vec<VersionInfo>,
    show_prerelease: bool,
    show_all: bool,
) -> (Vec<VersionInfo>, usize, usize) {
    let prerelease_count = versions
        .iter()
        .filter(|v| is_prerelease_version(&v.version))
        .count();

    if !show_prerelease {
        versions.retain(|v| !is_prerelease_version(&v.version));
    }

    let total_stable = versions
        .iter()
        .filter(|v| !is_prerelease_version(&v.version))
        .count();

    if !show_prerelease && !show_all && versions.len() > 20 {
        versions.truncate(20);
    }

    (versions, prerelease_count, total_stable)
}

fn extract_pypi_version(name: &str, filename: &str) -> Option<String> {
    // Handle both .tar.gz and .whl files
    let clean_name = name.replace('-', "_");

    if ends_with_ci(filename, ".tar.gz") {
        // package-1.0.0.tar.gz
        let base = filename.strip_suffix(".tar.gz")?;
        let version = base
            .strip_prefix(&format!("{}-", name))
            .or_else(|| base.strip_prefix(&format!("{}-", clean_name)))?;
        Some(version.to_string())
    } else if ends_with_ci(filename, ".whl") {
        // package-1.0.0-py3-none-any.whl
        let parts: Vec<_> = filename.split('-').collect();
        if parts.len() >= 2 {
            Some(parts[1].to_string())
        } else {
            None
        }
    } else {
        None
    }
}

pub async fn get_raw_detail(state: &AppState, group: &str) -> PackageDetail {
    let storage = &state.storage;
    let prefix = format!("raw/{}/", group);
    let keys = snapshot_keys(state, "raw", &prefix);

    let mut versions = Vec::new();

    if keys.is_empty() {
        // Root-level file: "raw/myfile.txt" (no subdirectory)
        let direct_key = format!("raw/{}", group);
        if let Some(meta) = snapshot_meta(state, "raw", &direct_key) {
            versions.push(VersionInfo {
                version: group.to_string(),
                size: meta.size,
                published: format_timestamp(meta.modified),
                cached: true,
            });
            return PackageDetail {
                versions,
                prerelease_count: 0,
                total_stable: 0,
                metadata: PackageMetadata::default(),
            };
        }
    }

    for key in &keys {
        if let Some(filename) = key.strip_prefix(&prefix) {
            let (size, published) = if let Some(meta) = display_stat(storage, key).await {
                (meta.size, format_timestamp(meta.modified))
            } else {
                (0, "N/A".to_string())
            };
            versions.push(VersionInfo {
                version: filename.to_string(),
                size,
                published,
                cached: true,
            });
        }
    }

    PackageDetail {
        versions,
        prerelease_count: 0,
        total_stable: 0,
        metadata: PackageMetadata::default(),
    }
}

/// List immediate children (subfolders + files) of a raw directory path.
/// Returns (entries, is_directory). If the path is a single file, returns empty vec + false.
pub async fn get_raw_dir_listing(state: &AppState, path: &str) -> (Vec<RepoInfo>, bool) {
    let storage = &state.storage;
    let prefix = format!("raw/{}/", path);
    let keys = snapshot_keys(state, "raw", &prefix);

    if keys.is_empty() {
        // Check if it's a direct file
        let direct_key = format!("raw/{}", path);
        if snapshot_meta(state, "raw", &direct_key).is_some() {
            return (vec![], false); // It's a file, not a directory
        }
        return (vec![], true); // Empty directory
    }

    // Group by immediate child segment
    // (object count, bytes, latest mtime, direct file, complete metadata)
    let mut groups: HashMap<String, (usize, u64, u64, bool, bool)> = HashMap::new();

    for key in &keys {
        if let Some(rest) = key.strip_prefix(&prefix) {
            if rest.is_empty() {
                continue;
            }
            let is_direct_file = !rest.contains('/');
            let child_name = rest.split('/').next().unwrap_or(rest).to_string();

            let entry = groups
                .entry(child_name)
                .or_insert((0, 0, 0, is_direct_file, true));
            entry.0 += 1;
            if let Some(meta) = display_stat(storage, key).await {
                entry.1 += meta.size;
                if meta.modified > entry.2 {
                    entry.2 = meta.modified;
                }
            } else {
                entry.4 = false;
            }
        }
    }

    let mut result: Vec<RepoInfo> = groups
        .into_iter()
        .map(
            |(name, (count, size, modified, is_file, size_available))| RepoInfo {
                name,
                versions: count,
                size,
                size_available,
                updated: format_timestamp(modified),
                is_file,
            },
        )
        .collect();

    // Sort: directories first, then files, alphabetical within each group
    result.sort_by(|a, b| a.is_file.cmp(&b.is_file).then_with(|| a.name.cmp(&b.name)));

    (result, true)
}

#[cfg(test)]
mod size_availability_tests {
    use super::*;

    #[tokio::test]
    async fn directory_sizes_require_complete_metadata() {
        let ctx = crate::test_helpers::create_test_context();
        let go_key = "go/example.com/acme/module/@v/v1.0.0.zip";
        let raw_key = "raw/example/dir/file.txt";
        ctx.state.storage.put(go_key, b"go-bytes").await.unwrap();
        ctx.state.storage.put(raw_key, b"raw-bytes").await.unwrap();
        // TestContext starts the index worker immediately, so force both
        // snapshots dirty after writing the fixtures.
        ctx.state.repo_index.invalidate("go");
        ctx.state.repo_index.invalidate("raw");
        assert!(
            ctx.state
                .repo_index
                .rebuild_for_test(RegistryType::Go, &ctx.state.storage)
                .await
        );
        assert!(
            ctx.state
                .repo_index
                .rebuild_for_test(RegistryType::Raw, &ctx.state.storage)
                .await
        );

        let (go_rows, go_is_leaf) = get_go_dir_listing(&ctx.state, "example.com").await;
        assert!(!go_is_leaf);
        assert_eq!(go_rows.len(), 1);
        assert_eq!(go_rows[0].size, b"go-bytes".len() as u64);
        assert!(go_rows[0].size_available);

        let (raw_rows, raw_is_directory) = get_raw_dir_listing(&ctx.state, "example").await;
        assert!(raw_is_directory);
        assert_eq!(
            raw_rows.len(),
            1,
            "raw snapshot: {:?}",
            ctx.state.repo_index.objects("raw")
        );
        assert_eq!(raw_rows[0].size, b"raw-bytes".len() as u64);
        assert!(raw_rows[0].size_available);

        let backend = crate::test_helpers::FaultInjectBackend::new(ctx.state.storage.clone())
            .fail_stat(go_key)
            .fail_stat(raw_key);
        let mut unavailable_state = ctx.state.clone();
        unavailable_state.storage = Storage::from_backend(std::sync::Arc::new(backend));

        let (go_rows, _) = get_go_dir_listing(&unavailable_state, "example.com").await;
        assert_eq!(go_rows.len(), 1);
        assert!(!go_rows[0].size_available);

        let (raw_rows, _) = get_raw_dir_listing(&unavailable_state, "example").await;
        assert_eq!(raw_rows.len(), 1);
        assert!(!raw_rows[0].size_available);
    }
}

#[cfg(test)]
mod named_npm_tests {
    use super::*;

    #[tokio::test]
    async fn browser_detail_handlers_never_scan_storage() {
        let ctx = crate::test_helpers::create_test_context();
        let backend = crate::test_helpers::FaultInjectBackend::new(ctx.state.storage.clone());
        let list_attempts = backend.list_attempts();
        let mut state = ctx.state.clone();
        state.storage = Storage::from_backend(std::sync::Arc::new(backend));
        state.repo_index = std::sync::Arc::new(crate::repo_index::RepoIndex::new());

        for registry in RegistryType::all() {
            assert!(
                state
                    .repo_index
                    .rebuild_for_test(*registry, &state.storage)
                    .await,
                "background snapshot must build for {}",
                registry.as_str()
            );
        }
        list_attempts.lock().clear();

        let _ = get_docker_detail(&state, "library/example").await;
        let _ = get_npm_detail(&state, "repositories/npm-private/example", true, true).await;
        let _ = get_cargo_detail(&state, "example", true, true).await;
        let _ = get_pypi_detail(&state, "example", true, true).await;
        let _ = get_go_dir_listing(&state, "example.com").await;
        let _ = get_go_detail(&state, "example.com/module", true, true).await;
        let _ = get_ansible_namespace_listing(&state, "").await;
        let _ = get_raw_dir_listing(&state, "example").await;
        let _ = get_raw_detail(&state, "example").await;
        for registry in [
            "nuget",
            "conan",
            "rpm",
            "deb",
            "gems",
            "pub",
            "ansible",
            "terraform",
        ] {
            let _ = get_generic_detail(&state, registry, "example", true, true).await;
        }

        assert!(
            list_attempts.lock().is_empty(),
            "browser request paths must read the published snapshot, not LIST storage"
        );
    }

    #[tokio::test]
    async fn detail_reads_named_hosted_manifest_and_tarball() {
        use base64::Engine as _;
        use sha2::Digest as _;

        let ctx = crate::test_helpers::create_test_context();
        let storage = ctx.state.storage.clone();
        let blob = b"tarball";
        let manifest = serde_json::to_vec(&serde_json::json!({
            "name": "@scope/pkg",
            "version": "1.2.3",
            "description": "example",
            "dist": {
                "integrity": format!(
                    "sha512-{}",
                    base64::engine::general_purpose::STANDARD
                        .encode(sha2::Sha512::digest(blob))
                )
            }
        }))
        .unwrap();
        storage
            .put(
                "npm/repositories/npm-private/@scope/pkg/versions/1.2.3.json",
                &manifest,
            )
            .await
            .unwrap();
        storage
            .put(
                "npm/repositories/npm-private/@scope/pkg/pkg.json",
                br#"{"name":"@scope/pkg","description":"example"}"#,
            )
            .await
            .unwrap();
        storage
            .put(
                &crate::npm_layout::hosted_blob_key_from_manifest(
                    "npm-private",
                    "@scope/pkg",
                    &manifest,
                )
                .unwrap(),
                blob,
            )
            .await
            .unwrap();

        // Direct fixture writes bypass the publish handlers, so reproduce the
        // generation invalidation a real hosted publish performs.
        ctx.state.repo_index.invalidate("npm");
        assert!(
            ctx.state
                .repo_index
                .rebuild_for_test(RegistryType::Npm, &storage)
                .await
        );
        let detail = get_npm_detail(
            &ctx.state,
            "repositories/npm-private/@scope/pkg",
            true,
            true,
        )
        .await;

        assert_eq!(detail.versions.len(), 1);
        assert_eq!(detail.versions[0].version, "1.2.3");
        assert!(detail.versions[0].cached);
        assert_eq!(detail.versions[0].size, 7);
        assert_eq!(detail.metadata.description.as_deref(), Some("example"));
    }
}
