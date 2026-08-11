// Copyright (c) 2026 The NORA Authors
// SPDX-License-Identifier: MIT

use super::components::{format_available_size, format_timestamp, html_escape, sanitize_href};
use super::i18n::Lang;
use super::templates::{
    encode_uri_component, render_registry_search_error, render_registry_search_results,
};
use crate::activity_log::ActivityEntry;
use crate::registry_type::RegistryType;
use crate::repo_index::{
    IndexStatus, LogicalIndexedObject, RepoInfo, RepoQuery, MAX_QUERY_EXAMINED,
};
use crate::validation::ends_with_ci;
use crate::AppState;
use crate::Storage;
use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Json, Response},
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
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
    pub download_base: String,
}

pub struct MavenDirectoryEnvelope {
    pub items: Vec<RepoInfo>,
    pub continuation_token: Option<String>,
    pub is_leaf: bool,
}

pub struct MavenDetailEnvelope {
    pub detail: MavenDetail,
    pub continuation_token: Option<String>,
}

#[derive(Deserialize)]
pub struct SearchQuery {
    pub q: Option<String>,
    pub continuation_token: Option<String>,
    pub limit: Option<usize>,
    pub lang: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct IndexListQuery {
    pub q: Option<String>,
    pub continuation_token: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct IndexDetailQuery {
    pub continuation_token: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct IndexPageEnvelope {
    pub items: Vec<RepoInfo>,
    pub continuation_token: Option<String>,
    pub generation: u64,
    pub index_state: IndexStatus,
}

#[derive(Serialize)]
pub struct NpmDetailEnvelope {
    #[serde(flatten)]
    pub detail: PackageDetail,
    pub continuation_token: Option<String>,
    pub generation: u64,
    pub index_state: IndexStatus,
}

#[derive(Debug, Serialize, Deserialize)]
struct IndexCursor {
    schema: u8,
    registry: String,
    filter: String,
    generation: u64,
    config_digest: String,
    last_key: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PageError {
    BadCursor,
    GenerationChanged,
    Unavailable,
}

impl PageError {
    pub(crate) fn response(self) -> Response {
        let (status, code) = match self {
            Self::BadCursor => (StatusCode::BAD_REQUEST, "invalid_continuation_token"),
            Self::GenerationChanged => (StatusCode::CONFLICT, "index_generation_changed"),
            Self::Unavailable => (StatusCode::SERVICE_UNAVAILABLE, "index_unavailable"),
        };
        let mut response = (status, Json(serde_json::json!({ "error": code }))).into_response();
        if self == Self::Unavailable {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("2"));
            response
                .headers_mut()
                .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        }
        response
    }
}

#[cfg(test)]
mod page_error_response_tests {
    use super::*;

    #[tokio::test]
    async fn unavailable_is_retryable_json_but_cursor_errors_are_not() {
        let unavailable = PageError::Unavailable.response();
        assert_eq!(unavailable.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            unavailable.headers().get(header::RETRY_AFTER),
            Some(&HeaderValue::from_static("2"))
        );
        assert_eq!(
            unavailable.headers().get(header::CACHE_CONTROL),
            Some(&HeaderValue::from_static("no-store"))
        );
        let body = axum::body::to_bytes(unavailable.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            serde_json::json!({"error": "index_unavailable"})
        );

        for (error, status) in [
            (PageError::BadCursor, StatusCode::BAD_REQUEST),
            (PageError::GenerationChanged, StatusCode::CONFLICT),
        ] {
            let response = error.response();
            assert_eq!(response.status(), status);
            assert!(!response.headers().contains_key(header::RETRY_AFTER));
        }
    }
}

fn encode_cursor(cursor: &IndexCursor) -> Option<String> {
    serde_json::to_vec(cursor)
        .ok()
        .map(|bytes| URL_SAFE_NO_PAD.encode(bytes))
}

fn decode_cursor(token: &str) -> Result<IndexCursor, PageError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(token)
        .map_err(|_| PageError::BadCursor)?;
    let cursor: IndexCursor = serde_json::from_slice(&bytes).map_err(|_| PageError::BadCursor)?;
    if cursor.schema != 1 {
        return Err(PageError::BadCursor);
    }
    Ok(cursor)
}

pub(crate) async fn persistent_page(
    state: &AppState,
    registry: &str,
    filter: Option<String>,
    continuation_token: Option<String>,
    limit: usize,
) -> Result<IndexPageEnvelope, PageError> {
    let filter = filter.unwrap_or_default().trim().to_lowercase();
    let cursor = continuation_token
        .as_deref()
        .map(decode_cursor)
        .transpose()?;
    if cursor
        .as_ref()
        .is_some_and(|cursor| cursor.registry != registry || cursor.filter != filter)
    {
        return Err(PageError::BadCursor);
    }
    let after = cursor
        .as_ref()
        .map(|cursor| URL_SAFE_NO_PAD.decode(&cursor.last_key))
        .transpose()
        .map_err(|_| PageError::BadCursor)?;

    let (name_prefix, before_name, allowed_repositories) = match registry {
        "maven" if state.config.maven.repositories.is_empty() => {
            (None, Some("repositories/".to_string()), None)
        }
        "maven" => (
            Some("repositories/".to_string()),
            None,
            Some(
                state
                    .config
                    .maven
                    .repositories
                    .iter()
                    .map(|repository| repository.name().to_string())
                    .collect(),
            ),
        ),
        "npm" if !state.config.npm.repositories.is_empty() => (
            Some("repositories/".to_string()),
            None,
            Some(
                state
                    .config
                    .npm
                    .repositories
                    .iter()
                    .map(|repository| repository.name().to_string())
                    .collect(),
            ),
        ),
        "npm" => (Some("repositories/".to_string()), None, None),
        _ => (None, None, None),
    };
    let page = state
        .repo_index
        .persistent_repo_page(RepoQuery {
            registry: registry.to_string(),
            after,
            filter: (!filter.is_empty()).then_some(filter.clone()),
            limit,
            max_examined: MAX_QUERY_EXAMINED,
            deadline: std::time::Duration::from_millis(250),
            name_prefix,
            before_name,
            allowed_repositories,
        })
        .await
        .map_err(|_| PageError::Unavailable)?;
    if cursor.as_ref().is_some_and(|cursor| {
        cursor.generation != page.generation || cursor.config_digest != page.config_digest
    }) {
        return Err(PageError::GenerationChanged);
    }
    let mut items = page.items;
    if registry == "maven" {
        if state.config.maven.repositories.is_empty() {
            items.retain(|row| !row.name.starts_with("repositories/"));
        } else {
            items.retain_mut(|row| {
                let Some(logical) = row.name.strip_prefix("repositories/") else {
                    return false;
                };
                row.name = logical.to_string();
                true
            });
        }
    }
    let continuation_token = page.next_after.and_then(|last_key| {
        encode_cursor(&IndexCursor {
            schema: 1,
            registry: registry.to_string(),
            filter,
            generation: page.generation,
            config_digest: page.config_digest,
            last_key: URL_SAFE_NO_PAD.encode(last_key),
        })
    });
    Ok(IndexPageEnvelope {
        items,
        continuation_token,
        generation: page.generation,
        index_state: state
            .repo_index
            .persistent_status()
            .unwrap_or(IndexStatus::Warming),
    })
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
    let cached_counts = state.repo_index.counts();
    let cached_sizes = state.repo_index.sizes();

    for reg in RegistryType::all() {
        if !state.enabled_registries.contains(reg) {
            continue;
        }

        let name = reg.as_str();
        let repos = state.repo_index.get(name, &state.storage).await;
        let persistent = state.repo_index.has_persistent()
            && matches!(reg, RegistryType::Maven | RegistryType::Npm);
        let size_available = if persistent {
            state.repo_index.status(name) == Some(IndexStatus::Ready)
        } else {
            *reg != RegistryType::Npm
                && if repos.is_empty() {
                    state.repo_index.status(name) == Some(IndexStatus::Ready)
                } else {
                    repos.iter().all(|repo| repo.size_available)
                }
        };
        let size = if size_available {
            if persistent {
                cached_sizes.get(reg).copied().unwrap_or(0)
            } else {
                repos.iter().map(|r| r.size).sum()
            }
        } else {
            0
        };
        let versions = if persistent {
            cached_counts.get(reg).copied().unwrap_or(0)
        } else {
            repos.iter().map(|r| r.versions).sum()
        };

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
    Query(query): Query<IndexListQuery>,
) -> Response {
    if matches!(registry_type.as_str(), "maven" | "npm") {
        let limit = query.limit.unwrap_or(50).clamp(1, 100);
        return match persistent_page(
            &state,
            &registry_type,
            query.q,
            query.continuation_token,
            limit,
        )
        .await
        {
            Ok(page) => Json(page).into_response(),
            Err(error) => error.response(),
        };
    }
    let repos = state.repo_index.get(&registry_type, &state.storage).await;
    Json((*repos).clone()).into_response()
}

pub async fn api_detail(
    State(state): State<AppState>,
    Path((registry_type, name)): Path<(String, String)>,
    Query(query): Query<IndexDetailQuery>,
) -> Response {
    match registry_type.as_str() {
        "docker" => {
            let detail = get_docker_detail(&state, &name).await;
            Json(serde_json::to_value(detail).unwrap_or_default()).into_response()
        }
        "npm" if state.repo_index.has_persistent() => match get_npm_detail_page(
            &state,
            &name,
            true,
            query.continuation_token,
            query.limit.unwrap_or(100).clamp(1, 100),
        )
        .await
        {
            Ok(page) => Json(serde_json::to_value(page).unwrap_or_default()).into_response(),
            Err(error) => error.response(),
        },
        "npm" => match get_npm_detail(&state, &name, true, true).await {
            Ok(detail) => Json(serde_json::to_value(detail).unwrap_or_default()).into_response(),
            Err(error) => error.response(),
        },
        "cargo" => {
            let detail = get_cargo_detail(&state, &name, true, true).await;
            Json(serde_json::to_value(detail).unwrap_or_default()).into_response()
        }
        _ => Json(serde_json::json!({})).into_response(),
    }
}

pub async fn api_search(
    State(state): State<AppState>,
    Path(registry_type): Path<String>,
    Query(params): Query<SearchQuery>,
) -> Response {
    let query = params.q.unwrap_or_default().trim().to_string();
    let persistent = matches!(registry_type.as_str(), "maven" | "npm");
    if persistent {
        let limit = params.limit.unwrap_or(50).clamp(1, 100);
        let lang = params
            .lang
            .as_deref()
            .map(Lang::from_str)
            .unwrap_or_default();
        let page = if registry_type == "maven" && query.is_empty() {
            get_maven_dir_page(&state, "", params.continuation_token, limit)
                .await
                .map(|page| IndexPageEnvelope {
                    items: page.items,
                    continuation_token: page.continuation_token,
                    generation: 0,
                    index_state: IndexStatus::Ready,
                })
        } else {
            persistent_page(
                &state,
                &registry_type,
                Some(query.clone()),
                params.continuation_token,
                limit,
            )
            .await
        };
        return match page {
            Ok(page) => {
                let title = if registry_type == "maven" {
                    "Maven Repository"
                } else {
                    "npm Registry"
                };
                let html = render_registry_search_results(
                    &registry_type,
                    title,
                    &page.items,
                    limit,
                    page.continuation_token.as_deref(),
                    &query,
                    lang,
                );
                let mut response = axum::response::Html(html).into_response();
                response
                    .headers_mut()
                    .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
                let page_url = format!(
                    "/ui/{}?q={}&limit={}&lang={}",
                    encode_uri_component(&registry_type),
                    encode_uri_component(&query),
                    limit,
                    lang.code(),
                );
                if let Ok(value) = HeaderValue::from_str(&page_url) {
                    response.headers_mut().insert("hx-replace-url", value);
                }
                response
            }
            Err(error) => {
                let unavailable = error == PageError::Unavailable;
                let html =
                    render_registry_search_error(&registry_type, &query, limit, lang, unavailable);
                let status = match error {
                    PageError::BadCursor => StatusCode::BAD_REQUEST,
                    PageError::GenerationChanged => StatusCode::CONFLICT,
                    PageError::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
                };
                let mut response = (status, axum::response::Html(html)).into_response();
                response
                    .headers_mut()
                    .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
                if unavailable {
                    response
                        .headers_mut()
                        .insert(header::RETRY_AFTER, HeaderValue::from_static("2"));
                }
                response
            }
        };
    }

    let snapshot = state.repo_index.get(&registry_type, &state.storage).await;
    let normalized_query = query.to_lowercase();
    let persistent_items = (*snapshot).clone();
    let repos = persistent_items
        .iter()
        .filter(|r| r.name.to_lowercase().contains(&normalized_query))
        .collect::<Vec<_>>();

    // Return HTML fragment for HTMX
    let html = if repos.is_empty() {
        r#"<tr><td colspan="4" class="px-6 py-12 text-center text-slate-500">
            <div class="text-4xl mb-2">🔍</div>
            <div>No matching repositories found</div>
        </td></tr>"#
            .to_string()
    } else {
        let folder_icon = r#"<svg class="w-4 h-4 flex-shrink-0 text-slate-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 7v10a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-6l-2-2H5a2 2 0 00-2 2z"/></svg>"#;
        repos
            .iter()
            .map(|repo| {
                let detail_url =
                    format!("/ui/{}/{}", registry_type, encode_uri_component(&repo.name));
                format!(
                    r#"
                <tr class="hover:bg-slate-700">
                    <td class="px-3 md:px-6 py-3 md:py-4">
                        <div class="flex items-center gap-3">{}<a href="{}" class="text-blue-400 hover:text-blue-300 font-medium">{}</a></div>
                    </td>
                    <td class="px-3 md:px-6 py-3 md:py-4 text-slate-400">{}</td>
                    <td class="px-3 md:px-6 py-3 md:py-4 text-slate-400 hidden md:table-cell">{}</td>
                    <td class="px-3 md:px-6 py-3 md:py-4 text-slate-500 text-sm hidden md:table-cell">{}</td>
                </tr>
            "#,
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

    axum::response::Html(html).into_response()
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

fn maven_prefixes(state: &AppState, repository: &str) -> Option<Vec<String>> {
    use crate::config::MavenRepository;

    match state.config.maven.repository(repository)? {
        MavenRepository::Hosted { .. } | MavenRepository::Proxy { .. } => {
            Some(vec![format!("maven/repositories/{repository}/")])
        }
        MavenRepository::Group { members, .. } => Some(
            members
                .iter()
                .map(|member| format!("maven/repositories/{member}/"))
                .collect(),
        ),
    }
}

async fn indexed_maven_objects(
    state: &AppState,
    repository: Option<&str>,
    logical_prefix: &str,
) -> Result<(Vec<LogicalIndexedObject>, bool), PageError> {
    let base_prefixes = match repository {
        Some(repository) => match maven_prefixes(state, repository) {
            Some(prefixes) => prefixes,
            None => return Ok((Vec::new(), false)),
        },
        None => vec!["maven/".to_string()],
    };
    let logical_prefix = logical_prefix.trim_matches('/');
    let prefixes = base_prefixes
        .iter()
        .map(|base| {
            if logical_prefix.is_empty() {
                base.clone()
            } else {
                format!("{base}{logical_prefix}/")
            }
        })
        .collect::<Vec<_>>();
    let restore_logical_prefix = |path: String| {
        if logical_prefix.is_empty() {
            path
        } else if path.is_empty() {
            logical_prefix.to_string()
        } else {
            format!("{logical_prefix}/{path}")
        }
    };
    if !state.repo_index.has_persistent() {
        let snapshot = state.repo_index.maven_objects();
        let mut selected = BTreeMap::new();
        for prefix in &prefixes {
            for object in snapshot.iter() {
                if let Some(path) = object.key.strip_prefix(prefix) {
                    selected
                        .entry(restore_logical_prefix(path.to_string()))
                        .or_insert_with(|| object.meta.clone());
                }
            }
        }
        let mut objects: Vec<_> = selected
            .into_iter()
            .map(|(path, meta)| LogicalIndexedObject { path, meta })
            .collect();
        if repository.is_none() {
            objects.retain(|object| !object.path.starts_with("repositories/"));
        }
        return Ok((objects, false));
    }
    match state
        .repo_index
        .persistent_logical_objects(&prefixes, MAX_QUERY_EXAMINED)
        .await
    {
        Ok((mut objects, _, truncated)) => {
            for object in &mut objects {
                object.path = restore_logical_prefix(std::mem::take(&mut object.path));
            }
            if repository.is_none() {
                objects.retain(|object| !object.path.starts_with("repositories/"));
            }
            Ok((objects, truncated))
        }
        Err(_) => Err(PageError::Unavailable),
    }
}

async fn maven_repository_rows(state: &AppState) -> Result<Vec<RepoInfo>, PageError> {
    let mut rows = Vec::with_capacity(state.config.maven.repositories.len());
    for repository in &state.config.maven.repositories {
        let (objects, truncated) =
            indexed_maven_objects(state, Some(repository.name()), "").await?;
        if truncated {
            return Err(PageError::Unavailable);
        }
        let versions = objects
            .iter()
            .filter(|object| {
                !crate::gc::is_checksum_sidecar(&object.path)
                    && !object.path.ends_with("maven-metadata.xml")
            })
            .count();
        let size = objects.iter().map(|object| object.meta.size).sum();
        let modified = objects
            .iter()
            .map(|object| object.meta.modified)
            .max()
            .unwrap_or(0);
        rows.push(RepoInfo {
            name: repository.name().to_string(),
            versions,
            size,
            size_available: true,
            updated: format_timestamp(modified),
            ..Default::default()
        });
    }
    Ok(rows)
}

/// List immediate children of a logical Maven repository path from the last
/// background snapshot. Named groups merge member objects in configured order;
/// groups never acquire a physical storage prefix of their own.
#[cfg(test)]
pub async fn get_maven_dir_listing(
    state: &AppState,
    path: &str,
) -> Result<(Vec<RepoInfo>, bool), PageError> {
    if state.repo_index.has_persistent() {
        let page = get_maven_dir_page(state, path, None, 100).await?;
        return Ok((page.items, page.is_leaf));
    }
    get_maven_dir_listing_legacy(state, path).await
}

async fn get_maven_dir_listing_legacy(
    state: &AppState,
    path: &str,
) -> Result<(Vec<RepoInfo>, bool), PageError> {
    if !state.config.maven.repositories.is_empty() && path.is_empty() {
        return Ok((maven_repository_rows(state).await?, false));
    }

    let (objects, logical_path) = if state.config.maven.repositories.is_empty() {
        let (objects, truncated) = indexed_maven_objects(state, None, path).await?;
        if truncated {
            return Err(PageError::Unavailable);
        }
        (objects, path)
    } else {
        let (repository, logical_path) = path.split_once('/').unwrap_or((path, ""));
        let (objects, truncated) =
            indexed_maven_objects(state, Some(repository), logical_path).await?;
        if truncated {
            return Err(PageError::Unavailable);
        }
        if objects.is_empty() && state.config.maven.repository(repository).is_none() {
            return Ok((Vec::new(), false));
        }
        (objects, logical_path)
    };
    let prefix = if logical_path.is_empty() {
        String::new()
    } else {
        format!("{logical_path}/")
    };
    let keys: Vec<_> = objects
        .iter()
        .filter_map(|object| {
            object
                .path
                .strip_prefix(&prefix)
                .map(|rest| (rest, &object.meta))
        })
        .collect();
    if keys.is_empty() {
        return Ok((Vec::new(), false));
    }

    // Leaf = no subdirectories (only direct files like JARs, POMs, checksums).
    let has_subdirs = keys
        .iter()
        .any(|(rest, _)| !rest.is_empty() && rest.contains('/'));
    if !has_subdirs {
        return Ok((vec![], true));
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

    Ok((result, false))
}

pub async fn get_maven_dir_page(
    state: &AppState,
    path: &str,
    continuation_token: Option<String>,
    limit: usize,
) -> Result<MavenDirectoryEnvelope, PageError> {
    let limit = limit.clamp(1, 100);
    let logical_cursor = format!("maven-dir:{}", path.trim_matches('/'));
    let cursor = continuation_token
        .as_deref()
        .map(decode_cursor)
        .transpose()?;
    if cursor
        .as_ref()
        .is_some_and(|cursor| cursor.registry != logical_cursor || !cursor.filter.is_empty())
    {
        return Err(PageError::BadCursor);
    }
    let after = cursor
        .as_ref()
        .map(|cursor| URL_SAFE_NO_PAD.decode(&cursor.last_key))
        .transpose()
        .map_err(|_| PageError::BadCursor)?
        .map(String::from_utf8)
        .transpose()
        .map_err(|_| PageError::BadCursor)?;

    if !state.repo_index.has_persistent() {
        if cursor
            .as_ref()
            .is_some_and(|cursor| cursor.generation != 0 || cursor.config_digest != "volatile")
        {
            return Err(PageError::GenerationChanged);
        }
        let (mut items, is_leaf) = get_maven_dir_listing_legacy(state, path).await?;
        items.retain(|row| after.as_ref().is_none_or(|after| row.name > *after));
        let has_more = items.len() > limit;
        items.truncate(limit);
        let continuation_token = has_more
            .then(|| items.last().map(|row| row.name.as_bytes().to_vec()))
            .flatten()
            .and_then(|last_key| {
                encode_cursor(&IndexCursor {
                    schema: 1,
                    registry: logical_cursor,
                    filter: String::new(),
                    generation: 0,
                    config_digest: "volatile".to_string(),
                    last_key: URL_SAFE_NO_PAD.encode(last_key),
                })
            });
        return Ok(MavenDirectoryEnvelope {
            items,
            continuation_token,
            is_leaf,
        });
    }

    if !state.config.maven.repositories.is_empty() && path.is_empty() {
        let identity = state
            .repo_index
            .persistent_maven_children(Vec::new(), String::new(), None, 1)
            .await
            .map_err(|_| PageError::Unavailable)?;
        if cursor.as_ref().is_some_and(|cursor| {
            cursor.generation != identity.generation
                || cursor.config_digest != identity.config_digest
        }) {
            return Err(PageError::GenerationChanged);
        }
        let mut names = state
            .config
            .maven
            .repositories
            .iter()
            .map(|repository| repository.name().to_string())
            .collect::<Vec<_>>();
        names.sort();
        names.dedup();
        let mut rows = names
            .into_iter()
            .filter(|name| after.as_ref().is_none_or(|after| name > after))
            .take(limit + 1)
            .map(|name| RepoInfo {
                name,
                versions: 0,
                size: 0,
                size_available: false,
                updated: "N/A".to_string(),
                is_file: false,
            })
            .collect::<Vec<_>>();
        let has_more = rows.len() > limit;
        rows.truncate(limit);
        let continuation_token = has_more
            .then(|| rows.last().map(|row| row.name.as_bytes().to_vec()))
            .flatten()
            .and_then(|last_key| {
                encode_cursor(&IndexCursor {
                    schema: 1,
                    registry: logical_cursor,
                    filter: String::new(),
                    generation: identity.generation,
                    config_digest: identity.config_digest,
                    last_key: URL_SAFE_NO_PAD.encode(last_key),
                })
            });
        return Ok(MavenDirectoryEnvelope {
            items: rows,
            continuation_token,
            is_leaf: false,
        });
    }

    let (prefixes, logical_path) = if state.config.maven.repositories.is_empty() {
        (
            vec!["maven/".to_string()],
            path.trim_matches('/').to_string(),
        )
    } else {
        let (repository, logical_path) = path.split_once('/').unwrap_or((path, ""));
        let Some(prefixes) = maven_prefixes(state, repository) else {
            return Ok(MavenDirectoryEnvelope {
                items: Vec::new(),
                continuation_token: None,
                is_leaf: false,
            });
        };
        (prefixes, logical_path.trim_matches('/').to_string())
    };
    let mut page = state
        .repo_index
        .persistent_maven_children(prefixes, logical_path, after, limit)
        .await
        .map_err(|_| PageError::Unavailable)?;
    if cursor.as_ref().is_some_and(|cursor| {
        cursor.generation != page.generation || cursor.config_digest != page.config_digest
    }) {
        return Err(PageError::GenerationChanged);
    }
    if state.config.maven.repositories.is_empty() && path.is_empty() {
        page.items.retain(|row| row.name != "repositories");
    }
    let continuation_token = page.next_after.and_then(|last_key| {
        encode_cursor(&IndexCursor {
            schema: 1,
            registry: logical_cursor,
            filter: String::new(),
            generation: page.generation,
            config_digest: page.config_digest,
            last_key: URL_SAFE_NO_PAD.encode(last_key.as_bytes()),
        })
    });
    Ok(MavenDirectoryEnvelope {
        is_leaf: page.items.is_empty() && page.has_direct_files,
        items: page.items,
        continuation_token,
    })
}

#[cfg(test)]
pub async fn get_maven_detail(state: &AppState, path: &str) -> Result<MavenDetail, PageError> {
    if state.repo_index.has_persistent() {
        return get_maven_detail_page(state, path, None, 100)
            .await
            .map(|page| page.detail);
    }
    get_maven_detail_legacy(state, path).await
}

async fn get_maven_detail_legacy(state: &AppState, path: &str) -> Result<MavenDetail, PageError> {
    let (objects, logical_path, download_base) = if state.config.maven.repositories.is_empty() {
        let (objects, truncated) = indexed_maven_objects(state, None, path).await?;
        if truncated {
            return Err(PageError::Unavailable);
        }
        (
            objects,
            path,
            format!("/maven2/{}", encode_path_segments(path)),
        )
    } else {
        let (repository, logical_path) = path.split_once('/').unwrap_or((path, ""));
        let (objects, truncated) =
            indexed_maven_objects(state, Some(repository), logical_path).await?;
        if truncated {
            return Err(PageError::Unavailable);
        }
        if state.config.maven.repository(repository).is_none() {
            return Ok(MavenDetail {
                artifacts: vec![],
                download_base: String::new(),
            });
        }
        (
            objects,
            logical_path,
            format!(
                "/repository/{}/{}",
                encode_uri_component(repository),
                encode_path_segments(logical_path)
            ),
        )
    };
    let prefix = if logical_path.is_empty() {
        String::new()
    } else {
        format!("{logical_path}/")
    };

    let mut artifacts = Vec::new();
    for object in objects {
        if let Some(filename) = object.path.strip_prefix(&prefix) {
            if filename.contains('/') {
                continue;
            }
            artifacts.push(MavenArtifact {
                filename: filename.to_string(),
                size: object.meta.size,
            });
        }
    }
    artifacts.sort_by(|left, right| left.filename.cmp(&right.filename));

    Ok(MavenDetail {
        artifacts,
        download_base: download_base.trim_end_matches('/').to_string(),
    })
}

pub async fn get_maven_detail_page(
    state: &AppState,
    path: &str,
    continuation_token: Option<String>,
    limit: usize,
) -> Result<MavenDetailEnvelope, PageError> {
    let limit = limit.clamp(1, 100);
    let logical_cursor = format!("maven-files:{}", path.trim_matches('/'));
    let cursor = continuation_token
        .as_deref()
        .map(decode_cursor)
        .transpose()?;
    if cursor
        .as_ref()
        .is_some_and(|cursor| cursor.registry != logical_cursor || !cursor.filter.is_empty())
    {
        return Err(PageError::BadCursor);
    }
    let after = cursor
        .as_ref()
        .map(|cursor| URL_SAFE_NO_PAD.decode(&cursor.last_key))
        .transpose()
        .map_err(|_| PageError::BadCursor)?
        .map(String::from_utf8)
        .transpose()
        .map_err(|_| PageError::BadCursor)?;

    if !state.repo_index.has_persistent() {
        if cursor
            .as_ref()
            .is_some_and(|cursor| cursor.generation != 0 || cursor.config_digest != "volatile")
        {
            return Err(PageError::GenerationChanged);
        }
        let mut detail = get_maven_detail_legacy(state, path).await?;
        detail.artifacts.retain(|artifact| {
            after
                .as_ref()
                .is_none_or(|after| artifact.filename > *after)
        });
        let has_more = detail.artifacts.len() > limit;
        detail.artifacts.truncate(limit);
        let continuation_token = has_more
            .then(|| {
                detail
                    .artifacts
                    .last()
                    .map(|artifact| artifact.filename.as_bytes().to_vec())
            })
            .flatten()
            .and_then(|last_key| {
                encode_cursor(&IndexCursor {
                    schema: 1,
                    registry: logical_cursor,
                    filter: String::new(),
                    generation: 0,
                    config_digest: "volatile".to_string(),
                    last_key: URL_SAFE_NO_PAD.encode(last_key),
                })
            });
        return Ok(MavenDetailEnvelope {
            detail,
            continuation_token,
        });
    }
    let (prefixes, logical_path, download_base) = if state.config.maven.repositories.is_empty() {
        (
            vec!["maven/".to_string()],
            path.trim_matches('/').to_string(),
            format!("/maven2/{}", encode_path_segments(path)),
        )
    } else {
        let (repository, logical_path) = path.split_once('/').unwrap_or((path, ""));
        let Some(prefixes) = maven_prefixes(state, repository) else {
            return Ok(MavenDetailEnvelope {
                detail: MavenDetail {
                    artifacts: Vec::new(),
                    download_base: String::new(),
                },
                continuation_token: None,
            });
        };
        (
            prefixes,
            logical_path.trim_matches('/').to_string(),
            format!(
                "/repository/{}/{}",
                encode_uri_component(repository),
                encode_path_segments(logical_path)
            ),
        )
    };
    let page = state
        .repo_index
        .persistent_maven_files(prefixes, logical_path, after, limit)
        .await
        .map_err(|_| PageError::Unavailable)?;
    if cursor.as_ref().is_some_and(|cursor| {
        cursor.generation != page.generation || cursor.config_digest != page.config_digest
    }) {
        return Err(PageError::GenerationChanged);
    }
    let continuation_token = page.next_after.and_then(|last_key| {
        encode_cursor(&IndexCursor {
            schema: 1,
            registry: logical_cursor,
            filter: String::new(),
            generation: page.generation,
            config_digest: page.config_digest,
            last_key: URL_SAFE_NO_PAD.encode(last_key.as_bytes()),
        })
    });
    Ok(MavenDetailEnvelope {
        detail: MavenDetail {
            artifacts: page
                .items
                .into_iter()
                .map(|(filename, meta)| MavenArtifact {
                    filename,
                    size: meta.size,
                })
                .collect(),
            download_base: download_base.trim_end_matches('/').to_string(),
        },
        continuation_token,
    })
}

fn encode_path_segments(path: &str) -> String {
    path.split('/')
        .filter(|segment| !segment.is_empty())
        .map(encode_uri_component)
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(test)]
mod named_maven_tests {
    use super::*;
    use crate::config::{MavenRepository, MavenVersionPolicy, MavenWritePolicy};
    use std::sync::Arc;

    #[tokio::test]
    async fn persistent_search_returns_atomic_fragment_without_storage_io() {
        let index_dir = tempfile::tempdir().unwrap();
        let ctx = crate::test_helpers::create_test_context_with_config(|config| {
            config.index.path = index_dir
                .path()
                .join("index.redb")
                .to_string_lossy()
                .into_owned();
            config.maven.repositories = vec![MavenRepository::Hosted {
                name: "releases".to_string(),
                version_policy: MavenVersionPolicy::Mixed,
                write_policy: MavenWritePolicy::AllowOnce,
            }];
        });
        ctx.state
            .storage
            .put(
                "maven/repositories/releases/com/acme/app/1.0/app-1.0.jar",
                b"jar",
            )
            .await
            .unwrap();
        let index = crate::repo_index::RepoIndex::open_persistent_for_test(
            &ctx.state.config,
            ctx.state.enabled_registries.as_ref(),
            ctx.state.storage.clone(),
        )
        .await
        .unwrap();
        index.reconcile_persistent_for_test().await.unwrap();

        let backend = crate::test_helpers::FaultInjectBackend::new(ctx.state.storage.clone());
        let list_attempts = backend.list_attempts();
        let get_attempts = backend.get_attempts();
        let mut indexed_only = ctx.state.clone();
        indexed_only.storage = Storage::from_backend(Arc::new(backend));
        indexed_only.repo_index = Arc::clone(&index);

        let response = api_search(
            State(indexed_only),
            Path("maven".to_string()),
            Query(SearchQuery {
                q: Some("App".to_string()),
                continuation_token: None,
                limit: Some(50),
                lang: Some("en".to_string()),
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL).unwrap(),
            "no-store"
        );
        assert_eq!(
            response.headers().get("hx-replace-url").unwrap(),
            "/ui/maven?q=App&limit=50&lang=en"
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();
        assert!(html.contains("id=\"repo-results\""));
        assert!(html.contains("data-search-announcement=\"1 result for “App” on this page\""));
        assert!(list_attempts.lock().is_empty());
        assert!(get_attempts.lock().is_empty());
        index.shutdown_persistent().await;
    }

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

        let (objects, _) = indexed_maven_objects(&snapshot_only, Some("public"), "")
            .await
            .unwrap();
        let duplicate = objects
            .iter()
            .find(|object| object.path == logical)
            .unwrap();
        assert_eq!(duplicate.meta.size, 5, "first group member must win");
        assert_eq!(
            objects.len(),
            2,
            "duplicate logical path must be counted once"
        );

        let detail = get_maven_detail(&snapshot_only, "public/com/example/app/1.0")
            .await
            .unwrap();
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

    #[tokio::test]
    async fn persistent_group_browser_uses_only_the_requested_redb_prefix() {
        let index_dir = tempfile::tempdir().unwrap();
        let ctx = crate::test_helpers::create_test_context_with_config(|config| {
            config.index.path = index_dir
                .path()
                .join("index.redb")
                .to_string_lossy()
                .into_owned();
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

        let index = crate::repo_index::RepoIndex::open_persistent_for_test(
            &ctx.state.config,
            ctx.state.enabled_registries.as_ref(),
            ctx.state.storage.clone(),
        )
        .await
        .unwrap();
        index.reconcile_persistent_for_test().await.unwrap();

        let backend = crate::test_helpers::FaultInjectBackend::new(ctx.state.storage.clone());
        let list_attempts = backend.list_attempts();
        let mut indexed_only = ctx.state.clone();
        indexed_only.storage = Storage::from_backend(std::sync::Arc::new(backend));
        indexed_only.repo_index = Arc::clone(&index);

        let (entries, leaf) = get_maven_dir_listing(&indexed_only, "public/com/example/app/1.0")
            .await
            .unwrap();
        assert!(leaf);
        assert!(entries.is_empty());
        let detail = get_maven_detail(&indexed_only, "public/com/example/app/1.0")
            .await
            .unwrap();
        assert_eq!(
            detail
                .artifacts
                .iter()
                .map(|artifact| artifact.filename.as_str())
                .collect::<Vec<_>>(),
            vec!["app-1.0.jar", "app-1.0.pom"]
        );
        assert_eq!(
            detail.download_base,
            "/repository/public/com/example/app/1.0"
        );
        assert!(list_attempts.lock().is_empty());
        index.shutdown_persistent().await;
    }

    #[tokio::test]
    async fn persistent_maven_children_and_files_use_bounded_keyset_pages_without_storage_io() {
        let index_dir = tempfile::tempdir().unwrap();
        let ctx = crate::test_helpers::create_test_context_with_config(|config| {
            config.index.path = index_dir
                .path()
                .join("index.redb")
                .to_string_lossy()
                .into_owned();
            config.maven.repositories = vec![MavenRepository::Hosted {
                name: "releases".to_string(),
                version_policy: MavenVersionPolicy::Mixed,
                write_policy: MavenWritePolicy::AllowOnce,
            }];
        });
        for number in 0..105 {
            ctx.state
                .storage
                .put(
                    &format!(
                        "maven/repositories/releases/com/example/artifact{number:03}/1.0/artifact{number:03}-1.0.jar"
                    ),
                    b"jar",
                )
                .await
                .unwrap();
            ctx.state
                .storage
                .put(
                    &format!(
                        "maven/repositories/releases/com/example/bundle/1.0/file{number:03}.jar"
                    ),
                    b"file",
                )
                .await
                .unwrap();
        }
        let index = crate::repo_index::RepoIndex::open_persistent_for_test(
            &ctx.state.config,
            ctx.state.enabled_registries.as_ref(),
            ctx.state.storage.clone(),
        )
        .await
        .unwrap();
        index.reconcile_persistent_for_test().await.unwrap();

        let backend = crate::test_helpers::FaultInjectBackend::new(ctx.state.storage.clone());
        let list_attempts = backend.list_attempts();
        let get_attempts = backend.get_attempts();
        let mut indexed_only = ctx.state.clone();
        indexed_only.storage = Storage::from_backend(std::sync::Arc::new(backend));
        indexed_only.repo_index = Arc::clone(&index);

        let first = get_maven_dir_page(&indexed_only, "releases/com/example", None, 100)
            .await
            .unwrap();
        assert_eq!(first.items.len(), 100);
        let token = first
            .continuation_token
            .expect("children need a second page");
        let second = get_maven_dir_page(&indexed_only, "releases/com/example", Some(token), 100)
            .await
            .unwrap();
        let child_names = first
            .items
            .into_iter()
            .chain(second.items)
            .map(|row| row.name)
            .collect::<Vec<_>>();
        assert_eq!(child_names.len(), 106, "105 artifacts plus bundle");
        assert_eq!(child_names.first().unwrap(), "artifact000");
        assert_eq!(child_names.last().unwrap(), "bundle");
        assert!(second.continuation_token.is_none());

        let first =
            get_maven_detail_page(&indexed_only, "releases/com/example/bundle/1.0", None, 100)
                .await
                .unwrap();
        assert_eq!(first.detail.artifacts.len(), 100);
        let token = first.continuation_token.expect("files need a second page");
        let second = get_maven_detail_page(
            &indexed_only,
            "releases/com/example/bundle/1.0",
            Some(token),
            100,
        )
        .await
        .unwrap();
        let filenames = first
            .detail
            .artifacts
            .into_iter()
            .chain(second.detail.artifacts)
            .map(|artifact| artifact.filename)
            .collect::<Vec<_>>();
        assert_eq!(filenames.len(), 105);
        assert_eq!(filenames.first().unwrap(), "file000.jar");
        assert_eq!(filenames.last().unwrap(), "file104.jar");
        assert!(second.continuation_token.is_none());
        assert!(list_attempts.lock().is_empty());
        assert!(get_attempts.lock().is_empty());
        index.shutdown_persistent().await;
    }

    #[tokio::test]
    async fn persistent_cursor_binds_generation_filter_and_topology() {
        let index_dir = tempfile::tempdir().unwrap();
        let ctx = crate::test_helpers::create_test_context_with_config(|config| {
            config.index.path = index_dir
                .path()
                .join("index.redb")
                .to_string_lossy()
                .into_owned();
            config.maven.repositories = vec![MavenRepository::Hosted {
                name: "releases".to_string(),
                version_policy: MavenVersionPolicy::Mixed,
                write_policy: MavenWritePolicy::AllowOnce,
            }];
        });
        for artifact in ["alpha", "beta"] {
            ctx.state
                .storage
                .put(
                    &format!(
                        "maven/repositories/releases/com/example/{artifact}/1.0/{artifact}-1.0.jar"
                    ),
                    artifact.as_bytes(),
                )
                .await
                .unwrap();
        }
        let index = crate::repo_index::RepoIndex::open_persistent_for_test(
            &ctx.state.config,
            ctx.state.enabled_registries.as_ref(),
            ctx.state.storage.clone(),
        )
        .await
        .unwrap();
        index.reconcile_persistent_for_test().await.unwrap();
        let mut state = ctx.state.clone();
        state.repo_index = Arc::clone(&index);

        let first = persistent_page(&state, "maven", None, None, 1)
            .await
            .unwrap();
        assert_eq!(first.items.len(), 1);
        assert!(first.items[0].name.starts_with("releases/"));
        let token = first.continuation_token.expect("second page cursor");
        let second = persistent_page(&state, "maven", None, Some(token.clone()), 1)
            .await
            .unwrap();
        assert_eq!(second.items.len(), 1);
        assert_ne!(first.items[0].name, second.items[0].name);
        assert_eq!(
            persistent_page(
                &state,
                "maven",
                Some("different".to_string()),
                Some(token.clone()),
                1,
            )
            .await
            .unwrap_err(),
            PageError::BadCursor
        );

        let mut decoded = decode_cursor(&token).unwrap();
        decoded.config_digest = "different-topology".to_string();
        let wrong_topology = encode_cursor(&decoded).unwrap();
        assert_eq!(
            persistent_page(&state, "maven", None, Some(wrong_topology), 1)
                .await
                .unwrap_err(),
            PageError::GenerationChanged
        );

        ctx.state
            .storage
            .put(
                "maven/repositories/releases/com/example/gamma/1.0/gamma-1.0.jar",
                b"gamma",
            )
            .await
            .unwrap();
        index.reconcile_persistent_for_test().await.unwrap();
        assert_eq!(
            persistent_page(&state, "maven", None, Some(token), 1)
                .await
                .unwrap_err(),
            PageError::GenerationChanged
        );
        index.shutdown_persistent().await;
    }
}

pub async fn get_npm_detail(
    state: &AppState,
    name: &str,
    show_prerelease: bool,
    show_all: bool,
) -> Result<PackageDetail, PageError> {
    let storage = &state.storage;
    let Some((repository, package)) = name
        .strip_prefix("repositories/")
        .and_then(|rest| rest.split_once('/'))
    else {
        return Ok(PackageDetail {
            versions: vec![],
            prerelease_count: 0,
            total_stable: 0,
            metadata: PackageMetadata::default(),
        });
    };

    if state.repo_index.has_persistent() {
        return get_npm_detail_page(
            state,
            name,
            show_prerelease,
            None,
            if show_all { 100 } else { 20 },
        )
        .await
        .map(|page| page.detail);
    }

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

    Ok(PackageDetail {
        versions: stable_versions,
        prerelease_count,
        total_stable,
        metadata,
    })
}

pub async fn get_npm_detail_page(
    state: &AppState,
    name: &str,
    show_prerelease: bool,
    continuation_token: Option<String>,
    limit: usize,
) -> Result<NpmDetailEnvelope, PageError> {
    let Some((repository, package)) = name
        .strip_prefix("repositories/")
        .and_then(|rest| rest.split_once('/'))
    else {
        return Ok(NpmDetailEnvelope {
            detail: empty_package_detail(),
            continuation_token: None,
            generation: 0,
            index_state: state
                .repo_index
                .persistent_status()
                .unwrap_or(IndexStatus::Warming),
        });
    };
    let limit = limit.clamp(1, 100);
    let cursor_registry = format!("npm-detail:{repository}/{package}");
    let cursor_filter = format!("prerelease={show_prerelease}");
    let cursor = continuation_token
        .as_deref()
        .map(decode_cursor)
        .transpose()?;
    if cursor
        .as_ref()
        .is_some_and(|cursor| cursor.registry != cursor_registry || cursor.filter != cursor_filter)
    {
        return Err(PageError::BadCursor);
    }
    let after = cursor
        .as_ref()
        .map(|cursor| URL_SAFE_NO_PAD.decode(&cursor.last_key))
        .transpose()
        .map_err(|_| PageError::BadCursor)?;
    let (package_row, rows, next_after, generation, config_digest) = state
        .repo_index
        .persistent_npm_package(repository, package, after, limit)
        .await
        .map_err(|_| PageError::Unavailable)?;
    if cursor.as_ref().is_some_and(|cursor| {
        cursor.generation != generation || cursor.config_digest != config_digest
    }) {
        return Err(PageError::GenerationChanged);
    }
    let Some(package_row) = package_row else {
        return Ok(NpmDetailEnvelope {
            detail: empty_package_detail(),
            continuation_token: None,
            generation,
            index_state: state
                .repo_index
                .persistent_status()
                .unwrap_or(IndexStatus::Warming),
        });
    };
    let metadata_json = package_row
        .search
        .as_ref()
        .map(|search| serde_json::Value::Object(search.fields.clone()))
        .unwrap_or_else(|| serde_json::json!({}));
    let versions = rows
        .into_iter()
        .map(|row| VersionInfo {
            version: row.version,
            size: row
                .payload_meta
                .as_ref()
                .map_or(row.declared_size, |meta| meta.size),
            published: row.published,
            cached: row.payload_meta.is_some(),
        })
        .collect();
    let mut detail = finish_package_detail(versions, Some(metadata_json), show_prerelease, true);
    detail.total_stable = usize::try_from(package_row.stable_versions).unwrap_or(usize::MAX);
    detail.prerelease_count =
        usize::try_from(package_row.prerelease_versions).unwrap_or(usize::MAX);
    let continuation_token = next_after.and_then(|last_key| {
        encode_cursor(&IndexCursor {
            schema: 1,
            registry: cursor_registry,
            filter: cursor_filter,
            generation,
            config_digest,
            last_key: URL_SAFE_NO_PAD.encode(last_key),
        })
    });
    Ok(NpmDetailEnvelope {
        detail,
        continuation_token,
        generation,
        index_state: state
            .repo_index
            .persistent_status()
            .unwrap_or(IndexStatus::Warming),
    })
}

fn empty_package_detail() -> PackageDetail {
    PackageDetail {
        versions: Vec::new(),
        prerelease_count: 0,
        total_stable: 0,
        metadata: PackageMetadata::default(),
    }
}

fn finish_package_detail(
    mut versions: Vec<VersionInfo>,
    metadata_json: Option<serde_json::Value>,
    show_prerelease: bool,
    show_all: bool,
) -> PackageDetail {
    versions.sort_by(|left, right| {
        let parsed_left = semver::Version::parse(left.version.trim_start_matches('v'));
        let parsed_right = semver::Version::parse(right.version.trim_start_matches('v'));
        match (parsed_left, parsed_right) {
            (Ok(parsed_left), Ok(parsed_right)) => parsed_right.cmp(&parsed_left),
            _ => right.version.cmp(&left.version),
        }
    });
    let prerelease_count = versions
        .iter()
        .filter(|version| version.version.contains('-'))
        .count();
    let total_stable = versions
        .iter()
        .filter(|version| !version.version.contains('-'))
        .count();
    if !show_prerelease {
        versions.retain(|version| !version.version.contains('-'));
    }
    if !show_all && versions.len() > 20 {
        versions.truncate(20);
    }
    let mut metadata = PackageMetadata::default();
    if let Some(meta_json) = metadata_json {
        metadata.description = meta_json
            .get("description")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        metadata.license = meta_json
            .get("license")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        metadata.author = meta_json.get("author").and_then(|value| {
            value.as_str().map(str::to_string).or_else(|| {
                value
                    .get("name")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            })
        });
        metadata.homepage = meta_json
            .get("homepage")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty() && sanitize_href(value).is_some())
            .map(str::to_string);
        metadata.repository = meta_json
            .get("repository")
            .and_then(|value| {
                value.as_str().map(str::to_string).or_else(|| {
                    value
                        .get("url")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string)
                })
            })
            .map(|value| {
                value
                    .trim_start_matches("git+")
                    .trim_end_matches(".git")
                    .to_string()
            })
            .filter(|value| sanitize_href(value).is_some());
        metadata.keywords = meta_json
            .get("keywords")
            .and_then(serde_json::Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(|value| value.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
    }
    PackageDetail {
        versions,
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
        .await
        .unwrap();

        assert_eq!(detail.versions.len(), 1);
        assert_eq!(detail.versions[0].version, "1.2.3");
        assert!(detail.versions[0].cached);
        assert_eq!(detail.versions[0].size, 7);
        assert_eq!(detail.metadata.description.as_deref(), Some("example"));
    }
}
