// Copyright (c) 2026 The NORA Authors
// SPDX-License-Identifier: MIT

pub(crate) mod api;
pub mod components;
pub mod i18n;

mod static_assets;
mod templates;

use crate::repo_index::paginate;
#[cfg(test)]
use crate::repo_index::IndexStatus;
use crate::tokens::Role;
use crate::AppState;
use axum::{
    body::Body,
    extract::{OriginalUri, Path, Query, Request, State},
    http::{header, HeaderName, HeaderValue, StatusCode, Uri},
    middleware::Next,
    response::{Html, IntoResponse, Redirect, Response},
    routing::{get, post},
    Extension, Form, Router,
};

use crate::auth::{AuthenticatedRole, AuthenticatedUser};
use api::*;
use i18n::Lang;
use percent_encoding::percent_decode_str;
use templates::*;

/// Returns base URL for UI install commands.
///
/// Thin wrapper over [`ServerConfig::public_base_url`] — the single source of
/// truth for client-facing URLs.
fn resolve_base_url(state: &AppState) -> String {
    state.config.server.public_base_url()
}

#[derive(Debug, serde::Deserialize)]
struct LangQuery {
    lang: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct IndexStatusQuery {
    registry: Option<String>,
    lang: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct DetailQuery {
    lang: Option<String>,
    prerelease: Option<bool>,
    all: Option<bool>,
    continuation_token: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, serde::Deserialize)]
struct ListQuery {
    lang: Option<String>,
    q: Option<String>,
    page: Option<usize>,
    limit: Option<usize>,
    continuation_token: Option<String>,
    view: Option<String>,
}

const DEFAULT_PAGE_SIZE: usize = 50;

fn extract_lang(query: &Query<LangQuery>, cookie_header: Option<&str>) -> Lang {
    // Priority: query param > cookie > default
    if let Some(ref lang) = query.lang {
        return Lang::from_str(lang);
    }

    // Try cookie
    if let Some(cookies) = cookie_header {
        for part in cookies.split(';') {
            let part = part.trim();
            if let Some(value) = part.strip_prefix("nora_lang=") {
                return Lang::from_str(value);
            }
        }
    }

    Lang::default()
}

fn extract_lang_from_list(query: &ListQuery, cookie_header: Option<&str>) -> Lang {
    if let Some(ref lang) = query.lang {
        return Lang::from_str(lang);
    }

    if let Some(cookies) = cookie_header {
        for part in cookies.split(';') {
            let part = part.trim();
            if let Some(value) = part.strip_prefix("nora_lang=") {
                return Lang::from_str(value);
            }
        }
    }

    Lang::default()
}

fn extract_lang_from_headers(headers: &axum::http::HeaderMap) -> Lang {
    // Try cookie
    if let Some(cookies) = headers.get("cookie").and_then(|v| v.to_str().ok()) {
        for part in cookies.split(';') {
            let part = part.trim();
            if let Some(value) = part.strip_prefix("nora_lang=") {
                return Lang::from_str(value);
            }
        }
    }
    Lang::default()
}

/// Extract username from Basic Auth header (already validated by auth middleware)
fn extract_basic_auth_user(headers: &axum::http::HeaderMap) -> Option<String> {
    use base64::{engine::general_purpose::STANDARD, Engine};
    let auth_header = headers.get("authorization")?.to_str().ok()?;
    let encoded = auth_header.strip_prefix("Basic ")?;
    let decoded = String::from_utf8(STANDARD.decode(encoded).ok()?).ok()?;
    let (user, _) = decoded.split_once(':')?;
    Some(user.to_string())
}

pub fn routes() -> Router<AppState> {
    Router::new()
        // UI Pages
        .route("/", get(|| async { Redirect::to("/ui/") }))
        .route("/ui", get(|| async { Redirect::to("/ui/") }))
        .route("/ui/", get(dashboard))
        // Browsers probe `/favicon.ico` even when a page declares an SVG icon.
        // Serve both paths from the same embedded, storage-independent asset.
        .route("/favicon.ico", get(static_assets::serve_favicon))
        .route("/favicon.svg", get(static_assets::serve_favicon))
        .route("/ui/docker", get(docker_list))
        .route("/ui/docker/{name}", get(docker_detail))
        .route("/ui/maven", get(maven_list))
        .route("/ui/maven/{*path}", get(maven_detail))
        .route("/ui/npm", get(npm_list))
        .route("/ui/npm/{*name}", get(npm_detail))
        .route("/ui/cargo", get(cargo_list))
        .route("/ui/cargo/{name}", get(cargo_detail))
        .route("/ui/pypi", get(pypi_list))
        .route("/ui/pypi/{name}", get(pypi_detail))
        .route("/ui/go", get(go_list))
        .route("/ui/go/{*name}", get(go_detail))
        .route("/ui/raw", get(raw_list))
        .route("/ui/raw/{*name}", get(raw_detail))
        // New registries (v0.7 — generic list pages)
        .route("/ui/gems", get(generic_registry_list))
        .route("/ui/terraform", get(generic_registry_list))
        .route("/ui/ansible", get(ansible_browse_root))
        .route("/ui/ansible/{*path}", get(ansible_browse))
        .route("/ui/nuget", get(generic_registry_list))
        .route("/ui/nuget/{name}", get(generic_registry_detail))
        .route("/ui/pub", get(generic_registry_list))
        .route("/ui/pub/{name}", get(generic_registry_detail))
        .route("/ui/conan", get(generic_registry_list))
        .route("/ui/conan/{name}", get(generic_registry_detail))
        .route("/ui/rpm", get(generic_registry_list))
        .route("/ui/rpm/{name}", get(generic_registry_detail))
        .route("/ui/deb", get(generic_registry_list))
        .route("/ui/deb/{name}", get(generic_registry_detail))
        .route("/ui/gems/{name}", get(generic_registry_detail))
        .route("/ui/terraform/{name}", get(generic_registry_detail))
        // Token management UI (protected by auth middleware)
        .route("/ui/tokens", get(tokens_page))
        // Token management API (HTMX endpoints)
        .route("/api/ui/tokens/create", post(tokens_create))
        .route("/api/ui/tokens/list", get(tokens_list))
        .route("/api/ui/tokens/{file_id}/revoke", post(tokens_revoke))
        // Static assets (embedded)
        .route(
            "/ui/static/tailwind.css",
            get(static_assets::serve_tailwind_css),
        )
        .route("/ui/static/htmx.min.js", get(static_assets::serve_htmx_js))
        // API endpoints for HTMX
        .route("/api/ui/stats", get(api_stats))
        .route("/api/ui/dashboard", get(api_dashboard))
        .route("/api/ui/index-status", get(index_status))
        .route("/api/ui/{registry_type}/list", get(api_list))
        .route("/api/ui/{registry_type}/{*name}", get(api_detail))
        .route("/api/ui/{registry_type}/search", get(api_search))
}

fn index_loading_response(
    state: &AppState,
    registry_type: &str,
    registry_title: &str,
    lang: Lang,
    auth_enabled: bool,
) -> Response {
    let mut response = Html(render_index_loading(
        registry_type,
        registry_title,
        lang,
        auth_enabled,
        state.repo_index.persistent_index_progress(),
    ))
    .into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn index_page_error_response(
    error: PageError,
    state: &AppState,
    registry_type: &str,
    registry_title: &str,
    lang: Lang,
    auth_enabled: bool,
    request_uri: &Uri,
) -> Response {
    if error == PageError::Unavailable && !state.repo_index.persistent_projection_available() {
        index_loading_response(state, registry_type, registry_title, lang, auth_enabled)
    } else if matches!(error, PageError::BadCursor | PageError::GenerationChanged) {
        let mut response = Redirect::to(&cursor_reset_location(request_uri)).into_response();
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        response
    } else if error == PageError::Unavailable {
        let mut response = (
            StatusCode::SERVICE_UNAVAILABLE,
            Html(render_index_unavailable(
                registry_type,
                registry_title,
                &cursor_reset_location(request_uri),
                lang,
                auth_enabled,
            )),
        )
            .into_response();
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from_static("2"));
        response
    } else {
        error.response()
    }
}

/// Drop only the opaque cursor from a browser URL. The remaining raw query
/// pairs are already percent-encoded, so preserving them verbatim avoids
/// changing the user's search text, language, page size or detail view.
fn cursor_reset_location(uri: &Uri) -> String {
    let retained = uri
        .query()
        .into_iter()
        .flat_map(|query| query.split('&'))
        .filter(|pair| !query_key_matches(pair, b"continuation_token"))
        .collect::<Vec<_>>()
        .join("&");
    if retained.is_empty() {
        uri.path().to_string()
    } else {
        format!("{}?{retained}", uri.path())
    }
}

fn query_key_matches(pair: &str, expected: &[u8]) -> bool {
    let key = pair.split_once('=').map_or(pair, |(key, _)| key);
    percent_decode_str(key)
        .decode_utf8()
        .is_ok_and(|decoded| decoded.as_bytes() == expected)
}

async fn index_status(
    State(state): State<AppState>,
    Query(query): Query<IndexStatusQuery>,
) -> Response {
    let (registry_type, registry_title) = match query.registry.as_deref().unwrap_or("maven") {
        "maven" => ("maven", "Maven"),
        "npm" => ("npm", "npm"),
        _ => return (StatusCode::BAD_REQUEST, "unsupported registry").into_response(),
    };
    let lang = query
        .lang
        .as_deref()
        .map_or_else(Lang::default, Lang::from_str);
    let available = state.repo_index.persistent_projection_available();
    let mut response = if available {
        StatusCode::OK.into_response()
    } else {
        Html(render_index_loading_fragment(
            registry_type,
            registry_title,
            lang,
            state.repo_index.persistent_index_progress(),
        ))
        .into_response()
    };
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    if available {
        response.headers_mut().insert(
            HeaderName::from_static("hx-refresh"),
            HeaderValue::from_static("true"),
        );
    }
    response
}

/// Prefix NORA's root-absolute UI self-links with `base` so the UI works when
/// NORA is mounted under a sub-path. Anchored on the quote that opens an HTML
/// attribute or JS string, so only emitted links (`href`/`src`/`hx-*`/`fetch(`)
/// are rewritten — never a link-like substring in page text. No-op when empty.
fn apply_base_path(html: &str, base: &str) -> String {
    if base.is_empty() {
        return html.to_string();
    }
    html.replace("\"/ui", &format!("\"{base}/ui"))
        .replace("'/ui", &format!("'{base}/ui"))
        .replace("\"/api/ui", &format!("\"{base}/api/ui"))
        .replace("'/api/ui", &format!("'{base}/api/ui"))
        // The API-docs (Swagger UI) entry link in the nav and the root-absolute
        // spec URL the Swagger initializer embeds ("/api-docs/openapi.json").
        .replace("\"/api-docs", &format!("\"{base}/api-docs"))
        .replace("'/api-docs", &format!("'{base}/api-docs"))
        .replace("\"/favicon", &format!("\"{base}/favicon"))
        .replace("'/favicon", &format!("'{base}/favicon"))
}

/// Prefix one response-navigation header when it points at a NORA UI route.
/// Besides ordinary redirects, HTMX uses `HX-Replace-Url` to update browser
/// history after an in-place search. Both must honor `public_url`'s path.
fn prefix_ui_response_header(headers: &mut axum::http::HeaderMap, name: HeaderName, base: &str) {
    let Some(value) = headers
        .get(&name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
    else {
        return;
    };
    let is_ui_path = value == "/ui"
        || value.starts_with("/ui/")
        || value == "/api/ui"
        || value.starts_with("/api/ui/")
        || value == "/api-docs"
        || value.starts_with("/api-docs/");
    if is_ui_path && !value.starts_with(base) {
        if let Ok(value) = HeaderValue::from_str(&format!("{base}{value}")) {
            headers.insert(name, value);
        }
    }
}

/// Response middleware: rewrite the UI's root-absolute self-links to carry the
/// configured `public_url` path prefix. Covers HTML bodies (links + inline JS
/// `fetch`), the Swagger UI initializer's spec URL, and browser-navigation
/// headers (`Location` and HTMX's `HX-Replace-Url`). A no-op when `base_path`
/// is empty (the router keeps serving `/ui`, `/api/ui` and `/api-docs`; the
/// proxy strips the prefix).
pub(crate) async fn rewrite_ui_base_path(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    let base = state.config.server.base_path();
    // Capture the path before the request is consumed: the Swagger initializer
    // is served as JS (not HTML) yet embeds a root-absolute spec URL that needs
    // the same prefix, so it is rewritten by path rather than by content type.
    let path = req.uri().path().to_string();
    let mut resp = next.run(req).await;
    if base.is_empty() {
        return resp;
    }
    // Both an ordinary redirect and HTMX's history replacement must carry the
    // prefix. Otherwise an in-place search under `/nora` silently changes the
    // address bar to `/ui/...`, and the next reload escapes the deployment.
    for name in [header::LOCATION, HeaderName::from_static("hx-replace-url")] {
        prefix_ui_response_header(resp.headers_mut(), name, &base);
    }
    let is_html = resp
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|c| c.contains("text/html"));
    // The Swagger UI initializer is JS but embeds a root-absolute spec URL
    // ("/api-docs/openapi.json"); rewrite it too so the docs load under a
    // sub-path. Scoped to that one small file — large JS bundles pass through.
    let is_swagger_init = path.ends_with("/swagger-initializer.js");
    if !is_html && !is_swagger_init {
        return resp;
    }
    let (mut parts, body) = resp.into_parts();
    let bytes = match axum::body::to_bytes(body, usize::MAX).await {
        Ok(b) => b,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    let rewritten = apply_base_path(&String::from_utf8_lossy(&bytes), &base);
    // The body length changed; drop the stale Content-Length so it is recomputed.
    parts.headers.remove(header::CONTENT_LENGTH);
    Response::from_parts(parts, Body::from(rewritten))
}

// Dashboard page
async fn dashboard(
    State(state): State<AppState>,
    Query(query): Query<LangQuery>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    let lang = extract_lang(
        &Query(query),
        headers.get("cookie").and_then(|v| v.to_str().ok()),
    );
    let auth_enabled = state.auth.is_some();
    let response = api_dashboard(State(state)).await.0;
    Html(render_dashboard(&response, lang, auth_enabled))
}

// Docker pages
async fn docker_list(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    let lang = extract_lang_from_list(&query, headers.get("cookie").and_then(|v| v.to_str().ok()));
    let page = query.page.unwrap_or(1).max(1);
    let limit = query.limit.unwrap_or(DEFAULT_PAGE_SIZE).clamp(1, 100);
    let auth_enabled = state.auth.is_some();

    let all_repos = state.repo_index.get("docker", &state.storage).await;
    let (repos, total) = paginate(&all_repos, page, limit);

    Html(render_registry_list_paginated(
        "docker",
        "Docker Registry",
        &repos,
        page,
        limit,
        total,
        lang,
        auth_enabled,
    ))
}

async fn docker_detail(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(query): Query<LangQuery>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    let lang = extract_lang(
        &Query(query),
        headers.get("cookie").and_then(|v| v.to_str().ok()),
    );
    let base_url = resolve_base_url(&state);
    let auth_enabled = state.auth.is_some();
    let detail = get_docker_detail(&state, &name).await;
    Html(render_docker_detail(
        &name,
        &detail,
        lang,
        &base_url,
        auth_enabled,
    ))
}

// Maven pages
async fn maven_list(
    State(state): State<AppState>,
    OriginalUri(request_uri): OriginalUri,
    Query(query): Query<ListQuery>,
    headers: axum::http::HeaderMap,
) -> Response {
    let lang = extract_lang_from_list(&query, headers.get("cookie").and_then(|v| v.to_str().ok()));
    let auth_enabled = state.auth.is_some();

    if !state.repo_index.persistent_projection_available() {
        return index_loading_response(&state, "maven", "Maven", lang, auth_enabled);
    }

    let limit = query.limit.unwrap_or(DEFAULT_PAGE_SIZE).clamp(1, 100);
    let search_query = query.q.as_deref().map(str::trim).filter(|q| !q.is_empty());
    if let Some(search_query) = search_query {
        let page = match api::persistent_page(
            &state,
            "maven",
            Some(search_query.to_string()),
            query.continuation_token.clone(),
            limit,
        )
        .await
        {
            Ok(page) => page,
            Err(error) => {
                return index_page_error_response(
                    error,
                    &state,
                    "maven",
                    "Maven",
                    lang,
                    auth_enabled,
                    &request_uri,
                )
            }
        };
        return Html(render_registry_list_cursor(
            "maven",
            "Maven Repository",
            &page.items,
            limit,
            page.continuation_token.as_deref(),
            search_query,
            lang,
            auth_enabled,
        ))
        .into_response();
    }
    let page = match api::get_maven_dir_page(&state, "", query.continuation_token, limit).await {
        Ok(page) => page,
        Err(error) => {
            return index_page_error_response(
                error,
                &state,
                "maven",
                "Maven",
                lang,
                auth_enabled,
                &request_uri,
            )
        }
    };
    let total = page.items.len();

    Html(templates::render_maven_dir_cursor(
        "",
        &page.items,
        total,
        page.continuation_token.as_deref(),
        limit,
        lang,
        auth_enabled,
    ))
    .into_response()
}

async fn maven_detail(
    State(state): State<AppState>,
    Path(path): Path<String>,
    OriginalUri(request_uri): OriginalUri,
    Query(query): Query<ListQuery>,
    headers: axum::http::HeaderMap,
) -> Response {
    let lang = extract_lang_from_list(&query, headers.get("cookie").and_then(|v| v.to_str().ok()));
    let auth_enabled = state.auth.is_some();
    let limit = query.limit.unwrap_or(DEFAULT_PAGE_SIZE).clamp(1, 100);

    if !state.repo_index.persistent_projection_available() {
        return index_loading_response(&state, "maven", "Maven", lang, auth_enabled);
    }

    if query.view.as_deref() == Some("files") {
        let page = match get_maven_detail_page(&state, &path, query.continuation_token, limit).await
        {
            Ok(page) => page,
            Err(error) => {
                return index_page_error_response(
                    error,
                    &state,
                    "maven",
                    "Maven",
                    lang,
                    auth_enabled,
                    &request_uri,
                )
            }
        };
        return Html(render_maven_detail_cursor(
            &path,
            &page.detail,
            page.continuation_token.as_deref(),
            limit,
            lang,
            auth_enabled,
        ))
        .into_response();
    }

    // Try hierarchical browsing: check if this is a directory or leaf artifact
    let page = match api::get_maven_dir_page(&state, &path, query.continuation_token, limit).await {
        Ok(page) => page,
        Err(error) => {
            return index_page_error_response(
                error,
                &state,
                "maven",
                "Maven",
                lang,
                auth_enabled,
                &request_uri,
            )
        }
    };

    if page.is_leaf || page.items.is_empty() {
        // Leaf artifact — show files (JARs, POMs, etc.)
        let detail_page = match get_maven_detail_page(&state, &path, None, limit).await {
            Ok(detail) => detail,
            Err(error) => {
                return index_page_error_response(
                    error,
                    &state,
                    "maven",
                    "Maven",
                    lang,
                    auth_enabled,
                    &request_uri,
                )
            }
        };
        Html(render_maven_detail_cursor(
            &path,
            &detail_page.detail,
            detail_page.continuation_token.as_deref(),
            limit,
            lang,
            auth_enabled,
        ))
        .into_response()
    } else {
        // Namespace directory — show children
        let total = page.items.len();
        Html(templates::render_maven_dir_cursor(
            &path,
            &page.items,
            total,
            page.continuation_token.as_deref(),
            limit,
            lang,
            auth_enabled,
        ))
        .into_response()
    }
}

// npm pages
async fn npm_list(
    State(state): State<AppState>,
    OriginalUri(request_uri): OriginalUri,
    Query(query): Query<ListQuery>,
    headers: axum::http::HeaderMap,
) -> Response {
    let lang = extract_lang_from_list(&query, headers.get("cookie").and_then(|v| v.to_str().ok()));
    let limit = query.limit.unwrap_or(DEFAULT_PAGE_SIZE).clamp(1, 100);
    let auth_enabled = state.auth.is_some();

    if !state.repo_index.persistent_projection_available() {
        return index_loading_response(&state, "npm", "npm", lang, auth_enabled);
    }

    let search_query = query.q.as_deref().map(str::trim).filter(|q| !q.is_empty());
    let page = api::persistent_page(
        &state,
        "npm",
        search_query.map(str::to_string),
        query.continuation_token.clone(),
        limit,
    )
    .await;
    let (packages, next) = match page {
        Ok(page) => (page.items, page.continuation_token),
        Err(error) => {
            return index_page_error_response(
                error,
                &state,
                "npm",
                "npm",
                lang,
                auth_enabled,
                &request_uri,
            )
        }
    };

    Html(render_registry_list_cursor(
        "npm",
        "npm Registry",
        &packages,
        limit,
        next.as_deref(),
        search_query.unwrap_or_default(),
        lang,
        auth_enabled,
    ))
    .into_response()
}

async fn npm_detail(
    State(state): State<AppState>,
    Path(name): Path<String>,
    OriginalUri(request_uri): OriginalUri,
    Query(query): Query<DetailQuery>,
    headers: axum::http::HeaderMap,
) -> Response {
    let lang = {
        let lang_q = LangQuery {
            lang: query.lang.clone(),
        };
        extract_lang(
            &Query(lang_q),
            headers.get("cookie").and_then(|v| v.to_str().ok()),
        )
    };
    let base_url = resolve_base_url(&state);
    let auth_enabled = state.auth.is_some();
    let show_prerelease = query.prerelease.unwrap_or(false);
    let show_all = query.all.unwrap_or(false);
    if state.repo_index.has_persistent() {
        if !state.repo_index.persistent_projection_available() {
            return index_loading_response(&state, "npm", "npm", lang, auth_enabled);
        }
        let limit = query.limit.unwrap_or(50).clamp(1, 100);
        let page = match get_npm_detail_page(
            &state,
            &name,
            show_prerelease,
            query.continuation_token,
            limit,
        )
        .await
        {
            Ok(page) => page,
            Err(error) => {
                return index_page_error_response(
                    error,
                    &state,
                    "npm",
                    "npm",
                    lang,
                    auth_enabled,
                    &request_uri,
                )
            }
        };
        return Html(render_package_detail_cursor(
            "npm",
            &name,
            &page.detail,
            page.continuation_token.as_deref(),
            limit,
            show_prerelease,
            lang,
            &base_url,
            auth_enabled,
        ))
        .into_response();
    }
    let detail = match get_npm_detail(&state, &name, show_prerelease, show_all).await {
        Ok(detail) => detail,
        Err(error) => return error.response(),
    };
    Html(render_package_detail(
        "npm",
        &name,
        &detail,
        lang,
        &base_url,
        auth_enabled,
    ))
    .into_response()
}

// Cargo pages
async fn cargo_list(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    let lang = extract_lang_from_list(&query, headers.get("cookie").and_then(|v| v.to_str().ok()));
    let page = query.page.unwrap_or(1).max(1);
    let limit = query.limit.unwrap_or(DEFAULT_PAGE_SIZE).min(100);
    let auth_enabled = state.auth.is_some();

    let all_crates = state.repo_index.get("cargo", &state.storage).await;
    let (crates, total) = paginate(&all_crates, page, limit);

    Html(render_registry_list_paginated(
        "cargo",
        "Cargo Registry",
        &crates,
        page,
        limit,
        total,
        lang,
        auth_enabled,
    ))
}

async fn cargo_detail(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(query): Query<DetailQuery>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    let lang = {
        let lang_q = LangQuery {
            lang: query.lang.clone(),
        };
        extract_lang(
            &Query(lang_q),
            headers.get("cookie").and_then(|v| v.to_str().ok()),
        )
    };
    let base_url = resolve_base_url(&state);
    let auth_enabled = state.auth.is_some();
    let show_prerelease = query.prerelease.unwrap_or(false);
    let show_all = query.all.unwrap_or(false);
    let detail = get_cargo_detail(&state, &name, show_prerelease, show_all).await;
    Html(render_package_detail(
        "cargo",
        &name,
        &detail,
        lang,
        &base_url,
        auth_enabled,
    ))
}

// PyPI pages
async fn pypi_list(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    let lang = extract_lang_from_list(&query, headers.get("cookie").and_then(|v| v.to_str().ok()));
    let page = query.page.unwrap_or(1).max(1);
    let limit = query.limit.unwrap_or(DEFAULT_PAGE_SIZE).min(100);
    let auth_enabled = state.auth.is_some();

    let all_packages = state.repo_index.get("pypi", &state.storage).await;
    let (packages, total) = paginate(&all_packages, page, limit);

    Html(render_registry_list_paginated(
        "pypi",
        "PyPI Repository",
        &packages,
        page,
        limit,
        total,
        lang,
        auth_enabled,
    ))
}

async fn pypi_detail(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(query): Query<DetailQuery>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    let lang = {
        let lang_q = LangQuery {
            lang: query.lang.clone(),
        };
        extract_lang(
            &Query(lang_q),
            headers.get("cookie").and_then(|v| v.to_str().ok()),
        )
    };
    let base_url = resolve_base_url(&state);
    let auth_enabled = state.auth.is_some();
    let show_prerelease = query.prerelease.unwrap_or(false);
    let show_all = query.all.unwrap_or(false);
    let detail = get_pypi_detail(&state, &name, show_prerelease, show_all).await;
    Html(render_package_detail(
        "pypi",
        &name,
        &detail,
        lang,
        &base_url,
        auth_enabled,
    ))
}

// Go pages
async fn go_list(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    let lang = extract_lang_from_list(&query, headers.get("cookie").and_then(|v| v.to_str().ok()));
    let auth_enabled = state.auth.is_some();

    // Show top-level namespace directories (github.com, golang.org, etc.)
    let (entries, _) = api::get_go_dir_listing(&state, "").await;
    let total = entries.len();

    Html(templates::render_go_dir(
        "",
        &entries,
        total,
        lang,
        auth_enabled,
    ))
}

async fn go_detail(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(query): Query<DetailQuery>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    let lang = {
        let lang_q = LangQuery {
            lang: query.lang.clone(),
        };
        extract_lang(
            &Query(lang_q),
            headers.get("cookie").and_then(|v| v.to_str().ok()),
        )
    };
    let auth_enabled = state.auth.is_some();

    // Try hierarchical browsing: check if this is a directory or leaf module
    let (entries, is_leaf) = api::get_go_dir_listing(&state, &name).await;

    if is_leaf || entries.is_empty() {
        // Leaf module — show version detail page
        let base_url = resolve_base_url(&state);
        let show_prerelease = query.prerelease.unwrap_or(false);
        let show_all = query.all.unwrap_or(false);
        let detail = get_go_detail(&state, &name, show_prerelease, show_all).await;
        Html(render_package_detail(
            "go",
            &name,
            &detail,
            lang,
            &base_url,
            auth_enabled,
        ))
    } else {
        // Namespace directory — show children
        let total = entries.len();
        Html(templates::render_go_dir(
            &name,
            &entries,
            total,
            lang,
            auth_enabled,
        ))
    }
}

// Raw pages
async fn raw_list(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    let lang = extract_lang_from_list(&query, headers.get("cookie").and_then(|v| v.to_str().ok()));
    let page = query.page.unwrap_or(1).max(1);
    let limit = query.limit.unwrap_or(DEFAULT_PAGE_SIZE).min(100);
    let auth_enabled = state.auth.is_some();

    let all_files = state.repo_index.get("raw", &state.storage).await;
    let (files, total) = paginate(&all_files, page, limit);

    Html(render_registry_list_paginated(
        "raw",
        "Raw Storage",
        &files,
        page,
        limit,
        total,
        lang,
        auth_enabled,
    ))
}

async fn raw_detail(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(query): Query<LangQuery>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    let lang = extract_lang(
        &Query(query),
        headers.get("cookie").and_then(|v| v.to_str().ok()),
    );
    let base_url = resolve_base_url(&state);
    let auth_enabled = state.auth.is_some();

    // Check if this path is a directory (has children) or a single file
    let (entries, is_dir) = api::get_raw_dir_listing(&state, &name).await;

    if is_dir && !entries.is_empty() {
        // Directory with children — render as browsable folder listing
        let total = entries.len();
        Html(templates::render_raw_dir(
            &name,
            &entries,
            total,
            lang,
            auth_enabled,
        ))
    } else {
        // Single file or leaf directory — render detail page
        let detail = api::get_raw_detail(&state, &name).await;
        Html(templates::render_package_detail(
            "raw",
            &name,
            &detail,
            lang,
            &base_url,
            auth_enabled,
        ))
    }
}

// Generic registry list handler for new formats (v0.7)
async fn generic_registry_list(
    State(state): State<AppState>,
    Query(query): Query<ListQuery>,
    headers: axum::http::HeaderMap,
    uri: axum::http::Uri,
) -> impl IntoResponse {
    let lang = extract_lang_from_list(&query, headers.get("cookie").and_then(|v| v.to_str().ok()));
    let page = query.page.unwrap_or(1).max(1);
    let limit = query.limit.unwrap_or(DEFAULT_PAGE_SIZE).min(100);
    let auth_enabled = state.auth.is_some();

    // Extract registry type from URI path: /ui/{type}
    let registry_key = uri.path().strip_prefix("/ui/").unwrap_or("raw");
    let title = match registry_key {
        "gems" => "RubyGems",
        "terraform" => "Terraform Registry",
        "ansible" => "Ansible Galaxy",
        "nuget" => "NuGet Gallery",
        "pub" => "Pub (Dart/Flutter)",
        "conan" => "Conan (C/C++)",
        "rpm" => "RPM (yum/dnf)",
        "deb" => "Debian (APT)",
        _ => registry_key,
    };

    let all_items = state.repo_index.get(registry_key, &state.storage).await;
    let (items, total) = paginate(&all_items, page, limit);

    Html(render_registry_list_paginated(
        registry_key,
        title,
        &items,
        page,
        limit,
        total,
        lang,
        auth_enabled,
    ))
}

async fn generic_registry_detail(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(query): Query<DetailQuery>,
    headers: axum::http::HeaderMap,
    uri: axum::http::Uri,
) -> impl IntoResponse {
    let lang = {
        let lang_q = LangQuery {
            lang: query.lang.clone(),
        };
        extract_lang(
            &Query(lang_q),
            headers.get("cookie").and_then(|v| v.to_str().ok()),
        )
    };
    let base_url = resolve_base_url(&state);
    let auth_enabled = state.auth.is_some();
    let show_prerelease = query.prerelease.unwrap_or(false);
    let show_all = query.all.unwrap_or(false);

    // Extract registry type from URI: /ui/{type}/{name}
    let registry_key = uri
        .path()
        .strip_prefix("/ui/")
        .and_then(|s| s.split('/').next())
        .unwrap_or("raw");

    let detail = get_generic_detail(&state, registry_key, &name, show_prerelease, show_all).await;
    Html(render_package_detail(
        registry_key,
        &name,
        &detail,
        lang,
        &base_url,
        auth_enabled,
    ))
}

// Ansible Galaxy hierarchical browsing (namespace → collection → versions)
async fn ansible_browse_root(
    State(state): State<AppState>,
    Query(query): Query<LangQuery>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    let lang = extract_lang(
        &Query(query),
        headers.get("cookie").and_then(|v| v.to_str().ok()),
    );
    let auth_enabled = state.auth.is_some();

    let entries = api::get_ansible_namespace_listing(&state, "").await;
    let total = entries.len();

    Html(templates::render_ansible_dir(
        "",
        &entries,
        total,
        lang,
        auth_enabled,
    ))
}

async fn ansible_browse(
    State(state): State<AppState>,
    Path(path): Path<String>,
    Query(query): Query<DetailQuery>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    let lang = {
        let lang_q = LangQuery {
            lang: query.lang.clone(),
        };
        extract_lang(
            &Query(lang_q),
            headers.get("cookie").and_then(|v| v.to_str().ok()),
        )
    };
    let auth_enabled = state.auth.is_some();

    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    match segments.len() {
        // /ui/ansible/community → list collections in namespace
        1 => {
            let entries = api::get_ansible_namespace_listing(&state, &path).await;
            let total = entries.len();
            Html(templates::render_ansible_dir(
                &path,
                &entries,
                total,
                lang,
                auth_enabled,
            ))
        }
        // /ui/ansible/community/general → version detail
        2 => {
            let base_url = resolve_base_url(&state);
            let show_prerelease = query.prerelease.unwrap_or(false);
            let show_all = query.all.unwrap_or(false);
            let full_name = format!("{}.{}", segments[0], segments[1]);
            let detail =
                get_generic_detail(&state, "ansible", &full_name, show_prerelease, show_all).await;
            Html(render_package_detail(
                "ansible",
                &full_name,
                &detail,
                lang,
                &base_url,
                auth_enabled,
            ))
        }
        // Deeper paths: 404
        _ => Html(templates::render_ansible_dir(
            &path,
            &[],
            0,
            lang,
            auth_enabled,
        )),
    }
}

// ==================== Token Management Handlers ====================

/// Token management page (GET /ui/tokens)
async fn tokens_page(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Extension(role): Extension<AuthenticatedRole>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    let lang = extract_lang_from_headers(&headers);

    // Owner-scope: non-admins see only their own tokens; admins see all
    // (GHSA-78cx-cfhm-rgmx — cross-user token enumeration).
    let tokens = match &state.tokens {
        Some(store) if role.0.can_admin() => store.list_all_tokens(),
        Some(store) => store.list_tokens(&user.0),
        None => vec![],
    };

    Html(render_tokens_page(&tokens, lang, true))
}

/// Create token (POST /api/ui/tokens/create)
#[derive(serde::Deserialize)]
struct CreateTokenForm {
    description: String,
    role: String,
    ttl_days: Option<u64>,
}

async fn tokens_create(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Form(form): Form<CreateTokenForm>,
) -> impl IntoResponse {
    // CSRF check: require HX-Request header (HTMX sets this automatically)
    if headers.get("hx-request").is_none() {
        return (StatusCode::FORBIDDEN, Html("Forbidden".to_string()));
    }

    let lang = extract_lang_from_headers(&headers);

    let store = match &state.tokens {
        Some(store) => store,
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Html("Token store not configured".to_string()),
            );
        }
    };

    // Get authenticated user from Basic Auth header — token creation requires
    // knowing who created it, so we reject requests without Basic auth identity.
    let user = match extract_basic_auth_user(&headers) {
        Some(u) => u,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Html(
                    r##"<div class="bg-red-900/30 border border-red-700 rounded-lg p-4 text-red-400">Token creation requires Basic authentication to identify the owner</div>"##.to_string(),
                ),
            );
        }
    };

    let role = match form.role.as_str() {
        "read" => Role::Read,
        "write" => Role::Write,
        "admin" => Role::Admin,
        _ => Role::Read,
    };

    let ttl_days = form.ttl_days.unwrap_or(90).clamp(1, 3650);

    let description = if form.description.trim().is_empty() {
        None
    } else {
        Some(form.description.trim().to_string())
    };

    match store.create_token(&user, ttl_days, description, role) {
        Ok(raw_token) => {
            let html = render_token_created_fragment(&raw_token, lang);
            (StatusCode::OK, Html(html))
        }
        Err(e) => {
            let html = format!(
                r##"<div class="bg-red-900/30 border border-red-700 rounded-lg p-4 text-red-400">Error: {}</div>"##,
                components::html_escape(&e.to_string())
            );
            (StatusCode::INTERNAL_SERVER_ERROR, Html(html))
        }
    }
}

/// List tokens HTMX fragment (GET /api/ui/tokens/list)
async fn tokens_list(
    State(state): State<AppState>,
    Extension(user): Extension<AuthenticatedUser>,
    Extension(role): Extension<AuthenticatedRole>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    let lang = extract_lang_from_headers(&headers);

    // Owner-scope: non-admins see only their own tokens; admins see all
    // (GHSA-78cx-cfhm-rgmx — cross-user token enumeration).
    let tokens = match &state.tokens {
        Some(store) if role.0.can_admin() => store.list_all_tokens(),
        Some(store) => store.list_tokens(&user.0),
        None => vec![],
    };

    Html(render_token_list_fragment(&tokens, lang))
}

/// Revoke token (POST /api/ui/tokens/{file_id}/revoke)
async fn tokens_revoke(
    State(state): State<AppState>,
    Path(file_id): Path<String>,
    Extension(user): Extension<AuthenticatedUser>,
    Extension(role): Extension<AuthenticatedRole>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    // CSRF check
    if headers.get("hx-request").is_none() {
        return (StatusCode::FORBIDDEN, Html("Forbidden".to_string()));
    }

    // Validate file_id: must be exactly 16 hex chars (path traversal prevention)
    if file_id.len() != 16 || !file_id.chars().all(|c| c.is_ascii_hexdigit()) {
        return (
            StatusCode::BAD_REQUEST,
            Html("Invalid token ID".to_string()),
        );
    }

    let lang = extract_lang_from_headers(&headers);

    let store = match &state.tokens {
        Some(store) => store,
        None => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Html("Token store not configured".to_string()),
            );
        }
    };

    // Owner-scope: non-admins may revoke only their own tokens. Return 404 (not
    // 403) so a non-owner cannot probe which token IDs exist
    // (GHSA-78cx-cfhm-rgmx — cross-user token revocation).
    let is_admin = role.0.can_admin();
    if !is_admin
        && !store
            .list_tokens(&user.0)
            .iter()
            .any(|t| t.file_id == file_id)
    {
        return (StatusCode::NOT_FOUND, Html("Token not found".to_string()));
    }

    match store.revoke_token(&file_id) {
        Ok(()) => {
            // Return refreshed token list (same owner-scope as the list view)
            let tokens = if is_admin {
                store.list_all_tokens()
            } else {
                store.list_tokens(&user.0)
            };
            (
                StatusCode::OK,
                Html(render_token_list_fragment(&tokens, lang)),
            )
        }
        Err(crate::tokens::TokenError::NotFound) => {
            (StatusCode::NOT_FOUND, Html("Token not found".to_string()))
        }
        Err(e) => {
            let html = format!(
                r##"<div class="bg-red-900/30 border border-red-700 rounded-lg p-4 text-red-400">Error: {}</div>"##,
                components::html_escape(&e.to_string())
            );
            (StatusCode::INTERNAL_SERVER_ERROR, Html(html))
        }
    }
}

#[cfg(test)]
mod base_path_tests {
    use super::*;

    #[test]
    fn cursor_reset_preserves_browser_state_and_removes_encoded_cursor_keys() {
        let npm: Uri = "/ui/npm?q=%40Scope%2FPkg&limit=1&continuation_token=old&lang=ru"
            .parse()
            .unwrap();
        assert_eq!(
            cursor_reset_location(&npm),
            "/ui/npm?q=%40Scope%2FPkg&limit=1&lang=ru"
        );

        let maven: Uri =
            "/ui/maven/repo/com%20acme?view=files&limit=25&continuation%5Ftoken=old&lang=zh"
                .parse()
                .unwrap();
        assert_eq!(
            cursor_reset_location(&maven),
            "/ui/maven/repo/com%20acme?view=files&limit=25&lang=zh"
        );
    }

    #[test]
    fn stale_browser_cursor_redirects_without_rendering_json() {
        let ctx = crate::test_helpers::create_test_context();
        let uri: Uri = "/ui/npm?q=pkg&limit=10&continuation_token=stale&lang=en"
            .parse()
            .unwrap();
        let response = index_page_error_response(
            PageError::GenerationChanged,
            &ctx.state,
            "npm",
            "npm",
            Lang::En,
            false,
            &uri,
        );
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            response.headers().get(header::LOCATION).unwrap(),
            "/ui/npm?q=pkg&limit=10&lang=en"
        );
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL).unwrap(),
            "no-store"
        );
    }

    #[test]
    fn apply_base_path_is_noop_when_empty() {
        let html = r#"<a href="/ui/docker">d</a><script src="/ui/static/x.js"></script>"#;
        assert_eq!(apply_base_path(html, ""), html);
    }

    #[test]
    fn apply_base_path_prefixes_ui_and_api_links() {
        let html = concat!(
            r#"<link rel="icon" href="/favicon.svg">"#,
            r#"<link href="/ui/static/tailwind.css">"#,
            r#"<a href="/ui/docker">d</a>"#,
            r#"<script>fetch('/api/ui/dashboard')</script>"#,
            r#"<form action="/api/ui/tokens/create">"#,
            r#"<div hx-get="/api/ui/index-status"></div>"#,
        );
        let out = apply_base_path(html, "/nora");
        assert!(out.contains(r#"href="/nora/favicon.svg""#));
        assert!(out.contains(r#"href="/nora/ui/static/tailwind.css""#));
        assert!(out.contains(r#"href="/nora/ui/docker""#));
        assert!(out.contains("fetch('/nora/api/ui/dashboard')"));
        assert!(out.contains(r#"action="/nora/api/ui/tokens/create""#));
        assert!(out.contains(r#"hx-get="/nora/api/ui/index-status""#));
        // No leftover bare links, no double-prefix.
        assert!(!out.contains(r#"href="/ui/"#));
        assert!(!out.contains("fetch('/api/ui"));
        assert!(!out.contains("/nora/nora/"));
    }

    #[test]
    fn base_path_prefixes_redirect_and_htmx_history_headers() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            header::LOCATION,
            HeaderValue::from_static("/api-docs/?lang=en"),
        );
        headers.insert(
            HeaderName::from_static("hx-replace-url"),
            HeaderValue::from_static("/ui/npm?q=%40Scope%2FPkg&limit=50&lang=en"),
        );

        prefix_ui_response_header(&mut headers, header::LOCATION, "/nora");
        prefix_ui_response_header(
            &mut headers,
            HeaderName::from_static("hx-replace-url"),
            "/nora",
        );

        assert_eq!(
            headers.get(header::LOCATION).unwrap(),
            "/nora/api-docs/?lang=en"
        );
        assert_eq!(
            headers.get("hx-replace-url").unwrap(),
            "/nora/ui/npm?q=%40Scope%2FPkg&limit=50&lang=en"
        );

        // A second middleware pass is idempotent, and an unrelated absolute
        // path is never captured just because it starts with similar text.
        prefix_ui_response_header(
            &mut headers,
            HeaderName::from_static("hx-replace-url"),
            "/nora",
        );
        headers.insert(
            HeaderName::from_static("hx-location"),
            HeaderValue::from_static("/ui-not-nora"),
        );
        prefix_ui_response_header(
            &mut headers,
            HeaderName::from_static("hx-location"),
            "/nora",
        );
        assert_eq!(
            headers.get("hx-replace-url").unwrap(),
            "/nora/ui/npm?q=%40Scope%2FPkg&limit=50&lang=en"
        );
        assert_eq!(headers.get("hx-location").unwrap(), "/ui-not-nora");
    }

    #[tokio::test]
    async fn embedded_favicon_is_available_on_declared_and_legacy_paths() {
        let ctx = crate::test_helpers::create_test_context();
        for path in ["/favicon.svg", "/favicon.ico"] {
            let response =
                crate::test_helpers::send(&ctx.app, axum::http::Method::GET, path, "").await;
            assert_eq!(response.status(), StatusCode::OK, "{path}");
            assert_eq!(
                response.headers().get(header::CONTENT_TYPE),
                Some(&HeaderValue::from_static("image/svg+xml; charset=utf-8")),
                "{path}"
            );
        }
    }

    #[test]
    fn apply_base_path_prefixes_api_docs_link_and_spec_url() {
        // The "API Docs" nav link (HTML) and the Swagger initializer's embedded
        // spec URL (JS) are both root-absolute /api-docs paths that break under a
        // sub-path unless prefixed. Regression guard for the #686 residual.
        let html = r#"<a href="/api-docs" title="API Docs">d</a>"#;
        let init = r#"window.ui = SwaggerUIBundle({ "url": "/api-docs/openapi.json" });"#;
        assert!(apply_base_path(html, "/nora").contains(r#"href="/nora/api-docs""#));
        let out = apply_base_path(init, "/nora");
        assert!(out.contains(r#""url": "/nora/api-docs/openapi.json""#));
        assert!(!out.contains(r#""/api-docs/openapi.json""#));
        // Empty base is still a no-op for the api-docs paths.
        assert_eq!(apply_base_path(html, ""), html);
    }

    #[test]
    fn apply_base_path_leaves_non_link_text_untouched() {
        // A link-like substring not anchored on an attribute/JS quote (e.g. in
        // body text) is not a self-link and must not be rewritten.
        let html = r#"<p>the path /ui/docker is shown</p>"#;
        assert_eq!(apply_base_path(html, "/nora"), html);
    }
}

#[cfg(test)]
mod index_loading_route_tests {
    use super::*;
    use crate::test_helpers::{body_bytes, create_test_context, send};
    use axum::http::Method;

    #[tokio::test]
    async fn persistent_ui_loads_friendly_shell_then_refreshes_when_projection_is_ready() {
        let context = create_test_context();
        let index = crate::repo_index::RepoIndex::open_persistent_for_test(
            &context.state.config,
            context.state.enabled_registries.as_ref(),
            context.state.storage.clone(),
        )
        .await
        .unwrap();
        assert_eq!(index.persistent_status(), Some(IndexStatus::Warming));
        assert!(!index.persistent_projection_available());

        let mut state = context.state.clone();
        state.repo_index = index.clone();
        let app = super::routes().with_state(state);

        for path in [
            "/ui/maven",
            "/ui/maven/com/example",
            "/ui/npm",
            "/ui/npm/example",
        ] {
            let response = send(&app, Method::GET, path, "").await;
            assert_eq!(response.status(), StatusCode::OK, "{path}");
            assert_eq!(
                response.headers().get(header::CACHE_CONTROL),
                Some(&HeaderValue::from_static("no-store")),
                "{path}"
            );
            let html = String::from_utf8(body_bytes(response).await.to_vec()).unwrap();
            assert!(html.contains("id=\"index-loading-status\""), "{path}");
            assert!(!html.contains("\"error\":\"index_unavailable\""), "{path}");
        }

        for path in ["/api/ui/maven/list", "/api/ui/npm/list"] {
            let response = send(&app, Method::GET, path, "").await;
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE, "{path}");
            assert_eq!(
                response.headers().get(header::RETRY_AFTER),
                Some(&HeaderValue::from_static("2")),
                "{path}"
            );
        }

        let warming = send(
            &app,
            Method::GET,
            "/api/ui/index-status?registry=maven&lang=ru",
            "",
        )
        .await;
        assert_eq!(warming.status(), StatusCode::OK);
        assert_eq!(
            warming.headers().get(header::CACHE_CONTROL),
            Some(&HeaderValue::from_static("no-store"))
        );
        assert!(!warming.headers().contains_key(header::RETRY_AFTER));
        assert!(!warming.headers().contains_key("hx-refresh"));
        let warming_body = String::from_utf8(body_bytes(warming).await.to_vec()).unwrap();
        assert!(warming_body.contains("Подготовка локального индекса"));
        assert!(warming_body.contains("Объектов Maven проиндексировано"));
        assert!(warming_body.contains("hx-swap=\"outerHTML\""));
        assert!(!warming_body.contains("aria-valuenow"));

        let invalid = send(&app, Method::GET, "/api/ui/index-status?registry=raw", "").await;
        assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);

        index.reconcile_persistent_for_test().await.unwrap();
        assert_eq!(index.persistent_status(), Some(IndexStatus::Ready));
        assert!(index.persistent_projection_available());
        assert_eq!(
            index.persistent_index_progress().phase,
            crate::repo_index::PersistentIndexPhase::Idle
        );

        let ready = send(
            &app,
            Method::GET,
            "/api/ui/index-status?registry=maven&lang=ru",
            "",
        )
        .await;
        assert_eq!(ready.status(), StatusCode::OK);
        assert_eq!(
            ready.headers().get("hx-refresh"),
            Some(&HeaderValue::from_static("true"))
        );
        assert!(!ready.headers().contains_key(header::RETRY_AFTER));

        for path in ["/ui/maven", "/ui/npm"] {
            let response = send(&app, Method::GET, path, "").await;
            assert_eq!(response.status(), StatusCode::OK, "{path}");
            let html = String::from_utf8(body_bytes(response).await.to_vec()).unwrap();
            assert!(!html.contains("id=\"index-loading-status\""), "{path}");
        }

        index.shutdown_persistent().await;
    }
}

#[cfg(test)]
mod named_npm_route_tests {
    use crate::test_helpers::{body_bytes, create_test_context_with_config, send};
    use axum::http::{Method, StatusCode};

    #[tokio::test]
    async fn encoded_repository_qualified_npm_link_opens_detail_page() {
        use base64::Engine as _;
        use sha2::Digest as _;

        let context = create_test_context_with_config(|config| {
            config.npm.repositories = vec![crate::config::NpmRepository::Hosted {
                name: "npm-private".to_string(),
                write_policy: crate::config::NpmWritePolicy::AllowOnce,
            }];
            config.npm.default_repository = Some("npm-private".to_string());
        });
        let blob = b"tarball";
        let manifest = serde_json::to_vec(&serde_json::json!({
            "name": "@scope/pkg",
            "version": "1.0.0",
            "dist": {
                "integrity": format!(
                    "sha512-{}",
                    base64::engine::general_purpose::STANDARD
                        .encode(sha2::Sha512::digest(blob))
                )
            }
        }))
        .unwrap();
        context
            .state
            .storage
            .put(
                "npm/repositories/npm-private/@scope/pkg/versions/1.0.0.json",
                &manifest,
            )
            .await
            .unwrap();
        context
            .state
            .storage
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
        let manifest_value: serde_json::Value = serde_json::from_slice(&manifest).unwrap();
        let packument = serde_json::json!({
            "name": "@scope/pkg",
            "versions": {"1.0.0": manifest_value},
            "dist-tags": {"latest": "1.0.0"}
        });
        let full = serde_json::to_vec(&packument).unwrap();
        let pointer = crate::registry::write_hosted_packument_generation_documents(
            &context.state.storage,
            "npm-private",
            "@scope/pkg",
            &packument,
            &full,
        )
        .await
        .unwrap();
        crate::registry::commit_hosted_packument_pointer(
            &context.state.storage,
            "npm-private",
            "@scope/pkg",
            &pointer,
        )
        .await
        .unwrap();
        let index = crate::repo_index::RepoIndex::open_persistent_for_test(
            &context.state.config,
            context.state.enabled_registries.as_ref(),
            context.state.storage.clone(),
        )
        .await
        .unwrap();
        index.reconcile_persistent_for_test().await.unwrap();
        let mut state = context.state.clone();
        state.repo_index = index.clone();
        let app = super::routes().with_state(state);

        let list = send(&app, Method::GET, "/ui/npm", "").await;
        assert_eq!(list.status(), StatusCode::OK);
        let list_html = String::from_utf8(body_bytes(list).await.to_vec()).unwrap();
        let detail_path = "/ui/npm/repositories%2Fnpm-private%2F%40scope%2Fpkg";
        assert!(list_html.contains(detail_path));

        let clamped = send(&app, Method::GET, "/ui/npm?limit=0", "").await;
        assert_eq!(clamped.status(), StatusCode::OK);
        let clamped_html = String::from_utf8(body_bytes(clamped).await.to_vec()).unwrap();
        assert!(clamped_html.contains("name=\"limit\" value=\"1\""));
        assert!(clamped_html.contains("/api/ui/npm/search?limit=1&amp;lang=en"));
        assert!(!clamped_html.contains("limit=0"));

        let detail = send(&app, Method::GET, detail_path, "").await;
        assert_eq!(detail.status(), StatusCode::OK);
        let detail_html = String::from_utf8(body_bytes(detail).await.to_vec()).unwrap();
        assert!(detail_html.contains(&format!(
            "npm install @scope/pkg --registry {}/repository/npm-private",
            context.state.config.server.public_base_url()
        )));
        index.shutdown_persistent().await;
    }
}
