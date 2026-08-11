// Copyright (c) 2026 The NORA Authors
// SPDX-License-Identifier: MIT

use axum::http::header;
use axum::response::IntoResponse;
use sha2::{Digest, Sha256};
use std::sync::LazyLock;

/// Embedded Tailwind CSS (purged, minified)
const TAILWIND_CSS: &str = include_str!("static/tailwind.css");

/// Embedded htmx 1.9.10 (minified)
const HTMX_JS: &str = include_str!("static/htmx.min.js");

/// Small embedded NORA mark. Keeping it in the binary makes the browser's
/// root `/favicon.ico` probe independent of the storage backend and PVC.
const FAVICON_SVG: &str = r##"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32"><rect width="32" height="32" rx="7" fill="#0f172a"/><path d="M7 24V8h4l10 10V8h4v16h-4L11 14v10z" fill="#38bdf8"/></svg>"##;

static TAILWIND_VERSION: LazyLock<String> = LazyLock::new(|| content_version(TAILWIND_CSS));
static HTMX_VERSION: LazyLock<String> = LazyLock::new(|| content_version(HTMX_JS));
static FAVICON_VERSION: LazyLock<String> = LazyLock::new(|| content_version(FAVICON_SVG));

fn content_version(content: &str) -> String {
    hex::encode(Sha256::digest(content.as_bytes()))
}

pub(crate) fn tailwind_asset_version() -> &'static str {
    TAILWIND_VERSION.as_str()
}

pub(crate) fn htmx_asset_version() -> &'static str {
    HTMX_VERSION.as_str()
}

pub(crate) fn favicon_asset_version() -> &'static str {
    FAVICON_VERSION.as_str()
}

pub async fn serve_tailwind_css() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "text/css; charset=utf-8"),
            (header::CACHE_CONTROL, "public, max-age=86400"),
        ],
        TAILWIND_CSS,
    )
}

pub async fn serve_htmx_js() -> impl IntoResponse {
    (
        [
            (
                header::CONTENT_TYPE,
                "application/javascript; charset=utf-8",
            ),
            (header::CACHE_CONTROL, "public, max-age=86400"),
        ],
        HTMX_JS,
    )
}

pub async fn serve_favicon() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "image/svg+xml; charset=utf-8"),
            (header::CACHE_CONTROL, "public, max-age=86400"),
        ],
        FAVICON_SVG,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_asset_versions_are_exact_content_sha256_values() {
        for (version, content) in [
            (tailwind_asset_version(), TAILWIND_CSS),
            (htmx_asset_version(), HTMX_JS),
            (favicon_asset_version(), FAVICON_SVG),
        ] {
            assert_eq!(version.len(), 64);
            assert_eq!(version, content_version(content));
            assert!(version.bytes().all(|byte| byte.is_ascii_hexdigit()));
        }
        assert_ne!(tailwind_asset_version(), htmx_asset_version());
        assert_ne!(tailwind_asset_version(), favicon_asset_version());
    }
}
